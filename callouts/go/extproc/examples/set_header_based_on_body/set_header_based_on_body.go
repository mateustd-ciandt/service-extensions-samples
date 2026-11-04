// Copyright 2026 Google LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package set_header_based_on_body

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extproc "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	extproctype "github.com/envoyproxy/go-control-plane/envoy/type/v3"
)

const (
	maxBufferSize = 2 * 1024 * 1024 // 2MB max buffer size

	// maxStreamedChunkSize is the maximum size of a single body chunk sent back
	// to Envoy. The gRPC max message size is 128 KiB; using 62,000 bytes gives
	// a safety margin for protobuf overhead, matching common Envoy ext_proc
	// implementations.
	maxStreamedChunkSize = 62000
)

// errStopStream signals that processing should end (e.g. after an ImmediateResponse).
var errStopStream = errors.New("stop processing stream")

// ExampleCalloutService implements a stateful External Processing service for Envoy.
// It buffers request and response body chunks and evaluates their contents to compute
// header mutations, maintaining stream-local context without global locks.
type ExampleCalloutService struct {
	extproc.UnimplementedExternalProcessorServer
}

// NewExampleCalloutService creates a new instance of the callout service.
func NewExampleCalloutService() *ExampleCalloutService {
	return &ExampleCalloutService{}
}

// Process handles the bidirectional stream of processing requests from Envoy.
func (s *ExampleCalloutService) Process(stream extproc.ExternalProcessor_ProcessServer) error {
	req := &sideState{isRequest: true}
	resp := &sideState{isRequest: false}

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			if err := req.finalize(stream); err != nil {
				return err
			}
			return resp.finalize(stream)
		}
		if err != nil {
			return err
		}

		switch m := msg.Request.(type) {
		case *extproc.ProcessingRequest_RequestHeaders:
			if err := req.onHeaders(stream, m.RequestHeaders.EndOfStream); err != nil {
				return err
			}
		case *extproc.ProcessingRequest_RequestBody:
			if err := req.onBody(stream, m.RequestBody.Body, m.RequestBody.EndOfStream); err != nil {
				if errors.Is(err, errStopStream) {
					return nil
				}
				return err
			}
		case *extproc.ProcessingRequest_RequestTrailers:
			if err := req.onTrailers(stream); err != nil {
				return err
			}
		case *extproc.ProcessingRequest_ResponseHeaders:
			if err := resp.onHeaders(stream, m.ResponseHeaders.EndOfStream); err != nil {
				return err
			}
		case *extproc.ProcessingRequest_ResponseBody:
			if err := resp.onBody(stream, m.ResponseBody.Body, m.ResponseBody.EndOfStream); err != nil {
				if errors.Is(err, errStopStream) {
					return nil
				}
				return err
			}
		case *extproc.ProcessingRequest_ResponseTrailers:
			if err := resp.onTrailers(stream); err != nil {
				return err
			}
		}
	}
}

// sideState tracks the processing state for one direction (request or response).
type sideState struct {
	isRequest      bool
	body           bytes.Buffer
	truncated      bool
	headersDelayed bool
	owedAcks       int

	// mutatedBody holds the body content that must be sent back to Envoy
	// (chunked) instead of an empty ack. It stays nil when the service does
	// not modify the body.
	mutatedBody []byte
}

// onHeaders holds the header response until body/trailers arrive, unless Envoy
// signals end-of-stream (no body will follow).
func (s *sideState) onHeaders(stream extproc.ExternalProcessor_ProcessServer, eos bool) error {
	if eos {
		return s.sendHeadersResponse(stream)
	}
	s.headersDelayed = true
	return nil
}

// onBody buffers the chunk, queues its ack, and finalizes when EOS arrives.
// In FULL_DUPLEX_STREAMED mode chunks never carry EOS; trailers drive finalize.
func (s *sideState) onBody(stream extproc.ExternalProcessor_ProcessServer, chunk []byte, eos bool) error {
	if s.body.Len()+len(chunk) > maxBufferSize {
		if s.isRequest && !s.truncated {
			if err := sendImmediateResponse(stream, extproctype.StatusCode_PayloadTooLarge, "request_body_buffer_exceeded"); err != nil {
				return err
			}
			return errStopStream
		}
		s.truncated = true
	}
	if !s.truncated {
		s.body.Write(chunk)
	}
	s.owedAcks++

	if eos {
		return s.finalize(stream)
	}
	return nil
}

// onTrailers finalizes pending state, then acknowledges the trailers.
func (s *sideState) onTrailers(stream extproc.ExternalProcessor_ProcessServer) error {
	if err := s.finalize(stream); err != nil {
		return err
	}

	var resp *extproc.ProcessingResponse
	if s.isRequest {
		resp = &extproc.ProcessingResponse{
			Response: &extproc.ProcessingResponse_RequestTrailers{
				RequestTrailers: &extproc.TrailersResponse{},
			},
		}
	} else {
		resp = &extproc.ProcessingResponse{
			Response: &extproc.ProcessingResponse_ResponseTrailers{
				ResponseTrailers: &extproc.TrailersResponse{},
			},
		}
	}
	return stream.Send(resp)
}

// finalize flushes a delayed header response followed by all pending body acks,
// preserving the Headers -> Body -> Trailers order expected by Envoy.
func (s *sideState) finalize(stream extproc.ExternalProcessor_ProcessServer) error {
	if s.headersDelayed {
		if err := s.sendHeadersResponse(stream); err != nil {
			return err
		}
		s.headersDelayed = false
	}

	// If the body was mutated, send the mutated content in chunks instead of
	// empty acknowledgements. Each chunk counts as the response for one
	// outstanding body request; any remaining acks are sent as empty responses.
	bodyToSend := s.mutatedBody
	for s.owedAcks > 0 {
		if len(bodyToSend) > 0 {
			chunk, eos := nextChunk(bodyToSend)
			if err := s.sendStreamedBody(stream, chunk, eos); err != nil {
				return err
			}
			bodyToSend = bodyToSend[len(chunk):]
		} else {
			if err := s.sendEmptyBodyAck(stream); err != nil {
				return err
			}
		}
		s.owedAcks--
	}
	return nil
}

// nextChunk returns the next slice of body to send, capped at maxStreamedChunkSize,
// and reports whether this is the final chunk.
func nextChunk(body []byte) ([]byte, bool) {
	n := len(body)
	if n > maxStreamedChunkSize {
		n = maxStreamedChunkSize
	}
	return body[:n], n == len(body)
}

// sendStreamedBody emits a streamed body chunk (mutated body path).
func (s *sideState) sendStreamedBody(stream extproc.ExternalProcessor_ProcessServer, chunk []byte, eos bool) error {
	common := &extproc.CommonResponse{
		BodyMutation: &extproc.BodyMutation{
			Mutation: &extproc.BodyMutation_StreamedResponse{
				StreamedResponse: &extproc.StreamedBodyResponse{
					Body:        chunk,
					EndOfStream: eos,
				},
			},
		},
	}

	var resp *extproc.ProcessingResponse
	if s.isRequest {
		resp = &extproc.ProcessingResponse{
			Response: &extproc.ProcessingResponse_RequestBody{
				RequestBody: &extproc.BodyResponse{Response: common},
			},
		}
	} else {
		resp = &extproc.ProcessingResponse{
			Response: &extproc.ProcessingResponse_ResponseBody{
				ResponseBody: &extproc.BodyResponse{Response: common},
			},
		}
	}
	return stream.Send(resp)
}

// sendEmptyBodyAck emits an empty body response (no mutation).
func (s *sideState) sendEmptyBodyAck(stream extproc.ExternalProcessor_ProcessServer) error {
	var resp *extproc.ProcessingResponse
	if s.isRequest {
		resp = &extproc.ProcessingResponse{
			Response: &extproc.ProcessingResponse_RequestBody{
				RequestBody: &extproc.BodyResponse{},
			},
		}
	} else {
		resp = &extproc.ProcessingResponse{
			Response: &extproc.ProcessingResponse_ResponseBody{
				ResponseBody: &extproc.BodyResponse{},
			},
		}
	}
	return stream.Send(resp)
}

// sendHeadersResponse computes header mutations based on buffered body and dispatches the response.
func (s *sideState) sendHeadersResponse(stream extproc.ExternalProcessor_ProcessServer) error {
	var mutation *extproc.HeaderMutation
	if s.body.Len() > 0 || s.truncated {
		headers := analyzeBodyAndCreateHeaders(s.body.String())
		if s.truncated {
			headers["x-body-truncated"] = "true"
		}
		mutation = &extproc.HeaderMutation{SetHeaders: convertToEnvoyHeaders(headers)}
	}

	var resp *extproc.ProcessingResponse
	if s.isRequest {
		resp = &extproc.ProcessingResponse{
			Response: &extproc.ProcessingResponse_RequestHeaders{
				RequestHeaders: &extproc.HeadersResponse{
					Response: &extproc.CommonResponse{HeaderMutation: mutation},
				},
			},
		}
	} else {
		resp = &extproc.ProcessingResponse{
			Response: &extproc.ProcessingResponse_ResponseHeaders{
				ResponseHeaders: &extproc.HeadersResponse{
					Response: &extproc.CommonResponse{HeaderMutation: mutation},
				},
			},
		}
	}
	return stream.Send(resp)
}

// sendImmediateResponse cleanly rejects the request when safety limits are breached.
func sendImmediateResponse(stream extproc.ExternalProcessor_ProcessServer, code extproctype.StatusCode, details string) error {
	resp := &extproc.ProcessingResponse{
		Response: &extproc.ProcessingResponse_ImmediateResponse{
			ImmediateResponse: &extproc.ImmediateResponse{
				Status:  &extproctype.HttpStatus{Code: code},
				Details: details,
			},
		},
	}
	return stream.Send(resp)
}

// analyzeBodyAndCreateHeaders determines new header states via body string inspection.
func analyzeBodyAndCreateHeaders(bodyContent string) map[string]string {
	headers := make(map[string]string)
	headers["x-body-size"] = fmt.Sprintf("%d", len(bodyContent))

	if strings.Contains(strings.ToLower(bodyContent), "error") {
		headers["x-body-has-error"] = "true"
	}
	if strings.Contains(strings.ToLower(bodyContent), "warning") {
		headers["x-body-has-warning"] = "true"
	}

	trimmedBody := strings.TrimSpace(bodyContent)
	if (strings.HasPrefix(trimmedBody, "{") && strings.HasSuffix(trimmedBody, "}")) ||
		(strings.HasPrefix(trimmedBody, "[") && strings.HasSuffix(trimmedBody, "]")) {
		headers["x-body-format"] = "json"
	} else if strings.HasPrefix(trimmedBody, "<") && strings.HasSuffix(trimmedBody, ">") {
		headers["x-body-format"] = "xml"
	} else {
		headers["x-body-format"] = "text"
	}

	return headers
}

// convertToEnvoyHeaders transforms standard map pairs into the structured protobuf type
// using the explicit OVERWRITE_IF_EXISTS_OR_ADD action to prevent duplicates.
func convertToEnvoyHeaders(headers map[string]string) []*core.HeaderValueOption {
	var result []*core.HeaderValueOption
	for key, value := range headers {
		result = append(result, &core.HeaderValueOption{
			Header: &core.HeaderValue{
				Key:   key,
				Value: value,
			},
			AppendAction: core.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD,
		})
	}
	return result
}
