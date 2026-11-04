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
	"io"
	"testing"

	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extproc "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
)

// mockProcessStream simulates Envoy's gRPC stream for testing.
type mockProcessStream struct {
	extproc.ExternalProcessor_ProcessServer
	reqs  []*extproc.ProcessingRequest
	resps []*extproc.ProcessingResponse
}

func (m *mockProcessStream) Recv() (*extproc.ProcessingRequest, error) {
	if len(m.reqs) == 0 {
		return nil, io.EOF
	}
	req := m.reqs[0]
	m.reqs = m.reqs[1:]
	return req, nil
}

func (m *mockProcessStream) Send(resp *extproc.ProcessingResponse) error {
	m.resps = append(m.resps, resp)
	return nil
}

func TestHeadersOnlyWithEndOfStream(t *testing.T) {
	service := NewExampleCalloutService()
	mockStream := &mockProcessStream{
		reqs: []*extproc.ProcessingRequest{
			{
				Request: &extproc.ProcessingRequest_RequestHeaders{
					RequestHeaders: &extproc.HttpHeaders{EndOfStream: true},
				},
			},
		},
	}

	if err := service.Process(mockStream); err != nil {
		t.Fatalf("Process() returned unexpected error: %v", err)
	}
	if len(mockStream.resps) != 1 {
		t.Fatalf("Expected 1 response, got %d", len(mockStream.resps))
	}

	respHeaders, ok := mockStream.resps[0].Response.(*extproc.ProcessingResponse_RequestHeaders)
	if !ok {
		t.Fatalf("Expected RequestHeaders response")
	}
	if hm := respHeaders.RequestHeaders.Response.HeaderMutation; hm != nil {
		if n := len(hm.SetHeaders); n > 0 {
			t.Errorf("Expected no header mutation for empty body, got %d headers", n)
		}
	}
}

func TestHeadersMutatedWhenBodyPresent(t *testing.T) {
	service := NewExampleCalloutService()
	mockStream := &mockProcessStream{
		reqs: []*extproc.ProcessingRequest{
			{
				Request: &extproc.ProcessingRequest_RequestHeaders{
					RequestHeaders: &extproc.HttpHeaders{EndOfStream: false},
				},
			},
			{
				Request: &extproc.ProcessingRequest_RequestBody{
					RequestBody: &extproc.HttpBody{
						Body:        []byte(`{"k":"v"}`),
						EndOfStream: true,
					},
				},
			},
		},
	}

	if err := service.Process(mockStream); err != nil {
		t.Fatalf("Process() returned unexpected error: %v", err)
	}
	if len(mockStream.resps) < 1 {
		t.Fatalf("Expected at least 1 response, got %d", len(mockStream.resps))
	}

	respHeaders, ok := mockStream.resps[0].Response.(*extproc.ProcessingResponse_RequestHeaders)
	if !ok {
		t.Fatalf("Expected first response to be RequestHeaders")
	}
	hm := respHeaders.RequestHeaders.Response.HeaderMutation
	if hm == nil || len(hm.SetHeaders) == 0 {
		t.Fatalf("Expected header mutation, got none")
	}
	for _, h := range hm.SetHeaders {
		if h.AppendAction != core.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD {
			t.Errorf("Expected OVERWRITE_IF_EXISTS_OR_ADD for %s, got %v", h.Header.Key, h.AppendAction)
		}
	}
}

func TestFullDuplexProcessing(t *testing.T) {
	service := NewExampleCalloutService()
	mockStream := &mockProcessStream{
		reqs: []*extproc.ProcessingRequest{
			{
				Request: &extproc.ProcessingRequest_RequestHeaders{
					RequestHeaders: &extproc.HttpHeaders{EndOfStream: false},
				},
			},
			{
				Request: &extproc.ProcessingRequest_RequestBody{
					RequestBody: &extproc.HttpBody{Body: []byte(`{"status": `), EndOfStream: false},
				},
			},
			{
				Request: &extproc.ProcessingRequest_RequestBody{
					RequestBody: &extproc.HttpBody{Body: []byte(`"error"}`), EndOfStream: true},
				},
			},
		},
	}

	if err := service.Process(mockStream); err != nil {
		t.Fatalf("Process() returned unexpected error: %v", err)
	}
	if len(mockStream.resps) != 3 {
		t.Fatalf("Expected 3 responses (1 Headers, 2 Body chunks), got %d", len(mockStream.resps))
	}

	respHeaders, ok := mockStream.resps[0].Response.(*extproc.ProcessingResponse_RequestHeaders)
	if !ok {
		t.Fatalf("Expected first response to be RequestHeaders")
	}
	headerMap := make(map[string]string)
	for _, header := range respHeaders.RequestHeaders.Response.HeaderMutation.SetHeaders {
		headerMap[header.Header.Key] = header.Header.Value
	}
	expectedHeaders := map[string]string{
		"x-body-size":      "19",
		"x-body-has-error": "true",
		"x-body-format":    "json",
	}
	for k, v := range expectedHeaders {
		if headerMap[k] != v {
			t.Errorf("Expected header %s=%s, got %s", k, v, headerMap[k])
		}
	}
}

// TestFullDuplexStreamedWithTrailers simulates FULL_DUPLEX_STREAMED: body chunks
// never carry EOS; the end of the body is signaled by trailers.
func TestFullDuplexStreamedWithTrailers(t *testing.T) {
	service := NewExampleCalloutService()
	mockStream := &mockProcessStream{
		reqs: []*extproc.ProcessingRequest{
			{
				Request: &extproc.ProcessingRequest_RequestHeaders{
					RequestHeaders: &extproc.HttpHeaders{EndOfStream: false},
				},
			},
			{
				Request: &extproc.ProcessingRequest_RequestBody{
					RequestBody: &extproc.HttpBody{
						Body:        []byte(`<warning>xml</warning>`),
						EndOfStream: false,
					},
				},
			},
			{
				Request: &extproc.ProcessingRequest_RequestTrailers{
					RequestTrailers: &extproc.HttpTrailers{},
				},
			},
		},
	}

	if err := service.Process(mockStream); err != nil {
		t.Fatalf("Process() returned unexpected error: %v", err)
	}
	if len(mockStream.resps) != 3 {
		t.Fatalf("Expected 3 responses (Headers, Body, Trailers), got %d", len(mockStream.resps))
	}
	if _, ok := mockStream.resps[0].Response.(*extproc.ProcessingResponse_RequestHeaders); !ok {
		t.Errorf("First response should be RequestHeaders")
	}
	if _, ok := mockStream.resps[1].Response.(*extproc.ProcessingResponse_RequestBody); !ok {
		t.Errorf("Second response should be RequestBody")
	}
	if _, ok := mockStream.resps[2].Response.(*extproc.ProcessingResponse_RequestTrailers); !ok {
		t.Errorf("Third response should be RequestTrailers")
	}
}

// TestResponseTruncationFlag ensures that exceeding maxBufferSize on the response
// side does NOT replace the upstream response with an ImmediateResponse; instead
// the truncation is flagged via header.
func TestResponseTruncationFlag(t *testing.T) {
	service := NewExampleCalloutService()
	largeChunk := make([]byte, maxBufferSize+1)
	for i := range largeChunk {
		largeChunk[i] = 'a'
	}

	mockStream := &mockProcessStream{
		reqs: []*extproc.ProcessingRequest{
			{
				Request: &extproc.ProcessingRequest_ResponseHeaders{
					ResponseHeaders: &extproc.HttpHeaders{EndOfStream: false},
				},
			},
			{
				Request: &extproc.ProcessingRequest_ResponseBody{
					ResponseBody: &extproc.HttpBody{Body: largeChunk, EndOfStream: true},
				},
			},
		},
	}

	if err := service.Process(mockStream); err != nil {
		t.Fatalf("Process() returned unexpected error: %v", err)
	}
	if len(mockStream.resps) < 1 {
		t.Fatalf("Expected at least 1 response, got %d", len(mockStream.resps))
	}

	respHeaders, ok := mockStream.resps[0].Response.(*extproc.ProcessingResponse_ResponseHeaders)
	if !ok {
		t.Fatalf("Expected first response to be ResponseHeaders")
	}
	hm := respHeaders.ResponseHeaders.Response.HeaderMutation
	if hm == nil {
		t.Fatalf("Expected header mutation, got none")
	}
	found := false
	for _, h := range hm.SetHeaders {
		if h.Header.Key == "x-body-truncated" && h.Header.Value == "true" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected x-body-truncated: true header, not found")
	}
}
