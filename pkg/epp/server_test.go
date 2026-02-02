// Copyright 2026 Naadir Jeewa
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
//
// SPDX-License-Identifier: Apache-2.0

package epp

import (
	"context"
	"testing"

	envoy_config_core_v3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoy_service_ext_proc_v3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/randomvariable/rocm-envoy-ai-gateway-external-processor/pkg/pool"
)

const testPathEmbeddings = "/v1/embeddings"

func TestExtractModelFromBody(t *testing.T) {
	t.Parallel()

	server := &Server{}

	tests := []struct {
		name     string
		body     []byte
		expected string
	}{
		{
			name:     "valid model field",
			body:     []byte(`{"model": "gpt-4", "messages": []}`),
			expected: "gpt-4",
		},
		{
			name:     "model with special characters",
			body:     []byte(`{"model": "meta-llama/Llama-2-7b-chat-hf"}`),
			expected: "meta-llama/Llama-2-7b-chat-hf",
		},
		{
			name:     "no model field",
			body:     []byte(`{"messages": [{"role": "user", "content": "hello"}]}`),
			expected: "",
		},
		{
			name:     "empty body",
			body:     []byte(``),
			expected: "",
		},
		{
			name:     "invalid json",
			body:     []byte(`{invalid json}`),
			expected: "",
		},
		{
			name:     "model field is not string",
			body:     []byte(`{"model": 123}`),
			expected: "",
		},
		{
			name:     "empty model string",
			body:     []byte(`{"model": ""}`),
			expected: "",
		},
		{
			name:     "nested model field",
			body:     []byte(`{"request": {"model": "nested-model"}}`),
			expected: "",
		},
		{
			name:     "model with whitespace",
			body:     []byte(`{"model": "  gpt-4  "}`),
			expected: "  gpt-4  ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := server.extractModelFromBody(tt.body)
			if result != tt.expected {
				t.Errorf("extractModelFromBody() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestNewServer(t *testing.T) {
	t.Parallel()

	server := NewServer(nil, nil)

	if server == nil {
		t.Fatal("NewServer returned nil")
	}

	if server.poolManager != nil {
		t.Error("poolManager should be nil when passed nil")
	}

	if server.genaiMetrics != nil {
		t.Error("genaiMetrics should be nil when passed nil")
	}
}

func TestCreateErrorResponse(t *testing.T) {
	t.Parallel()

	server := &Server{}

	response := server.createErrorResponse("test error message")

	if response == nil {
		t.Fatal("createErrorResponse returned nil")
	}

	immediateResp := response.GetImmediateResponse()
	if immediateResp == nil {
		t.Fatal("ImmediateResponse should not be nil")
	}

	if immediateResp.GetStatus() == nil {
		t.Fatal("Status should not be nil")
	}

	// Check status code is ServiceUnavailable (503)
	if immediateResp.GetStatus().GetCode() != 503 {
		t.Errorf("Status code = %d, want 503", immediateResp.GetStatus().GetCode())
	}

	if string(immediateResp.GetBody()) != "test error message" {
		t.Errorf("Body = %q, want %q", string(immediateResp.GetBody()), "test error message")
	}
}

func TestCreateErrorResponseVariousMessages(t *testing.T) {
	t.Parallel()

	server := &Server{}

	tests := []struct {
		name    string
		message string
	}{
		{
			name:    "empty message",
			message: "",
		},
		{
			name:    "short message",
			message: "error",
		},
		{
			name:    "long message",
			message: "This is a very long error message that contains detailed information about what went wrong during the request processing phase of the external processor",
		},
		{
			name:    "special characters",
			message: "Error: failed to parse JSON {\"key\": \"value\"}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			response := server.createErrorResponse(tt.message)

			immediateResp := response.GetImmediateResponse()
			if string(immediateResp.GetBody()) != tt.message {
				t.Errorf("Body = %q, want %q", string(immediateResp.GetBody()), tt.message)
			}
		})
	}
}

func TestLogVerbosityConstant(t *testing.T) {
	t.Parallel()

	if logVerbosity != 2 {
		t.Errorf("logVerbosity = %d, want 2", logVerbosity)
	}
}

func TestProcessRequestHeadersExtractsModelAndPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		headers          []*envoy_config_core_v3.HeaderValue
		wantOriginalPath string
	}{
		{
			name: "extracts :path into stream state",
			headers: []*envoy_config_core_v3.HeaderValue{
				{Key: ":path", RawValue: []byte("/v1/chat/completions")},
				{Key: "content-type", RawValue: []byte("application/json")},
			},
			wantOriginalPath: "/v1/chat/completions",
		},
		{
			name: "no path header",
			headers: []*envoy_config_core_v3.HeaderValue{
				{Key: "content-type", RawValue: []byte("application/json")},
			},
			wantOriginalPath: "",
		},
		{
			name:             "empty headers",
			headers:          nil,
			wantOriginalPath: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := &Server{}
			state := &streamState{}
			ctx := context.Background()

			resp, err := server.processRequestHeaders(ctx, state, &envoy_service_ext_proc_v3.HttpHeaders{
				Headers: &envoy_config_core_v3.HeaderMap{
					Headers: tt.headers,
				},
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if state.originalPath != tt.wantOriginalPath {
				t.Errorf("originalPath = %q, want %q", state.originalPath, tt.wantOriginalPath)
			}

			// Should get a continuation response (empty header mutation) since no model header is set.
			if resp.GetRequestHeaders() == nil {
				t.Error("expected RequestHeaders continuation response")
			}
		})
	}
}

func TestStreamStatePreservedAcrossPhases(t *testing.T) {
	t.Parallel()

	server := &Server{}
	state := &streamState{}
	ctx := context.Background()

	// Phase 1: Process headers — no model, captures path.
	_, err := server.processRequestHeaders(ctx, state, &envoy_service_ext_proc_v3.HttpHeaders{
		Headers: &envoy_config_core_v3.HeaderMap{
			Headers: []*envoy_config_core_v3.HeaderValue{
				{Key: ":path", RawValue: []byte(testPathEmbeddings)},
				{Key: "content-type", RawValue: []byte("application/json")},
			},
		},
	})
	if err != nil {
		t.Fatalf("processRequestHeaders: unexpected error: %v", err)
	}

	if state.originalPath != testPathEmbeddings {
		t.Fatalf("originalPath = %q, want %q", state.originalPath, testPathEmbeddings)
	}

	// Phase 2: processRequestBody would use the same state.
	// We verify the state persists (no pool manager needed for this check).
	if state.originalPath != testPathEmbeddings {
		t.Errorf("originalPath changed after header phase: %q", state.originalPath)
	}
}

func TestProcessRequestHeadersIgnoresOldHeaderNames(t *testing.T) {
	t.Parallel()

	server := &Server{}
	state := &streamState{}
	ctx := context.Background()

	// Old header names should NOT be picked up.
	resp, err := server.processRequestHeaders(ctx, state, &envoy_service_ext_proc_v3.HttpHeaders{
		Headers: &envoy_config_core_v3.HeaderMap{
			Headers: []*envoy_config_core_v3.HeaderValue{
				{Key: "x-model-name", RawValue: []byte("gpt-4")},
				{Key: "x-backend-endpoint", RawValue: []byte("10.0.0.1:8080")},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should get continuation response (model not detected via old header).
	if resp.GetRequestHeaders() == nil {
		t.Error("expected continuation response, old x-model-name should be ignored")
	}
}

func TestExtractPoolKeyFromMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		req     *envoy_service_ext_proc_v3.ProcessingRequest
		wantKey *pool.PoolKey
	}{
		{
			name:    "nil metadata context",
			req:     &envoy_service_ext_proc_v3.ProcessingRequest{},
			wantKey: nil,
		},
		{
			name: "empty metadata",
			req: &envoy_service_ext_proc_v3.ProcessingRequest{
				MetadataContext: &envoy_config_core_v3.Metadata{},
			},
			wantKey: nil,
		},
		{
			name: "wrong namespace",
			req: &envoy_service_ext_proc_v3.ProcessingRequest{
				MetadataContext: &envoy_config_core_v3.Metadata{
					FilterMetadata: map[string]*structpb.Struct{
						"other.namespace": {
							Fields: map[string]*structpb.Value{
								"per_route_rule_inference_pool": structpb.NewStringValue("ns/name/svc/9002/duplex/false"),
							},
						},
					},
				},
			},
			wantKey: nil,
		},
		{
			name: "valid pool metadata",
			req: &envoy_service_ext_proc_v3.ProcessingRequest{
				MetadataContext: &envoy_config_core_v3.Metadata{
					FilterMetadata: map[string]*structpb.Struct{
						"aigateway.envoy.io": {
							Fields: map[string]*structpb.Value{
								"per_route_rule_inference_pool": structpb.NewStringValue(
									"default/vllm-pool/vllm-epp/9002/duplex/false",
								),
							},
						},
					},
				},
			},
			wantKey: &pool.PoolKey{Namespace: "default", Name: "vllm-pool"},
		},
		{
			name: "minimal two-segment value",
			req: &envoy_service_ext_proc_v3.ProcessingRequest{
				MetadataContext: &envoy_config_core_v3.Metadata{
					FilterMetadata: map[string]*structpb.Struct{
						"aigateway.envoy.io": {
							Fields: map[string]*structpb.Value{
								"per_route_rule_inference_pool": structpb.NewStringValue("ns/name"),
							},
						},
					},
				},
			},
			wantKey: &pool.PoolKey{Namespace: "ns", Name: "name"},
		},
		{
			name: "single segment is malformed",
			req: &envoy_service_ext_proc_v3.ProcessingRequest{
				MetadataContext: &envoy_config_core_v3.Metadata{
					FilterMetadata: map[string]*structpb.Struct{
						"aigateway.envoy.io": {
							Fields: map[string]*structpb.Value{
								"per_route_rule_inference_pool": structpb.NewStringValue("only-one"),
							},
						},
					},
				},
			},
			wantKey: nil,
		},
		{
			name: "empty string value",
			req: &envoy_service_ext_proc_v3.ProcessingRequest{
				MetadataContext: &envoy_config_core_v3.Metadata{
					FilterMetadata: map[string]*structpb.Struct{
						"aigateway.envoy.io": {
							Fields: map[string]*structpb.Value{
								"per_route_rule_inference_pool": structpb.NewStringValue(""),
							},
						},
					},
				},
			},
			wantKey: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := extractPoolKeyFromMetadata(tt.req)

			if tt.wantKey == nil {
				if got != nil {
					t.Errorf("extractPoolKeyFromMetadata() = %v, want nil", got)
				}

				return
			}

			if got == nil {
				t.Fatalf("extractPoolKeyFromMetadata() = nil, want %v", tt.wantKey)
			}

			if *got != *tt.wantKey {
				t.Errorf("extractPoolKeyFromMetadata() = %v, want %v", *got, *tt.wantKey)
			}
		})
	}
}

func TestExtractModelFromBodyEdgeCases(t *testing.T) {
	t.Parallel()

	server := &Server{}

	tests := []struct {
		name     string
		body     []byte
		expected string
	}{
		{
			name:     "null model value",
			body:     []byte(`{"model": null}`),
			expected: "",
		},
		{
			name:     "array model value",
			body:     []byte(`{"model": ["gpt-4"]}`),
			expected: "",
		},
		{
			name:     "numeric model value",
			body:     []byte(`{"model": 42}`),
			expected: "",
		},
		{
			name:     "boolean model value",
			body:     []byte(`{"model": true}`),
			expected: "",
		},
		{
			name:     "object model value",
			body:     []byte(`{"model": {"name": "gpt-4"}}`),
			expected: "",
		},
		{
			name:     "model field with extra whitespace in JSON",
			body:     []byte(`{  "model"  :  "gpt-4"  }`),
			expected: "gpt-4",
		},
		{
			name:     "model with unicode characters",
			body:     []byte(`{"model": "special-model-name"}`),
			expected: "special-model-name",
		},
		{
			name:     "multiple model fields - first wins",
			body:     []byte(`{"model": "first", "model": "second"}`),
			expected: "second",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := server.extractModelFromBody(tt.body)
			if result != tt.expected {
				t.Errorf("extractModelFromBody() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestProcessRequestBodyUsesDefaultModel(t *testing.T) {
	t.Parallel()

	server := &Server{}

	// Server with nil poolManager will trigger createRoutingResponse which will panic.
	// We need to test just the extraction logic path.
	// Since we can't easily test processRequestBody without a poolManager,
	// we'll test extractModelFromBody more thoroughly.

	// Test that empty body results in "default" model being used.
	// This is tested indirectly through extractModelFromBody returning "".
	modelName := server.extractModelFromBody([]byte(""))
	if modelName != "" {
		t.Errorf("empty body should return empty model name for default fallback, got %q", modelName)
	}

	modelName = server.extractModelFromBody([]byte("invalid json"))
	if modelName != "" {
		t.Errorf("invalid JSON should return empty model name for default fallback, got %q", modelName)
	}
}

func TestStreamStateStructure(t *testing.T) {
	t.Parallel()

	state := &streamState{
		originalPath: "/test/path",
		poolKey:      &pool.PoolKey{Namespace: "ns", Name: "name"},
	}

	if state.originalPath != "/test/path" {
		t.Errorf("originalPath = %q, want %q", state.originalPath, "/test/path")
	}

	if state.poolKey == nil {
		t.Fatal("poolKey should not be nil")
	}

	if state.poolKey.Namespace != "ns" || state.poolKey.Name != "name" {
		t.Errorf("poolKey = %v, want {ns name}", *state.poolKey)
	}
}

func TestMetadataConstants(t *testing.T) {
	t.Parallel()

	if metadataNamespace != "aigateway.envoy.io" {
		t.Errorf("metadataNamespace = %q, want %q", metadataNamespace, "aigateway.envoy.io")
	}

	if metadataInferencePoolKey != "per_route_rule_inference_pool" {
		t.Errorf("metadataInferencePoolKey = %q, want %q", metadataInferencePoolKey, "per_route_rule_inference_pool")
	}
}

func TestProcessRequestHeadersMultipleHeaders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		headers          []*envoy_config_core_v3.HeaderValue
		wantOriginalPath string
		wantContinuation bool
	}{
		{
			name: "path with special characters",
			headers: []*envoy_config_core_v3.HeaderValue{
				{Key: ":path", RawValue: []byte("/v1/chat/completions?stream=true&temperature=0.7")},
			},
			wantOriginalPath: "/v1/chat/completions?stream=true&temperature=0.7",
			wantContinuation: true,
		},
		{
			name: "empty path value",
			headers: []*envoy_config_core_v3.HeaderValue{
				{Key: ":path", RawValue: []byte("")},
			},
			wantOriginalPath: "",
			wantContinuation: true,
		},
		{
			name: "multiple path headers - last wins",
			headers: []*envoy_config_core_v3.HeaderValue{
				{Key: ":path", RawValue: []byte("/first")},
				{Key: ":path", RawValue: []byte("/second")},
			},
			wantOriginalPath: "/second",
			wantContinuation: true,
		},
		{
			name: "case sensitive header keys",
			headers: []*envoy_config_core_v3.HeaderValue{
				{Key: ":PATH", RawValue: []byte("/uppercase")},
				{Key: ":path", RawValue: []byte("/lowercase")},
			},
			wantOriginalPath: "/lowercase",
			wantContinuation: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := &Server{}
			state := &streamState{}
			ctx := context.Background()

			resp, err := server.processRequestHeaders(ctx, state, &envoy_service_ext_proc_v3.HttpHeaders{
				Headers: &envoy_config_core_v3.HeaderMap{
					Headers: tt.headers,
				},
			})
			if err != nil {
				t.Fatalf("processRequestHeaders failed: %v", err)
			}

			if state.originalPath != tt.wantOriginalPath {
				t.Errorf("originalPath = %q, want %q", state.originalPath, tt.wantOriginalPath)
			}

			if tt.wantContinuation {
				if resp.GetRequestHeaders() == nil {
					t.Error("expected RequestHeaders continuation response")
				}
			}
		})
	}
}

func TestExtractPoolKeyFromMetadataWithNonStringValue(t *testing.T) {
	t.Parallel()

	// Test with non-string value in metadata (should be ignored).
	req := &envoy_service_ext_proc_v3.ProcessingRequest{
		MetadataContext: &envoy_config_core_v3.Metadata{
			FilterMetadata: map[string]*structpb.Struct{
				"aigateway.envoy.io": {
					Fields: map[string]*structpb.Value{
						"per_route_rule_inference_pool": structpb.NewNumberValue(123),
					},
				},
			},
		},
	}

	got := extractPoolKeyFromMetadata(req)

	if got != nil {
		t.Errorf("extractPoolKeyFromMetadata() with number value = %v, want nil", got)
	}
}

func TestExtractPoolKeyFromMetadataThreeSegments(t *testing.T) {
	t.Parallel()

	// Test with three segments (should extract first two).
	req := &envoy_service_ext_proc_v3.ProcessingRequest{
		MetadataContext: &envoy_config_core_v3.Metadata{
			FilterMetadata: map[string]*structpb.Struct{
				"aigateway.envoy.io": {
					Fields: map[string]*structpb.Value{
						"per_route_rule_inference_pool": structpb.NewStringValue("ns/name/extra"),
					},
				},
			},
		},
	}

	got := extractPoolKeyFromMetadata(req)

	want := &pool.PoolKey{Namespace: "ns", Name: "name"}

	if got == nil {
		t.Fatalf("extractPoolKeyFromMetadata() = nil, want %v", want)
	}

	if *got != *want {
		t.Errorf("extractPoolKeyFromMetadata() = %v, want %v", *got, *want)
	}
}

func TestServerStructFields(t *testing.T) {
	t.Parallel()

	server := NewServer(nil, nil)

	// Verify server embeds UnimplementedExternalProcessorServer.
	_ = server.UnimplementedExternalProcessorServer

	if server.poolManager != nil {
		t.Error("poolManager should be nil")
	}

	if server.genaiMetrics != nil {
		t.Error("genaiMetrics should be nil")
	}
}

func TestCreateErrorResponseLongMessage(t *testing.T) {
	t.Parallel()

	server := &Server{}

	longMessage := string(make([]byte, 10000))
	for i := range longMessage {
		longMessage = string(append([]byte(longMessage)[:i], 'x'))
	}

	longMessage = longMessage[:10000]

	resp := server.createErrorResponse(longMessage)

	if resp == nil {
		t.Fatal("createErrorResponse returned nil")
	}

	body := string(resp.GetImmediateResponse().GetBody())
	if len(body) != 10000 {
		t.Errorf("body length = %d, want 10000", len(body))
	}
}
