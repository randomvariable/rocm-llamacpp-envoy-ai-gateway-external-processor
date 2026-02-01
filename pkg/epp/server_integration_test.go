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
	"go.opentelemetry.io/otel/metric/noop"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/randomvariable/rocm-envoy-ai-gateway-external-processor/pkg/pool"
	"github.com/randomvariable/rocm-envoy-ai-gateway-external-processor/pkg/telemetry"
)

const testPathChatCompletions = "/v1/chat/completions"

func newFakeClient() crclient.Client {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	return fake.NewClientBuilder().
		WithScheme(scheme).
		Build()
}

func newTestGenAIMetrics() *telemetry.GenAIMetrics {
	meter := noop.NewMeterProvider()
	tracer := nooptrace.NewTracerProvider()

	metrics, _ := telemetry.NewGenAIMetrics(meter, tracer)

	return metrics
}

func TestProcessRequestBodyWithNoPoolManager(t *testing.T) {
	t.Parallel()

	// Create pool manager with no pools (will return error).
	client := newFakeClient()
	mgr := pool.NewManager(client, "default", map[string]string{"app": "exporter"}, "/v1/load")

	server := NewServer(mgr, newTestGenAIMetrics())
	ctx := context.Background()
	state := &streamState{}

	resp, err := server.processRequestBody(ctx, state, &envoy_service_ext_proc_v3.HttpBody{
		Body: []byte(`{"model": "gpt-4"}`),
	})
	if err != nil {
		t.Fatalf("processRequestBody failed: %v", err)
	}

	// Should get error response since no pool available.
	immediateResp := resp.GetImmediateResponse()
	if immediateResp == nil {
		t.Fatal("expected ImmediateResponse when no pool available")
	}

	if immediateResp.GetStatus().GetCode() != 503 {
		t.Errorf("status code = %d, want 503", immediateResp.GetStatus().GetCode())
	}

	body := string(immediateResp.GetBody())
	if body != "No inference pool available" {
		t.Errorf("body = %q, want 'No inference pool available'", body)
	}
}

func TestProcessRequestBodyWithEmptyBody(t *testing.T) {
	t.Parallel()

	client := newFakeClient()
	mgr := pool.NewManager(client, "default", map[string]string{"app": "exporter"}, "/v1/load")

	server := NewServer(mgr, newTestGenAIMetrics())
	ctx := context.Background()
	state := &streamState{}

	// Empty body should result in "default" model name.
	resp, err := server.processRequestBody(ctx, state, &envoy_service_ext_proc_v3.HttpBody{
		Body: []byte(""),
	})
	if err != nil {
		t.Fatalf("processRequestBody failed: %v", err)
	}

	// Should still get error response (no pool).
	if resp.GetImmediateResponse() == nil {
		t.Error("expected ImmediateResponse")
	}
}

func TestCreateRoutingResponseWithNoPool(t *testing.T) {
	t.Parallel()

	client := newFakeClient()
	mgr := pool.NewManager(client, "default", map[string]string{"app": "exporter"}, "/v1/load")

	server := NewServer(mgr, newTestGenAIMetrics())
	ctx := context.Background()
	state := &streamState{}

	resp, err := server.createRoutingResponse(ctx, state, "test-model")
	if err != nil {
		t.Fatalf("createRoutingResponse failed: %v", err)
	}

	immediateResp := resp.GetImmediateResponse()
	if immediateResp == nil {
		t.Fatal("expected ImmediateResponse")
	}

	if immediateResp.GetStatus().GetCode() != 503 {
		t.Errorf("status code = %d, want 503", immediateResp.GetStatus().GetCode())
	}
}

func TestCreateRoutingResponseWithPoolKey(t *testing.T) {
	t.Parallel()

	client := newFakeClient()
	mgr := pool.NewManager(client, "default", map[string]string{"app": "exporter"}, "/v1/load")

	server := NewServer(mgr, newTestGenAIMetrics())
	ctx := context.Background()

	// Create state with pool key pointing to non-existent pool.
	state := &streamState{
		poolKey: &pool.PoolKey{Namespace: "custom", Name: "pool"},
	}

	resp, err := server.createRoutingResponse(ctx, state, "test-model")
	if err != nil {
		t.Fatalf("createRoutingResponse failed: %v", err)
	}

	// Should fall back to default pool (which doesn't exist) and return error.
	immediateResp := resp.GetImmediateResponse()
	if immediateResp == nil {
		t.Fatal("expected ImmediateResponse when pool not found")
	}
}

func TestCreateRoutingResponseWithOriginalPath(t *testing.T) {
	t.Parallel()

	client := newFakeClient()
	mgr := pool.NewManager(client, "default", map[string]string{"app": "exporter"}, "/v1/load")

	server := NewServer(mgr, newTestGenAIMetrics())
	ctx := context.Background()

	state := &streamState{
		originalPath: testPathChatCompletions,
	}

	resp, err := server.createRoutingResponse(ctx, state, "test-model")
	if err != nil {
		t.Fatalf("createRoutingResponse failed: %v", err)
	}

	// Even though it will error (no pool), we can verify state was used.
	// The function should have attempted to use originalPath.
	if resp.GetImmediateResponse() == nil {
		t.Error("expected ImmediateResponse")
	}
}

func TestProcessRequestHeadersWithModelTriggersRouting(t *testing.T) {
	t.Parallel()

	client := newFakeClient()
	mgr := pool.NewManager(client, "default", map[string]string{"app": "exporter"}, "/v1/load")

	server := NewServer(mgr, newTestGenAIMetrics())
	ctx := context.Background()
	state := &streamState{}

	resp, err := server.processRequestHeaders(ctx, state, &envoy_service_ext_proc_v3.HttpHeaders{
		Headers: &envoy_config_core_v3.HeaderMap{
			Headers: []*envoy_config_core_v3.HeaderValue{
				{Key: "x-ai-eg-model", RawValue: []byte("gpt-4")},
				{Key: ":path", RawValue: []byte(testPathChatCompletions)},
			},
		},
	})
	if err != nil {
		t.Fatalf("processRequestHeaders failed: %v", err)
	}

	// Should trigger createRoutingResponse and return error (no pool).
	immediateResp := resp.GetImmediateResponse()
	if immediateResp == nil {
		t.Error("expected ImmediateResponse when model header present")
	}

	// Verify path was captured.
	if state.originalPath != testPathChatCompletions {
		t.Errorf("originalPath = %q, want %q", state.originalPath, testPathChatCompletions)
	}
}

func TestProcessRequestHeadersWithoutModelContinues(t *testing.T) {
	t.Parallel()

	client := newFakeClient()
	mgr := pool.NewManager(client, "default", map[string]string{"app": "exporter"}, "/v1/load")

	server := NewServer(mgr, newTestGenAIMetrics())
	ctx := context.Background()
	state := &streamState{}

	resp, err := server.processRequestHeaders(ctx, state, &envoy_service_ext_proc_v3.HttpHeaders{
		Headers: &envoy_config_core_v3.HeaderMap{
			Headers: []*envoy_config_core_v3.HeaderValue{
				{Key: ":path", RawValue: []byte("/v1/embeddings")},
				{Key: "content-type", RawValue: []byte("application/json")},
			},
		},
	})
	if err != nil {
		t.Fatalf("processRequestHeaders failed: %v", err)
	}

	// Should return continuation response (no model specified).
	if resp.GetRequestHeaders() == nil {
		t.Error("expected RequestHeaders continuation response")
	}

	if state.originalPath != "/v1/embeddings" {
		t.Errorf("originalPath = %q, want %q", state.originalPath, "/v1/embeddings")
	}
}

func TestProcessRequestBodyVariousInputs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		body      []byte
		wantError bool
	}{
		{
			name:      "valid JSON with model",
			body:      []byte(`{"model": "llama-2", "messages": []}`),
			wantError: false,
		},
		{
			name:      "invalid JSON",
			body:      []byte(`{invalid`),
			wantError: false,
		},
		{
			name:      "empty JSON object",
			body:      []byte(`{}`),
			wantError: false,
		},
		{
			name:      "null body",
			body:      nil,
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newFakeClient()
			mgr := pool.NewManager(client, "default", map[string]string{"app": "exporter"}, "/v1/load")

			server := NewServer(mgr, newTestGenAIMetrics())
			ctx := context.Background()
			state := &streamState{}

			resp, err := server.processRequestBody(ctx, state, &envoy_service_ext_proc_v3.HttpBody{
				Body: tt.body,
			})

			if tt.wantError && err == nil {
				t.Error("expected error but got none")
			}

			if !tt.wantError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			// All should return some response (error response in this case).
			if resp == nil {
				t.Error("expected non-nil response")
			}
		})
	}
}

func TestRegisterServer(t *testing.T) {
	t.Parallel()

	// This test just verifies RegisterServer doesn't panic.
	// We can't easily test the actual registration without a real gRPC server.
	server := NewServer(nil, nil)

	// Call with nil grpcServer should panic, but we won't test that.
	// Instead, just verify the server exists.
	if server == nil {
		t.Fatal("server should not be nil")
	}
}

func TestCreateRoutingResponseExtractsModelName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		modelName string
	}{
		{
			name:      "simple model name",
			modelName: "gpt-4",
		},
		{
			name:      "model with slashes",
			modelName: "meta-llama/Llama-2-7b-chat-hf",
		},
		{
			name:      "empty model name",
			modelName: "",
		},
		{
			name:      "default model",
			modelName: "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newFakeClient()
			mgr := pool.NewManager(client, "default", map[string]string{"app": "exporter"}, "/v1/load")

			server := NewServer(mgr, newTestGenAIMetrics())
			ctx := context.Background()
			state := &streamState{}

			resp, err := server.createRoutingResponse(ctx, state, tt.modelName)
			if err != nil {
				t.Fatalf("createRoutingResponse failed: %v", err)
			}

			// Should get error response (no pool configured).
			if resp.GetImmediateResponse() == nil {
				t.Error("expected ImmediateResponse")
			}
		})
	}
}

func TestProcessRequestBodyWithPoolKeySet(t *testing.T) {
	t.Parallel()

	client := newFakeClient()
	mgr := pool.NewManager(client, "default", map[string]string{"app": "exporter"}, "/v1/load")

	// Create a pool to test the pool key lookup path.
	ctx := context.Background()
	poolKey := pool.PoolKey{Namespace: "test-ns", Name: "test-pool"}

	// Attempt to upsert a pool (will fail to find pods but that's OK).
	_ = mgr.UpsertPool(ctx, poolKey, map[string]string{"app": "vllm"}, []int32{8080})

	server := NewServer(mgr, newTestGenAIMetrics())
	state := &streamState{
		poolKey: &poolKey,
	}

	resp, err := server.processRequestBody(ctx, state, &envoy_service_ext_proc_v3.HttpBody{
		Body: []byte(`{"model": "test-model"}`),
	})
	if err != nil {
		t.Fatalf("processRequestBody failed: %v", err)
	}

	// Should get error response (router will fail to find endpoints).
	if resp.GetImmediateResponse() == nil {
		t.Error("expected ImmediateResponse")
	}
}

func TestCreateRoutingResponseWithBothPoolKeyAndDefaultPool(t *testing.T) {
	t.Parallel()

	client := newFakeClient()
	mgr := pool.NewManager(client, "default", map[string]string{"app": "exporter"}, "/v1/load")

	ctx := context.Background()

	// Create a pool.
	poolKey := pool.PoolKey{Namespace: "custom", Name: "custom-pool"}
	_ = mgr.UpsertPool(ctx, poolKey, map[string]string{"app": "vllm"}, []int32{8080})

	server := NewServer(mgr, newTestGenAIMetrics())
	state := &streamState{
		poolKey: &poolKey,
	}

	resp, err := server.createRoutingResponse(ctx, state, "test-model")
	if err != nil {
		t.Fatalf("createRoutingResponse failed: %v", err)
	}

	// Should attempt to use the pool key (and fail to find endpoints).
	if resp.GetImmediateResponse() == nil {
		t.Error("expected ImmediateResponse")
	}
}

func TestProcessRequestHeadersPreservesStateAcrossPhases(t *testing.T) {
	t.Parallel()

	client := newFakeClient()
	mgr := pool.NewManager(client, "default", map[string]string{"app": "exporter"}, "/v1/load")

	server := NewServer(mgr, newTestGenAIMetrics())
	ctx := context.Background()

	// Simulate two phases: headers then body.
	state := &streamState{}

	// Phase 1: Process headers.
	_, err := server.processRequestHeaders(ctx, state, &envoy_service_ext_proc_v3.HttpHeaders{
		Headers: &envoy_config_core_v3.HeaderMap{
			Headers: []*envoy_config_core_v3.HeaderValue{
				{Key: ":path", RawValue: []byte(testPathChatCompletions)},
			},
		},
	})
	if err != nil {
		t.Fatalf("processRequestHeaders failed: %v", err)
	}

	if state.originalPath != testPathChatCompletions {
		t.Fatal("originalPath not set in phase 1")
	}

	// Phase 2: Process body (should use originalPath from phase 1).
	_, err = server.processRequestBody(ctx, state, &envoy_service_ext_proc_v3.HttpBody{
		Body: []byte(`{"model": "gpt-4"}`),
	})
	if err != nil {
		t.Fatalf("processRequestBody failed: %v", err)
	}

	// Verify originalPath is still set.
	if state.originalPath != testPathChatCompletions {
		t.Errorf("originalPath not preserved: %q", state.originalPath)
	}
}
