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

package telemetry

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

func TestAttributeKeys(t *testing.T) {
	t.Parallel()

	// GenAI Semantic Convention attributes
	tests := []struct {
		name     string
		got      string
		expected string
	}{
		{"AttrGenAIOperationName", AttrGenAIOperationName, "gen_ai.operation.name"},
		{"AttrGenAIRequestModel", AttrGenAIRequestModel, "gen_ai.request.model"},
		{"AttrGenAIResponseModel", AttrGenAIResponseModel, "gen_ai.response.model"},
		{"AttrGenAIProviderName", AttrGenAIProviderName, "gen_ai.provider.name"},
		{"AttrGenAISystem", AttrGenAISystem, "gen_ai.system"},
		{"AttrGenAITokenTypeKey", AttrGenAITokenTypeKey, "gen_ai.token.type"},
		{"AttrGenAIUsageInputTokensKey", AttrGenAIUsageInputTokensKey, "gen_ai.usage.input_tokens"},
		{"AttrGenAIUsageOutputTokensKey", AttrGenAIUsageOutputTokensKey, "gen_ai.usage.output_tokens"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.got != tt.expected {
				t.Errorf("%s = %q, want %q", tt.name, tt.got, tt.expected)
			}
		})
	}
}

func TestOpenInferenceAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		got      string
		expected string
	}{
		{"AttrOpenInferenceSpanKind", AttrOpenInferenceSpanKind, "openinference.span.kind"},
		{"AttrLLMModelName", AttrLLMModelName, "llm.model_name"},
		{"AttrLLMTokenCountPromptKey", AttrLLMTokenCountPromptKey, "llm.token_count.prompt"},
		{"AttrLLMTokenCountCompletionKey", AttrLLMTokenCountCompletionKey, "llm.token_count.completion"},
		{"AttrLLMTokenCountTotalKey", AttrLLMTokenCountTotalKey, "llm.token_count.total"},
		{"AttrLLMInvocationParameters", AttrLLMInvocationParameters, "llm.invocation_parameters"},
		{"AttrEmbeddingModelName", AttrEmbeddingModelName, "embedding.model_name"},
		{"AttrEmbeddingEmbeddings", AttrEmbeddingEmbeddings, "embedding.embeddings"},
		{"AttrRerankQueryText", AttrRerankQueryText, "reranker.query"},
		{"AttrRerankTopK", AttrRerankTopK, "reranker.top_k"},
		{"AttrRetrieverQueryText", AttrRetrieverQueryText, "retriever.query"},
		{"AttrRetrieverDocuments", AttrRetrieverDocuments, "retriever.documents"},
		{"AttrToolName", AttrToolName, "tool.name"},
		{"AttrToolDescription", AttrToolDescription, "tool.description"},
		{"AttrToolParameters", AttrToolParameters, "tool.parameters"},
		{"AttrSessionID", AttrSessionID, "session.id"},
		{"AttrUserID", AttrUserID, "user.id"},
		{"AttrMetadataKey", AttrMetadataKey, "metadata"},
		{"AttrTagsKey", AttrTagsKey, "tag.tags"},
		{"AttrInputValueKey", AttrInputValueKey, "input.value"},
		{"AttrInputMimeType", AttrInputMimeType, "input.mime_type"},
		{"AttrOutputValueKey", AttrOutputValueKey, "output.value"},
		{"AttrOutputMimeType", AttrOutputMimeType, "output.mime_type"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.got != tt.expected {
				t.Errorf("%s = %q, want %q", tt.name, tt.got, tt.expected)
			}
		})
	}
}

func TestOperationNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		got      string
		expected string
	}{
		{"OperationChat", OperationChat, "chat"},
		{"OperationCompletion", OperationCompletion, "text_completion"},
		{"OperationEmbedding", OperationEmbedding, "embeddings"},
		{"OperationRerank", OperationRerank, "rerank"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.got != tt.expected {
				t.Errorf("%s = %q, want %q", tt.name, tt.got, tt.expected)
			}
		})
	}
}

func TestSpanKinds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		got      string
		expected string
	}{
		{"SpanKindLLM", SpanKindLLM, "LLM"},
		{"SpanKindEmbedding", SpanKindEmbedding, "EMBEDDING"},
		{"SpanKindChain", SpanKindChain, "CHAIN"},
		{"SpanKindRetriever", SpanKindRetriever, "RETRIEVER"},
		{"SpanKindReranker", SpanKindReranker, "RERANKER"},
		{"SpanKindTool", SpanKindTool, "TOOL"},
		{"SpanKindAgent", SpanKindAgent, "AGENT"},
		{"SpanKindGuardrail", SpanKindGuardrail, "GUARDRAIL"},
		{"SpanKindEvaluator", SpanKindEvaluator, "EVALUATOR"},
		{"SpanKindPrompt", SpanKindPrompt, "PROMPT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.got != tt.expected {
				t.Errorf("%s = %q, want %q", tt.name, tt.got, tt.expected)
			}
		})
	}
}

func TestTokenTypes(t *testing.T) {
	t.Parallel()

	if TokenTypeInput != "input" {
		t.Errorf("TokenTypeInput = %q, want %q", TokenTypeInput, "input")
	}

	if TokenTypeOutput != "output" {
		t.Errorf("TokenTypeOutput = %q, want %q", TokenTypeOutput, "output")
	}
}

func TestHistogramBuckets(t *testing.T) {
	t.Parallel()

	// Test all histogram buckets are non-empty and in ascending order
	testCases := []struct {
		name    string
		buckets []float64
	}{
		{"RequestDurationBuckets", RequestDurationBuckets},
		{"TimeToFirstTokenBuckets", TimeToFirstTokenBuckets},
		{"TimePerTokenBuckets", TimePerTokenBuckets},
		{"ModelLoadDurationBuckets", ModelLoadDurationBuckets},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertBucketsAscending(t, tc.name, tc.buckets)
		})
	}
}

// assertBucketsAscending verifies that histogram buckets are non-empty and in ascending order.
func assertBucketsAscending(t *testing.T, name string, buckets []float64) {
	t.Helper()

	if len(buckets) == 0 {
		t.Errorf("%s should not be empty", name)

		return
	}

	for i := 1; i < len(buckets); i++ {
		if buckets[i] <= buckets[i-1] {
			t.Errorf("%s not in ascending order at index %d", name, i)
		}
	}
}

func TestNewGenAIMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		meterProvider  metric.MeterProvider
		tracerProvider trace.TracerProvider
	}{
		{
			name:           "noop providers",
			meterProvider:  metricnoop.NewMeterProvider(),
			tracerProvider: tracenoop.NewTracerProvider(),
		},
		{
			name:           "nil tracer provider uses global",
			meterProvider:  metricnoop.NewMeterProvider(),
			tracerProvider: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			metrics, err := NewGenAIMetrics(tt.meterProvider, tt.tracerProvider)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if metrics == nil {
				t.Fatal("expected non-nil metrics")
			}

			if metrics.tracer == nil {
				t.Error("expected non-nil tracer")
			}
		})
	}
}

func TestGetTracerProvider(t *testing.T) {
	t.Parallel()

	t.Run("nil returns global", func(t *testing.T) {
		t.Parallel()

		result := getTracerProvider(nil)
		if result == nil {
			t.Error("expected non-nil tracer provider")
		}
	})

	t.Run("non-nil returns provided", func(t *testing.T) {
		t.Parallel()

		tp := tracenoop.NewTracerProvider()
		result := getTracerProvider(tp)

		if result != tp {
			t.Error("expected to receive provided tracer provider")
		}
	})
}

func TestStartRequestAndEnd(t *testing.T) {
	t.Parallel()

	metrics, err := NewGenAIMetrics(metricnoop.NewMeterProvider(), tracenoop.NewTracerProvider())
	if err != nil {
		t.Fatalf("failed to create metrics: %v", err)
	}

	tests := []struct {
		name   string
		params *RequestParams
		resp   ResponseParams
	}{
		{
			name: "basic chat",
			params: &RequestParams{
				Operation:   OperationChat,
				Model:       "llama-3-8b",
				Provider:    "rocm-vllm",
				System:      "vllm",
				InputTokens: 100,
			},
			resp: ResponseParams{OutputTokens: 50, ResponseModel: "llama-3-8b"},
		},
		{
			name: "with routing",
			params: &RequestParams{
				Operation:   OperationChat,
				Model:       "mistral-7b",
				TargetPod:   "vllm-pod-1",
				TargetNode:  "node-gpu-1",
				InputTokens: 200,
			},
			resp: ResponseParams{OutputTokens: 100},
		},
		{
			name: "with span kind",
			params: &RequestParams{
				Operation: OperationChat,
				Model:     "llama-3-8b",
				SpanKind:  SpanKindLLM,
			},
			resp: ResponseParams{OutputTokens: 25},
		},
		{
			name: "with custom attributes",
			params: &RequestParams{
				Operation:  OperationEmbedding,
				Model:      "embed-model",
				Attributes: map[string]string{"custom.key": "value"},
			},
			resp: ResponseParams{},
		},
		{
			name: "with cache hit and error",
			params: &RequestParams{
				Operation:   OperationChat,
				Model:       "llama-3-8b",
				InputTokens: 150,
			},
			resp: ResponseParams{
				OutputTokens:   75,
				CacheHitTokens: 50,
				Error:          errors.New("test error"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			recorder := metrics.StartRequest(ctx, tt.params)

			if recorder == nil {
				t.Fatal("expected non-nil recorder")
			}

			if recorder.span == nil {
				t.Error("expected non-nil span")
			}

			recorder.End(tt.resp)
		})
	}
}

func TestRecordFirstToken(t *testing.T) {
	t.Parallel()

	metrics, err := NewGenAIMetrics(metricnoop.NewMeterProvider(), tracenoop.NewTracerProvider())
	if err != nil {
		t.Fatalf("failed to create metrics: %v", err)
	}

	ctx := context.Background()
	recorder := metrics.StartRequest(ctx, &RequestParams{
		Operation:   OperationChat,
		Model:       "llama-3-8b",
		InputTokens: 100,
	})

	time.Sleep(time.Millisecond)
	recorder.RecordFirstToken()

	if recorder.firstTokenT == nil {
		t.Error("expected firstTokenT to be set")
	}

	// End with output tokens to trigger time-per-token calculation.
	recorder.End(ResponseParams{OutputTokens: 50})
}

func TestRequestRecorderContext(t *testing.T) {
	t.Parallel()

	metrics, err := NewGenAIMetrics(metricnoop.NewMeterProvider(), tracenoop.NewTracerProvider())
	if err != nil {
		t.Fatalf("failed to create metrics: %v", err)
	}

	recorder := metrics.StartRequest(context.Background(), &RequestParams{
		Operation: OperationChat,
		Model:     "llama-3-8b",
	})

	if recorder.Context() == nil {
		t.Error("expected non-nil context")
	}

	recorder.End(ResponseParams{})
}

func TestRecordRoutingDecision(t *testing.T) {
	t.Parallel()

	metrics, err := NewGenAIMetrics(metricnoop.NewMeterProvider(), tracenoop.NewTracerProvider())
	if err != nil {
		t.Fatalf("failed to create metrics: %v", err)
	}

	// Should not panic.
	metrics.RecordRoutingDecision(context.Background(), "llama-3-8b", "pod-1", "node-1", "vram_available")
}

func TestRecordModelLoad(t *testing.T) {
	t.Parallel()

	metrics, err := NewGenAIMetrics(metricnoop.NewMeterProvider(), tracenoop.NewTracerProvider())
	if err != nil {
		t.Fatalf("failed to create metrics: %v", err)
	}

	metrics.RecordModelLoad(context.Background(), "llama-3-8b", "node-1", 5*time.Second, 8*1024*1024*1024)
}

func TestRecordModelMemory(t *testing.T) {
	t.Parallel()

	metrics, err := NewGenAIMetrics(metricnoop.NewMeterProvider(), tracenoop.NewTracerProvider())
	if err != nil {
		t.Fatalf("failed to create metrics: %v", err)
	}

	metrics.RecordModelMemory(context.Background(), "llama-3-8b", "node-1", 8*1024*1024*1024)
}

func TestBuildAttributes(t *testing.T) {
	t.Parallel()

	metrics, err := NewGenAIMetrics(metricnoop.NewMeterProvider(), tracenoop.NewTracerProvider())
	if err != nil {
		t.Fatalf("failed to create metrics: %v", err)
	}

	tests := []struct {
		name       string
		params     *RequestParams
		wantMinLen int
	}{
		{
			name:       "minimal",
			params:     &RequestParams{Operation: OperationChat, Model: "m"},
			wantMinLen: 2,
		},
		{
			name:       "with provider and system",
			params:     &RequestParams{Operation: OperationChat, Model: "m", Provider: "p", System: "s"},
			wantMinLen: 4,
		},
		{
			name:       "with routing",
			params:     &RequestParams{Operation: OperationChat, Model: "m", TargetPod: "pod", TargetNode: "node"},
			wantMinLen: 4,
		},
		{
			name:       "with span kind",
			params:     &RequestParams{Operation: OperationChat, Model: "m", SpanKind: SpanKindLLM},
			wantMinLen: 3,
		},
		{
			name:       "with input tokens",
			params:     &RequestParams{Operation: OperationChat, Model: "m", InputTokens: 100},
			wantMinLen: 3,
		},
		{
			name:       "with custom attributes",
			params:     &RequestParams{Operation: OperationChat, Model: "m", Attributes: map[string]string{"k": "v"}},
			wantMinLen: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			attrs := metrics.buildAttributes(tt.params)
			if len(attrs) < tt.wantMinLen {
				t.Errorf("got %d attributes, want at least %d", len(attrs), tt.wantMinLen)
			}
		})
	}
}

func TestSpanNameFunc(t *testing.T) {
	t.Parallel()

	metrics, err := NewGenAIMetrics(metricnoop.NewMeterProvider(), tracenoop.NewTracerProvider())
	if err != nil {
		t.Fatalf("failed to create metrics: %v", err)
	}

	tests := []struct {
		name     string
		params   *RequestParams
		expected string
	}{
		{
			name:     "operation only",
			params:   &RequestParams{Operation: OperationChat, Model: "llama"},
			expected: "chat llama",
		},
		{
			name:     "span kind overrides",
			params:   &RequestParams{Operation: OperationChat, Model: "llama", SpanKind: SpanKindLLM},
			expected: "LLM llama",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := metrics.spanName(tt.params); got != tt.expected {
				t.Errorf("spanName() = %q, want %q", got, tt.expected)
			}
		})
	}
}
