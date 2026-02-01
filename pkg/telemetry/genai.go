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

// genai.go implements OpenTelemetry Gen AI Semantic Conventions metrics
// (https://opentelemetry.io/docs/specs/semconv/gen-ai/) and OpenInference
// span attributes (https://github.com/Arize-ai/openinference/tree/main/spec).

package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	pkgversion "github.com/randomvariable/rocm-envoy-ai-gateway-external-processor/pkg/version"
)

// Histogram bucket boundaries for different metric types.
var (
	// RequestDurationBuckets defines bucket boundaries for request duration in seconds.
	RequestDurationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}

	// TimeToFirstTokenBuckets defines bucket boundaries for TTFT in seconds.
	TimeToFirstTokenBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

	// TimePerTokenBuckets defines bucket boundaries for time per output token in seconds.
	TimePerTokenBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1}

	// ModelLoadDurationBuckets defines bucket boundaries for model loading in seconds.
	ModelLoadDurationBuckets = []float64{1, 5, 10, 30, 60, 120, 300, 600}
)

// GenAI Semantic Convention attribute keys (https://opentelemetry.io/docs/specs/semconv/gen-ai/).
const (
	// AttrGenAIOperationName is the operation type (chat, completion, embedding, rerank).
	AttrGenAIOperationName = "gen_ai.operation.name"
	// AttrGenAIRequestModel is the requested model name.
	AttrGenAIRequestModel = "gen_ai.request.model"
	// AttrGenAIResponseModel is the actual model used in the response.
	AttrGenAIResponseModel = "gen_ai.response.model"
	// AttrGenAIProviderName is the provider name (e.g., "rocm-vllm").
	AttrGenAIProviderName = "gen_ai.provider.name"
	// AttrGenAISystem is the GenAI system (e.g., "vllm").
	AttrGenAISystem = "gen_ai.system"
)

// Token-related attribute keys.
const (
	// AttrGenAITokenTypeKey is the token type (input/output).
	AttrGenAITokenTypeKey = "gen_ai.token.type" //nolint:gosec // Not a credential, it's a metric attribute name.
	// AttrGenAIUsageInputTokensKey is the input token count attribute.
	AttrGenAIUsageInputTokensKey = "gen_ai.usage.input_tokens" //nolint:gosec // Metric attribute name.
	// AttrGenAIUsageOutputTokensKey is the output token count attribute.
	AttrGenAIUsageOutputTokensKey = "gen_ai.usage.output_tokens" //nolint:gosec // Metric attribute name.
)

// OpenInference span attributes (https://github.com/Arize-ai/openinference/tree/main/spec).
const (
	// AttrOpenInferenceSpanKind is the OpenInference span kind (LLM, EMBEDDING, etc.).
	AttrOpenInferenceSpanKind = "openinference.span.kind"
	// AttrLLMModelName is the model name for LLM operations.
	AttrLLMModelName = "llm.model_name"
	// AttrLLMTokenCountPromptKey is the prompt token count.
	AttrLLMTokenCountPromptKey = "llm.token_count.prompt" //nolint:gosec // Metric attribute name.
	// AttrLLMTokenCountCompletionKey is the completion token count.
	AttrLLMTokenCountCompletionKey = "llm.token_count.completion" //nolint:gosec // Metric attribute name.
	// AttrLLMTokenCountTotalKey is the total token count.
	AttrLLMTokenCountTotalKey = "llm.token_count.total" //nolint:gosec // Metric attribute name.
	// AttrLLMInvocationParameters are the model invocation parameters.
	AttrLLMInvocationParameters = "llm.invocation_parameters"
	// AttrEmbeddingModelName is the embedding model name.
	AttrEmbeddingModelName = "embedding.model_name"
	// AttrEmbeddingEmbeddings are the embedding vectors.
	AttrEmbeddingEmbeddings = "embedding.embeddings"
	// AttrRerankQueryText is the reranker query.
	AttrRerankQueryText = "reranker.query"
	// AttrRerankTopK is the reranker top-k value.
	AttrRerankTopK = "reranker.top_k"
	// AttrRetrieverQueryText is the retriever query.
	AttrRetrieverQueryText = "retriever.query"
	// AttrRetrieverDocuments are the retrieved documents.
	AttrRetrieverDocuments = "retriever.documents"
	// AttrToolName is the tool name.
	AttrToolName = "tool.name"
	// AttrToolDescription is the tool description.
	AttrToolDescription = "tool.description"
	// AttrToolParameters are the tool parameters.
	AttrToolParameters = "tool.parameters"
	// AttrSessionID is the session identifier.
	AttrSessionID = "session.id"
	// AttrUserID is the user identifier.
	AttrUserID = "user.id"
	// AttrMetadataKey is the metadata key prefix.
	AttrMetadataKey = "metadata"
	// AttrTagsKey is the tags key.
	AttrTagsKey = "tag.tags"
	// AttrInputValueKey is the input value.
	AttrInputValueKey = "input.value"
	// AttrInputMimeType is the input MIME type.
	AttrInputMimeType = "input.mime_type"
	// AttrOutputValueKey is the output value.
	AttrOutputValueKey = "output.value"
	// AttrOutputMimeType is the output MIME type.
	AttrOutputMimeType = "output.mime_type"
)

// GenAI operation names.
const (
	// OperationChat is a chat completion operation.
	OperationChat = "chat"
	// OperationCompletion is a text completion operation.
	OperationCompletion = "text_completion"
	// OperationEmbedding is an embedding operation.
	OperationEmbedding = "embeddings"
	// OperationRerank is a reranking operation.
	OperationRerank = "rerank"
)

// OpenInference span kinds.
const (
	// SpanKindLLM is an LLM span.
	SpanKindLLM = "LLM"
	// SpanKindEmbedding is an embedding span.
	SpanKindEmbedding = "EMBEDDING"
	// SpanKindChain is a chain span.
	SpanKindChain = "CHAIN"
	// SpanKindRetriever is a retriever span.
	SpanKindRetriever = "RETRIEVER"
	// SpanKindReranker is a reranker span.
	SpanKindReranker = "RERANKER"
	// SpanKindTool is a tool span.
	SpanKindTool = "TOOL"
	// SpanKindAgent is an agent span.
	SpanKindAgent = "AGENT"
	// SpanKindGuardrail is a guardrail span.
	SpanKindGuardrail = "GUARDRAIL"
	// SpanKindEvaluator is an evaluator span.
	SpanKindEvaluator = "EVALUATOR"
	// SpanKindPrompt is a prompt span.
	SpanKindPrompt = "PROMPT"
)

// Token types.
const (
	// TokenTypeInput is an input token.
	TokenTypeInput = "input"
	// TokenTypeOutput is an output token.
	TokenTypeOutput = "output"
)

// GenAIMetrics provides OpenTelemetry metrics following Gen AI Semantic Conventions.
type GenAIMetrics struct {
	tokenUsage           metric.Int64Counter
	requestDuration      metric.Float64Histogram
	timeToFirstToken     metric.Float64Histogram
	timePerOutputToken   metric.Float64Histogram
	activeRequests       metric.Int64UpDownCounter
	requestsTotal        metric.Int64Counter
	requestTokensTotal   metric.Int64Counter
	responseTokensTotal  metric.Int64Counter
	cacheHitTokensTotal  metric.Int64Counter
	modelLoadDuration    metric.Float64Histogram
	modelMemoryUsage     metric.Int64Gauge
	routingDecisionCount metric.Int64Counter
	tracer               trace.Tracer
}

// NewGenAIMetrics creates a new GenAIMetrics instance with all metrics registered.
func NewGenAIMetrics(meterProvider metric.MeterProvider, tracerProvider trace.TracerProvider) (*GenAIMetrics, error) {
	meter := meterProvider.Meter("rocm.envoy.ai.gateway.genai",
		metric.WithInstrumentationVersion(pkgversion.GetVersion()),
	)

	tracerToUse := getTracerProvider(tracerProvider)
	tracer := tracerToUse.Tracer("rocm.envoy.ai.gateway.genai",
		trace.WithInstrumentationVersion(pkgversion.GetVersion()),
	)

	metrics, err := createMetrics(meter)
	if err != nil {
		return nil, err
	}

	metrics.tracer = tracer

	return metrics, nil
}

func getTracerProvider(tp trace.TracerProvider) trace.TracerProvider {
	if tp != nil {
		return tp
	}

	return otel.GetTracerProvider()
}

func createMetrics(meter metric.Meter) (*GenAIMetrics, error) {
	genaiMetrics := &GenAIMetrics{}

	err := createCoreMetrics(meter, genaiMetrics)
	if err != nil {
		return nil, err
	}

	err = createTokenMetrics(meter, genaiMetrics)
	if err != nil {
		return nil, err
	}

	err = createModelMetrics(meter, genaiMetrics)
	if err != nil {
		return nil, err
	}

	return genaiMetrics, nil
}

func createCoreMetrics(meter metric.Meter, genaiMetrics *GenAIMetrics) error {
	var err error

	genaiMetrics.tokenUsage, err = meter.Int64Counter("gen_ai.client.token.usage",
		metric.WithDescription("Number of tokens used in GenAI operations"),
		metric.WithUnit("{token}"),
	)
	if err != nil {
		return fmt.Errorf("creating token usage counter: %w", err)
	}

	genaiMetrics.requestDuration, err = meter.Float64Histogram("gen_ai.server.request.duration",
		metric.WithDescription("Duration of GenAI requests"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(RequestDurationBuckets...),
	)
	if err != nil {
		return fmt.Errorf("creating request duration histogram: %w", err)
	}

	genaiMetrics.timeToFirstToken, err = meter.Float64Histogram("gen_ai.server.time_to_first_token",
		metric.WithDescription("Time from request start to first token received"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(TimeToFirstTokenBuckets...),
	)
	if err != nil {
		return fmt.Errorf("creating time to first token histogram: %w", err)
	}

	genaiMetrics.timePerOutputToken, err = meter.Float64Histogram("gen_ai.server.time_per_output_token",
		metric.WithDescription("Average time per output token generated"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(TimePerTokenBuckets...),
	)
	if err != nil {
		return fmt.Errorf("creating time per output token histogram: %w", err)
	}

	genaiMetrics.activeRequests, err = meter.Int64UpDownCounter("gen_ai.server.active_requests",
		metric.WithDescription("Number of active GenAI requests"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		return fmt.Errorf("creating active requests counter: %w", err)
	}

	genaiMetrics.requestsTotal, err = meter.Int64Counter("gen_ai.server.requests_total",
		metric.WithDescription("Total number of GenAI requests"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		return fmt.Errorf("creating requests total counter: %w", err)
	}

	return nil
}

func createTokenMetrics(meter metric.Meter, genaiMetrics *GenAIMetrics) error {
	var err error

	genaiMetrics.requestTokensTotal, err = meter.Int64Counter("gen_ai.server.request_tokens_total",
		metric.WithDescription("Total input tokens across all requests"),
		metric.WithUnit("{token}"),
	)
	if err != nil {
		return fmt.Errorf("creating request tokens total counter: %w", err)
	}

	genaiMetrics.responseTokensTotal, err = meter.Int64Counter("gen_ai.server.response_tokens_total",
		metric.WithDescription("Total output tokens across all requests"),
		metric.WithUnit("{token}"),
	)
	if err != nil {
		return fmt.Errorf("creating response tokens total counter: %w", err)
	}

	genaiMetrics.cacheHitTokensTotal, err = meter.Int64Counter("gen_ai.server.cache_hit_tokens_total",
		metric.WithDescription("Total tokens served from KV cache"),
		metric.WithUnit("{token}"),
	)
	if err != nil {
		return fmt.Errorf("creating cache hit tokens total counter: %w", err)
	}

	return nil
}

func createModelMetrics(meter metric.Meter, genaiMetrics *GenAIMetrics) error {
	var err error

	genaiMetrics.modelLoadDuration, err = meter.Float64Histogram("gen_ai.server.model_load.duration",
		metric.WithDescription("Time to load a model into VRAM"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(ModelLoadDurationBuckets...),
	)
	if err != nil {
		return fmt.Errorf("creating model load duration histogram: %w", err)
	}

	genaiMetrics.modelMemoryUsage, err = meter.Int64Gauge("gen_ai.server.model.memory_usage",
		metric.WithDescription("Current VRAM usage by model"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return fmt.Errorf("creating model memory usage gauge: %w", err)
	}

	genaiMetrics.routingDecisionCount, err = meter.Int64Counter("gen_ai.server.routing.decisions_total",
		metric.WithDescription("Total routing decisions made by the gateway"),
		metric.WithUnit("{decision}"),
	)
	if err != nil {
		return fmt.Errorf("creating routing decision counter: %w", err)
	}

	return nil
}

// RequestParams holds parameters for recording GenAI request metrics.
type RequestParams struct {
	Operation   string            // Operation type (chat, completion, embedding, rerank)
	Model       string            // Requested model name
	Provider    string            // Provider name (e.g., "rocm-vllm")
	System      string            // GenAI system (e.g., "vllm")
	TargetPod   string            // Target pod for routing
	TargetNode  string            // Target node for routing
	SpanKind    string            // OpenInference span kind
	Attributes  map[string]string // Additional attributes
	InputTokens int64             // Number of input tokens
}

// RequestRecorder tracks an in-flight request and records metrics on completion.
type RequestRecorder struct {
	metrics     *GenAIMetrics
	params      *RequestParams
	startTime   time.Time
	span        trace.Span
	spanContext context.Context //nolint:containedctx // Required for span propagation in request tracking.
	firstTokenT *time.Time
}

// StartRequest begins tracking a GenAI request.
// The caller MUST call RequestRecorder.End() to complete tracking and end the span.
//
//nolint:spancheck // Span is intentionally returned for caller to manage via RequestRecorder.End().
func (m *GenAIMetrics) StartRequest(ctx context.Context, params *RequestParams) *RequestRecorder {
	attrs := m.buildAttributes(params)

	// Increment active requests.
	m.activeRequests.Add(ctx, 1, metric.WithAttributes(attrs...))
	m.requestsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))

	// Record input tokens if provided.
	if params.InputTokens > 0 {
		tokenAttrs := make([]attribute.KeyValue, len(attrs), len(attrs)+1)
		copy(tokenAttrs, attrs)
		tokenAttrs = append(tokenAttrs, attribute.String(AttrGenAITokenTypeKey, TokenTypeInput))
		m.tokenUsage.Add(ctx, params.InputTokens, metric.WithAttributes(tokenAttrs...))
		m.requestTokensTotal.Add(ctx, params.InputTokens, metric.WithAttributes(attrs...))
	}

	// Start span for distributed tracing.
	spanCtx, span := m.tracer.Start(ctx, m.spanName(params),
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(attrs...),
	)

	return &RequestRecorder{
		metrics:     m,
		params:      params,
		startTime:   time.Now(),
		span:        span,
		spanContext: spanCtx,
	}
}

// RecordFirstToken records the time to first token for streaming responses.
func (r *RequestRecorder) RecordFirstToken() {
	now := time.Now()
	r.firstTokenT = &now
	ttft := now.Sub(r.startTime).Seconds()

	attrs := r.metrics.buildAttributes(r.params)
	r.metrics.timeToFirstToken.Record(r.spanContext, ttft, metric.WithAttributes(attrs...))

	r.span.AddEvent("first_token_received", trace.WithAttributes(
		attribute.Float64("time_to_first_token_seconds", ttft),
	))
}

// ResponseParams holds response completion parameters.
type ResponseParams struct {
	OutputTokens   int64  // Number of output tokens generated
	CacheHitTokens int64  // Tokens served from cache
	ResponseModel  string // Actual model used (may differ from request)
	Error          error  // Error if request failed
}

// End completes request tracking and records final metrics.
func (r *RequestRecorder) End(resp ResponseParams) {
	defer r.span.End()

	duration := time.Since(r.startTime).Seconds()
	attrs := r.metrics.buildAttributes(r.params)

	// Decrement active requests.
	r.metrics.activeRequests.Add(r.spanContext, -1, metric.WithAttributes(attrs...))

	// Record request duration.
	r.metrics.requestDuration.Record(r.spanContext, duration, metric.WithAttributes(attrs...))

	// Record output tokens.
	if resp.OutputTokens > 0 {
		tokenAttrs := make([]attribute.KeyValue, len(attrs), len(attrs)+1)
		copy(tokenAttrs, attrs)
		tokenAttrs = append(tokenAttrs, attribute.String(AttrGenAITokenTypeKey, TokenTypeOutput))
		r.metrics.tokenUsage.Add(r.spanContext, resp.OutputTokens, metric.WithAttributes(tokenAttrs...))
		r.metrics.responseTokensTotal.Add(r.spanContext, resp.OutputTokens, metric.WithAttributes(attrs...))

		// Calculate time per output token.
		if r.firstTokenT != nil {
			genDuration := time.Since(*r.firstTokenT).Seconds()
			avgTimePerToken := genDuration / float64(resp.OutputTokens)
			r.metrics.timePerOutputToken.Record(r.spanContext, avgTimePerToken, metric.WithAttributes(attrs...))
		}
	}

	// Record cache hit tokens.
	if resp.CacheHitTokens > 0 {
		r.metrics.cacheHitTokensTotal.Add(r.spanContext, resp.CacheHitTokens, metric.WithAttributes(attrs...))
	}

	// Add span attributes.
	r.span.SetAttributes(
		attribute.Float64("duration_seconds", duration),
		attribute.Int64(AttrLLMTokenCountCompletionKey, resp.OutputTokens),
		attribute.Int64(AttrLLMTokenCountTotalKey, r.params.InputTokens+resp.OutputTokens),
	)

	if resp.ResponseModel != "" {
		r.span.SetAttributes(attribute.String(AttrGenAIResponseModel, resp.ResponseModel))
	}

	if resp.Error != nil {
		r.span.RecordError(resp.Error)
	}
}

// Context returns the span context for propagation.
func (r *RequestRecorder) Context() context.Context {
	return r.spanContext
}

// RecordRoutingDecision records a routing decision.
func (m *GenAIMetrics) RecordRoutingDecision(ctx context.Context, model, targetPod, targetNode, reason string) {
	attrs := []attribute.KeyValue{
		attribute.String(AttrGenAIRequestModel, model),
		attribute.String("target.pod", targetPod),
		attribute.String("target.node", targetNode),
		attribute.String("routing.reason", reason),
	}
	m.routingDecisionCount.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordModelLoad records model loading metrics.
func (m *GenAIMetrics) RecordModelLoad(ctx context.Context, model, node string, duration time.Duration, memoryBytes int64) {
	attrs := []attribute.KeyValue{
		attribute.String(AttrGenAIRequestModel, model),
		attribute.String("node", node),
	}
	m.modelLoadDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
	m.modelMemoryUsage.Record(ctx, memoryBytes, metric.WithAttributes(attrs...))
}

// RecordModelMemory records current model memory usage.
func (m *GenAIMetrics) RecordModelMemory(ctx context.Context, model, node string, memoryBytes int64) {
	attrs := []attribute.KeyValue{
		attribute.String(AttrGenAIRequestModel, model),
		attribute.String("node", node),
	}
	m.modelMemoryUsage.Record(ctx, memoryBytes, metric.WithAttributes(attrs...))
}

// buildAttributes builds common attributes for metrics.
func (m *GenAIMetrics) buildAttributes(params *RequestParams) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String(AttrGenAIOperationName, params.Operation),
		attribute.String(AttrGenAIRequestModel, params.Model),
	}

	if params.Provider != "" {
		attrs = append(attrs, attribute.String(AttrGenAIProviderName, params.Provider))
	}

	if params.System != "" {
		attrs = append(attrs, attribute.String(AttrGenAISystem, params.System))
	}

	if params.TargetPod != "" {
		attrs = append(attrs, attribute.String("target.pod", params.TargetPod))
	}

	if params.TargetNode != "" {
		attrs = append(attrs, attribute.String("target.node", params.TargetNode))
	}

	if params.SpanKind != "" {
		attrs = append(attrs, attribute.String(AttrOpenInferenceSpanKind, params.SpanKind))
	}

	// Add input tokens as attribute.
	if params.InputTokens > 0 {
		attrs = append(attrs, attribute.Int64(AttrLLMTokenCountPromptKey, params.InputTokens))
	}

	// Add custom attributes.
	for k, v := range params.Attributes {
		attrs = append(attrs, attribute.String(k, v))
	}

	return attrs
}

// spanName generates a span name following OpenInference conventions.
func (m *GenAIMetrics) spanName(params *RequestParams) string {
	if params.SpanKind != "" {
		return params.SpanKind + " " + params.Model
	}

	return params.Operation + " " + params.Model
}
