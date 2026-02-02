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

// Package epp implements the Envoy External Processing Protocol for endpoint picking.
package epp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	envoy_config_core_v3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoy_service_ext_proc_v3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	envoy_type_v3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"

	"github.com/randomvariable/rocm-envoy-ai-gateway-external-processor/pkg/pool"
	"github.com/randomvariable/rocm-envoy-ai-gateway-external-processor/pkg/router"
	"github.com/randomvariable/rocm-envoy-ai-gateway-external-processor/pkg/telemetry"
)

// Log verbosity level for debug messages.
const logVerbosity = 2

// Server implements the Envoy External Processing Protocol for endpoint picking.
//
// UnimplementedExternalProcessorServer is embedded to provide forward compatibility
// with the gRPC service interface. This is a standard Go gRPC pattern that ensures
// the server will compile even if new methods are added to the ExternalProcessor
// service definition in future Envoy versions. Any unimplemented methods will return
// an "unimplemented" gRPC error, while we override the Process method with our
// VRAM-aware routing logic.
type Server struct {
	envoy_service_ext_proc_v3.UnimplementedExternalProcessorServer

	poolManager  *pool.Manager
	genaiMetrics *telemetry.GenAIMetrics
}

// NewServer creates a new EPP server.
func NewServer(poolManager *pool.Manager, genaiMetrics *telemetry.GenAIMetrics) *Server {
	return &Server{
		poolManager:  poolManager,
		genaiMetrics: genaiMetrics,
	}
}

// RegisterServer registers the EPP server with a gRPC server.
func (s *Server) RegisterServer(grpcServer *grpc.Server) {
	envoy_service_ext_proc_v3.RegisterExternalProcessorServer(grpcServer, s)
}

const (
	// metadataNamespace is the Envoy filter metadata namespace used by AI Gateway.
	metadataNamespace = "aigateway.envoy.io"
	// metadataInferencePoolKey is the metadata key storing the inference pool identity.
	// Format: "namespace/name/serviceName/port/processingBodyMode/allowModeOverride".
	metadataInferencePoolKey = "per_route_rule_inference_pool"
)

// streamState holds per-stream state extracted from earlier processing phases.
type streamState struct {
	originalPath string
	poolKey      *pool.PoolKey
}

// Process handles the external processing stream from Envoy.
func (s *Server) Process(stream envoy_service_ext_proc_v3.ExternalProcessor_ProcessServer) error {
	ctx := stream.Context()
	state := &streamState{}

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled: %w", ctx.Err())
		default:
		}

		req, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("failed to receive request: %w", err)
		}

		// Extract pool key from metadata context if not yet set.
		if state.poolKey == nil {
			state.poolKey = extractPoolKeyFromMetadata(req)
		}

		var resp *envoy_service_ext_proc_v3.ProcessingResponse

		switch v := req.GetRequest().(type) {
		case *envoy_service_ext_proc_v3.ProcessingRequest_RequestHeaders:
			resp, err = s.processRequestHeaders(ctx, state, v.RequestHeaders)
		case *envoy_service_ext_proc_v3.ProcessingRequest_RequestBody:
			resp, err = s.processRequestBody(ctx, state, v.RequestBody)
		case *envoy_service_ext_proc_v3.ProcessingRequest_ResponseHeaders:
			// We don't need to process response headers.
			resp = &envoy_service_ext_proc_v3.ProcessingResponse{}
		default:
			klog.V(logVerbosity).Infof("Unknown request type: %T", v)

			resp = &envoy_service_ext_proc_v3.ProcessingResponse{}
		}

		if err != nil {
			klog.Errorf("Error processing request: %v", err)

			return err
		}

		sendErr := stream.Send(resp)
		if sendErr != nil {
			return fmt.Errorf("failed to send response: %w", sendErr)
		}
	}
}

// processRequestHeaders processes request headers to extract model information.
func (s *Server) processRequestHeaders(
	ctx context.Context,
	state *streamState,
	headers *envoy_service_ext_proc_v3.HttpHeaders,
) (*envoy_service_ext_proc_v3.ProcessingResponse, error) {
	modelName := ""

	for _, header := range headers.GetHeaders().GetHeaders() {
		switch header.GetKey() {
		case "x-ai-eg-model":
			modelName = string(header.GetRawValue())
		case ":path":
			state.originalPath = string(header.GetRawValue())
		default:
			// Ignore other headers.
		}
	}

	klog.V(logVerbosity).Infof("Processing request for model: %s, path: %s", modelName, state.originalPath)

	// If model is specified in headers, route directly.
	if modelName != "" {
		return s.createRoutingResponse(ctx, state, modelName)
	}

	// Otherwise, continue to request body processing.
	return &envoy_service_ext_proc_v3.ProcessingResponse{
		Response: &envoy_service_ext_proc_v3.ProcessingResponse_RequestHeaders{
			RequestHeaders: &envoy_service_ext_proc_v3.HeadersResponse{
				Response: &envoy_service_ext_proc_v3.CommonResponse{
					HeaderMutation: &envoy_service_ext_proc_v3.HeaderMutation{},
				},
			},
		},
	}, nil
}

// processRequestBody processes request body to extract model and select endpoint.
func (s *Server) processRequestBody(ctx context.Context, state *streamState, body *envoy_service_ext_proc_v3.HttpBody) (*envoy_service_ext_proc_v3.ProcessingResponse, error) {
	// Parse body to extract model name.
	modelName := s.extractModelFromBody(body.GetBody())

	if modelName == "" {
		modelName = "default"
	}

	return s.createRoutingResponse(ctx, state, modelName)
}

// createRoutingResponse selects an endpoint for the model and creates the response.
func (s *Server) createRoutingResponse(ctx context.Context, state *streamState, modelName string) (*envoy_service_ext_proc_v3.ProcessingResponse, error) {
	klog.Infof("Selecting endpoint for model: %s, pool: %v", modelName, state.poolKey)

	// Select router: use pool from metadata if available, otherwise default.
	var endpointRouter *router.Router

	var err error

	if state.poolKey != nil {
		endpointRouter, err = s.poolManager.GetRouter(*state.poolKey)
	} else {
		endpointRouter, err = s.poolManager.GetDefaultRouter()
	}

	if err != nil {
		klog.Errorf("No router available: %v", err)

		s.genaiMetrics.RecordRoutingDecision(ctx, modelName, "", "", "error: no pool available")

		return s.createErrorResponse("No inference pool available"), nil
	}

	// Get endpoint for model (may trigger model loading).
	endpoint, err := endpointRouter.GetModelEndpoint(ctx, modelName)
	if err != nil {
		klog.Errorf("Failed to get endpoint for model %s: %v", modelName, err)

		// Record failed routing decision.
		s.genaiMetrics.RecordRoutingDecision(ctx, modelName, "", "", "error: "+err.Error())

		return s.createErrorResponse(fmt.Sprintf("Failed to route request: %v", err)), nil
	}

	klog.Infof("Selected endpoint: %s", endpoint)

	// Record successful routing decision.
	// The endpoint string contains pod:port, we extract pod name for metrics.
	s.genaiMetrics.RecordRoutingDecision(ctx, modelName, endpoint, "", "vram_available")

	// Return response with endpoint information in header.
	// Note: We use RequestHeaders response type even when processing body
	// because we're adding a header for routing purposes.
	setHeaders := []*envoy_config_core_v3.HeaderValueOption{
		{
			Header: &envoy_config_core_v3.HeaderValue{
				Key:      "x-gateway-destination-endpoint",
				RawValue: []byte(endpoint),
			},
		},
		{
			Header: &envoy_config_core_v3.HeaderValue{
				Key:      "x-ai-eg-model",
				RawValue: []byte(modelName),
			},
		},
	}

	if state.originalPath != "" {
		setHeaders = append(setHeaders,
			&envoy_config_core_v3.HeaderValueOption{
				Header: &envoy_config_core_v3.HeaderValue{
					Key:      "x-ai-eg-original-path",
					RawValue: []byte(state.originalPath),
				},
			},
			&envoy_config_core_v3.HeaderValueOption{
				Header: &envoy_config_core_v3.HeaderValue{
					Key:      "x-envoy-original-path",
					RawValue: []byte(state.originalPath),
				},
			},
		)
	}

	return &envoy_service_ext_proc_v3.ProcessingResponse{
		Response: &envoy_service_ext_proc_v3.ProcessingResponse_RequestHeaders{
			RequestHeaders: &envoy_service_ext_proc_v3.HeadersResponse{
				Response: &envoy_service_ext_proc_v3.CommonResponse{
					HeaderMutation: &envoy_service_ext_proc_v3.HeaderMutation{
						SetHeaders: setHeaders,
					},
				},
			},
		},
	}, nil
}

// extractModelFromBody extracts model name from request body.
func (s *Server) extractModelFromBody(body []byte) string {
	// Parse JSON to extract model field.
	var data map[string]any

	err := json.Unmarshal(body, &data)
	if err != nil {
		klog.V(logVerbosity).Infof("Failed to parse request body as JSON: %v", err)

		return ""
	}

	// Extract model field.
	if model, ok := data["model"].(string); ok {
		return model
	}

	return ""
}

// extractPoolKeyFromMetadata extracts the InferencePool identity from the
// Envoy filter metadata attached to the ProcessingRequest. The AI Gateway
// extension server stores pool info under the "aigateway.envoy.io" namespace
// with key "per_route_rule_inference_pool" in the format:
// "namespace/name/serviceName/port/processingBodyMode/allowModeOverride".
func extractPoolKeyFromMetadata(
	req *envoy_service_ext_proc_v3.ProcessingRequest,
) *pool.PoolKey {
	md := req.GetMetadataContext()
	if md == nil {
		return nil
	}

	ns := md.GetFilterMetadata()[metadataNamespace]
	if ns == nil {
		return nil
	}

	poolVal := ns.GetFields()[metadataInferencePoolKey]
	if poolVal == nil {
		return nil
	}

	raw := poolVal.GetStringValue()
	if raw == "" {
		return nil
	}

	// Format: "namespace/name/..." — we only need the first two segments.
	const minPoolKeySegments = 2

	parts := strings.SplitN(raw, "/", minPoolKeySegments+1)
	if len(parts) < minPoolKeySegments {
		klog.V(logVerbosity).Infof(
			"Ignoring malformed inference pool metadata: %s", raw,
		)

		return nil
	}

	return &pool.PoolKey{Namespace: parts[0], Name: parts[1]}
}

// createErrorResponse creates an error response.
func (s *Server) createErrorResponse(message string) *envoy_service_ext_proc_v3.ProcessingResponse {
	return &envoy_service_ext_proc_v3.ProcessingResponse{
		Response: &envoy_service_ext_proc_v3.ProcessingResponse_ImmediateResponse{
			ImmediateResponse: &envoy_service_ext_proc_v3.ImmediateResponse{
				Status: &envoy_type_v3.HttpStatus{
					Code: envoy_type_v3.StatusCode_ServiceUnavailable,
				},
				Body: []byte(message),
			},
		},
	}
}
