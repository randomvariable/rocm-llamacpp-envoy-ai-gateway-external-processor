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

// Package telemetry provides OpenTelemetry instrumentation for the external processor.
package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/host"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"k8s.io/klog/v2"

	pkgversion "github.com/randomvariable/rocm-llamacpp-envoy-ai-gateway-external-processor/internal/version"
)

const (
	// DefaultMetricInterval is the default interval for metric collection.
	DefaultMetricInterval = 10 * time.Second
)

// Config holds the OpenTelemetry configuration.
type Config struct {
	// OTLPEndpoint is the OTLP endpoint for traces and metrics (e.g., "localhost:4317").
	OTLPEndpoint string
	// OTLPTracesEnabled enables OTLP exporter for traces.
	OTLPTracesEnabled bool
	// OTLPMetricsEnabled enables OTLP exporter for metrics.
	OTLPMetricsEnabled bool
	// PrometheusEnabled enables Prometheus exporter for metrics (default).
	PrometheusEnabled bool
	// RuntimeInstrumentationEnabled enables runtime instrumentation (goroutines, memory, GC).
	RuntimeInstrumentationEnabled bool
	// HostInstrumentationEnabled enables host instrumentation (CPU, memory, network).
	HostInstrumentationEnabled bool
	// MetricInterval is the metric collection interval.
	MetricInterval time.Duration
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		OTLPEndpoint:                  "",
		OTLPTracesEnabled:             false,
		OTLPMetricsEnabled:            false,
		PrometheusEnabled:             true,
		RuntimeInstrumentationEnabled: true,
		HostInstrumentationEnabled:    true,
		MetricInterval:                DefaultMetricInterval,
	}
}

// Provider holds the initialized OpenTelemetry providers.
type Provider struct {
	TracerProvider *sdktrace.TracerProvider
	MeterProvider  *sdkmetric.MeterProvider
	shutdownFuncs  []func(context.Context) error
}

// Shutdown gracefully shuts down all providers.
func (p *Provider) Shutdown(ctx context.Context) error {
	var lastErr error

	for _, fn := range p.shutdownFuncs {
		err := fn(ctx)
		if err != nil {
			klog.Errorf("Error during telemetry shutdown: %v", err)
			lastErr = err
		}
	}

	return lastErr
}

// Initialize sets up OpenTelemetry with the given configuration.
func Initialize(ctx context.Context, cfg *Config) (*Provider, error) {
	provider := &Provider{}

	// Create resource with service information.
	// Use resource.Default() and add our attributes to avoid schema URL conflicts.
	res, err := resource.New(ctx,
		resource.WithFromEnv(),      // Discover and provide attributes from OTEL_RESOURCE_ATTRIBUTES and OTEL_SERVICE_NAME environment variables
		resource.WithTelemetrySDK(), // Discover and provide information about the OpenTelemetry SDK
		resource.WithProcess(),      // Discover and provide process information
		resource.WithOS(),           // Discover and provide OS information
		resource.WithContainer(),    // Discover and provide container information
		resource.WithHost(),         // Discover and provide host information
		resource.WithAttributes(
			semconv.ServiceName(pkgversion.ServiceName),
			semconv.ServiceVersion(pkgversion.GetVersion()),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Setup trace provider if OTLP traces are enabled.
	if cfg.OTLPTracesEnabled && cfg.OTLPEndpoint != "" {
		traceProvider, traceErr := setupTraceProvider(ctx, cfg, res)
		if traceErr != nil {
			return nil, fmt.Errorf("failed to setup trace provider: %w", traceErr)
		}

		provider.TracerProvider = traceProvider
		provider.shutdownFuncs = append(provider.shutdownFuncs, traceProvider.Shutdown)
		otel.SetTracerProvider(traceProvider)
		klog.Infof("OTLP trace exporter enabled, endpoint: %s", cfg.OTLPEndpoint)
	}

	// Setup text map propagator for distributed tracing.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// Setup meter provider with appropriate exporters.
	meterProvider, err := setupMeterProvider(ctx, cfg, res)
	if err != nil {
		return nil, fmt.Errorf("failed to setup meter provider: %w", err)
	}

	provider.MeterProvider = meterProvider
	provider.shutdownFuncs = append(provider.shutdownFuncs, meterProvider.Shutdown)
	otel.SetMeterProvider(meterProvider)

	// Start runtime instrumentation if enabled.
	if cfg.RuntimeInstrumentationEnabled {
		err := runtime.Start(
			runtime.WithMinimumReadMemStatsInterval(cfg.MetricInterval),
		)
		if err != nil {
			klog.Warningf("Failed to start runtime instrumentation: %v", err)
		} else {
			klog.Info("Runtime instrumentation enabled")
		}
	}

	// Start host instrumentation if enabled.
	if cfg.HostInstrumentationEnabled {
		err := host.Start(
			host.WithMeterProvider(meterProvider),
		)
		if err != nil {
			klog.Warningf("Failed to start host instrumentation: %v", err)
		} else {
			klog.Info("Host instrumentation enabled")
		}
	}

	return provider, nil
}

// setupTraceProvider creates and configures the trace provider.
func setupTraceProvider(ctx context.Context, cfg *Config, res *resource.Resource) (*sdktrace.TracerProvider, error) {
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
		otlptracegrpc.WithInsecure(), // Use WithTLSCredentials for production.
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	traceProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	return traceProvider, nil
}

// setupMeterProvider creates and configures the meter provider.
func setupMeterProvider(ctx context.Context, cfg *Config, res *resource.Resource) (*sdkmetric.MeterProvider, error) {
	var opts []sdkmetric.Option

	opts = append(opts, sdkmetric.WithResource(res))

	// Add Prometheus exporter if enabled.
	if cfg.PrometheusEnabled {
		promExporter, err := prometheus.New()
		if err != nil {
			return nil, fmt.Errorf("failed to create prometheus exporter: %w", err)
		}

		opts = append(opts, sdkmetric.WithReader(promExporter))

		klog.Info("Prometheus metrics exporter enabled")
	}

	// Add OTLP metrics exporter if enabled.
	if cfg.OTLPMetricsEnabled && cfg.OTLPEndpoint != "" {
		otlpExporter, err := otlpmetricgrpc.New(ctx,
			otlpmetricgrpc.WithEndpoint(cfg.OTLPEndpoint),
			otlpmetricgrpc.WithInsecure(), // Use WithTLSCredentials for production.
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create OTLP metrics exporter: %w", err)
		}

		opts = append(opts, sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(otlpExporter,
				sdkmetric.WithInterval(cfg.MetricInterval),
			),
		))
		klog.Infof("OTLP metrics exporter enabled, endpoint: %s", cfg.OTLPEndpoint)
	}

	return sdkmetric.NewMeterProvider(opts...), nil
}
