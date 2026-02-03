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
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()

	if cfg == nil {
		t.Fatal("DefaultConfig returned nil")
	}

	if cfg.OTLPEndpoint != "" {
		t.Errorf("OTLPEndpoint = %q, want empty string", cfg.OTLPEndpoint)
	}

	if cfg.OTLPTracesEnabled {
		t.Error("OTLPTracesEnabled should be false by default")
	}

	if cfg.OTLPMetricsEnabled {
		t.Error("OTLPMetricsEnabled should be false by default")
	}

	if !cfg.PrometheusEnabled {
		t.Error("PrometheusEnabled should be true by default")
	}

	if !cfg.RuntimeInstrumentationEnabled {
		t.Error("RuntimeInstrumentationEnabled should be true by default")
	}

	if !cfg.HostInstrumentationEnabled {
		t.Error("HostInstrumentationEnabled should be true by default")
	}

	if cfg.MetricInterval != DefaultMetricInterval {
		t.Errorf("MetricInterval = %v, want %v", cfg.MetricInterval, DefaultMetricInterval)
	}
}

func TestConfigValues(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		OTLPEndpoint:                  "localhost:4317",
		OTLPTracesEnabled:             true,
		OTLPMetricsEnabled:            true,
		PrometheusEnabled:             false,
		RuntimeInstrumentationEnabled: false,
		HostInstrumentationEnabled:    false,
		MetricInterval:                5 * time.Second,
	}

	if cfg.OTLPEndpoint != "localhost:4317" {
		t.Errorf("OTLPEndpoint = %q, want %q", cfg.OTLPEndpoint, "localhost:4317")
	}

	if !cfg.OTLPTracesEnabled {
		t.Error("OTLPTracesEnabled should be true")
	}

	if !cfg.OTLPMetricsEnabled {
		t.Error("OTLPMetricsEnabled should be true")
	}

	if cfg.PrometheusEnabled {
		t.Error("PrometheusEnabled should be false")
	}

	if cfg.RuntimeInstrumentationEnabled {
		t.Error("RuntimeInstrumentationEnabled should be false")
	}

	if cfg.HostInstrumentationEnabled {
		t.Error("HostInstrumentationEnabled should be false")
	}

	if cfg.MetricInterval != 5*time.Second {
		t.Errorf("MetricInterval = %v, want %v", cfg.MetricInterval, 5*time.Second)
	}
}

func TestDefaultMetricInterval(t *testing.T) {
	t.Parallel()

	if DefaultMetricInterval != 10*time.Second {
		t.Errorf("DefaultMetricInterval = %v, want %v", DefaultMetricInterval, 10*time.Second)
	}
}

func TestProviderShutdownEmpty(t *testing.T) {
	t.Parallel()

	provider := &Provider{}

	// Should not panic with empty shutdown funcs
	err := provider.Shutdown(t.Context())
	if err != nil {
		t.Errorf("Shutdown should not error with empty funcs: %v", err)
	}
}

// TestInitializeNoSchemaURLConflict ensures that telemetry initialization
// does not fail with conflicting schema URL errors. This test addresses the issue:
// "failed to merge resource: conflicting Schema URL: https://opentelemetry.io/schemas/1.39.0
// and https://opentelemetry.io/schemas/1.26.0"
func TestInitializeNoSchemaURLConflict(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	cfg := &Config{
		OTLPEndpoint:                  "",
		OTLPTracesEnabled:             false,
		OTLPMetricsEnabled:            false,
		PrometheusEnabled:             true,
		RuntimeInstrumentationEnabled: false,
		HostInstrumentationEnabled:    false,
		MetricInterval:                DefaultMetricInterval,
	}

	provider, err := Initialize(ctx, cfg)
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err := provider.Shutdown(shutdownCtx)
		if err != nil {
			t.Errorf("provider.Shutdown failed: %v", err)
		}
	})

	if provider == nil {
		t.Fatal("Initialize returned nil provider")
	}

	if provider.MeterProvider == nil {
		t.Fatal("MeterProvider should not be nil")
	}
}
