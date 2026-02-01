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

package vram

import (
	"testing"
	"time"

	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

func TestNewOTelInstrumentation(t *testing.T) {
	t.Parallel()

	meter := metricnoop.NewMeterProvider().Meter("test")

	inst, err := NewOTelInstrumentation(meter)
	if err != nil {
		t.Fatalf("NewOTelInstrumentation failed: %v", err)
	}

	if inst == nil {
		t.Fatal("NewOTelInstrumentation returned nil")
	}

	if inst.totalGauge == nil {
		t.Error("totalGauge should be initialized")
	}

	if inst.usedGauge == nil {
		t.Error("usedGauge should be initialized")
	}

	if inst.availableGauge == nil {
		t.Error("availableGauge should be initialized")
	}

	// Test cleanup.
	closeErr := inst.Close()
	if closeErr != nil {
		t.Errorf("Close failed: %v", closeErr)
	}
}

func TestOTelInstrumentationRecordMetrics(t *testing.T) {
	t.Parallel()

	meter := metricnoop.NewMeterProvider().Meter("test")

	inst, err := NewOTelInstrumentation(meter)
	if err != nil {
		t.Fatalf("NewOTelInstrumentation failed: %v", err)
	}

	defer func() { _ = inst.Close() }()

	metrics := map[string]*Metrics{
		"node1": {
			TotalVRAM:     32 * BytesPerGB,
			UsedVRAM:      8 * BytesPerGB,
			AvailableVRAM: 24 * BytesPerGB,
			LastUpdate:    time.Now(),
		},
		"node2": {
			TotalVRAM:     64 * BytesPerGB,
			UsedVRAM:      32 * BytesPerGB,
			AvailableVRAM: 32 * BytesPerGB,
			LastUpdate:    time.Now(),
		},
	}

	// Should not panic.
	inst.RecordMetrics(metrics)

	// Verify internal state.
	inst.mu.RLock()
	defer inst.mu.RUnlock()

	if len(inst.currentMetrics) != 2 {
		t.Errorf("expected 2 metrics, got %d", len(inst.currentMetrics))
	}

	if inst.currentMetrics["node1"] == nil {
		t.Error("node1 metrics should be recorded")
	}

	if inst.currentMetrics["node2"] == nil {
		t.Error("node2 metrics should be recorded")
	}
}

func TestOTelInstrumentationRecordMetricsEmpty(t *testing.T) {
	t.Parallel()

	meter := metricnoop.NewMeterProvider().Meter("test")

	inst, err := NewOTelInstrumentation(meter)
	if err != nil {
		t.Fatalf("NewOTelInstrumentation failed: %v", err)
	}

	defer func() { _ = inst.Close() }()

	// Record some metrics.
	inst.RecordMetrics(map[string]*Metrics{
		"node1": {TotalVRAM: 32 * BytesPerGB},
	})

	// Record empty metrics - should clear.
	inst.RecordMetrics(map[string]*Metrics{})

	inst.mu.RLock()
	defer inst.mu.RUnlock()

	if len(inst.currentMetrics) != 0 {
		t.Errorf("expected 0 metrics after empty record, got %d", len(inst.currentMetrics))
	}
}

func TestMustNewOTelInstrumentation(t *testing.T) {
	t.Parallel()

	meter := metricnoop.NewMeterProvider().Meter("test")

	inst := MustNewOTelInstrumentation(meter)

	if inst == nil {
		t.Fatal("MustNewOTelInstrumentation returned nil")
	}

	_ = inst.Close()
}

func TestNoopInstrumentation(t *testing.T) {
	t.Parallel()

	noop := &NoopInstrumentation{}

	// Should not panic.
	noop.RecordMetrics(map[string]*Metrics{
		"node1": {TotalVRAM: 32 * BytesPerGB},
	})

	noop.RecordMetrics(nil)
}

func TestOTelInstrumentationCloseNilRegistration(t *testing.T) {
	t.Parallel()

	inst := &OTelInstrumentation{
		registration: nil,
	}

	// Should not error with nil registration.
	err := inst.Close()
	if err != nil {
		t.Errorf("Close with nil registration should not error: %v", err)
	}
}

func TestOTelInstrumentationDeepCopy(t *testing.T) {
	t.Parallel()

	meter := metricnoop.NewMeterProvider().Meter("test")

	inst, err := NewOTelInstrumentation(meter)
	if err != nil {
		t.Fatalf("NewOTelInstrumentation failed: %v", err)
	}

	defer func() { _ = inst.Close() }()

	original := map[string]*Metrics{
		"node1": {
			TotalVRAM:     32 * BytesPerGB,
			UsedVRAM:      8 * BytesPerGB,
			AvailableVRAM: 24 * BytesPerGB,
		},
	}

	inst.RecordMetrics(original)

	// Modify original after recording.
	original["node1"].TotalVRAM = 64 * BytesPerGB

	// Verify internal copy is unaffected.
	inst.mu.RLock()
	defer inst.mu.RUnlock()

	if inst.currentMetrics["node1"].TotalVRAM != 32*BytesPerGB {
		t.Errorf("internal metrics should be a deep copy, got TotalVRAM=%d", inst.currentMetrics["node1"].TotalVRAM)
	}
}
