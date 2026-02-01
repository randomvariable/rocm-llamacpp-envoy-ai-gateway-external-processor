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
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"k8s.io/klog/v2"
)

// OTelInstrumentation provides OpenTelemetry metrics for VRAM tracking.
type OTelInstrumentation struct {
	meter metric.Meter

	totalGauge     metric.Int64ObservableGauge
	usedGauge      metric.Int64ObservableGauge
	availableGauge metric.Int64ObservableGauge

	// currentMetrics holds the latest metrics for the callback.
	currentMetrics map[string]*Metrics
	mu             sync.RWMutex

	// registration holds the callback registration for cleanup.
	registration metric.Registration
}

// Verify interface compliance.
var _ Instrumentation = (*OTelInstrumentation)(nil)

// NewOTelInstrumentation creates OpenTelemetry instrumentation with the given meter.
func NewOTelInstrumentation(meter metric.Meter) (*OTelInstrumentation, error) {
	inst := &OTelInstrumentation{
		meter:          meter,
		currentMetrics: make(map[string]*Metrics),
	}

	var err error

	inst.totalGauge, err = meter.Int64ObservableGauge(
		"vram_total_bytes",
		metric.WithDescription("Total VRAM in bytes on node"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create total gauge: %w", err)
	}

	inst.usedGauge, err = meter.Int64ObservableGauge(
		"vram_used_bytes",
		metric.WithDescription("Used VRAM in bytes on node"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create used gauge: %w", err)
	}

	inst.availableGauge, err = meter.Int64ObservableGauge(
		"vram_available_bytes",
		metric.WithDescription("Available VRAM in bytes on node"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create available gauge: %w", err)
	}

	// Register the callback for all gauges.
	inst.registration, err = meter.RegisterCallback(
		inst.observeCallback,
		inst.totalGauge, inst.usedGauge, inst.availableGauge,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register callback: %w", err)
	}

	return inst, nil
}

// MustNewOTelInstrumentation creates OpenTelemetry instrumentation, logging errors but not failing.
// This is useful when instrumentation is optional.
func MustNewOTelInstrumentation(meter metric.Meter) *OTelInstrumentation {
	inst, err := NewOTelInstrumentation(meter)
	if err != nil {
		klog.Errorf("Failed to create OTel instrumentation: %v", err)

		return nil
	}

	return inst
}

// RecordMetrics updates the metrics state for the callback.
func (o *OTelInstrumentation) RecordMetrics(metrics map[string]*Metrics) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Create a deep copy to avoid race conditions.
	o.currentMetrics = make(map[string]*Metrics, len(metrics))
	for k, v := range metrics {
		o.currentMetrics[k] = &Metrics{
			TotalVRAM:     v.TotalVRAM,
			UsedVRAM:      v.UsedVRAM,
			AvailableVRAM: v.AvailableVRAM,
			LastUpdate:    v.LastUpdate,
		}
	}
}

// Close unregisters the callback. Should be called when the instrumentation is no longer needed.
func (o *OTelInstrumentation) Close() error {
	if o.registration != nil {
		err := o.registration.Unregister()
		if err != nil {
			return fmt.Errorf("failed to unregister callback: %w", err)
		}
	}

	return nil
}

// observeCallback is called by OTel to collect gauge values.
func (o *OTelInstrumentation) observeCallback(_ context.Context, observer metric.Observer) error {
	o.mu.RLock()
	defer o.mu.RUnlock()

	for nodeName, nodeMetrics := range o.currentMetrics {
		nodeAttr := attribute.String("node", nodeName)
		observer.ObserveInt64(o.totalGauge, nodeMetrics.TotalVRAM, metric.WithAttributes(nodeAttr))
		observer.ObserveInt64(o.usedGauge, nodeMetrics.UsedVRAM, metric.WithAttributes(nodeAttr))
		observer.ObserveInt64(o.availableGauge, nodeMetrics.AvailableVRAM, metric.WithAttributes(nodeAttr))
	}

	return nil
}

// NoopInstrumentation is an instrumentation that does nothing.
// Useful when metrics are not needed.
type NoopInstrumentation struct{}

// Verify interface compliance.
var _ Instrumentation = (*NoopInstrumentation)(nil)

// RecordMetrics does nothing.
func (n *NoopInstrumentation) RecordMetrics(_ map[string]*Metrics) {}
