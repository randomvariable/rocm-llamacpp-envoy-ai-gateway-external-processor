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
	"errors"
	"time"
)

// Configuration constants.
const (
	// DefaultScrapeInterval is the default interval for scraping VRAM metrics.
	DefaultScrapeInterval = 30 * time.Second

	// DefaultMetricsQueryTimeout is the default timeout for metrics queries.
	DefaultMetricsQueryTimeout = 5 * time.Second

	// BytesPerGB is the number of bytes in a gigabyte.
	BytesPerGB = 1024 * 1024 * 1024

	// DefaultTotalVRAMGB is the default total VRAM assumed when metrics are unavailable.
	DefaultTotalVRAMGB = 32
)

// Error definitions for the vram package.
var (
	// ErrClientNil is returned when a required client is nil.
	ErrClientNil = errors.New("client is nil")

	// ErrNoNodes is returned when no nodes with VRAM metrics are available.
	ErrNoNodes = errors.New("no nodes with VRAM metrics available")

	// ErrNoSuitableNode is returned when no suitable node can be found.
	ErrNoSuitableNode = errors.New("no suitable node found")

	// ErrNoMetrics is returned when metrics are not available for a node.
	ErrNoMetrics = errors.New("no metrics available for node")

	// ErrUnexpectedStatus is returned when an unexpected HTTP status is received.
	ErrUnexpectedStatus = errors.New("unexpected status code")

	// ErrNoRunningExporterPod is returned when no running exporter pod is found.
	ErrNoRunningExporterPod = errors.New("no running exporter pod found on node")

	// ErrNoTCPPortInExporterPod is returned when no TCP port is found in exporter pod.
	ErrNoTCPPortInExporterPod = errors.New("no TCP port found in exporter pod")

	// ErrEndpointNotFound is returned when a metrics endpoint cannot be found.
	ErrEndpointNotFound = errors.New("metrics endpoint not found")
)

// Metrics represents VRAM usage metrics for a node.
type Metrics struct {
	// TotalVRAM is the total VRAM capacity in bytes.
	TotalVRAM int64

	// UsedVRAM is the currently used VRAM in bytes.
	UsedVRAM int64

	// AvailableVRAM is the available VRAM in bytes (TotalVRAM - UsedVRAM).
	AvailableVRAM int64

	// LastUpdate is the timestamp of the last metrics update.
	LastUpdate time.Time
}

// MetricsEndpoint represents a network endpoint where VRAM metrics can be scraped.
type MetricsEndpoint struct {
	// NodeName is the name of the node this endpoint belongs to.
	NodeName string

	// Address is the IP address or hostname.
	Address string

	// Port is the TCP port number.
	Port int32
}

// MetricsScraper defines the interface for fetching VRAM metrics from an endpoint.
// Implementations can support different metric formats (Prometheus, JSON, etc.).
type MetricsScraper interface {
	// Scrape fetches VRAM metrics from the given endpoint.
	// Returns the metrics or an error if scraping fails.
	Scrape(ctx context.Context, endpoint MetricsEndpoint) (*Metrics, error)
}

// NodeDiscovery defines the interface for discovering nodes and their metric endpoints.
// Implementations can support different discovery mechanisms (Kubernetes, static config, etc.).
type NodeDiscovery interface {
	// DiscoverEndpoints returns a list of metrics endpoints for nodes matching the criteria.
	// The implementation determines what criteria to use (labels, selectors, etc.).
	DiscoverEndpoints(ctx context.Context) ([]MetricsEndpoint, error)
}

// TrackerOption is a functional option for configuring a Tracker.
type TrackerOption func(*trackerOptions)

type trackerOptions struct {
	scrapeInterval  time.Duration
	instrumentation Instrumentation
}

// WithScrapeInterval sets the interval between metric scrapes.
func WithScrapeInterval(interval time.Duration) TrackerOption {
	return func(o *trackerOptions) {
		o.scrapeInterval = interval
	}
}

// WithInstrumentation sets the instrumentation for the tracker.
func WithInstrumentation(inst Instrumentation) TrackerOption {
	return func(o *trackerOptions) {
		o.instrumentation = inst
	}
}

// Instrumentation defines the interface for emitting metrics about the tracker itself.
// This allows integration with different observability backends (OpenTelemetry, Prometheus, etc.).
type Instrumentation interface {
	// RecordMetrics is called after each successful scrape with the current metrics state.
	RecordMetrics(metrics map[string]*Metrics)
}
