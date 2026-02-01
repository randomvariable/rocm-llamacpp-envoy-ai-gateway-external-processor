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
	"net"
	"net/http"
	"strconv"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// Default ROCm metric names.
const (
	DefaultTotalMetricName = "rocm_memory_total_bytes"
	DefaultUsedMetricName  = "rocm_memory_used_bytes"
)

// PrometheusScraperConfig configures the Prometheus scraper.
type PrometheusScraperConfig struct {
	// TotalMetricName is the Prometheus metric name for total VRAM.
	// Defaults to "rocm_memory_total_bytes".
	TotalMetricName string

	// UsedMetricName is the Prometheus metric name for used VRAM.
	// Defaults to "rocm_memory_used_bytes".
	UsedMetricName string

	// MetricsPath is the HTTP path to scrape metrics from.
	// Defaults to "/metrics".
	MetricsPath string

	// Timeout is the HTTP request timeout.
	// Defaults to DefaultMetricsQueryTimeout.
	Timeout time.Duration

	// HTTPClient is an optional custom HTTP client.
	// If nil, a default client with the configured timeout is used.
	HTTPClient *http.Client
}

// PrometheusScraper scrapes VRAM metrics from Prometheus-format endpoints.
type PrometheusScraper struct {
	totalMetric string
	usedMetric  string
	metricsPath string
	client      *http.Client
}

// Verify interface compliance.
var _ MetricsScraper = (*PrometheusScraper)(nil)

// NewPrometheusScraper creates a new Prometheus scraper with default configuration.
func NewPrometheusScraper() *PrometheusScraper {
	return NewPrometheusScraperWithConfig(PrometheusScraperConfig{})
}

// NewPrometheusScraperWithConfig creates a new Prometheus scraper with custom configuration.
func NewPrometheusScraperWithConfig(config PrometheusScraperConfig) *PrometheusScraper {
	totalMetric := config.TotalMetricName
	if totalMetric == "" {
		totalMetric = DefaultTotalMetricName
	}

	usedMetric := config.UsedMetricName
	if usedMetric == "" {
		usedMetric = DefaultUsedMetricName
	}

	metricsPath := config.MetricsPath
	if metricsPath == "" {
		metricsPath = "/metrics"
	}

	timeout := config.Timeout
	if timeout == 0 {
		timeout = DefaultMetricsQueryTimeout
	}

	client := config.HTTPClient
	if client == nil {
		client = &http.Client{
			Timeout: timeout,
		}
	}

	return &PrometheusScraper{
		totalMetric: totalMetric,
		usedMetric:  usedMetric,
		metricsPath: metricsPath,
		client:      client,
	}
}

// Scrape fetches VRAM metrics from a Prometheus endpoint.
func (s *PrometheusScraper) Scrape(ctx context.Context, endpoint MetricsEndpoint) (*Metrics, error) {
	url := "http://" + net.JoinHostPort(endpoint.Address, strconv.Itoa(int(endpoint.Port))) + s.metricsPath

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metrics: %w", err)
	}

	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: %d", ErrUnexpectedStatus, resp.StatusCode)
	}

	// Parse Prometheus metrics using expfmt.
	parser := expfmt.NewTextParser(model.LegacyValidation)

	metricFamilies, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse metrics: %w", err)
	}

	metrics := &Metrics{
		LastUpdate: time.Now(),
		// Default to 32GB if metrics cannot be parsed.
		TotalVRAM: DefaultTotalVRAMGB * BytesPerGB,
		UsedVRAM:  0,
	}

	// Extract VRAM metrics from parsed data.
	if mf, ok := metricFamilies[s.totalMetric]; ok {
		if len(mf.GetMetric()) > 0 {
			metrics.TotalVRAM = int64(getMetricValue(mf.GetMetric()[0]))
		}
	}

	if mf, ok := metricFamilies[s.usedMetric]; ok {
		if len(mf.GetMetric()) > 0 {
			metrics.UsedVRAM = int64(getMetricValue(mf.GetMetric()[0]))
		}
	}

	metrics.AvailableVRAM = max(metrics.TotalVRAM-metrics.UsedVRAM, 0)

	return metrics, nil
}

// getMetricValue extracts the value from a Prometheus metric.
func getMetricValue(promMetric *dto.Metric) float64 {
	if promMetric.GetGauge() != nil {
		return promMetric.GetGauge().GetValue()
	}

	if promMetric.GetCounter() != nil {
		return promMetric.GetCounter().GetValue()
	}

	if promMetric.GetUntyped() != nil {
		return promMetric.GetUntyped().GetValue()
	}

	return 0
}

// StaticScraper is a scraper that returns pre-configured metrics.
// Useful for testing or when metrics are obtained through other means.
type StaticScraper struct {
	metrics map[string]*Metrics
}

// Verify interface compliance.
var _ MetricsScraper = (*StaticScraper)(nil)

// NewStaticScraper creates a scraper with static metrics.
func NewStaticScraper(metrics map[string]*Metrics) *StaticScraper {
	return &StaticScraper{
		metrics: metrics,
	}
}

// Scrape returns the pre-configured metrics for the endpoint's node.
func (s *StaticScraper) Scrape(_ context.Context, endpoint MetricsEndpoint) (*Metrics, error) {
	if m, ok := s.metrics[endpoint.NodeName]; ok {
		return &Metrics{
			TotalVRAM:     m.TotalVRAM,
			UsedVRAM:      m.UsedVRAM,
			AvailableVRAM: m.AvailableVRAM,
			LastUpdate:    time.Now(),
		}, nil
	}

	return nil, fmt.Errorf("%w: %s", ErrNoMetrics, endpoint.NodeName)
}

// SetMetrics updates the metrics for a node.
func (s *StaticScraper) SetMetrics(nodeName string, metrics *Metrics) {
	s.metrics[nodeName] = metrics
}
