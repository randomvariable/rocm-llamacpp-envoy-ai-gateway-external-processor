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
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

func TestNewPrometheusScraper(t *testing.T) {
	t.Parallel()

	scraper := NewPrometheusScraper()

	if scraper == nil {
		t.Fatal("NewPrometheusScraper returned nil")
	}

	if scraper.totalMetric != DefaultTotalMetricName {
		t.Errorf("totalMetric = %q, want %q", scraper.totalMetric, DefaultTotalMetricName)
	}

	if scraper.usedMetric != DefaultUsedMetricName {
		t.Errorf("usedMetric = %q, want %q", scraper.usedMetric, DefaultUsedMetricName)
	}

	if scraper.metricsPath != "/metrics" {
		t.Errorf("metricsPath = %q, want %q", scraper.metricsPath, "/metrics")
	}
}

func TestNewPrometheusScraperWithConfig(t *testing.T) {
	t.Parallel()

	config := PrometheusScraperConfig{
		TotalMetricName: "custom_total",
		UsedMetricName:  "custom_used",
		MetricsPath:     "/custom/metrics",
		Timeout:         10 * time.Second,
	}

	scraper := NewPrometheusScraperWithConfig(config)

	if scraper.totalMetric != "custom_total" {
		t.Errorf("totalMetric = %q, want %q", scraper.totalMetric, "custom_total")
	}

	if scraper.usedMetric != "custom_used" {
		t.Errorf("usedMetric = %q, want %q", scraper.usedMetric, "custom_used")
	}

	if scraper.metricsPath != "/custom/metrics" {
		t.Errorf("metricsPath = %q, want %q", scraper.metricsPath, "/custom/metrics")
	}
}

func TestPrometheusScraperScrape(t *testing.T) {
	t.Parallel()

	metricsResponse := `
# HELP rocm_memory_total_bytes Total VRAM in bytes
# TYPE rocm_memory_total_bytes gauge
rocm_memory_total_bytes 34359738368
# HELP rocm_memory_used_bytes Used VRAM in bytes
# TYPE rocm_memory_used_bytes gauge
rocm_memory_used_bytes 8589934592
`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte(metricsResponse))
	}))
	defer server.Close()

	serverURL := strings.TrimPrefix(server.URL, "http://")
	parts := strings.Split(serverURL, ":")
	host := parts[0]

	port, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		t.Fatalf("Failed to parse port: %v", err)
	}

	scraper := NewPrometheusScraper()
	endpoint := MetricsEndpoint{
		NodeName: "test-node",
		Address:  host,
		Port:     int32(port),
	}

	metrics, err := scraper.Scrape(context.Background(), endpoint)
	if err != nil {
		t.Fatalf("Scrape failed: %v", err)
	}

	if metrics.TotalVRAM != 34359738368 {
		t.Errorf("TotalVRAM = %d, want 34359738368", metrics.TotalVRAM)
	}

	if metrics.UsedVRAM != 8589934592 {
		t.Errorf("UsedVRAM = %d, want 8589934592", metrics.UsedVRAM)
	}

	expectedAvailable := int64(34359738368 - 8589934592)
	if metrics.AvailableVRAM != expectedAvailable {
		t.Errorf("AvailableVRAM = %d, want %d", metrics.AvailableVRAM, expectedAvailable)
	}
}

func TestPrometheusScraperScrapeError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	serverURL := strings.TrimPrefix(server.URL, "http://")
	parts := strings.Split(serverURL, ":")
	host := parts[0]

	port, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		t.Fatalf("Failed to parse port: %v", err)
	}

	scraper := NewPrometheusScraper()
	endpoint := MetricsEndpoint{
		NodeName: "test-node",
		Address:  host,
		Port:     int32(port),
	}

	_, err = scraper.Scrape(context.Background(), endpoint)
	if err == nil {
		t.Error("Scrape should return error for non-200 status")
	}

	if !errors.Is(err, ErrUnexpectedStatus) {
		t.Errorf("expected ErrUnexpectedStatus, got %v", err)
	}
}

func TestPrometheusScraperScrapeMissingMetrics(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("# HELP other_metric Some other metric\n# TYPE other_metric gauge\nother_metric 42\n"))
	}))
	defer server.Close()

	serverURL := strings.TrimPrefix(server.URL, "http://")
	parts := strings.Split(serverURL, ":")
	host := parts[0]

	port, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		t.Fatalf("Failed to parse port: %v", err)
	}

	scraper := NewPrometheusScraper()
	endpoint := MetricsEndpoint{
		NodeName: "test-node",
		Address:  host,
		Port:     int32(port),
	}

	metrics, err := scraper.Scrape(context.Background(), endpoint)
	if err != nil {
		t.Fatalf("Scrape failed: %v", err)
	}

	// Should return defaults.
	if metrics.TotalVRAM != DefaultTotalVRAMGB*BytesPerGB {
		t.Errorf("TotalVRAM = %d, want %d (default)", metrics.TotalVRAM, DefaultTotalVRAMGB*BytesPerGB)
	}

	if metrics.UsedVRAM != 0 {
		t.Errorf("UsedVRAM = %d, want 0", metrics.UsedVRAM)
	}
}

func TestPrometheusScraperScrapeInvalidURL(t *testing.T) {
	t.Parallel()

	scraper := NewPrometheusScraper()
	endpoint := MetricsEndpoint{
		NodeName: "test-node",
		Address:  "invalid-address-that-does-not-exist",
		Port:     9100,
	}

	_, err := scraper.Scrape(context.Background(), endpoint)
	if err == nil {
		t.Error("Scrape should fail with invalid address")
	}
}

func TestPrometheusScraperWithCustomHTTPClient(t *testing.T) {
	t.Parallel()

	customClient := &http.Client{
		Timeout: 1 * time.Second,
	}

	config := PrometheusScraperConfig{
		HTTPClient: customClient,
	}

	scraper := NewPrometheusScraperWithConfig(config)

	if scraper.client != customClient {
		t.Error("scraper should use custom HTTP client")
	}
}

func TestGetMetricValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		metric   *dto.Metric
		expected float64
	}{
		{
			name: "gauge metric",
			metric: func() *dto.Metric {
				val := 42.5

				return &dto.Metric{Gauge: &dto.Gauge{Value: &val}}
			}(),
			expected: 42.5,
		},
		{
			name: "counter metric",
			metric: func() *dto.Metric {
				val := 100.0

				return &dto.Metric{Counter: &dto.Counter{Value: &val}}
			}(),
			expected: 100.0,
		},
		{
			name: "untyped metric",
			metric: func() *dto.Metric {
				val := 75.3

				return &dto.Metric{Untyped: &dto.Untyped{Value: &val}}
			}(),
			expected: 75.3,
		},
		{
			name:     "nil metric fields",
			metric:   &dto.Metric{},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := getMetricValue(tt.metric)
			if result != tt.expected {
				t.Errorf("getMetricValue() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestStaticScraperScrape(t *testing.T) {
	t.Parallel()

	metrics := map[string]*Metrics{
		"node1": {TotalVRAM: 32 * BytesPerGB, UsedVRAM: 8 * BytesPerGB, AvailableVRAM: 24 * BytesPerGB},
	}

	scraper := NewStaticScraper(metrics)

	result, err := scraper.Scrape(context.Background(), MetricsEndpoint{NodeName: "node1"})
	if err != nil {
		t.Fatalf("Scrape failed: %v", err)
	}

	if result.TotalVRAM != 32*BytesPerGB {
		t.Errorf("expected TotalVRAM %d, got %d", 32*BytesPerGB, result.TotalVRAM)
	}
}

func TestStaticScraperScrapeNotFound(t *testing.T) {
	t.Parallel()

	scraper := NewStaticScraper(map[string]*Metrics{})

	_, err := scraper.Scrape(context.Background(), MetricsEndpoint{NodeName: "nonexistent"})
	if err == nil {
		t.Error("expected error for non-existent node")
	}
}

func TestStaticScraperSetMetrics(t *testing.T) {
	t.Parallel()

	scraper := NewStaticScraper(map[string]*Metrics{})

	scraper.SetMetrics("node1", &Metrics{TotalVRAM: 64 * BytesPerGB})

	result, err := scraper.Scrape(context.Background(), MetricsEndpoint{NodeName: "node1"})
	if err != nil {
		t.Fatalf("Scrape failed: %v", err)
	}

	if result.TotalVRAM != 64*BytesPerGB {
		t.Errorf("expected TotalVRAM %d, got %d", 64*BytesPerGB, result.TotalVRAM)
	}
}
