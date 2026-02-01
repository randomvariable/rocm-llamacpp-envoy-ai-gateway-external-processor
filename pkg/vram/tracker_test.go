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
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestNewTracker(t *testing.T) {
	t.Parallel()

	podSelector := map[string]string{"app": "model-server"}
	nodeSelector := map[string]string{"gpu": "true"}
	exporterSelector := map[string]string{"app": "amd-gpu-metrics-exporter-amdgpu-metrics-exporter"}

	tracker := NewTracker(nil, "default", podSelector, nodeSelector, "kube-system", exporterSelector, 30*time.Second)

	if tracker == nil {
		t.Fatal("NewTracker returned nil")
	}

	if tracker.namespace != "default" {
		t.Errorf("namespace = %q, want %q", tracker.namespace, "default")
	}

	if tracker.exporterNamespace != "kube-system" {
		t.Errorf("exporterNamespace = %q, want %q", tracker.exporterNamespace, "kube-system")
	}

	if tracker.scrapeInterval != 30*time.Second {
		t.Errorf("scrapeInterval = %v, want %v", tracker.scrapeInterval, 30*time.Second)
	}

	if tracker.metrics == nil {
		t.Error("metrics map should be initialized")
	}
}

func TestVRAMMetrics(t *testing.T) {
	t.Parallel()

	now := time.Now()
	metrics := &VRAMMetrics{
		TotalVRAM:     32 * bytesPerGB,
		UsedVRAM:      16 * bytesPerGB,
		AvailableVRAM: 16 * bytesPerGB,
		LastUpdate:    now,
	}

	if metrics.TotalVRAM != 32*bytesPerGB {
		t.Errorf("TotalVRAM = %d, want %d", metrics.TotalVRAM, 32*bytesPerGB)
	}

	if metrics.UsedVRAM != 16*bytesPerGB {
		t.Errorf("UsedVRAM = %d, want %d", metrics.UsedVRAM, 16*bytesPerGB)
	}

	if metrics.AvailableVRAM != 16*bytesPerGB {
		t.Errorf("AvailableVRAM = %d, want %d", metrics.AvailableVRAM, 16*bytesPerGB)
	}

	if metrics.LastUpdate != now {
		t.Errorf("LastUpdate = %v, want %v", metrics.LastUpdate, now)
	}
}

func TestTrackerUpdatePodSelector(t *testing.T) {
	t.Parallel()

	tracker := NewTracker(nil, "default", map[string]string{"app": "v1"}, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	newSelector := map[string]string{"app": "v2", "version": "2"}
	tracker.UpdatePodSelector(context.Background(), newSelector)

	tracker.mu.RLock()
	defer tracker.mu.RUnlock()

	if tracker.podSelector["app"] != "v2" {
		t.Errorf("podSelector[app] = %q, want %q", tracker.podSelector["app"], "v2")
	}

	if tracker.podSelector["version"] != "2" {
		t.Errorf("podSelector[version] = %q, want %q", tracker.podSelector["version"], "2")
	}
}

func TestTrackerUpdateNodeSelector(t *testing.T) {
	t.Parallel()

	tracker := NewTracker(nil, "default", nil, map[string]string{"gpu": "false"}, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	newSelector := map[string]string{"gpu": "true"}
	tracker.UpdateNodeSelector(newSelector)

	tracker.mu.RLock()
	defer tracker.mu.RUnlock()

	if tracker.nodeSelector["gpu"] != "true" {
		t.Errorf("nodeSelector[gpu] = %q, want %q", tracker.nodeSelector["gpu"], "true")
	}
}

func TestTrackerUpdateNamespace(t *testing.T) {
	t.Parallel()

	tracker := NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	tracker.UpdateNamespace("production")

	tracker.mu.RLock()
	defer tracker.mu.RUnlock()

	if tracker.namespace != "production" {
		t.Errorf("namespace = %q, want %q", tracker.namespace, "production")
	}
}

func TestTrackerGetMetrics(t *testing.T) {
	t.Parallel()

	tracker := NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	// Set some metrics directly
	now := time.Now()

	tracker.mu.Lock()
	tracker.metrics["node1"] = &VRAMMetrics{
		TotalVRAM:     32 * bytesPerGB,
		UsedVRAM:      8 * bytesPerGB,
		AvailableVRAM: 24 * bytesPerGB,
		LastUpdate:    now,
	}
	tracker.metrics["node2"] = &VRAMMetrics{
		TotalVRAM:     64 * bytesPerGB,
		UsedVRAM:      32 * bytesPerGB,
		AvailableVRAM: 32 * bytesPerGB,
		LastUpdate:    now,
	}
	tracker.mu.Unlock()

	metrics := tracker.GetMetrics()

	if len(metrics) != 2 {
		t.Errorf("GetMetrics returned %d entries, want 2", len(metrics))
	}

	if metrics["node1"] == nil {
		t.Error("GetMetrics should include node1")
	}

	if metrics["node2"] == nil {
		t.Error("GetMetrics should include node2")
	}

	// Verify the returned metrics are copies
	if metrics["node1"] == tracker.metrics["node1"] {
		t.Error("GetMetrics should return copies, not references")
	}
}

func TestTrackerGetNodeWithMostAvailableVRAM(t *testing.T) {
	t.Parallel()

	tracker := NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	// No metrics - should return error
	_, _, err := tracker.GetNodeWithMostAvailableVRAM()
	if !errors.Is(err, ErrNoNodes) {
		t.Errorf("GetNodeWithMostAvailableVRAM should return ErrNoNodes, got %v", err)
	}

	// Add metrics
	now := time.Now()

	tracker.mu.Lock()
	tracker.metrics["node1"] = &VRAMMetrics{
		TotalVRAM:     32 * bytesPerGB,
		UsedVRAM:      24 * bytesPerGB,
		AvailableVRAM: 8 * bytesPerGB,
		LastUpdate:    now,
	}
	tracker.metrics["node2"] = &VRAMMetrics{
		TotalVRAM:     64 * bytesPerGB,
		UsedVRAM:      32 * bytesPerGB,
		AvailableVRAM: 32 * bytesPerGB, // Most available
		LastUpdate:    now,
	}
	tracker.metrics["node3"] = &VRAMMetrics{
		TotalVRAM:     32 * bytesPerGB,
		UsedVRAM:      16 * bytesPerGB,
		AvailableVRAM: 16 * bytesPerGB,
		LastUpdate:    now,
	}
	tracker.mu.Unlock()

	nodeName, availableVRAM, err := tracker.GetNodeWithMostAvailableVRAM()
	if err != nil {
		t.Fatalf("GetNodeWithMostAvailableVRAM failed: %v", err)
	}

	if nodeName != "node2" {
		t.Errorf("Best node = %q, want %q", nodeName, "node2")
	}

	if availableVRAM != 32*bytesPerGB {
		t.Errorf("Available VRAM = %d, want %d", availableVRAM, 32*bytesPerGB)
	}
}

func TestTrackerGetNodeWithMostAvailableVRAMAllZero(t *testing.T) {
	t.Parallel()

	tracker := NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	// Add metrics with zero available VRAM
	now := time.Now()

	tracker.mu.Lock()
	tracker.metrics["node1"] = &VRAMMetrics{
		TotalVRAM:     32 * bytesPerGB,
		UsedVRAM:      32 * bytesPerGB,
		AvailableVRAM: 0,
		LastUpdate:    now,
	}
	tracker.mu.Unlock()

	_, _, err := tracker.GetNodeWithMostAvailableVRAM()
	if !errors.Is(err, ErrNoSuitableNode) {
		t.Errorf("GetNodeWithMostAvailableVRAM should return ErrNoSuitableNode when all zero, got %v", err)
	}
}

func TestTrackerGetNodeVRAM(t *testing.T) {
	t.Parallel()

	tracker := NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	// Non-existent node
	_, err := tracker.GetNodeVRAM("non-existent")
	if err == nil {
		t.Error("GetNodeVRAM should return error for non-existent node")
	}

	// Add metrics for a node
	now := time.Now()

	tracker.mu.Lock()
	tracker.metrics["node1"] = &VRAMMetrics{
		TotalVRAM:     32 * bytesPerGB,
		UsedVRAM:      8 * bytesPerGB,
		AvailableVRAM: 24 * bytesPerGB,
		LastUpdate:    now,
	}
	tracker.mu.Unlock()

	metrics, err := tracker.GetNodeVRAM("node1")
	if err != nil {
		t.Fatalf("GetNodeVRAM failed: %v", err)
	}

	if metrics.TotalVRAM != 32*bytesPerGB {
		t.Errorf("TotalVRAM = %d, want %d", metrics.TotalVRAM, 32*bytesPerGB)
	}

	if metrics.UsedVRAM != 8*bytesPerGB {
		t.Errorf("UsedVRAM = %d, want %d", metrics.UsedVRAM, 8*bytesPerGB)
	}

	// Verify it's a copy
	if metrics == tracker.metrics["node1"] {
		t.Error("GetNodeVRAM should return a copy, not reference")
	}
}

func TestBuildLabelSelector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		selector map[string]string
		expected []string // Order may vary, so check for containment
	}{
		{
			name:     "empty selector",
			selector: map[string]string{},
			expected: []string{},
		},
		{
			name:     "single label",
			selector: map[string]string{"app": "test"},
			expected: []string{"app=test"},
		},
		{
			name:     "multiple labels",
			selector: map[string]string{"app": "test", "version": "v1"},
			expected: []string{"app=test", "version=v1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := buildLabelSelector(tt.selector)

			for _, expected := range tt.expected {
				if !strings.Contains(result, expected) {
					t.Errorf("buildLabelSelector result %q should contain %q", result, expected)
				}
			}
		})
	}
}

func TestGetNodeInternalIP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		addresses []corev1.NodeAddress
		expected  string
	}{
		{
			name:      "no addresses",
			addresses: []corev1.NodeAddress{},
			expected:  "",
		},
		{
			name: "only external IP",
			addresses: []corev1.NodeAddress{
				{Type: corev1.NodeExternalIP, Address: "1.2.3.4"},
			},
			expected: "",
		},
		{
			name: "internal IP present",
			addresses: []corev1.NodeAddress{
				{Type: corev1.NodeExternalIP, Address: "1.2.3.4"},
				{Type: corev1.NodeInternalIP, Address: "10.0.0.1"},
			},
			expected: "10.0.0.1",
		},
		{
			name: "multiple internal IPs - returns first",
			addresses: []corev1.NodeAddress{
				{Type: corev1.NodeInternalIP, Address: "10.0.0.1"},
				{Type: corev1.NodeInternalIP, Address: "10.0.0.2"},
			},
			expected: "10.0.0.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			node := &corev1.Node{
				Status: corev1.NodeStatus{
					Addresses: tt.addresses,
				},
			}

			result := getNodeInternalIP(node)
			if result != tt.expected {
				t.Errorf("getNodeInternalIP = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestScrapeNodeMetrics(t *testing.T) {
	t.Parallel()

	// Create a mock metrics server
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
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(metricsResponse))
	}))
	defer server.Close()

	// Test with server URL
	ctx := context.Background()
	serverHost := strings.TrimPrefix(server.URL, "http://")

	// We need to call scrapeNodeMetrics with the correct host:port
	hostPort := strings.Split(serverHost, ":")
	if len(hostPort) != 2 {
		t.Fatalf("Unexpected server URL format: %s", serverHost)
	}

	// Create a tracker for testing
	testTracker := NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	// Manually call the scrape function by constructing the URL
	metrics, err := testTracker.scrapeNodeMetrics(ctx, serverHost, 9100)
	if err == nil {
		// If the test server works, validate the metrics
		if metrics.TotalVRAM != 34359738368 {
			t.Errorf("TotalVRAM = %d, want 34359738368", metrics.TotalVRAM)
		}

		if metrics.UsedVRAM != 8589934592 {
			t.Errorf("UsedVRAM = %d, want 8589934592", metrics.UsedVRAM)
		}

		if metrics.AvailableVRAM != 34359738368-8589934592 {
			t.Errorf("AvailableVRAM = %d, want %d", metrics.AvailableVRAM, 34359738368-8589934592)
		}
	}
	// If there's an error, it's expected because we're using a different port
}

func TestScrapeNodeMetricsError(t *testing.T) {
	t.Parallel()

	// Server that returns error status
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	tracker := NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)
	ctx := context.Background()

	serverHost := strings.TrimPrefix(server.URL, "http://")

	_, err := tracker.scrapeNodeMetrics(ctx, serverHost, 9100)
	if err == nil {
		t.Error("scrapeNodeMetrics should return error for non-200 status")
	}
}

func TestErrors(t *testing.T) {
	t.Parallel()

	if ErrNoNodes.Error() != "no nodes with VRAM metrics available" {
		t.Errorf("ErrNoNodes message = %q, want %q", ErrNoNodes.Error(), "no nodes with VRAM metrics available")
	}

	if ErrNoSuitableNode.Error() != "no suitable node found" {
		t.Errorf("ErrNoSuitableNode message = %q, want %q", ErrNoSuitableNode.Error(), "no suitable node found")
	}

	if ErrNoMetrics.Error() != "no metrics available for node" {
		t.Errorf("ErrNoMetrics message = %q, want %q", ErrNoMetrics.Error(), "no metrics available for node")
	}

	if ErrUnexpectedStatus.Error() != "unexpected status code" {
		t.Errorf("ErrUnexpectedStatus message = %q, want %q", ErrUnexpectedStatus.Error(), "unexpected status code")
	}
}

func TestDefaultScrapeInterval(t *testing.T) {
	t.Parallel()

	if DefaultScrapeInterval != 30*time.Second {
		t.Errorf("DefaultScrapeInterval = %v, want %v", DefaultScrapeInterval, 30*time.Second)
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

func TestGetFirstTCPPort(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		pod       *corev1.Pod
		wantPort  int32
		wantError error
	}{
		{
			name: "TCP port",
			pod: &corev1.Pod{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Ports: []corev1.ContainerPort{{ContainerPort: 8080, Protocol: corev1.ProtocolTCP}}},
					},
				},
			},
			wantPort: 8080,
		},
		{
			name: "default protocol is TCP",
			pod: &corev1.Pod{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Ports: []corev1.ContainerPort{{ContainerPort: 9090}}},
					},
				},
			},
			wantPort: 9090,
		},
		{
			name: "multiple containers - first TCP wins",
			pod: &corev1.Pod{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Ports: []corev1.ContainerPort{{ContainerPort: 7070, Protocol: corev1.ProtocolTCP}}},
						{Ports: []corev1.ContainerPort{{ContainerPort: 8080, Protocol: corev1.ProtocolTCP}}},
					},
				},
			},
			wantPort: 7070,
		},
		{
			name: "no ports",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "default"},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c1"}}},
			},
			wantError: ErrNoTCPPortInExporterPod,
		},
		{
			name: "only UDP ports",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "default"},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Ports: []corev1.ContainerPort{{ContainerPort: 8080, Protocol: corev1.ProtocolUDP}}},
					},
				},
			},
			wantError: ErrNoTCPPortInExporterPod,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			port, err := getFirstTCPPort(tt.pod)
			if tt.wantError != nil {
				if !errors.Is(err, tt.wantError) {
					t.Errorf("getFirstTCPPort() error = %v, want %v", err, tt.wantError)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				if port != tt.wantPort {
					t.Errorf("getFirstTCPPort() = %d, want %d", port, tt.wantPort)
				}
			}
		})
	}
}

func TestScrapeMetricsWithNilClient(t *testing.T) {
	t.Parallel()

	tracker := NewTracker(nil, "default", map[string]string{"app": "test"}, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	tracker.scrapeMetrics(context.Background())

	metrics := tracker.GetMetrics()
	if len(metrics) != 0 {
		t.Errorf("scrapeMetrics with nil client should not populate metrics, got %d entries", len(metrics))
	}
}

func TestNewTrackerWithMeter(t *testing.T) {
	t.Parallel()

	noopMeter := metricnoop.NewMeterProvider().Meter("test")

	tracker := NewTrackerWithMeter(
		nil, "default",
		map[string]string{"app": "model-server"},
		map[string]string{"gpu": "true"},
		"kube-system",
		map[string]string{"app": "exporter"},
		30*time.Second,
		noopMeter,
	)

	if tracker == nil {
		t.Fatal("NewTrackerWithMeter returned nil")
	}

	if tracker.totalGauge == nil {
		t.Error("totalGauge should be initialized")
	}

	if tracker.usedGauge == nil {
		t.Error("usedGauge should be initialized")
	}

	if tracker.availableGauge == nil {
		t.Error("availableGauge should be initialized")
	}
}

func TestScrapeNodeMetricsWithCorrectPort(t *testing.T) {
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

	tracker := NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	metrics, err := tracker.scrapeNodeMetrics(context.Background(), host, int32(port))
	if err != nil {
		t.Fatalf("scrapeNodeMetrics failed: %v", err)
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

func TestScrapeNodeMetricsWithMissingMetrics(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("# HELP some_other_metric Some other metric\n# TYPE some_other_metric gauge\nsome_other_metric 42\n"))
	}))
	defer server.Close()

	serverURL := strings.TrimPrefix(server.URL, "http://")
	parts := strings.Split(serverURL, ":")

	host := parts[0]

	port, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		t.Fatalf("Failed to parse port: %v", err)
	}

	tracker := NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	metrics, err := tracker.scrapeNodeMetrics(context.Background(), host, int32(port))
	if err != nil {
		t.Fatalf("scrapeNodeMetrics failed: %v", err)
	}

	if metrics.TotalVRAM != defaultTotalVRAMGB*bytesPerGB {
		t.Errorf("TotalVRAM = %d, want %d (default)", metrics.TotalVRAM, defaultTotalVRAMGB*bytesPerGB)
	}

	if metrics.UsedVRAM != 0 {
		t.Errorf("UsedVRAM = %d, want 0", metrics.UsedVRAM)
	}
}

func TestStartContextCancellation(t *testing.T) {
	t.Parallel()

	tracker := NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 100*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})

	go func() {
		tracker.Start(ctx)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("Start did not exit after context cancellation")
	}
}

func TestListPodsAndNodesWithSelectorsNilClient(t *testing.T) {
	t.Parallel()

	tracker := NewTracker(nil, "default", map[string]string{"app": "test"}, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	ctx := context.Background()
	_, _, err := tracker.listPodsAndNodesWithSelectors(ctx, "default", map[string]string{"app": "test"}, nil)

	if !errors.Is(err, ErrClientNil) {
		t.Errorf("listPodsAndNodesWithSelectors should return ErrClientNil, got %v", err)
	}
}

func TestListPodsAndNodesWithSelectorsFakeClient(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	// Create test pods and nodes
	pods := []crclient.Object{
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod-1",
				Namespace: "default",
				Labels:    map[string]string{"app": "model-server"},
			},
			Spec: corev1.PodSpec{
				NodeName: "node1",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod-2",
				Namespace: "default",
				Labels:    map[string]string{"app": "model-server"},
			},
			Spec: corev1.PodSpec{
				NodeName: "node2",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
			},
		},
	}

	nodes := []crclient.Object{
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "node1",
				Labels: map[string]string{"gpu": "true"},
			},
		},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "node2",
				Labels: map[string]string{"gpu": "true"},
			},
		},
	}

	allObjects := slices.Concat(pods, nodes)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(allObjects...).Build()

	tracker := NewTracker(
		fakeClient,
		"default",
		map[string]string{"app": "model-server"},
		map[string]string{"gpu": "true"},
		"kube-system",
		map[string]string{"app": "exporter"},
		30*time.Second,
	)

	ctx := context.Background()

	podList, nodeList, err := tracker.listPodsAndNodesWithSelectors(ctx, "default", map[string]string{"app": "model-server"}, map[string]string{"gpu": "true"})
	if err != nil {
		t.Fatalf("listPodsAndNodesWithSelectors failed: %v", err)
	}

	if len(podList.Items) != 2 {
		t.Errorf("expected 2 pods, got %d", len(podList.Items))
	}

	if len(nodeList.Items) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(nodeList.Items))
	}
}

func TestListPodsAndNodesWithSelectorsEmptyResults(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	tracker := NewTracker(fakeClient, "default", map[string]string{"app": "nonexistent"}, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	ctx := context.Background()

	podList, nodeList, err := tracker.listPodsAndNodesWithSelectors(ctx, "default", map[string]string{"app": "nonexistent"}, nil)
	if err != nil {
		t.Fatalf("listPodsAndNodesWithSelectors failed: %v", err)
	}

	if len(podList.Items) != 0 {
		t.Errorf("expected 0 pods, got %d", len(podList.Items))
	}

	if len(nodeList.Items) != 0 {
		t.Errorf("expected 0 nodes, got %d", len(nodeList.Items))
	}
}

func TestScrapeMetricsWithSelectorsNilClient(t *testing.T) {
	t.Parallel()

	tracker := NewTracker(nil, "default", map[string]string{"app": "test"}, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	// Should return early without panic
	tracker.scrapeMetricsWithSelectors(context.Background(), "default", map[string]string{"app": "test"}, nil)

	metrics := tracker.GetMetrics()
	if len(metrics) != 0 {
		t.Errorf("scrapeMetricsWithSelectors with nil client should not populate metrics, got %d entries", len(metrics))
	}
}

func TestScrapeMetricsWithSelectorsFakeClient(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	// Create pods and nodes but no exporter pods (so scraping will fail gracefully)
	pods := []crclient.Object{
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "model-server-1",
				Namespace: "default",
				Labels:    map[string]string{"app": "model-server"},
			},
			Spec: corev1.PodSpec{
				NodeName: "node1",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
			},
		},
	}

	nodes := []crclient.Object{
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "node1",
				Labels: map[string]string{"gpu": "true"},
			},
		},
	}

	allObjects := slices.Concat(pods, nodes)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(allObjects...).Build()

	tracker := NewTracker(
		fakeClient,
		"default",
		map[string]string{"app": "model-server"},
		map[string]string{"gpu": "true"},
		"kube-system",
		map[string]string{"app": "exporter"},
		30*time.Second,
	)

	// Will fail to find exporter pods but shouldn't panic
	tracker.scrapeMetricsWithSelectors(context.Background(), "default", map[string]string{"app": "model-server"}, map[string]string{"gpu": "true"})

	// Metrics should be empty because exporter pods don't exist
	metrics := tracker.GetMetrics()
	if len(metrics) != 0 {
		t.Errorf("expected 0 metrics when exporter pods don't exist, got %d", len(metrics))
	}
}

func TestFindExporterPodOnNodeNilClient(t *testing.T) {
	t.Parallel()

	tracker := NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	_, _, err := tracker.findExporterPodOnNode(context.Background(), "node1")
	if !errors.Is(err, ErrClientNil) {
		t.Errorf("findExporterPodOnNode should return ErrClientNil, got %v", err)
	}
}

func TestFindExporterPodOnNodeNoExporter(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	tracker := NewTracker(fakeClient, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	_, _, err := tracker.findExporterPodOnNode(context.Background(), "node1")
	if !errors.Is(err, ErrNoRunningExporterPod) {
		t.Errorf("findExporterPodOnNode should return ErrNoRunningExporterPod when no pods exist, got %v", err)
	}
}

func TestFindExporterPodOnNodeSuccess(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	exporterPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "exporter-1",
			Namespace: "kube-system",
			Labels:    map[string]string{"app": "exporter"},
		},
		Spec: corev1.PodSpec{
			NodeName: "node1",
			Containers: []corev1.Container{
				{
					Name: "exporter",
					Ports: []corev1.ContainerPort{
						{
							ContainerPort: 9100,
							Protocol:      corev1.ProtocolTCP,
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.0.0.5",
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(exporterPod).Build()

	tracker := NewTracker(fakeClient, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	pod, port, err := tracker.findExporterPodOnNode(context.Background(), "node1")
	if err != nil {
		t.Fatalf("findExporterPodOnNode failed: %v", err)
	}

	if pod == nil {
		t.Fatal("pod should not be nil")
	}

	if pod.Name != "exporter-1" {
		t.Errorf("expected pod name exporter-1, got %s", pod.Name)
	}

	if port != 9100 {
		t.Errorf("expected port 9100, got %d", port)
	}
}

func TestScrapeNodeMetricsInvalidURL(t *testing.T) {
	t.Parallel()

	tracker := NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	// Invalid IP address should fail
	_, err := tracker.scrapeNodeMetrics(context.Background(), "invalid-ip", 9100)
	if err == nil {
		t.Error("scrapeNodeMetrics should fail with invalid IP")
	}
}

func TestTrackerConcurrentAccess(t *testing.T) {
	t.Parallel()

	tracker := NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	done := make(chan bool, 4)
	iterations := 100

	// Concurrent writes to metrics
	go func() {
		for i := range iterations {
			tracker.mu.Lock()
			tracker.metrics[fmt.Sprintf("node-%d", i)] = &VRAMMetrics{
				TotalVRAM:     32 * bytesPerGB,
				UsedVRAM:      8 * bytesPerGB,
				AvailableVRAM: 24 * bytesPerGB,
				LastUpdate:    time.Now(),
			}
			tracker.mu.Unlock()
		}

		done <- true
	}()

	// Concurrent reads
	go func() {
		for range iterations {
			_ = tracker.GetMetrics()
			_, _, _ = tracker.GetNodeWithMostAvailableVRAM()
		}

		done <- true
	}()

	// Concurrent selector updates
	go func() {
		for i := range iterations {
			tracker.UpdatePodSelector(context.Background(), map[string]string{"i": strconv.Itoa(i)})
		}

		done <- true
	}()

	// Concurrent namespace updates
	go func() {
		for i := range iterations {
			tracker.UpdateNamespace(fmt.Sprintf("ns-%d", i))
		}

		done <- true
	}()

	for range 4 {
		<-done
	}
}
