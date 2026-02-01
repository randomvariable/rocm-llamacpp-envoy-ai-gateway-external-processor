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
	"maps"
	"strconv"
	"testing"
	"time"

	metricnoop "go.opentelemetry.io/otel/metric/noop"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// Test constants.
const testNode1 = "node1"

func TestNewTracker(t *testing.T) {
	t.Parallel()

	podSelector := map[string]string{"app": "model-server"}
	nodeSelector := map[string]string{"gpu": "true"}
	exporterSelector := map[string]string{"app": "amd-gpu-metrics-exporter-amdgpu-metrics-exporter"}

	tracker := NewTracker(nil, "default", podSelector, nodeSelector, "kube-system", exporterSelector, 30*time.Second)

	if tracker == nil {
		t.Fatal("NewTracker returned nil")
	}

	if tracker.scrapeInterval != 30*time.Second {
		t.Errorf("scrapeInterval = %v, want %v", tracker.scrapeInterval, 30*time.Second)
	}

	if tracker.metrics == nil {
		t.Error("metrics map should be initialized")
	}

	if tracker.k8sDiscovery == nil {
		t.Error("k8sDiscovery should be initialized")
	}
}

func TestMetrics(t *testing.T) {
	t.Parallel()

	now := time.Now()
	metrics := &Metrics{
		TotalVRAM:     32 * BytesPerGB,
		UsedVRAM:      16 * BytesPerGB,
		AvailableVRAM: 16 * BytesPerGB,
		LastUpdate:    now,
	}

	if metrics.TotalVRAM != 32*BytesPerGB {
		t.Errorf("TotalVRAM = %d, want %d", metrics.TotalVRAM, 32*BytesPerGB)
	}

	if metrics.UsedVRAM != 16*BytesPerGB {
		t.Errorf("UsedVRAM = %d, want %d", metrics.UsedVRAM, 16*BytesPerGB)
	}

	if metrics.AvailableVRAM != 16*BytesPerGB {
		t.Errorf("AvailableVRAM = %d, want %d", metrics.AvailableVRAM, 16*BytesPerGB)
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

	// Verify the underlying discovery was updated.
	if tracker.k8sDiscovery == nil {
		t.Fatal("k8sDiscovery should not be nil")
	}
}

func TestTrackerUpdateNodeSelector(t *testing.T) {
	t.Parallel()

	tracker := NewTracker(nil, "default", nil, map[string]string{"gpu": "false"}, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	newSelector := map[string]string{"gpu": "true"}
	tracker.UpdateNodeSelector(newSelector)

	// Verify the tracker has Kubernetes discovery.
	if tracker.k8sDiscovery == nil {
		t.Fatal("k8sDiscovery should not be nil")
	}
}

func TestTrackerUpdateNamespace(t *testing.T) {
	t.Parallel()

	tracker := NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	tracker.UpdateNamespace("production")

	// Verify the tracker has Kubernetes discovery.
	if tracker.k8sDiscovery == nil {
		t.Fatal("k8sDiscovery should not be nil")
	}
}

func TestTrackerGetMetrics(t *testing.T) {
	t.Parallel()

	tracker := NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	// Set some metrics directly.
	now := time.Now()
	tracker.SetMetrics("node1", &Metrics{
		TotalVRAM:     32 * BytesPerGB,
		UsedVRAM:      8 * BytesPerGB,
		AvailableVRAM: 24 * BytesPerGB,
		LastUpdate:    now,
	})
	tracker.SetMetrics("node2", &Metrics{
		TotalVRAM:     64 * BytesPerGB,
		UsedVRAM:      32 * BytesPerGB,
		AvailableVRAM: 32 * BytesPerGB,
		LastUpdate:    now,
	})

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
}

func TestTrackerGetNodeWithMostAvailableVRAM(t *testing.T) {
	t.Parallel()

	tracker := NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	// No metrics - should return error.
	_, _, err := tracker.GetNodeWithMostAvailableVRAM()
	if !errors.Is(err, ErrNoNodes) {
		t.Errorf("GetNodeWithMostAvailableVRAM should return ErrNoNodes, got %v", err)
	}

	// Add metrics.
	now := time.Now()
	tracker.SetMetrics("node1", &Metrics{
		TotalVRAM:     32 * BytesPerGB,
		UsedVRAM:      24 * BytesPerGB,
		AvailableVRAM: 8 * BytesPerGB,
		LastUpdate:    now,
	})
	tracker.SetMetrics("node2", &Metrics{
		TotalVRAM:     64 * BytesPerGB,
		UsedVRAM:      32 * BytesPerGB,
		AvailableVRAM: 32 * BytesPerGB, // Most available.
		LastUpdate:    now,
	})
	tracker.SetMetrics("node3", &Metrics{
		TotalVRAM:     32 * BytesPerGB,
		UsedVRAM:      16 * BytesPerGB,
		AvailableVRAM: 16 * BytesPerGB,
		LastUpdate:    now,
	})

	nodeName, availableVRAM, err := tracker.GetNodeWithMostAvailableVRAM()
	if err != nil {
		t.Fatalf("GetNodeWithMostAvailableVRAM failed: %v", err)
	}

	if nodeName != "node2" {
		t.Errorf("Best node = %q, want %q", nodeName, "node2")
	}

	if availableVRAM != 32*BytesPerGB {
		t.Errorf("Available VRAM = %d, want %d", availableVRAM, 32*BytesPerGB)
	}
}

func TestTrackerGetNodeWithMostAvailableVRAMAllZero(t *testing.T) {
	t.Parallel()

	tracker := NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	// Add metrics with zero available VRAM.
	now := time.Now()
	tracker.SetMetrics("node1", &Metrics{
		TotalVRAM:     32 * BytesPerGB,
		UsedVRAM:      32 * BytesPerGB,
		AvailableVRAM: 0,
		LastUpdate:    now,
	})

	_, _, err := tracker.GetNodeWithMostAvailableVRAM()
	if !errors.Is(err, ErrNoSuitableNode) {
		t.Errorf("GetNodeWithMostAvailableVRAM should return ErrNoSuitableNode when all zero, got %v", err)
	}
}

func TestTrackerGetNodeVRAM(t *testing.T) {
	t.Parallel()

	tracker := NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	// Non-existent node.
	_, err := tracker.GetNodeVRAM("non-existent")
	if err == nil {
		t.Error("GetNodeVRAM should return error for non-existent node")
	}

	// Add metrics for a node.
	now := time.Now()
	tracker.SetMetrics("node1", &Metrics{
		TotalVRAM:     32 * BytesPerGB,
		UsedVRAM:      8 * BytesPerGB,
		AvailableVRAM: 24 * BytesPerGB,
		LastUpdate:    now,
	})

	metrics, err := tracker.GetNodeVRAM("node1")
	if err != nil {
		t.Fatalf("GetNodeVRAM failed: %v", err)
	}

	if metrics.TotalVRAM != 32*BytesPerGB {
		t.Errorf("TotalVRAM = %d, want %d", metrics.TotalVRAM, 32*BytesPerGB)
	}

	if metrics.UsedVRAM != 8*BytesPerGB {
		t.Errorf("UsedVRAM = %d, want %d", metrics.UsedVRAM, 8*BytesPerGB)
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

func TestDefaultScrapeIntervalConstant(t *testing.T) {
	t.Parallel()

	if DefaultScrapeInterval != 30*time.Second {
		t.Errorf("DefaultScrapeInterval = %v, want %v", DefaultScrapeInterval, 30*time.Second)
	}
}

func TestScrapeMetricsWithNilDiscovery(t *testing.T) {
	t.Parallel()

	tracker := &Tracker{
		scraper:   NewPrometheusScraper(),
		discovery: nil,
		metrics:   make(map[string]*Metrics),
	}

	tracker.scrapeMetrics(context.Background())

	metrics := tracker.GetMetrics()
	if len(metrics) != 0 {
		t.Errorf("scrapeMetrics with nil discovery should not populate metrics, got %d entries", len(metrics))
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

	if tracker.instrumentation == nil {
		t.Error("instrumentation should be initialized")
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

func TestTrackerConcurrentAccess(t *testing.T) {
	t.Parallel()

	tracker := NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	done := make(chan bool, 4)
	iterations := 100

	// Concurrent writes to metrics.
	go func() {
		for i := range iterations {
			tracker.SetMetrics(fmt.Sprintf("node-%d", i), &Metrics{
				TotalVRAM:     32 * BytesPerGB,
				UsedVRAM:      8 * BytesPerGB,
				AvailableVRAM: 24 * BytesPerGB,
				LastUpdate:    time.Now(),
			})
		}

		done <- true
	}()

	// Concurrent reads.
	go func() {
		for range iterations {
			_ = tracker.GetMetrics()
			_, _, _ = tracker.GetNodeWithMostAvailableVRAM()
		}

		done <- true
	}()

	// Concurrent selector updates.
	go func() {
		for i := range iterations {
			tracker.UpdatePodSelector(context.Background(), map[string]string{"i": strconv.Itoa(i)})
		}

		done <- true
	}()

	// Concurrent namespace updates.
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

func TestTrackerWithDeps(t *testing.T) {
	t.Parallel()

	staticMetrics := map[string]*Metrics{
		"node1": {TotalVRAM: 32 * BytesPerGB, UsedVRAM: 8 * BytesPerGB, AvailableVRAM: 24 * BytesPerGB},
		"node2": {TotalVRAM: 64 * BytesPerGB, UsedVRAM: 32 * BytesPerGB, AvailableVRAM: 32 * BytesPerGB},
	}

	scraper := NewStaticScraper(staticMetrics)
	discovery := NewStaticDiscovery([]MetricsEndpoint{
		{NodeName: "node1", Address: "10.0.0.1", Port: 9100},
		{NodeName: "node2", Address: "10.0.0.2", Port: 9100},
	})

	tracker := NewTrackerWithDeps(scraper, discovery, WithScrapeInterval(10*time.Second))

	if tracker == nil {
		t.Fatal("NewTrackerWithDeps returned nil")
	}

	if tracker.scrapeInterval != 10*time.Second {
		t.Errorf("scrapeInterval = %v, want %v", tracker.scrapeInterval, 10*time.Second)
	}

	// Trigger a scrape.
	tracker.scrapeMetrics(context.Background())

	metrics := tracker.GetMetrics()
	if len(metrics) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(metrics))
	}

	nodeName, available, err := tracker.GetNodeWithMostAvailableVRAM()
	if err != nil {
		t.Fatalf("GetNodeWithMostAvailableVRAM failed: %v", err)
	}

	if nodeName != "node2" {
		t.Errorf("expected node2, got %s", nodeName)
	}

	if available != 32*BytesPerGB {
		t.Errorf("expected %d available, got %d", 32*BytesPerGB, available)
	}
}

func TestTrackerWithInstrumentation(t *testing.T) {
	t.Parallel()

	scraper := NewStaticScraper(map[string]*Metrics{
		"node1": {TotalVRAM: 32 * BytesPerGB, UsedVRAM: 8 * BytesPerGB, AvailableVRAM: 24 * BytesPerGB},
	})
	discovery := NewStaticDiscovery([]MetricsEndpoint{
		{NodeName: "node1", Address: "10.0.0.1", Port: 9100},
	})

	recordedMetrics := make(map[string]*Metrics)
	inst := &testInstrumentation{metrics: &recordedMetrics}

	tracker := NewTrackerWithDeps(scraper, discovery, WithInstrumentation(inst))
	tracker.scrapeMetrics(context.Background())

	if len(*inst.metrics) != 1 {
		t.Errorf("expected 1 recorded metric, got %d", len(*inst.metrics))
	}
}

type testInstrumentation struct {
	metrics *map[string]*Metrics
}

func (t *testInstrumentation) RecordMetrics(metrics map[string]*Metrics) {
	maps.Copy((*t.metrics), metrics)
}

func TestKubernetesDiscoveryIntegration(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	// Create test pods and nodes with exporter.
	modelPod := &corev1.Pod{
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
	}

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
						{ContainerPort: 9100, Protocol: corev1.ProtocolTCP},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.0.0.5",
		},
	}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node1",
			Labels: map[string]string{"gpu": "true"},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(modelPod, exporterPod, node).Build()

	discovery := NewKubernetesDiscovery(KubernetesDiscoveryConfig{
		Client:              fakeClient,
		Namespace:           "default",
		PodSelector:         map[string]string{"app": "model-server"},
		NodeSelector:        map[string]string{"gpu": "true"},
		ExporterNamespace:   "kube-system",
		ExporterPodSelector: map[string]string{"app": "exporter"},
	})

	endpoints, err := discovery.DiscoverEndpoints(context.Background())
	if err != nil {
		t.Fatalf("DiscoverEndpoints failed: %v", err)
	}

	if len(endpoints) != 1 {
		t.Errorf("expected 1 endpoint, got %d", len(endpoints))
	}

	if endpoints[0].NodeName != testNode1 {
		t.Errorf("expected %s, got %s", testNode1, endpoints[0].NodeName)
	}

	if endpoints[0].Address != "10.0.0.5" {
		t.Errorf("expected 10.0.0.5, got %s", endpoints[0].Address)
	}

	if endpoints[0].Port != 9100 {
		t.Errorf("expected port 9100, got %d", endpoints[0].Port)
	}
}

func TestStaticDiscovery(t *testing.T) {
	t.Parallel()

	endpoints := []MetricsEndpoint{
		{NodeName: "node1", Address: "10.0.0.1", Port: 9100},
		{NodeName: "node2", Address: "10.0.0.2", Port: 9100},
	}

	discovery := NewStaticDiscovery(endpoints)

	result, err := discovery.DiscoverEndpoints(context.Background())
	if err != nil {
		t.Fatalf("DiscoverEndpoints failed: %v", err)
	}

	if len(result) != 2 {
		t.Errorf("expected 2 endpoints, got %d", len(result))
	}

	// Update endpoints.
	discovery.SetEndpoints([]MetricsEndpoint{
		{NodeName: "node3", Address: "10.0.0.3", Port: 9100},
	})

	result, err = discovery.DiscoverEndpoints(context.Background())
	if err != nil {
		t.Fatalf("DiscoverEndpoints failed: %v", err)
	}

	if len(result) != 1 {
		t.Errorf("expected 1 endpoint, got %d", len(result))
	}
}

func TestStaticScraper(t *testing.T) {
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

	// Non-existent node.
	_, err = scraper.Scrape(context.Background(), MetricsEndpoint{NodeName: "nonexistent"})
	if err == nil {
		t.Error("expected error for non-existent node")
	}

	// Update metrics.
	scraper.SetMetrics("node2", &Metrics{TotalVRAM: 64 * BytesPerGB})

	result, err = scraper.Scrape(context.Background(), MetricsEndpoint{NodeName: "node2"})
	if err != nil {
		t.Fatalf("Scrape failed: %v", err)
	}

	if result.TotalVRAM != 64*BytesPerGB {
		t.Errorf("expected TotalVRAM %d, got %d", 64*BytesPerGB, result.TotalVRAM)
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

func TestFindExporterPodOnNodeNoExporter(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	discovery := NewKubernetesDiscovery(KubernetesDiscoveryConfig{
		Client:              fakeClient,
		ExporterNamespace:   "kube-system",
		ExporterPodSelector: map[string]string{"app": "exporter"},
	})

	_, err := discovery.findExporterEndpoint(context.Background(), "node1")
	if !errors.Is(err, ErrNoRunningExporterPod) {
		t.Errorf("expected ErrNoRunningExporterPod, got %v", err)
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

	discovery := NewKubernetesDiscovery(KubernetesDiscoveryConfig{
		Client:              fakeClient,
		ExporterNamespace:   "kube-system",
		ExporterPodSelector: map[string]string{"app": "exporter"},
	})

	endpoint, err := discovery.findExporterEndpoint(context.Background(), "node1")
	if err != nil {
		t.Fatalf("findExporterEndpoint failed: %v", err)
	}

	if endpoint.NodeName != "node1" {
		t.Errorf("expected node name node1, got %s", endpoint.NodeName)
	}

	if endpoint.Port != 9100 {
		t.Errorf("expected port 9100, got %d", endpoint.Port)
	}
}

func TestKubernetesDiscoveryListPodsAndNodes(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	// Create test pods and nodes.
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

	allObjects := make([]crclient.Object, 0, len(pods)+len(nodes))
	allObjects = append(allObjects, pods...)
	allObjects = append(allObjects, nodes...)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(allObjects...).Build()

	discovery := NewKubernetesDiscovery(KubernetesDiscoveryConfig{
		Client:              fakeClient,
		Namespace:           "default",
		PodSelector:         map[string]string{"app": "model-server"},
		NodeSelector:        map[string]string{"gpu": "true"},
		ExporterNamespace:   "kube-system",
		ExporterPodSelector: map[string]string{"app": "exporter"},
	})

	// No exporter pods, so endpoints will be empty.
	endpoints, err := discovery.DiscoverEndpoints(context.Background())
	if err != nil {
		t.Fatalf("DiscoverEndpoints failed: %v", err)
	}

	// Should find 0 endpoints because there are no exporter pods.
	if len(endpoints) != 0 {
		t.Errorf("expected 0 endpoints (no exporters), got %d", len(endpoints))
	}
}
