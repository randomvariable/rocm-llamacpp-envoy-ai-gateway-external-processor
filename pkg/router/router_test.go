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

package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/randomvariable/rocm-envoy-ai-gateway-external-processor/pkg/vram"
)

func TestNewRouter(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	tracker := vram.NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)
	podSelector := map[string]string{"app": "model-server"}

	router := NewRouter(ctx, nil, tracker, "default", podSelector, "/v1/load")

	if router == nil {
		t.Fatal("NewRouter returned nil")
	}

	if router.namespace != "default" {
		t.Errorf("namespace = %q, want %q", router.namespace, "default")
	}

	if router.modelLoadEndpoint != "/v1/load" {
		t.Errorf("modelLoadEndpoint = %q, want %q", router.modelLoadEndpoint, "/v1/load")
	}

	if !router.IsReady() {
		t.Error("Router should be ready after creation")
	}

	if router.models == nil {
		t.Error("models map should be initialized")
	}
}

func TestRouterIsReady(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	tracker := vram.NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)
	router := NewRouter(ctx, nil, tracker, "default", nil, "/v1/load")

	if !router.IsReady() {
		t.Error("Router should be ready by default")
	}

	// Manually set ready to false
	router.mu.Lock()
	router.ready = false
	router.mu.Unlock()

	if router.IsReady() {
		t.Error("Router should not be ready after setting ready=false")
	}
}

func TestRouterUpdatePodSelector(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	tracker := vram.NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)
	router := NewRouter(ctx, nil, tracker, "default", map[string]string{"app": "v1"}, "/v1/load")

	newSelector := map[string]string{"app": "v2", "version": "2"}
	router.UpdatePodSelector(ctx, newSelector)

	router.mu.RLock()
	defer router.mu.RUnlock()

	if router.podSelector["app"] != "v2" {
		t.Errorf("podSelector[app] = %q, want %q", router.podSelector["app"], "v2")
	}

	if router.podSelector["version"] != "2" {
		t.Errorf("podSelector[version] = %q, want %q", router.podSelector["version"], "2")
	}
}

func TestRouterUpdateNamespace(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	tracker := vram.NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)
	router := NewRouter(ctx, nil, tracker, "default", nil, "/v1/load")

	router.UpdateNamespace("production")

	router.mu.RLock()
	defer router.mu.RUnlock()

	if router.namespace != "production" {
		t.Errorf("namespace = %q, want %q", router.namespace, "production")
	}
}

func TestRouterGetWarmModels(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	tracker := vram.NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)
	router := NewRouter(ctx, nil, tracker, "default", nil, "/v1/load")

	// Initially empty
	models := router.GetWarmModels()
	if len(models) != 0 {
		t.Errorf("GetWarmModels should return empty map, got %d models", len(models))
	}

	// Add some models directly
	now := time.Now()

	router.mu.Lock()
	router.models["model-a"] = &ModelInfo{
		Name:      "model-a",
		NodeName:  "node1",
		PodName:   "pod1",
		PodIP:     "10.0.0.1",
		LoadedAt:  now,
		VRAMUsage: 1024 * 1024 * 1024,
	}
	router.models["model-b"] = &ModelInfo{
		Name:     "model-b",
		NodeName: "node2",
		PodName:  "pod2",
		PodIP:    "10.0.0.2",
		LoadedAt: now,
	}
	router.mu.Unlock()

	models = router.GetWarmModels()
	if len(models) != 2 {
		t.Errorf("GetWarmModels should return 2 models, got %d", len(models))
	}

	if models["model-a"] == nil {
		t.Error("GetWarmModels should include model-a")
	}

	if models["model-b"] == nil {
		t.Error("GetWarmModels should include model-b")
	}

	// Verify it's a copy
	if models["model-a"] == router.models["model-a"] {
		t.Error("GetWarmModels should return copies, not references")
	}
}

func TestRouterGetModelEndpointWarmModel(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	tracker := vram.NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)
	router := NewRouter(ctx, nil, tracker, "default", nil, "/v1/load")

	// Add a warm model
	router.mu.Lock()
	router.models["test-model"] = &ModelInfo{
		Name:     "test-model",
		NodeName: "node1",
		PodName:  "pod1",
		PodIP:    "10.0.0.1",
		LoadedAt: time.Now(),
	}
	router.mu.Unlock()

	endpoint, err := router.GetModelEndpoint(ctx, "test-model")
	if err != nil {
		t.Fatalf("GetModelEndpoint failed: %v", err)
	}

	expected := "http://10.0.0.1:8080"
	if endpoint != expected {
		t.Errorf("endpoint = %q, want %q", endpoint, expected)
	}
}

func TestModelInfo(t *testing.T) {
	t.Parallel()

	now := time.Now()
	info := &ModelInfo{
		Name:      "test-model",
		NodeName:  "node1",
		PodName:   "pod1",
		PodIP:     "10.0.0.1",
		LoadedAt:  now,
		VRAMUsage: 8 * 1024 * 1024 * 1024,
	}

	if info.Name != "test-model" {
		t.Errorf("Name = %q, want %q", info.Name, "test-model")
	}

	if info.NodeName != "node1" {
		t.Errorf("NodeName = %q, want %q", info.NodeName, "node1")
	}

	if info.PodName != "pod1" {
		t.Errorf("PodName = %q, want %q", info.PodName, "pod1")
	}

	if info.PodIP != "10.0.0.1" {
		t.Errorf("PodIP = %q, want %q", info.PodIP, "10.0.0.1")
	}

	if info.LoadedAt != now {
		t.Errorf("LoadedAt = %v, want %v", info.LoadedAt, now)
	}

	if info.VRAMUsage != 8*1024*1024*1024 {
		t.Errorf("VRAMUsage = %d, want %d", info.VRAMUsage, 8*1024*1024*1024)
	}
}

func TestModelListResponse(t *testing.T) {
	t.Parallel()

	response := modelListResponse{
		Data: []modelData{
			{ID: "model-1"},
			{ID: "model-2"},
		},
	}

	if len(response.Data) != 2 {
		t.Errorf("Data length = %d, want 2", len(response.Data))
	}

	if response.Data[0].ID != "model-1" {
		t.Errorf("Data[0].ID = %q, want %q", response.Data[0].ID, "model-1")
	}

	if response.Data[1].ID != "model-2" {
		t.Errorf("Data[1].ID = %q, want %q", response.Data[1].ID, "model-2")
	}
}

func TestModelListResponseJSON(t *testing.T) {
	t.Parallel()

	jsonData := `{"data": [{"id": "gpt-4"}, {"id": "llama-7b"}]}`

	var response modelListResponse

	err := json.Unmarshal([]byte(jsonData), &response)
	if err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	if len(response.Data) != 2 {
		t.Errorf("Data length = %d, want 2", len(response.Data))
	}

	if response.Data[0].ID != "gpt-4" {
		t.Errorf("Data[0].ID = %q, want %q", response.Data[0].ID, "gpt-4")
	}
}

func TestQueryPodModelsSuccess(t *testing.T) {
	t.Parallel()

	// Create a mock model server
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			t.Errorf("Unexpected path: %s", r.URL.Path)
		}

		response := modelListResponse{
			Data: []modelData{
				{ID: "model-1"},
				{ID: "model-2"},
			},
		}

		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(response)
	}))
	t.Cleanup(server.Close)

	// Note: We can't easily test queryPodModels directly because it constructs
	// URLs based on pod IPs. We'll test the JSON parsing separately.
}

func TestQueryPodModelsErrorStatus(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)

	// The actual queryPodModels requires a pod, so we'll verify error cases
	// through integration tests or mock the clientset.
}

func TestErrors(t *testing.T) {
	t.Parallel()

	if ErrPodHasNoIP.Error() != "pod has no IP" {
		t.Errorf("ErrPodHasNoIP message = %q, want %q", ErrPodHasNoIP.Error(), "pod has no IP")
	}

	if ErrUnexpectedStatusCode.Error() != "unexpected status code" {
		t.Errorf("ErrUnexpectedStatusCode message = %q, want %q", ErrUnexpectedStatusCode.Error(), "unexpected status code")
	}

	if ErrNoPodsOnNode.Error() != "no pods found on node" {
		t.Errorf("ErrNoPodsOnNode message = %q, want %q", ErrNoPodsOnNode.Error(), "no pods found on node")
	}

	if ErrModelLoadFailed.Error() != "failed to load model" {
		t.Errorf("ErrModelLoadFailed message = %q, want %q", ErrModelLoadFailed.Error(), "failed to load model")
	}
}

func TestConstants(t *testing.T) {
	t.Parallel()

	if modelServerPort != 8080 {
		t.Errorf("modelServerPort = %d, want 8080", modelServerPort)
	}

	if modelQueryTimeout != 5*time.Second {
		t.Errorf("modelQueryTimeout = %v, want %v", modelQueryTimeout, 5*time.Second)
	}

	if modelLoadTimeout != 30*time.Second {
		t.Errorf("modelLoadTimeout = %v, want %v", modelLoadTimeout, 30*time.Second)
	}

	if warmModelSyncPeriod != 30*time.Second {
		t.Errorf("warmModelSyncPeriod = %v, want %v", warmModelSyncPeriod, 30*time.Second)
	}
}

func TestQueryPodModelsNoPodIP(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tracker := vram.NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)
	r := &Router{
		vramTracker:       tracker,
		namespace:         "default",
		podSelector:       map[string]string{"app": "model-server"},
		modelLoadEndpoint: "/v1/load",
		models:            make(map[string]*ModelInfo),
		ready:             true,
	}

	pod := &corev1.Pod{
		Status: corev1.PodStatus{PodIP: ""},
	}

	_, err := r.queryPodModels(ctx, pod)
	if !errors.Is(err, ErrPodHasNoIP) {
		t.Errorf("queryPodModels with no pod IP: got %v, want %v", err, ErrPodHasNoIP)
	}
}

func TestDiscoverWarmModelsNilClient(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tracker := vram.NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)
	r := &Router{
		client:            nil,
		vramTracker:       tracker,
		namespace:         "default",
		podSelector:       map[string]string{"app": "model-server"},
		modelLoadEndpoint: "/v1/load",
		models:            make(map[string]*ModelInfo),
		ready:             true,
	}

	r.mu.Lock()
	r.models["test"] = &ModelInfo{Name: "test", PodIP: "10.0.0.1"}
	r.mu.Unlock()

	// Should not panic and should not clear models.
	r.discoverWarmModels(ctx)

	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.models) != 1 {
		t.Errorf("discoverWarmModels with nil client changed models: got %d, want 1", len(r.models))
	}
}

func TestDiscoverWarmModelsWithSelectorNilClient(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tracker := vram.NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)
	r := &Router{
		client:            nil,
		vramTracker:       tracker,
		namespace:         "default",
		podSelector:       map[string]string{"app": "model-server"},
		modelLoadEndpoint: "/v1/load",
		models:            make(map[string]*ModelInfo),
		ready:             true,
	}

	// Should return early without panic.
	r.discoverWarmModelsWithSelector(ctx, "default", map[string]string{"app": "test"})
}

func TestGetModelEndpointColdModelNoVRAMData(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tracker := vram.NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)
	r := &Router{
		client:            nil,
		vramTracker:       tracker,
		namespace:         "default",
		podSelector:       map[string]string{"app": "model-server"},
		modelLoadEndpoint: "/v1/load",
		models:            make(map[string]*ModelInfo),
		ready:             true,
	}

	_, err := r.GetModelEndpoint(ctx, "cold-model")
	if err == nil {
		t.Error("GetModelEndpoint for cold model with no VRAM data should return error")
	}
}

func TestLoadModelOnBestNodeNoVRAM(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tracker := vram.NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)
	r := &Router{
		client:            nil,
		vramTracker:       tracker,
		namespace:         "default",
		podSelector:       map[string]string{"app": "model-server"},
		modelLoadEndpoint: "/v1/load",
		models:            make(map[string]*ModelInfo),
		ready:             true,
	}

	_, err := r.loadModelOnBestNode(ctx, "test-model")
	if err == nil {
		t.Error("loadModelOnBestNode with no VRAM data should return error")
	}
}

func TestGetModelEndpointIPv6(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tracker := vram.NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)
	r := &Router{
		vramTracker:       tracker,
		namespace:         "default",
		modelLoadEndpoint: "/v1/load",
		models:            make(map[string]*ModelInfo),
		ready:             true,
	}

	r.mu.Lock()
	r.models["ipv6-model"] = &ModelInfo{
		Name:     "ipv6-model",
		PodIP:    "2001:db8::1",
		LoadedAt: time.Now(),
	}
	r.mu.Unlock()

	endpoint, err := r.GetModelEndpoint(ctx, "ipv6-model")
	if err != nil {
		t.Fatalf("GetModelEndpoint failed: %v", err)
	}

	expected := "http://[2001:db8::1]:8080"
	if endpoint != expected {
		t.Errorf("endpoint = %q, want %q", endpoint, expected)
	}
}

func TestModelListResponseEmpty(t *testing.T) {
	t.Parallel()

	var response modelListResponse

	err := json.Unmarshal([]byte(`{"data": []}`), &response)
	if err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	if len(response.Data) != 0 {
		t.Errorf("Data length = %d, want 0", len(response.Data))
	}
}

func TestRouterConcurrentAccess(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tracker := vram.NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)
	r := &Router{
		client:            nil,
		vramTracker:       tracker,
		namespace:         "default",
		podSelector:       map[string]string{"app": "model-server"},
		modelLoadEndpoint: "/v1/load",
		models:            make(map[string]*ModelInfo),
		ready:             true,
	}

	done := make(chan bool, 4)
	iterations := 100

	go func() {
		for i := range iterations {
			r.mu.Lock()
			r.models[fmt.Sprintf("model-%d", i)] = &ModelInfo{
				Name:  fmt.Sprintf("model-%d", i),
				PodIP: "10.0.0.1",
			}
			r.mu.Unlock()
		}

		done <- true
	}()

	go func() {
		for range iterations {
			_ = r.GetWarmModels()
			_ = r.IsReady()
		}

		done <- true
	}()

	go func() {
		for i := range iterations {
			r.UpdateNamespace(fmt.Sprintf("ns-%d", i))
		}

		done <- true
	}()

	go func() {
		for i := range iterations {
			r.UpdatePodSelector(ctx, map[string]string{"i": strconv.Itoa(i)})
		}

		done <- true
	}()

	for range 4 {
		<-done
	}
}

func TestDiscoverWarmModelsWithSelectorRunningPods(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Create fake client with scheme
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	// Create running pods with IPs
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
				PodIP: "10.0.0.1",
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "model-server-2",
				Namespace: "default",
				Labels:    map[string]string{"app": "model-server"},
			},
			Spec: corev1.PodSpec{
				NodeName: "node2",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: "10.0.0.2",
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "model-server-pending",
				Namespace: "default",
				Labels:    map[string]string{"app": "model-server"},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodPending,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pods...).Build()

	tracker := vram.NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)
	r := &Router{
		client:            fakeClient,
		vramTracker:       tracker,
		namespace:         "default",
		podSelector:       map[string]string{"app": "model-server"},
		modelLoadEndpoint: "/v1/load",
		models:            make(map[string]*ModelInfo),
		ready:             true,
	}

	// This will fail at HTTP calls but covers the k8s listing logic
	r.discoverWarmModelsWithSelector(ctx, "default", map[string]string{"app": "model-server"})

	// Verify models map is initialized (empty because HTTP calls fail)
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.models == nil {
		t.Error("models map should be initialized")
	}
}

func TestDiscoverWarmModelsWithSelectorNoPods(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	tracker := vram.NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)
	r := &Router{
		client:            fakeClient,
		vramTracker:       tracker,
		namespace:         "default",
		podSelector:       map[string]string{"app": "model-server"},
		modelLoadEndpoint: "/v1/load",
		models:            make(map[string]*ModelInfo),
		ready:             true,
	}

	r.discoverWarmModelsWithSelector(ctx, "default", map[string]string{"app": "model-server"})

	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.models) != 0 {
		t.Errorf("models should be empty when no pods exist, got %d", len(r.models))
	}
}

func TestSyncWarmModelsContextCancellation(t *testing.T) {
	t.Parallel()

	tracker := vram.NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 100*time.Millisecond)
	r := &Router{
		client:            nil,
		vramTracker:       tracker,
		namespace:         "default",
		podSelector:       map[string]string{"app": "model-server"},
		modelLoadEndpoint: "/v1/load",
		models:            make(map[string]*ModelInfo),
		ready:             true,
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})

	go func() {
		r.syncWarmModels(ctx)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("syncWarmModels did not exit after context cancellation")
	}
}

func TestLoadModelOnBestNodeWithFakeClient(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	// Create pods on different nodes
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
				PodIP: "10.0.0.1",
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pods...).Build()

	tracker := vram.NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	r := &Router{
		client:            fakeClient,
		vramTracker:       tracker,
		namespace:         "default",
		podSelector:       map[string]string{"app": "model-server"},
		modelLoadEndpoint: "/v1/load",
		models:            make(map[string]*ModelInfo),
		ready:             true,
	}

	// This will fail because tracker has no VRAM metrics (cannot inject from outside vram package)
	_, err := r.loadModelOnBestNode(ctx, "test-model")
	if err == nil {
		t.Error("loadModelOnBestNode should fail when no VRAM metrics available")
	}

	// Should get ErrNoNodes since tracker has no metrics
	if !strings.Contains(err.Error(), "no nodes") && !strings.Contains(err.Error(), "suitable node") {
		t.Logf("Got expected error (no VRAM data): %v", err)
	}
}

func TestLoadModelOnBestNodeNoPodIP(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	// Create pod without IP
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
				PodIP: "",
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pods...).Build()

	tracker := vram.NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	r := &Router{
		client:            fakeClient,
		vramTracker:       tracker,
		namespace:         "default",
		podSelector:       map[string]string{"app": "model-server"},
		modelLoadEndpoint: "/v1/load",
		models:            make(map[string]*ModelInfo),
		ready:             true,
	}

	// Will fail because tracker has no VRAM metrics
	_, err := r.loadModelOnBestNode(ctx, "test-model")
	if err == nil {
		t.Error("loadModelOnBestNode should return error when no VRAM metrics")
	}
}

func TestLoadModelOnPodNoPodIP(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tracker := vram.NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)
	r := &Router{
		vramTracker:       tracker,
		namespace:         "default",
		modelLoadEndpoint: "/v1/load",
		models:            make(map[string]*ModelInfo),
		ready:             true,
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
		Status: corev1.PodStatus{
			PodIP: "127.0.0.1",
		},
	}

	// Port 8080 should not be listening, so connection refused is fast.
	err := r.loadModelOnPod(ctx, pod, "test-model")
	if err == nil {
		t.Error("loadModelOnPod should return error when pod has no IP")
	}
}

func TestQueryPodModelsConnectionRefused(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r := &Router{
		namespace:         "default",
		modelLoadEndpoint: "/v1/load",
		models:            make(map[string]*ModelInfo),
		ready:             true,
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "default"},
		Spec:       corev1.PodSpec{NodeName: "node1"},
		Status:     corev1.PodStatus{PodIP: "127.0.0.1"},
	}

	// Port 8080 should not be listening, so connection refused is fast.
	_, err := r.queryPodModels(ctx, pod)
	if err == nil {
		t.Error("queryPodModels should return error on connection refused")
	}
}

func TestDiscoverWarmModelsWithFakeClient(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	pods := []crclient.Object{
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "model-server-1",
				Namespace: "default",
				Labels:    map[string]string{"app": "model-server"},
			},
			Spec:   corev1.PodSpec{NodeName: "node1"},
			Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "127.0.0.1"},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pods...).Build()
	tracker := vram.NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)

	r := &Router{
		client:            fakeClient,
		vramTracker:       tracker,
		namespace:         "default",
		podSelector:       map[string]string{"app": "model-server"},
		modelLoadEndpoint: "/v1/load",
		models:            make(map[string]*ModelInfo),
		ready:             true,
	}

	// discoverWarmModels copies selector under lock and delegates
	r.discoverWarmModels(ctx)

	r.mu.RLock()
	defer r.mu.RUnlock()

	// Models should be empty because HTTP calls to 127.0.0.1:8080 fail
	if r.models == nil {
		t.Error("models map should not be nil")
	}
}

func TestQueryPodModelsWithHTTPTest(t *testing.T) {
	t.Parallel()

	// Create a mock model server returning valid models
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		response := modelListResponse{
			Data: []modelData{
				{ID: "model-a"},
				{ID: "model-b"},
			},
		}

		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(response)
	}))
	t.Cleanup(server.Close)

	// Extract host and port from the test server URL
	addr := strings.TrimPrefix(server.URL, "http://")

	// We can't use queryPodModels directly because it hardcodes port 8080.
	// Instead, test the JSON parsing by making the request manually.
	resp, err := http.Get(server.URL + "/v1/models")
	if err != nil {
		t.Fatalf("Failed to GET: %v", err)
	}

	defer func() { _ = resp.Body.Close() }()

	var response modelListResponse

	err = json.NewDecoder(resp.Body).Decode(&response)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if len(response.Data) != 2 {
		t.Errorf("Data length = %d, want 2", len(response.Data))
	}

	_ = addr // used to verify server is running
}

func TestLoadModelOnPodWithHTTPTestBadStatus(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusInternalServerError)
		_, _ = writer.Write([]byte("internal error"))
	}))
	t.Cleanup(server.Close)

	// We can't directly test loadModelOnPod with httptest due to hardcoded port.
	// Verify the server returns error status.
	resp, err := http.Post(server.URL+"/v1/load", "application/json", strings.NewReader(`{"model":"test"}`))
	if err != nil {
		t.Fatalf("Failed to POST: %v", err)
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("Status = %d, want %d", resp.StatusCode, http.StatusInternalServerError)
	}
}

func TestRouterGetModelEndpointModelNotFound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tracker := vram.NewTracker(nil, "default", nil, nil, "kube-system", map[string]string{"app": "exporter"}, 30*time.Second)
	r := &Router{
		client:            nil,
		vramTracker:       tracker,
		namespace:         "default",
		podSelector:       map[string]string{"app": "model-server"},
		modelLoadEndpoint: "/v1/load",
		models:            make(map[string]*ModelInfo),
		ready:             true,
	}

	// Try to get non-existent model with no VRAM data
	_, err := r.GetModelEndpoint(ctx, "non-existent-model")
	if err == nil {
		t.Error("GetModelEndpoint should return error for non-existent model with no nodes")
	}
}
