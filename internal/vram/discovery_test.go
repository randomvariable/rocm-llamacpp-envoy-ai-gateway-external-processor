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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// Test constants.
const testModifiedValue = "testModifiedValue"

func TestNewKubernetesDiscovery(t *testing.T) {
	t.Parallel()

	config := KubernetesDiscoveryConfig{
		Client:              nil,
		Namespace:           "default",
		PodSelector:         map[string]string{"app": "model"},
		NodeSelector:        map[string]string{"gpu": "true"},
		ExporterNamespace:   "kube-system",
		ExporterPodSelector: map[string]string{"app": "exporter"},
	}

	discovery := NewKubernetesDiscovery(config)

	if discovery == nil {
		t.Fatal("NewKubernetesDiscovery returned nil")
	}

	if discovery.namespace != "default" {
		t.Errorf("namespace = %q, want %q", discovery.namespace, "default")
	}

	if discovery.exporterNamespace != "kube-system" {
		t.Errorf("exporterNamespace = %q, want %q", discovery.exporterNamespace, "kube-system")
	}
}

func TestKubernetesDiscoveryNilClient(t *testing.T) {
	t.Parallel()

	discovery := NewKubernetesDiscovery(KubernetesDiscoveryConfig{
		Client: nil,
	})

	_, err := discovery.DiscoverEndpoints(context.Background())
	if !errors.Is(err, ErrClientNil) {
		t.Errorf("expected ErrClientNil, got %v", err)
	}
}

func TestKubernetesDiscoveryUpdatePodSelector(t *testing.T) {
	t.Parallel()

	discovery := NewKubernetesDiscovery(KubernetesDiscoveryConfig{
		PodSelector: map[string]string{"app": "v1"},
	})

	discovery.UpdatePodSelector(map[string]string{"app": "v2", "version": "2"})

	discovery.mu.RLock()
	defer discovery.mu.RUnlock()

	if discovery.podSelector["app"] != "v2" {
		t.Errorf("podSelector[app] = %q, want %q", discovery.podSelector["app"], "v2")
	}

	if discovery.podSelector["version"] != "2" {
		t.Errorf("podSelector[version] = %q, want %q", discovery.podSelector["version"], "2")
	}
}

func TestKubernetesDiscoveryUpdateNodeSelector(t *testing.T) {
	t.Parallel()

	discovery := NewKubernetesDiscovery(KubernetesDiscoveryConfig{
		NodeSelector: map[string]string{"gpu": "false"},
	})

	discovery.UpdateNodeSelector(map[string]string{"gpu": "true"})

	discovery.mu.RLock()
	defer discovery.mu.RUnlock()

	if discovery.nodeSelector["gpu"] != "true" {
		t.Errorf("nodeSelector[gpu] = %q, want %q", discovery.nodeSelector["gpu"], "true")
	}
}

func TestKubernetesDiscoveryUpdateNamespace(t *testing.T) {
	t.Parallel()

	discovery := NewKubernetesDiscovery(KubernetesDiscoveryConfig{
		Namespace: "default",
	})

	discovery.UpdateNamespace("production")

	discovery.mu.RLock()
	defer discovery.mu.RUnlock()

	if discovery.namespace != "production" {
		t.Errorf("namespace = %q, want %q", discovery.namespace, "production")
	}
}

func TestKubernetesDiscoveryDiscoverEndpoints(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	// Create test resources.
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

	if len(endpoints) > 0 {
		if endpoints[0].NodeName != "node1" {
			t.Errorf("expected node1, got %s", endpoints[0].NodeName)
		}

		if endpoints[0].Address != "10.0.0.5" {
			t.Errorf("expected 10.0.0.5, got %s", endpoints[0].Address)
		}

		if endpoints[0].Port != 9100 {
			t.Errorf("expected port 9100, got %d", endpoints[0].Port)
		}
	}
}

func TestKubernetesDiscoveryFilteredCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		podPhase   corev1.PodPhase
		nodeLabels map[string]string
	}{
		{
			name:       "non-running pods are filtered",
			podPhase:   corev1.PodPending,
			nodeLabels: map[string]string{"gpu": "true"},
		},
		{
			name:       "pods on non-matching nodes are filtered",
			podPhase:   corev1.PodRunning,
			nodeLabels: map[string]string{"gpu": "false"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "model-server-1",
					Namespace: "default",
					Labels:    map[string]string{"app": "model-server"},
				},
				Spec: corev1.PodSpec{
					NodeName: "node1",
				},
				Status: corev1.PodStatus{
					Phase: tt.podPhase,
				},
			}

			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "node1",
					Labels: tt.nodeLabels,
				},
			}

			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod, node).Build()

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

			if len(endpoints) != 0 {
				t.Errorf("expected 0 endpoints, got %d", len(endpoints))
			}
		})
	}
}

func TestStaticDiscoveryDiscoverEndpoints(t *testing.T) {
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
}

func TestStaticDiscoverySetEndpoints(t *testing.T) {
	t.Parallel()

	discovery := NewStaticDiscovery([]MetricsEndpoint{
		{NodeName: "node1", Address: "10.0.0.1", Port: 9100},
	})

	discovery.SetEndpoints([]MetricsEndpoint{
		{NodeName: "node2", Address: "10.0.0.2", Port: 9100},
		{NodeName: "node3", Address: "10.0.0.3", Port: 9100},
	})

	result, err := discovery.DiscoverEndpoints(context.Background())
	if err != nil {
		t.Fatalf("DiscoverEndpoints failed: %v", err)
	}

	if len(result) != 2 {
		t.Errorf("expected 2 endpoints, got %d", len(result))
	}
}

func TestStaticDiscoveryCopySlice(t *testing.T) {
	t.Parallel()

	original := []MetricsEndpoint{
		{NodeName: "node1", Address: "10.0.0.1", Port: 9100},
	}

	discovery := NewStaticDiscovery(original)

	result, _ := discovery.DiscoverEndpoints(context.Background())

	// Modify original.
	original[0].NodeName = testModifiedValue

	// Result should be unaffected.
	if result[0].NodeName == testModifiedValue {
		t.Error("DiscoverEndpoints should return a copy")
	}
}

func TestCopyMap(t *testing.T) {
	t.Parallel()

	original := map[string]string{"key1": "value1", "key2": "value2"}
	copied := copyMap(original)

	if len(copied) != 2 {
		t.Errorf("expected 2 entries, got %d", len(copied))
	}

	// Modify original.
	original["key1"] = testModifiedValue

	// Copy should be unaffected.
	if copied["key1"] != "value1" {
		t.Error("copyMap should create independent copy")
	}
}

func TestCopyMapNil(t *testing.T) {
	t.Parallel()

	result := copyMap(nil)
	if result != nil {
		t.Error("copyMap(nil) should return nil")
	}
}

func TestKubernetesDiscoveryEmptySelectors(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	discovery := NewKubernetesDiscovery(KubernetesDiscoveryConfig{
		Client:              fakeClient,
		Namespace:           "default",
		PodSelector:         map[string]string{},
		NodeSelector:        map[string]string{}, // Empty node selector should match all nodes.
		ExporterNamespace:   "kube-system",
		ExporterPodSelector: map[string]string{},
	})

	endpoints, err := discovery.DiscoverEndpoints(context.Background())
	if err != nil {
		t.Fatalf("DiscoverEndpoints failed: %v", err)
	}

	// Should return empty (no pods match empty label selector).
	if len(endpoints) != 0 {
		t.Errorf("expected 0 endpoints with no matching resources, got %d", len(endpoints))
	}
}

func TestKubernetesDiscoveryMultipleNodes(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	// Create multiple model pods on different nodes.
	objects := []crclient.Object{
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "model-1", Namespace: "default", Labels: map[string]string{"app": "model"}},
			Spec:       corev1.PodSpec{NodeName: "node1"},
			Status:     corev1.PodStatus{Phase: corev1.PodRunning},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "model-2", Namespace: "default", Labels: map[string]string{"app": "model"}},
			Spec:       corev1.PodSpec{NodeName: "node2"},
			Status:     corev1.PodStatus{Phase: corev1.PodRunning},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "exporter-1", Namespace: "kube-system", Labels: map[string]string{"app": "exporter"}},
			Spec: corev1.PodSpec{
				NodeName:   "node1",
				Containers: []corev1.Container{{Ports: []corev1.ContainerPort{{ContainerPort: 9100}}}},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.1"},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "exporter-2", Namespace: "kube-system", Labels: map[string]string{"app": "exporter"}},
			Spec: corev1.PodSpec{
				NodeName:   "node2",
				Containers: []corev1.Container{{Ports: []corev1.ContainerPort{{ContainerPort: 9100}}}},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.2"},
		},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{"gpu": "true"}}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node2", Labels: map[string]string{"gpu": "true"}}},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()

	discovery := NewKubernetesDiscovery(KubernetesDiscoveryConfig{
		Client:              fakeClient,
		Namespace:           "default",
		PodSelector:         map[string]string{"app": "model"},
		NodeSelector:        map[string]string{"gpu": "true"},
		ExporterNamespace:   "kube-system",
		ExporterPodSelector: map[string]string{"app": "exporter"},
	})

	endpoints, err := discovery.DiscoverEndpoints(context.Background())
	if err != nil {
		t.Fatalf("DiscoverEndpoints failed: %v", err)
	}

	if len(endpoints) != 2 {
		t.Errorf("expected 2 endpoints, got %d", len(endpoints))
	}
}
