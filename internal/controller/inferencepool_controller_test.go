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

package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	inferencev1 "sigs.k8s.io/gateway-api-inference-extension/api/v1"

	"github.com/randomvariable/rocm-llamacpp-envoy-ai-gateway-external-processor/internal/pool"
)

// testNamespace is the namespace used for all tests.
const testNamespace = "default"

// newTestScheme creates a scheme with all necessary types for testing.
func newTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = inferencev1.Install(scheme)

	return scheme
}

// newTestClient creates a fake controller-runtime client for testing.
func newTestClient(objs ...crclient.Object) crclient.Client {
	return fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(objs...).
		Build()
}

func TestReconcileCreatePool(t *testing.T) {
	t.Parallel()

	// Create an InferencePool
	inferencePool := &inferencev1.InferencePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pool",
			Namespace: testNamespace,
		},
		Spec: inferencev1.InferencePoolSpec{
			Selector: inferencev1.LabelSelector{
				MatchLabels: map[inferencev1.LabelKey]inferencev1.LabelValue{
					"app": "model-server",
				},
			},
			TargetPorts: []inferencev1.Port{
				{Number: 8080},
			},
		},
	}

	// Create fake client with the InferencePool
	fakeClient := newTestClient(inferencePool)

	// Create pool manager with the same fake client
	poolManager := pool.NewManager(fakeClient, "kube-system", map[string]string{"app": "amd-gpu-metrics-exporter-amdgpu-metrics-exporter"}, "/v1/load")

	// Create reconciler
	reconciler := &InferencePoolReconciler{
		Client:      fakeClient,
		PoolManager: poolManager,
	}

	// Reconcile
	req := reconcile.Request{}
	req.Namespace = testNamespace
	req.Name = "test-pool"

	result, err := reconciler.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	if !result.IsZero() {
		t.Error("Result should not requeue")
	}

	// Verify pool was created
	poolKey := pool.PoolKey{Namespace: testNamespace, Name: "test-pool"}

	createdPool, exists := poolManager.GetPool(poolKey)
	if !exists {
		t.Fatal("Pool should have been created")
	}

	if createdPool.PodSelector["app"] != "model-server" {
		t.Errorf("PodSelector[app] = %q, want %q", createdPool.PodSelector["app"], "model-server")
	}

	if len(createdPool.TargetPorts) != 1 || createdPool.TargetPorts[0] != 8080 {
		t.Errorf("TargetPorts = %v, want [8080]", createdPool.TargetPorts)
	}
}

func TestReconcileDeletePool(t *testing.T) {
	t.Parallel()

	// Create fake client without the pool (simulating deletion)
	fakeClient := newTestClient()

	// Create pool manager with existing pool
	poolManager := pool.NewManager(fakeClient, "kube-system", map[string]string{"app": "amd-gpu-metrics-exporter-amdgpu-metrics-exporter"}, "/v1/load")
	ctx := context.Background()
	poolKey := pool.PoolKey{Namespace: testNamespace, Name: "deleted-pool"}

	err := poolManager.UpsertPool(ctx, poolKey, map[string]string{"app": "test"}, []int32{8080})
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Verify pool exists
	_, exists := poolManager.GetPool(poolKey)
	if !exists {
		t.Fatal("Pool should exist before reconcile")
	}

	// Create reconciler
	reconciler := &InferencePoolReconciler{
		Client:      fakeClient,
		PoolManager: poolManager,
	}

	// Reconcile the deleted pool
	req := reconcile.Request{}
	req.Namespace = testNamespace
	req.Name = "deleted-pool"

	result, err := reconciler.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	if !result.IsZero() {
		t.Error("Result should not requeue")
	}

	// Verify pool was deleted
	_, exists = poolManager.GetPool(poolKey)
	if exists {
		t.Error("Pool should have been deleted")
	}
}

func TestReconcileUpdatePool(t *testing.T) {
	t.Parallel()

	// Create an InferencePool with updated spec
	inferencePool := &inferencev1.InferencePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pool",
			Namespace: testNamespace,
		},
		Spec: inferencev1.InferencePoolSpec{
			Selector: inferencev1.LabelSelector{
				MatchLabels: map[inferencev1.LabelKey]inferencev1.LabelValue{
					"app":     "model-server-v2",
					"version": "2",
				},
			},
			TargetPorts: []inferencev1.Port{
				{Number: 8081},
				{Number: 8082},
			},
		},
	}

	// Create fake client
	fakeClient := newTestClient(inferencePool)

	// Create pool manager with existing pool
	poolManager := pool.NewManager(fakeClient, "kube-system", map[string]string{"app": "amd-gpu-metrics-exporter-amdgpu-metrics-exporter"}, "/v1/load")
	ctx := context.Background()
	poolKey := pool.PoolKey{Namespace: testNamespace, Name: "test-pool"}

	err := poolManager.UpsertPool(ctx, poolKey, map[string]string{"app": "model-server-v1"}, []int32{8080})
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Create reconciler
	reconciler := &InferencePoolReconciler{
		Client:      fakeClient,
		PoolManager: poolManager,
	}

	// Reconcile
	req := reconcile.Request{}
	req.Namespace = testNamespace
	req.Name = "test-pool"

	result, err := reconciler.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	if !result.IsZero() {
		t.Error("Result should not requeue")
	}

	// Verify pool was updated
	updatedPool, exists := poolManager.GetPool(poolKey)
	if !exists {
		t.Fatal("Pool should exist after reconcile")
	}

	if updatedPool.PodSelector["app"] != "model-server-v2" {
		t.Errorf("PodSelector[app] = %q, want %q", updatedPool.PodSelector["app"], "model-server-v2")
	}

	if updatedPool.PodSelector["version"] != "2" {
		t.Errorf("PodSelector[version] = %q, want %q", updatedPool.PodSelector["version"], "2")
	}

	if len(updatedPool.TargetPorts) != 2 {
		t.Errorf("TargetPorts length = %d, want 2", len(updatedPool.TargetPorts))
	}
}

func TestReconcileMultiplePools(t *testing.T) {
	t.Parallel()

	// Create two InferencePools
	pool1 := &inferencev1.InferencePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-1",
			Namespace: testNamespace,
		},
		Spec: inferencev1.InferencePoolSpec{
			Selector: inferencev1.LabelSelector{
				MatchLabels: map[inferencev1.LabelKey]inferencev1.LabelValue{
					"app": "model-server-1",
				},
			},
			TargetPorts: []inferencev1.Port{
				{Number: 8080},
			},
		},
	}

	pool2 := &inferencev1.InferencePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-2",
			Namespace: "production",
		},
		Spec: inferencev1.InferencePoolSpec{
			Selector: inferencev1.LabelSelector{
				MatchLabels: map[inferencev1.LabelKey]inferencev1.LabelValue{
					"app": "model-server-2",
				},
			},
			TargetPorts: []inferencev1.Port{
				{Number: 9090},
			},
		},
	}

	// Create fake client
	fakeClient := newTestClient(pool1, pool2)

	// Create pool manager
	poolManager := pool.NewManager(fakeClient, "kube-system", map[string]string{"app": "amd-gpu-metrics-exporter-amdgpu-metrics-exporter"}, "/v1/load")
	ctx := context.Background()

	// Create reconciler
	reconciler := &InferencePoolReconciler{
		Client:      fakeClient,
		PoolManager: poolManager,
	}

	// Reconcile first pool
	req1 := reconcile.Request{}
	req1.Namespace = testNamespace
	req1.Name = "pool-1"

	_, err := reconciler.Reconcile(ctx, req1)
	if err != nil {
		t.Fatalf("Reconcile pool-1 failed: %v", err)
	}

	// Reconcile second pool
	req2 := reconcile.Request{}
	req2.Namespace = "production"
	req2.Name = "pool-2"

	_, err = reconciler.Reconcile(ctx, req2)
	if err != nil {
		t.Fatalf("Reconcile pool-2 failed: %v", err)
	}

	// Verify both pools exist
	if poolManager.PoolCount() != 2 {
		t.Errorf("PoolCount = %d, want 2", poolManager.PoolCount())
	}

	pool1Key := pool.PoolKey{Namespace: testNamespace, Name: "pool-1"}

	_, exists := poolManager.GetPool(pool1Key)
	if !exists {
		t.Error("pool-1 should exist")
	}

	pool2Key := pool.PoolKey{Namespace: "production", Name: "pool-2"}

	_, exists = poolManager.GetPool(pool2Key)
	if !exists {
		t.Error("pool-2 should exist")
	}
}
