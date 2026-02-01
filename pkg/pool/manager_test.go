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

package pool

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newFakeClient() crclient.Client {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	return fake.NewClientBuilder().
		WithScheme(scheme).
		Build()
}

func TestPoolKeyString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		key      PoolKey
		expected string
	}{
		{
			name:     "simple key",
			key:      PoolKey{Namespace: "default", Name: "test-pool"},
			expected: "default/test-pool",
		},
		{
			name:     "empty namespace",
			key:      PoolKey{Namespace: "", Name: "pool"},
			expected: "/pool",
		},
		{
			name:     "empty name",
			key:      PoolKey{Namespace: "ns", Name: ""},
			expected: "ns/",
		},
		{
			name:     "both empty",
			key:      PoolKey{Namespace: "", Name: ""},
			expected: "/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := tt.key.String()
			if result != tt.expected {
				t.Errorf("PoolKey.String() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestNewManager(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	manager := NewManager(fakeClient, "kube-system", map[string]string{"app": "amd-gpu-metrics-exporter-amdgpu-metrics-exporter"}, "/v1/load")

	if manager == nil {
		t.Fatal("NewManager returned nil")
	}

	if manager.pools == nil {
		t.Error("pools map should be initialized")
	}

	if manager.exporterNamespace != "kube-system" {
		t.Errorf("exporterNamespace = %q, want %q", manager.exporterNamespace, "kube-system")
	}

	if manager.modelLoadEndpoint != "/v1/load" {
		t.Errorf("modelLoadEndpoint = %q, want %q", manager.modelLoadEndpoint, "/v1/load")
	}
}

func TestManagerUpsertAndGetPool(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	manager := NewManager(fakeClient, "kube-system", map[string]string{"app": "amd-gpu-metrics-exporter-amdgpu-metrics-exporter"}, "/v1/load")
	ctx := context.Background()

	key := PoolKey{Namespace: "default", Name: "test-pool"}
	selector := map[string]string{"app": "model-server"}
	ports := []int32{8080}

	// Upsert a new pool
	err := manager.UpsertPool(ctx, key, selector, ports)
	if err != nil {
		t.Fatalf("UpsertPool failed: %v", err)
	}

	// Verify pool was created
	pool, exists := manager.GetPool(key)
	if !exists {
		t.Fatal("GetPool should return true for existing pool")
	}

	if pool == nil {
		t.Fatal("GetPool should return non-nil pool")
	}

	if pool.Key != key {
		t.Errorf("pool.Key = %v, want %v", pool.Key, key)
	}

	if pool.PodSelector["app"] != "model-server" {
		t.Errorf("pool.PodSelector[app] = %q, want %q", pool.PodSelector["app"], "model-server")
	}

	if len(pool.TargetPorts) != 1 || pool.TargetPorts[0] != 8080 {
		t.Errorf("pool.TargetPorts = %v, want [8080]", pool.TargetPorts)
	}
}

func TestManagerUpsertPoolUpdate(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	manager := NewManager(fakeClient, "kube-system", map[string]string{"app": "amd-gpu-metrics-exporter-amdgpu-metrics-exporter"}, "/v1/load")
	ctx := context.Background()

	key := PoolKey{Namespace: "default", Name: "test-pool"}

	// Create initial pool
	err := manager.UpsertPool(ctx, key, map[string]string{"app": "v1"}, []int32{8080})
	if err != nil {
		t.Fatalf("First UpsertPool failed: %v", err)
	}

	// Update the pool
	err = manager.UpsertPool(ctx, key, map[string]string{"app": "v2"}, []int32{8081, 8082})
	if err != nil {
		t.Fatalf("Second UpsertPool failed: %v", err)
	}

	// Verify pool was updated
	pool, exists := manager.GetPool(key)
	if !exists {
		t.Fatal("GetPool should return true")
	}

	if pool.PodSelector["app"] != "v2" {
		t.Errorf("pool.PodSelector[app] = %q, want %q", pool.PodSelector["app"], "v2")
	}

	if len(pool.TargetPorts) != 2 {
		t.Errorf("pool.TargetPorts length = %d, want 2", len(pool.TargetPorts))
	}
}

func TestManagerDeletePool(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	manager := NewManager(fakeClient, "kube-system", map[string]string{"app": "amd-gpu-metrics-exporter-amdgpu-metrics-exporter"}, "/v1/load")
	ctx := context.Background()

	key := PoolKey{Namespace: "default", Name: "test-pool"}
	selector := map[string]string{"app": "model-server"}

	// Create pool
	err := manager.UpsertPool(ctx, key, selector, []int32{8080})
	if err != nil {
		t.Fatalf("UpsertPool failed: %v", err)
	}

	// Delete pool
	manager.DeletePool(key)

	// Verify pool was deleted
	_, exists := manager.GetPool(key)
	if exists {
		t.Error("GetPool should return false after deletion")
	}
}

func TestManagerDeletePoolNonExistent(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	manager := NewManager(fakeClient, "kube-system", map[string]string{"app": "amd-gpu-metrics-exporter-amdgpu-metrics-exporter"}, "/v1/load")
	key := PoolKey{Namespace: "default", Name: "non-existent"}

	// Should not panic
	manager.DeletePool(key)
}

func TestManagerGetPoolNonExistent(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	manager := NewManager(fakeClient, "kube-system", map[string]string{"app": "amd-gpu-metrics-exporter-amdgpu-metrics-exporter"}, "/v1/load")
	key := PoolKey{Namespace: "default", Name: "non-existent"}

	pool, exists := manager.GetPool(key)
	if exists {
		t.Error("GetPool should return false for non-existent pool")
	}

	if pool != nil {
		t.Error("GetPool should return nil for non-existent pool")
	}
}

func TestManagerGetPoolByNamespacedName(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	manager := NewManager(fakeClient, "kube-system", map[string]string{"app": "amd-gpu-metrics-exporter-amdgpu-metrics-exporter"}, "/v1/load")
	ctx := context.Background()

	key := PoolKey{Namespace: "test-ns", Name: "test-pool"}
	selector := map[string]string{"app": "model-server"}

	err := manager.UpsertPool(ctx, key, selector, []int32{8080})
	if err != nil {
		t.Fatalf("UpsertPool failed: %v", err)
	}

	objectKey := crclient.ObjectKey{Namespace: "test-ns", Name: "test-pool"}
	pool, exists := manager.GetPoolByNamespacedName(objectKey)

	if !exists {
		t.Error("GetPoolByNamespacedName should return true for existing pool")
	}

	if pool == nil {
		t.Error("GetPoolByNamespacedName should return non-nil pool")
	}
}

func TestManagerDefaultPool(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	manager := NewManager(fakeClient, "kube-system", map[string]string{"app": "amd-gpu-metrics-exporter-amdgpu-metrics-exporter"}, "/v1/load")
	ctx := context.Background()

	// Initially no default pool
	defaultPool, exists := manager.GetDefaultPool()
	if exists {
		t.Error("GetDefaultPool should return false when no pools exist")
	}

	if defaultPool != nil {
		t.Error("GetDefaultPool should return nil when no pools exist")
	}

	// First pool becomes default
	key1 := PoolKey{Namespace: "default", Name: "pool-1"}

	err := manager.UpsertPool(ctx, key1, map[string]string{"app": "v1"}, []int32{8080})
	if err != nil {
		t.Fatalf("UpsertPool failed: %v", err)
	}

	defaultPool, exists = manager.GetDefaultPool()
	if !exists {
		t.Error("GetDefaultPool should return true after adding a pool")
	}

	if defaultPool.Key != key1 {
		t.Errorf("Default pool key = %v, want %v", defaultPool.Key, key1)
	}
}

func TestManagerDefaultPoolAfterDeletion(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	manager := NewManager(fakeClient, "kube-system", map[string]string{"app": "amd-gpu-metrics-exporter-amdgpu-metrics-exporter"}, "/v1/load")
	ctx := context.Background()

	// Create two pools
	key1 := PoolKey{Namespace: "default", Name: "pool-1"}
	key2 := PoolKey{Namespace: "default", Name: "pool-2"}

	err := manager.UpsertPool(ctx, key1, map[string]string{"app": "v1"}, []int32{8080})
	if err != nil {
		t.Fatalf("UpsertPool failed: %v", err)
	}

	err = manager.UpsertPool(ctx, key2, map[string]string{"app": "v2"}, []int32{8081})
	if err != nil {
		t.Fatalf("UpsertPool failed: %v", err)
	}

	// Delete default pool
	manager.DeletePool(key1)

	// A new default should be set
	defaultPool, exists := manager.GetDefaultPool()
	if !exists {
		t.Error("GetDefaultPool should return true when other pools exist")
	}

	if defaultPool == nil {
		t.Error("GetDefaultPool should return non-nil pool")
	}
}

func TestManagerGetRouter(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	manager := NewManager(fakeClient, "kube-system", map[string]string{"app": "amd-gpu-metrics-exporter-amdgpu-metrics-exporter"}, "/v1/load")
	ctx := context.Background()

	key := PoolKey{Namespace: "default", Name: "test-pool"}
	selector := map[string]string{"app": "model-server"}

	err := manager.UpsertPool(ctx, key, selector, []int32{8080})
	if err != nil {
		t.Fatalf("UpsertPool failed: %v", err)
	}

	router, err := manager.GetRouter(key)
	if err != nil {
		t.Fatalf("GetRouter failed: %v", err)
	}

	if router == nil {
		t.Error("GetRouter should return non-nil router")
	}
}

func TestManagerGetRouterNonExistent(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	manager := NewManager(fakeClient, "kube-system", map[string]string{"app": "amd-gpu-metrics-exporter-amdgpu-metrics-exporter"}, "/v1/load")
	key := PoolKey{Namespace: "default", Name: "non-existent"}

	_, err := manager.GetRouter(key)
	if !errors.Is(err, ErrNoPoolAvailable) {
		t.Errorf("GetRouter should return ErrNoPoolAvailable, got %v", err)
	}
}

func TestManagerGetRouterFallbackToDefault(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	manager := NewManager(fakeClient, "kube-system", map[string]string{"app": "amd-gpu-metrics-exporter-amdgpu-metrics-exporter"}, "/v1/load")
	ctx := context.Background()

	// Create a default pool
	key := PoolKey{Namespace: "default", Name: "default-pool"}
	selector := map[string]string{"app": "model-server"}

	err := manager.UpsertPool(ctx, key, selector, []int32{8080})
	if err != nil {
		t.Fatalf("UpsertPool failed: %v", err)
	}

	// Request router for non-existent pool - should fallback to default
	nonExistentKey := PoolKey{Namespace: "default", Name: "non-existent"}

	router, err := manager.GetRouter(nonExistentKey)
	if err != nil {
		t.Fatalf("GetRouter should not error when default pool exists: %v", err)
	}

	if router == nil {
		t.Error("GetRouter should return default pool's router")
	}
}

func TestManagerGetDefaultRouter(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	manager := NewManager(fakeClient, "kube-system", map[string]string{"app": "amd-gpu-metrics-exporter-amdgpu-metrics-exporter"}, "/v1/load")
	ctx := context.Background()

	// Initially no default router
	_, err := manager.GetDefaultRouter()
	if !errors.Is(err, ErrNoDefaultPoolAvailable) {
		t.Errorf("GetDefaultRouter should return ErrNoDefaultPoolAvailable, got %v", err)
	}

	// Add a pool
	key := PoolKey{Namespace: "default", Name: "test-pool"}

	err = manager.UpsertPool(ctx, key, map[string]string{"app": "v1"}, []int32{8080})
	if err != nil {
		t.Fatalf("UpsertPool failed: %v", err)
	}

	router, err := manager.GetDefaultRouter()
	if err != nil {
		t.Fatalf("GetDefaultRouter failed: %v", err)
	}

	if router == nil {
		t.Error("GetDefaultRouter should return non-nil router")
	}
}

func TestManagerListPools(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	manager := NewManager(fakeClient, "kube-system", map[string]string{"app": "amd-gpu-metrics-exporter-amdgpu-metrics-exporter"}, "/v1/load")
	ctx := context.Background()

	// Initially empty
	pools := manager.ListPools()
	if len(pools) != 0 {
		t.Errorf("ListPools should return empty slice, got %d pools", len(pools))
	}

	// Add pools
	key1 := PoolKey{Namespace: "ns1", Name: "pool-1"}
	key2 := PoolKey{Namespace: "ns2", Name: "pool-2"}

	err := manager.UpsertPool(ctx, key1, map[string]string{"app": "v1"}, []int32{8080})
	if err != nil {
		t.Fatalf("UpsertPool failed: %v", err)
	}

	err = manager.UpsertPool(ctx, key2, map[string]string{"app": "v2"}, []int32{8081})
	if err != nil {
		t.Fatalf("UpsertPool failed: %v", err)
	}

	pools = manager.ListPools()
	if len(pools) != 2 {
		t.Errorf("ListPools should return 2 pools, got %d", len(pools))
	}
}

func TestManagerPoolCount(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	manager := NewManager(fakeClient, "kube-system", map[string]string{"app": "amd-gpu-metrics-exporter-amdgpu-metrics-exporter"}, "/v1/load")
	ctx := context.Background()

	if manager.PoolCount() != 0 {
		t.Errorf("PoolCount should be 0, got %d", manager.PoolCount())
	}

	key := PoolKey{Namespace: "default", Name: "test-pool"}

	err := manager.UpsertPool(ctx, key, map[string]string{"app": "v1"}, []int32{8080})
	if err != nil {
		t.Fatalf("UpsertPool failed: %v", err)
	}

	if manager.PoolCount() != 1 {
		t.Errorf("PoolCount should be 1, got %d", manager.PoolCount())
	}

	manager.DeletePool(key)

	if manager.PoolCount() != 0 {
		t.Errorf("PoolCount should be 0 after deletion, got %d", manager.PoolCount())
	}
}

func TestErrors(t *testing.T) {
	t.Parallel()

	if ErrNoPoolAvailable.Error() != "no pool available" {
		t.Errorf("ErrNoPoolAvailable message = %q, want %q", ErrNoPoolAvailable.Error(), "no pool available")
	}

	if ErrNoDefaultPoolAvailable.Error() != "no default pool available" {
		t.Errorf("ErrNoDefaultPoolAvailable message = %q, want %q", ErrNoDefaultPoolAvailable.Error(), "no default pool available")
	}
}
