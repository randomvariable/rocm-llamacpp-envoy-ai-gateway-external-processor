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

// Package pool provides management of multiple InferencePools.
package pool

import (
	"context"
	"errors"
	"sync"

	"k8s.io/klog/v2"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/randomvariable/rocm-llamacpp-envoy-ai-gateway-external-processor/internal/router"
	"github.com/randomvariable/rocm-llamacpp-envoy-ai-gateway-external-processor/internal/vram"
)

// Error definitions for the pool package.
var (
	ErrNoPoolAvailable        = errors.New("no pool available")
	ErrNoDefaultPoolAvailable = errors.New("no default pool available")
)

// PoolKey uniquely identifies an InferencePool.
type PoolKey struct {
	Namespace string
	Name      string
}

// String returns a string representation of the pool key.
func (k PoolKey) String() string {
	return k.Namespace + "/" + k.Name
}

// PoolConfig contains the configuration for a single InferencePool.
type PoolConfig struct {
	Key         PoolKey
	PodSelector map[string]string
	TargetPorts []int32
	Router      *router.Router
	VRAMTracker *vram.Tracker
}

// Manager manages multiple InferencePools and their associated routers/trackers.
type Manager struct {
	client              crclient.Client
	exporterNamespace   string
	exporterPodSelector map[string]string
	modelLoadEndpoint   string

	mu    sync.RWMutex
	pools map[PoolKey]*PoolConfig

	// Default pool for backwards compatibility.
	defaultPool *PoolConfig
}

// NewManager creates a new pool manager.
func NewManager(
	k8sClient crclient.Client,
	exporterNamespace string,
	exporterPodSelector map[string]string,
	modelLoadEndpoint string,
) *Manager {
	return &Manager{
		client:              k8sClient,
		exporterNamespace:   exporterNamespace,
		exporterPodSelector: exporterPodSelector,
		modelLoadEndpoint:   modelLoadEndpoint,
		pools:               make(map[PoolKey]*PoolConfig),
	}
}

// UpsertPool creates or updates an InferencePool configuration.
// It creates dedicated Router and VRAMTracker instances for the pool.
// The VRAM tracker is started in a goroutine to begin collecting metrics.
func (m *Manager) UpsertPool(ctx context.Context, key PoolKey, podSelector map[string]string, targetPorts []int32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, exists := m.pools[key]
	if exists {
		// Update existing pool's selectors.
		klog.Infof("Updating existing pool %s with selector: %v", key, podSelector)

		existing.PodSelector = podSelector
		existing.TargetPorts = targetPorts

		// Update the router and tracker with new selectors.
		existing.Router.UpdatePodSelector(ctx, podSelector)
		existing.Router.UpdateNamespace(key.Namespace)
		existing.VRAMTracker.UpdatePodSelector(ctx, podSelector)
		existing.VRAMTracker.UpdateNamespace(key.Namespace)

		return nil
	}

	// Create new pool with dedicated router and tracker.
	klog.Infof("Creating new pool %s with selector: %v", key, podSelector)

	// Create VRAM tracker for this pool.
	tracker := vram.NewTracker(
		m.client,
		key.Namespace,
		podSelector,
		nil, // nodeSelector - can be extended later
		m.exporterNamespace,
		m.exporterPodSelector,
		vram.DefaultScrapeInterval,
	)

	// Start the VRAM tracker to begin collecting metrics.
	go tracker.Start(ctx)

	// Create router for this pool.
	poolRouter := router.NewRouter(
		ctx,
		m.client,
		tracker,
		key.Namespace,
		podSelector,
		m.modelLoadEndpoint,
	)

	pool := &PoolConfig{
		Key:         key,
		PodSelector: podSelector,
		TargetPorts: targetPorts,
		Router:      poolRouter,
		VRAMTracker: tracker,
	}

	m.pools[key] = pool

	// If this is the first pool, set it as default.
	if m.defaultPool == nil {
		m.defaultPool = pool

		klog.Infof("Set pool %s as default", key)
	}

	return nil
}

// DeletePool removes an InferencePool.
func (m *Manager) DeletePool(key PoolKey) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pool, exists := m.pools[key]
	if !exists {
		return
	}

	klog.Infof("Deleting pool %s", key)

	delete(m.pools, key)

	// If this was the default pool, select a new default.
	if m.defaultPool == pool {
		m.defaultPool = nil

		for _, p := range m.pools {
			m.defaultPool = p
			klog.Infof("Set pool %s as new default", p.Key)

			break
		}
	}
}

// GetPool returns the pool configuration for a given key.
func (m *Manager) GetPool(key PoolKey) (*PoolConfig, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	pool, exists := m.pools[key]

	return pool, exists
}

// GetPoolByNamespacedName returns the pool by namespace/name from a controller-runtime client key.
func (m *Manager) GetPoolByNamespacedName(key crclient.ObjectKey) (*PoolConfig, bool) {
	return m.GetPool(PoolKey{Namespace: key.Namespace, Name: key.Name})
}

// GetDefaultPool returns the default pool (first registered or explicitly set).
func (m *Manager) GetDefaultPool() (*PoolConfig, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.defaultPool, m.defaultPool != nil
}

// GetRouter returns the router for a specific pool.
// If the pool doesn't exist, falls back to the default pool.
func (m *Manager) GetRouter(key PoolKey) (*router.Router, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if pool, exists := m.pools[key]; exists {
		return pool.Router, nil
	}

	if m.defaultPool != nil {
		return m.defaultPool.Router, nil
	}

	return nil, ErrNoPoolAvailable
}

// GetDefaultRouter returns the router for the default pool.
func (m *Manager) GetDefaultRouter() (*router.Router, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.defaultPool != nil {
		return m.defaultPool.Router, nil
	}

	return nil, ErrNoDefaultPoolAvailable
}

// ListPools returns all registered pool keys.
func (m *Manager) ListPools() []PoolKey {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys := make([]PoolKey, 0, len(m.pools))

	for k := range m.pools {
		keys = append(keys, k)
	}

	return keys
}

// PoolCount returns the number of registered pools.
func (m *Manager) PoolCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.pools)
}

// IsReady returns true if at least one pool is registered and ready.
func (m *Manager) IsReady() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, pool := range m.pools {
		if pool.Router.IsReady() {
			return true
		}
	}

	return false
}
