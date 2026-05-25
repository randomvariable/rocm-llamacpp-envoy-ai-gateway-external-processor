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

// Package podstate exposes a tiny in-memory cache of per-pod load
// signals (running requests, queue depth, KV cache usage) that the
// scheduling plugins observe while filtering candidate pods.
//
// The modeltracker reads from this cache to pick a "winner" pod when
// it discovers the same model loaded on more than one pod and needs to
// reclaim VRAM by unloading the duplicate from the least-pressured pod.
// We can't reach into the gateway-api-inference-extension Datastore
// directly from the tracker (no public handle outside the scheduling
// cycle), so the loaded-model-filter populates this cache as a side
// effect of doing the work it was already doing.
//
// Pressure data is per-pod, last-write-wins, and may go stale if no
// requests have flowed through the EPP recently. Stale data is fine
// for dedup selection — at worst we pick a sub-optimal winner; we
// never unload the last copy.
package podstate

import (
	"sync"
	"time"
)

// Snapshot is one pod's most recent load signal.
type Snapshot struct {
	// PodKey is "namespace/name".
	PodKey string
	// RunningRequestsSize is the number of in-flight requests on the pod.
	RunningRequestsSize int
	// WaitingQueueSize is the number of requests queued behind running ones.
	WaitingQueueSize int
	// KVCacheUsagePercent is 0..1 KV-cache occupancy.
	KVCacheUsagePercent float64
	// UpdateTime is when the snapshot was last refreshed (UTC).
	UpdateTime time.Time
}

// Cache stores the most recent Snapshot per pod.
type Cache struct {
	mu   sync.RWMutex
	data map[string]Snapshot
}

// NewCache returns an empty Cache.
func NewCache() *Cache {
	return &Cache{data: make(map[string]Snapshot)}
}

// Update writes s into the cache, stamping UpdateTime to now.
// PodKey must be non-empty; otherwise the call is a no-op.
func (c *Cache) Update(s Snapshot) {
	if c == nil || s.PodKey == "" {
		return
	}

	s.UpdateTime = time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	c.data[s.PodKey] = s
}

// Pressure returns RunningRequestsSize + WaitingQueueSize as the load
// score for podKey. Returns 0 when the pod is unknown to the cache.
//
// Sum-of-counts is a deliberate cheap proxy: pods serving more
// concurrent work are worth preserving for that model.
func (c *Cache) Pressure(podKey string) float64 {
	if c == nil || podKey == "" {
		return 0
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	s, ok := c.data[podKey]
	if !ok {
		return 0
	}

	return float64(s.RunningRequestsSize + s.WaitingQueueSize)
}

// Get returns the raw Snapshot for podKey and whether it exists.
// Intended for tests and metrics; production callers should use Pressure.
func (c *Cache) Get(podKey string) (Snapshot, bool) {
	if c == nil {
		return Snapshot{}, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	s, ok := c.data[podKey]

	return s, ok
}
