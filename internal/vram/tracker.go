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
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
	"k8s.io/klog/v2"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

const logVerbosity = 2

// Tracker tracks VRAM usage across GPU nodes.
// It coordinates metric scraping from discovered endpoints and provides
// VRAM-aware node selection.
type Tracker struct {
	scraper         MetricsScraper
	discovery       NodeDiscovery
	scrapeInterval  time.Duration
	instrumentation Instrumentation

	mu      sync.RWMutex
	metrics map[string]*Metrics // node name -> metrics

	// k8sDiscovery is kept for access to UpdatePodSelector/UpdateNodeSelector/UpdateNamespace methods.
	k8sDiscovery *KubernetesDiscovery
}

// NewTrackerWithDeps creates a new VRAM tracker with custom dependencies.
// This is the recommended constructor for maximum flexibility.
func NewTrackerWithDeps(scraper MetricsScraper, discovery NodeDiscovery, opts ...TrackerOption) *Tracker {
	options := &trackerOptions{
		scrapeInterval: DefaultScrapeInterval,
	}

	for _, opt := range opts {
		opt(options)
	}

	return &Tracker{
		scraper:         scraper,
		discovery:       discovery,
		scrapeInterval:  options.scrapeInterval,
		instrumentation: options.instrumentation,
		metrics:         make(map[string]*Metrics),
	}
}

// NewTracker creates a new VRAM tracker with Kubernetes discovery and Prometheus scraping.
func NewTracker(
	k8sClient crclient.Client,
	namespace string,
	podSelector, nodeSelector map[string]string,
	exporterNamespace string,
	exporterPodSelector map[string]string,
	scrapeInterval time.Duration,
) *Tracker {
	k8sDiscovery := NewKubernetesDiscovery(KubernetesDiscoveryConfig{
		Client:              k8sClient,
		Namespace:           namespace,
		PodSelector:         podSelector,
		NodeSelector:        nodeSelector,
		ExporterNamespace:   exporterNamespace,
		ExporterPodSelector: exporterPodSelector,
	})

	scraper := NewPrometheusScraper()

	return &Tracker{
		scraper:        scraper,
		discovery:      k8sDiscovery,
		scrapeInterval: scrapeInterval,
		metrics:        make(map[string]*Metrics),
		k8sDiscovery:   k8sDiscovery, // Keep reference for legacy update methods.
	}
}

// NewTrackerWithMeter creates a new VRAM tracker with OpenTelemetry metrics.
func NewTrackerWithMeter(
	k8sClient crclient.Client,
	namespace string,
	podSelector, nodeSelector map[string]string,
	exporterNamespace string,
	exporterPodSelector map[string]string,
	scrapeInterval time.Duration,
	otelMeter metric.Meter,
) *Tracker {
	k8sDiscovery := NewKubernetesDiscovery(KubernetesDiscoveryConfig{
		Client:              k8sClient,
		Namespace:           namespace,
		PodSelector:         podSelector,
		NodeSelector:        nodeSelector,
		ExporterNamespace:   exporterNamespace,
		ExporterPodSelector: exporterPodSelector,
	})

	scraper := NewPrometheusScraper()
	instrumentation := MustNewOTelInstrumentation(otelMeter)

	return &Tracker{
		scraper:         scraper,
		discovery:       k8sDiscovery,
		scrapeInterval:  scrapeInterval,
		instrumentation: instrumentation,
		metrics:         make(map[string]*Metrics),
		k8sDiscovery:    k8sDiscovery,
	}
}

// Start begins tracking VRAM metrics.
// It runs until the context is cancelled.
func (t *Tracker) Start(ctx context.Context) {
	ticker := time.NewTicker(t.scrapeInterval)
	defer ticker.Stop()

	// Initial scrape.
	t.scrapeMetrics(ctx)

	for {
		select {
		case <-ctx.Done():
			klog.Info("VRAM tracker stopping")

			return
		case <-ticker.C:
			t.scrapeMetrics(ctx)
		}
	}
}

// GetMetrics returns current VRAM metrics for all nodes.
// Returns a copy of the metrics to avoid race conditions.
func (t *Tracker) GetMetrics() map[string]*Metrics {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make(map[string]*Metrics, len(t.metrics))

	for k, v := range t.metrics {
		result[k] = &Metrics{
			TotalVRAM:     v.TotalVRAM,
			UsedVRAM:      v.UsedVRAM,
			AvailableVRAM: v.AvailableVRAM,
			LastUpdate:    v.LastUpdate,
		}
	}

	return result
}

// GetNodeWithMostAvailableVRAM returns the node with the most available VRAM.
// Returns the node name, available VRAM in bytes, and any error.
func (t *Tracker) GetNodeWithMostAvailableVRAM() (nodeName string, availableVRAM int64, err error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if len(t.metrics) == 0 {
		return "", 0, ErrNoNodes
	}

	var (
		bestNode     string
		maxAvailable int64
	)

	for name, nodeMetrics := range t.metrics {
		if nodeMetrics.AvailableVRAM > maxAvailable {
			maxAvailable = nodeMetrics.AvailableVRAM
			bestNode = name
		}
	}

	if bestNode == "" {
		return "", 0, ErrNoSuitableNode
	}

	return bestNode, maxAvailable, nil
}

// GetNodeVRAM returns VRAM metrics for a specific node.
// Returns a copy of the metrics to avoid race conditions.
func (t *Tracker) GetNodeVRAM(nodeName string) (*Metrics, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	nodeMetrics, ok := t.metrics[nodeName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNoMetrics, nodeName)
	}

	return &Metrics{
		TotalVRAM:     nodeMetrics.TotalVRAM,
		UsedVRAM:      nodeMetrics.UsedVRAM,
		AvailableVRAM: nodeMetrics.AvailableVRAM,
		LastUpdate:    nodeMetrics.LastUpdate,
	}, nil
}

// UpdatePodSelector updates the pod selector used for discovering pods.
// Only works when using Kubernetes discovery.
func (t *Tracker) UpdatePodSelector(ctx context.Context, selector map[string]string) {
	if t.k8sDiscovery != nil {
		t.k8sDiscovery.UpdatePodSelector(selector)
		klog.Infof("VRAM tracker updated pod selector to: %v", selector)

		// Trigger immediate rescrape.
		go t.scrapeMetrics(ctx)
	}
}

// UpdateNodeSelector updates the node selector used for filtering nodes.
// Only works when using Kubernetes discovery.
func (t *Tracker) UpdateNodeSelector(selector map[string]string) {
	if t.k8sDiscovery != nil {
		t.k8sDiscovery.UpdateNodeSelector(selector)
		klog.Infof("VRAM tracker updated node selector to: %v", selector)

		// Trigger immediate rescrape.
		go t.scrapeMetrics(context.Background())
	}
}

// UpdateNamespace updates the namespace used for pod discovery.
// Only works when using Kubernetes discovery.
func (t *Tracker) UpdateNamespace(namespace string) {
	if t.k8sDiscovery != nil {
		t.k8sDiscovery.UpdateNamespace(namespace)
		klog.Infof("VRAM tracker updated namespace to: %s", namespace)
	}
}

// SetMetrics directly sets metrics for a node.
// Primarily intended for testing.
func (t *Tracker) SetMetrics(nodeName string, nodeMetrics *Metrics) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.metrics[nodeName] = &Metrics{
		TotalVRAM:     nodeMetrics.TotalVRAM,
		UsedVRAM:      nodeMetrics.UsedVRAM,
		AvailableVRAM: nodeMetrics.AvailableVRAM,
		LastUpdate:    nodeMetrics.LastUpdate,
	}
}

// scrapeMetrics discovers endpoints and scrapes metrics from each.
func (t *Tracker) scrapeMetrics(ctx context.Context) {
	if t.discovery == nil {
		klog.V(logVerbosity).Info("Discovery is nil, skipping VRAM scrape")

		return
	}

	endpoints, err := t.discovery.DiscoverEndpoints(ctx)
	if err != nil {
		klog.Errorf("Failed to discover endpoints: %v", err)

		return
	}

	for _, endpoint := range endpoints {
		nodeMetrics, err := t.scraper.Scrape(ctx, endpoint)
		if err != nil {
			klog.Errorf("Failed to scrape metrics from node %s (%s:%d): %v",
				endpoint.NodeName, endpoint.Address, endpoint.Port, err)

			continue
		}

		t.mu.Lock()
		t.metrics[endpoint.NodeName] = nodeMetrics
		t.mu.Unlock()

		klog.V(logVerbosity).Infof("Updated VRAM metrics for node %s: total=%d, used=%d, available=%d",
			endpoint.NodeName, nodeMetrics.TotalVRAM, nodeMetrics.UsedVRAM, nodeMetrics.AvailableVRAM)
	}

	// Record metrics for instrumentation.
	if t.instrumentation != nil {
		t.mu.RLock()

		metricsCopy := make(map[string]*Metrics, len(t.metrics))
		for k, v := range t.metrics {
			metricsCopy[k] = &Metrics{
				TotalVRAM:     v.TotalVRAM,
				UsedVRAM:      v.UsedVRAM,
				AvailableVRAM: v.AvailableVRAM,
				LastUpdate:    v.LastUpdate,
			}
		}

		t.mu.RUnlock()

		t.instrumentation.RecordMetrics(metricsCopy)
	}
}
