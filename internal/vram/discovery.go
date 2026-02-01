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
	"maps"
	"sync"

	corev1 "k8s.io/api/core/v1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// KubernetesDiscoveryConfig configures Kubernetes-based node discovery.
type KubernetesDiscoveryConfig struct {
	// Client is the Kubernetes client for API access.
	Client crclient.Client

	// Namespace is the namespace where model server pods run.
	Namespace string

	// PodSelector selects which pods to consider for routing.
	PodSelector map[string]string

	// NodeSelector filters which nodes to consider.
	NodeSelector map[string]string

	// ExporterNamespace is the namespace where exporter pods run.
	ExporterNamespace string

	// ExporterPodSelector selects which pods export VRAM metrics.
	ExporterPodSelector map[string]string
}

// KubernetesDiscovery discovers VRAM metric endpoints using Kubernetes API.
type KubernetesDiscovery struct {
	client              crclient.Client
	namespace           string
	podSelector         map[string]string
	nodeSelector        map[string]string
	exporterNamespace   string
	exporterPodSelector map[string]string

	mu sync.RWMutex
}

// Verify interface compliance.
var _ NodeDiscovery = (*KubernetesDiscovery)(nil)

// NewKubernetesDiscovery creates a new Kubernetes-based discovery.
func NewKubernetesDiscovery(config KubernetesDiscoveryConfig) *KubernetesDiscovery {
	return &KubernetesDiscovery{
		client:              config.Client,
		namespace:           config.Namespace,
		podSelector:         config.PodSelector,
		nodeSelector:        config.NodeSelector,
		exporterNamespace:   config.ExporterNamespace,
		exporterPodSelector: config.ExporterPodSelector,
	}
}

// DiscoverEndpoints finds all VRAM metric endpoints for nodes running model server pods.
func (d *KubernetesDiscovery) DiscoverEndpoints(ctx context.Context) ([]MetricsEndpoint, error) {
	if d.client == nil {
		return nil, ErrClientNil
	}

	// Copy selectors under lock to avoid race conditions.
	d.mu.RLock()
	namespace := d.namespace
	podSelector := copyMap(d.podSelector)
	nodeSelector := copyMap(d.nodeSelector)
	d.mu.RUnlock()

	// List model server pods.
	var pods corev1.PodList

	podListOpts := []crclient.ListOption{
		crclient.InNamespace(namespace),
		crclient.MatchingLabels(podSelector),
	}

	err := d.client.List(ctx, &pods, podListOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}

	// List nodes matching selector.
	var nodes corev1.NodeList

	nodeListOpts := []crclient.ListOption{}
	if len(nodeSelector) > 0 {
		nodeListOpts = append(nodeListOpts, crclient.MatchingLabels(nodeSelector))
	}

	err = d.client.List(ctx, &nodes, nodeListOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to list nodes: %w", err)
	}

	// Build a set of valid node names.
	nodeSet := make(map[string]struct{}, len(nodes.Items))
	for i := range nodes.Items {
		nodeSet[nodes.Items[i].Name] = struct{}{}
	}

	// Find unique nodes with running pods.
	seenNodes := make(map[string]struct{})

	for i := range pods.Items {
		pod := &pods.Items[i]

		if pod.Status.Phase != corev1.PodRunning {
			continue
		}

		nodeName := pod.Spec.NodeName
		if nodeName == "" {
			continue
		}

		// Check if node matches our selector.
		if _, ok := nodeSet[nodeName]; !ok {
			continue
		}

		seenNodes[nodeName] = struct{}{}
	}

	// Find exporter endpoints for each node.
	endpoints := make([]MetricsEndpoint, 0, len(seenNodes))

	for nodeName := range seenNodes {
		endpoint, err := d.findExporterEndpoint(ctx, nodeName)
		if err != nil {
			continue // Skip nodes without exporters.
		}

		endpoints = append(endpoints, endpoint)
	}

	return endpoints, nil
}

// UpdatePodSelector updates the pod selector for discovery.
func (d *KubernetesDiscovery) UpdatePodSelector(selector map[string]string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.podSelector = copyMap(selector)
}

// UpdateNodeSelector updates the node selector for discovery.
func (d *KubernetesDiscovery) UpdateNodeSelector(selector map[string]string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.nodeSelector = copyMap(selector)
}

// UpdateNamespace updates the namespace for discovery.
func (d *KubernetesDiscovery) UpdateNamespace(namespace string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.namespace = namespace
}

// findExporterEndpoint finds the metrics endpoint for a specific node.
func (d *KubernetesDiscovery) findExporterEndpoint(ctx context.Context, nodeName string) (MetricsEndpoint, error) {
	var pods corev1.PodList

	podListOpts := []crclient.ListOption{
		crclient.InNamespace(d.exporterNamespace),
		crclient.MatchingLabels(d.exporterPodSelector),
	}

	err := d.client.List(ctx, &pods, podListOpts...)
	if err != nil {
		return MetricsEndpoint{}, fmt.Errorf("failed to list exporter pods: %w", err)
	}

	for i := range pods.Items {
		pod := &pods.Items[i]

		if pod.Status.Phase != corev1.PodRunning {
			continue
		}

		if pod.Spec.NodeName != nodeName {
			continue
		}

		if pod.Status.PodIP == "" {
			continue
		}

		port, err := getFirstTCPPort(pod)
		if err != nil {
			continue
		}

		return MetricsEndpoint{
			NodeName: nodeName,
			Address:  pod.Status.PodIP,
			Port:     port,
		}, nil
	}

	return MetricsEndpoint{}, fmt.Errorf("%w: %s", ErrNoRunningExporterPod, nodeName)
}

// getFirstTCPPort extracts the first TCP port from a pod's container ports.
func getFirstTCPPort(pod *corev1.Pod) (int32, error) {
	for i := range pod.Spec.Containers {
		container := &pod.Spec.Containers[i]
		for _, port := range container.Ports {
			// Default protocol is TCP if not specified.
			if port.Protocol == "" || port.Protocol == corev1.ProtocolTCP {
				if port.ContainerPort > 0 {
					return port.ContainerPort, nil
				}
			}
		}
	}

	return 0, fmt.Errorf("%w: %s/%s", ErrNoTCPPortInExporterPod, pod.Namespace, pod.Name)
}

// copyMap creates a shallow copy of a string map.
func copyMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}

	result := make(map[string]string, len(source))
	maps.Copy(result, source)

	return result
}

// StaticDiscovery returns a fixed list of endpoints.
// Useful for testing or when endpoints are known statically.
type StaticDiscovery struct {
	endpoints []MetricsEndpoint
	mu        sync.RWMutex
}

// Verify interface compliance.
var _ NodeDiscovery = (*StaticDiscovery)(nil)

// NewStaticDiscovery creates a discovery with static endpoints.
func NewStaticDiscovery(endpoints []MetricsEndpoint) *StaticDiscovery {
	return &StaticDiscovery{
		endpoints: endpoints,
	}
}

// DiscoverEndpoints returns the static list of endpoints.
func (d *StaticDiscovery) DiscoverEndpoints(_ context.Context) ([]MetricsEndpoint, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	result := make([]MetricsEndpoint, len(d.endpoints))
	copy(result, d.endpoints)

	return result, nil
}

// SetEndpoints updates the static endpoints.
func (d *StaticDiscovery) SetEndpoints(endpoints []MetricsEndpoint) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.endpoints = endpoints
}
