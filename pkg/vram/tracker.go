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

// Package vram provides VRAM tracking and metrics for GPU nodes.
package vram

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// Configuration constants.
const (
	metricsQueryTimeout = 5 * time.Second
	logVerbosity        = 2
	bytesPerGB          = 1024 * 1024 * 1024
	defaultTotalVRAMGB  = 32

	// DefaultScrapeInterval is the default interval for scraping VRAM metrics.
	DefaultScrapeInterval = 30 * time.Second
)

// Error definitions for the vram package.
var (
	ErrClientNil              = errors.New("client is nil")
	ErrNoNodes                = errors.New("no nodes with VRAM metrics available")
	ErrNoSuitableNode         = errors.New("no suitable node found")
	ErrNoMetrics              = errors.New("no metrics available for node")
	ErrUnexpectedStatus       = errors.New("unexpected status code")
	ErrNoRunningExporterPod   = errors.New("no running exporter pod found on node")
	ErrNoTCPPortInExporterPod = errors.New("no TCP port found in exporter pod")
)

// VRAMMetrics represents VRAM usage metrics for a node.
type VRAMMetrics struct {
	TotalVRAM     int64
	UsedVRAM      int64
	AvailableVRAM int64
	LastUpdate    time.Time
}

// Tracker tracks VRAM usage across GPU nodes.
type Tracker struct {
	client              crclient.Client
	namespace           string
	podSelector         map[string]string
	nodeSelector        map[string]string
	exporterNamespace   string
	exporterPodSelector map[string]string
	scrapeInterval      time.Duration

	mu      sync.RWMutex
	metrics map[string]*VRAMMetrics // node name -> metrics

	// OpenTelemetry metrics.
	totalGauge     metric.Int64ObservableGauge
	usedGauge      metric.Int64ObservableGauge
	availableGauge metric.Int64ObservableGauge
}

// NewTracker creates a new VRAM tracker (without OpenTelemetry).
func NewTracker(
	k8sClient crclient.Client,
	namespace string,
	podSelector, nodeSelector map[string]string,
	exporterNamespace string,
	exporterPodSelector map[string]string,
	scrapeInterval time.Duration,
) *Tracker {
	return &Tracker{
		client:              k8sClient,
		namespace:           namespace,
		podSelector:         podSelector,
		nodeSelector:        nodeSelector,
		exporterNamespace:   exporterNamespace,
		exporterPodSelector: exporterPodSelector,
		scrapeInterval:      scrapeInterval,
		metrics:             make(map[string]*VRAMMetrics),
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
	tracker := &Tracker{
		client:              k8sClient,
		namespace:           namespace,
		podSelector:         podSelector,
		nodeSelector:        nodeSelector,
		exporterNamespace:   exporterNamespace,
		exporterPodSelector: exporterPodSelector,
		scrapeInterval:      scrapeInterval,
		metrics:             make(map[string]*VRAMMetrics),
	}

	// Register OpenTelemetry metrics.
	var err error

	tracker.totalGauge, err = otelMeter.Int64ObservableGauge(
		"vram_total_bytes",
		metric.WithDescription("Total VRAM in bytes on node"),
		metric.WithUnit("By"),
	)
	if err != nil {
		klog.Errorf("Failed to create totalGauge: %v", err)
	}

	tracker.usedGauge, err = otelMeter.Int64ObservableGauge(
		"vram_used_bytes",
		metric.WithDescription("Used VRAM in bytes on node"),
		metric.WithUnit("By"),
	)
	if err != nil {
		klog.Errorf("Failed to create usedGauge: %v", err)
	}

	tracker.availableGauge, err = otelMeter.Int64ObservableGauge(
		"vram_available_bytes",
		metric.WithDescription("Available VRAM in bytes on node"),
		metric.WithUnit("By"),
	)
	if err != nil {
		klog.Errorf("Failed to create availableGauge: %v", err)
	}

	// Register callbacks for the gauges.
	_, err = otelMeter.RegisterCallback(
		func(ctx context.Context, observer metric.Observer) error {
			tracker.mu.RLock()
			defer tracker.mu.RUnlock()

			for nodeName, nodeMetrics := range tracker.metrics {
				nodeAttr := attribute.String("node", nodeName)
				observer.ObserveInt64(tracker.totalGauge, nodeMetrics.TotalVRAM, metric.WithAttributes(nodeAttr))
				observer.ObserveInt64(tracker.usedGauge, nodeMetrics.UsedVRAM, metric.WithAttributes(nodeAttr))
				observer.ObserveInt64(tracker.availableGauge, nodeMetrics.AvailableVRAM, metric.WithAttributes(nodeAttr))
			}

			return nil
		},
		tracker.totalGauge, tracker.usedGauge, tracker.availableGauge,
	)
	if err != nil {
		klog.Errorf("Failed to register metric callback: %v", err)
	}

	return tracker
}

// Start begins tracking VRAM metrics.
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

// UpdatePodSelector updates the pod selector used for discovering pods.
func (t *Tracker) UpdatePodSelector(ctx context.Context, selector map[string]string) {
	t.mu.Lock()
	t.podSelector = selector

	// Copy values to use in goroutine to avoid race.
	namespace := t.namespace
	podSelector := make(map[string]string, len(t.podSelector))
	maps.Copy(podSelector, t.podSelector)

	nodeSelector := make(map[string]string, len(t.nodeSelector))
	maps.Copy(nodeSelector, t.nodeSelector)

	t.mu.Unlock()

	klog.Infof("VRAM tracker updated pod selector to: %v", selector)

	// Trigger immediate rescrape with copied values.
	go t.scrapeMetricsWithSelectors(ctx, namespace, podSelector, nodeSelector)
}

// UpdateNodeSelector updates the node selector used for filtering nodes.
func (t *Tracker) UpdateNodeSelector(selector map[string]string) {
	t.mu.Lock()
	t.nodeSelector = selector

	// Copy values to use in goroutine to avoid race.
	namespace := t.namespace
	podSelector := make(map[string]string, len(t.podSelector))
	maps.Copy(podSelector, t.podSelector)

	nodeSelector := make(map[string]string, len(t.nodeSelector))
	maps.Copy(nodeSelector, t.nodeSelector)

	t.mu.Unlock()

	klog.Infof("VRAM tracker updated node selector to: %v", selector)

	// Trigger immediate rescrape with copied values.
	go t.scrapeMetricsWithSelectors(context.Background(), namespace, podSelector, nodeSelector)
}

// UpdateNamespace updates the namespace used for pod discovery.
func (t *Tracker) UpdateNamespace(namespace string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.namespace = namespace
	klog.Infof("VRAM tracker updated namespace to: %s", namespace)
}

// GetMetrics returns current VRAM metrics for all nodes.
func (t *Tracker) GetMetrics() map[string]*VRAMMetrics {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make(map[string]*VRAMMetrics, len(t.metrics))

	for k, v := range t.metrics {
		result[k] = &VRAMMetrics{
			TotalVRAM:     v.TotalVRAM,
			UsedVRAM:      v.UsedVRAM,
			AvailableVRAM: v.AvailableVRAM,
			LastUpdate:    v.LastUpdate,
		}
	}

	return result
}

// GetNodeWithMostAvailableVRAM returns the node name and available VRAM for the node with most available VRAM.
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
func (t *Tracker) GetNodeVRAM(nodeName string) (*VRAMMetrics, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	nodeMetrics, ok := t.metrics[nodeName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNoMetrics, nodeName)
	}

	return &VRAMMetrics{
		TotalVRAM:     nodeMetrics.TotalVRAM,
		UsedVRAM:      nodeMetrics.UsedVRAM,
		AvailableVRAM: nodeMetrics.AvailableVRAM,
		LastUpdate:    nodeMetrics.LastUpdate,
	}, nil
}

// buildLabelSelector creates a Kubernetes label selector string from a map.
func buildLabelSelector(selector map[string]string) string {
	parts := make([]string, 0, len(selector))

	for k, v := range selector {
		parts = append(parts, fmt.Sprintf("%s=%s", k, v))
	}

	return strings.Join(parts, ",")
}

// getNodeInternalIP finds the internal IP address from a node's addresses.
func getNodeInternalIP(node *corev1.Node) string {
	for _, addr := range node.Status.Addresses {
		if addr.Type == corev1.NodeInternalIP {
			return addr.Address
		}
	}

	return ""
}

// scrapeMetrics scrapes VRAM metrics from all GPU nodes.
// It safely copies selector values under lock before proceeding.
func (t *Tracker) scrapeMetrics(ctx context.Context) {
	if t.client == nil {
		klog.V(logVerbosity).Info("Client is nil, skipping VRAM scrape")

		return
	}

	// Copy selector values under lock to avoid race.
	t.mu.RLock()
	namespace := t.namespace
	podSelector := make(map[string]string, len(t.podSelector))
	maps.Copy(podSelector, t.podSelector)

	nodeSelector := make(map[string]string, len(t.nodeSelector))
	maps.Copy(nodeSelector, t.nodeSelector)

	t.mu.RUnlock()

	t.scrapeMetricsWithSelectors(ctx, namespace, podSelector, nodeSelector)
}

// scrapeMetricsWithSelectors scrapes VRAM metrics using provided selectors
// (to avoid race conditions when called from goroutines).
func (t *Tracker) scrapeMetricsWithSelectors(ctx context.Context, namespace string, podSelector, nodeSelector map[string]string) {
	pods, nodes, err := t.listPodsAndNodesWithSelectors(ctx, namespace, podSelector, nodeSelector)
	if err != nil {
		klog.Errorf("Failed to list resources: %v", err)

		return
	}

	// Create a map of node names for quick lookup.
	nodeMap := make(map[string]*corev1.Node, len(nodes.Items))

	for i := range nodes.Items {
		node := &nodes.Items[i]
		nodeMap[node.Name] = node
	}

	t.scrapePodNodes(ctx, pods, nodeMap)
}

// listPodsAndNodesWithSelectors retrieves pods and nodes using provided selectors
// (to avoid race conditions when called with pre-copied values).
func (t *Tracker) listPodsAndNodesWithSelectors(ctx context.Context, namespace string, podSelector, nodeSelector map[string]string) (*corev1.PodList, *corev1.NodeList, error) {
	if t.client == nil {
		return nil, nil, ErrClientNil
	}

	var pods corev1.PodList

	podListOpts := []crclient.ListOption{
		crclient.InNamespace(namespace),
		crclient.MatchingLabels(podSelector),
	}

	err := t.client.List(ctx, &pods, podListOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list pods: %w", err)
	}

	var nodes corev1.NodeList

	nodeListOpts := []crclient.ListOption{}
	if len(nodeSelector) > 0 {
		nodeListOpts = append(nodeListOpts, crclient.MatchingLabels(nodeSelector))
	}

	err = t.client.List(ctx, &nodes, nodeListOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list nodes: %w", err)
	}

	return &pods, &nodes, nil
}

// scrapePodNodes scrapes metrics for each running pod's node.
func (t *Tracker) scrapePodNodes(ctx context.Context, pods *corev1.PodList, nodeMap map[string]*corev1.Node) {
	for i := range pods.Items {
		pod := &pods.Items[i]

		if pod.Status.Phase != corev1.PodRunning {
			continue
		}

		nodeName := pod.Spec.NodeName
		if nodeName == "" {
			continue
		}

		// Check if node is in our map (means it matched selector).
		_, ok := nodeMap[nodeName]
		if !ok {
			klog.V(logVerbosity).Infof("Skipping pod %s on node %s (node doesn't match selector)", pod.Name, nodeName)

			continue
		}

		// Find exporter pod on this node.
		exporterPod, exporterPort, err := t.findExporterPodOnNode(ctx, nodeName)
		if err != nil {
			klog.Errorf("Failed to find exporter pod on node %s: %v", nodeName, err)

			continue
		}

		if exporterPod == nil {
			klog.Errorf("No exporter pod found on node %s", nodeName)

			continue
		}

		exporterIP := exporterPod.Status.PodIP
		if exporterIP == "" {
			klog.Errorf("Exporter pod on node %s has no IP", nodeName)

			continue
		}

		// Scrape metrics from exporter pod.
		nodeMetrics, err := t.scrapeNodeMetrics(ctx, exporterIP, exporterPort)
		if err != nil {
			klog.Errorf("Failed to scrape metrics from node %s (exporter %s:%d): %v", nodeName, exporterIP, exporterPort, err)

			continue
		}

		t.mu.Lock()
		t.metrics[nodeName] = nodeMetrics
		t.mu.Unlock()

		klog.V(logVerbosity).Infof("Updated VRAM metrics for node %s: total=%d, used=%d, available=%d",
			nodeName, nodeMetrics.TotalVRAM, nodeMetrics.UsedVRAM, nodeMetrics.AvailableVRAM)
	}
}

// scrapeNodeMetrics scrapes VRAM metrics from a specific exporter pod.
func (t *Tracker) scrapeNodeMetrics(ctx context.Context, podIP string, podPort int32) (*VRAMMetrics, error) {
	url := "http://" + net.JoinHostPort(podIP, strconv.Itoa(int(podPort))) + "/metrics"

	client := &http.Client{
		Timeout: metricsQueryTimeout,
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metrics: %w", err)
	}

	defer func() {
		closeErr := resp.Body.Close()
		if closeErr != nil {
			klog.Errorf("Failed to close response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: %d", ErrUnexpectedStatus, resp.StatusCode)
	}

	// Parse Prometheus metrics using expfmt.
	parser := expfmt.NewTextParser(model.LegacyValidation)

	metricFamilies, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse metrics: %w", err)
	}

	nodeMetrics := &VRAMMetrics{
		LastUpdate: time.Now(),
		// Default to 32GB if metrics cannot be parsed.
		// This should be made configurable in a production environment.
		TotalVRAM: defaultTotalVRAMGB * bytesPerGB,
		UsedVRAM:  0,
	}

	// Extract VRAM metrics from parsed data.
	if mf, ok := metricFamilies["rocm_memory_total_bytes"]; ok {
		if len(mf.GetMetric()) > 0 {
			nodeMetrics.TotalVRAM = int64(getMetricValue(mf.GetMetric()[0]))
		}
	}

	if mf, ok := metricFamilies["rocm_memory_used_bytes"]; ok {
		if len(mf.GetMetric()) > 0 {
			nodeMetrics.UsedVRAM = int64(getMetricValue(mf.GetMetric()[0]))
		}
	}

	nodeMetrics.AvailableVRAM = max(nodeMetrics.TotalVRAM-nodeMetrics.UsedVRAM, 0)

	return nodeMetrics, nil
}

// getMetricValue extracts the value from a Prometheus metric.
func getMetricValue(promMetric *dto.Metric) float64 {
	if promMetric.GetGauge() != nil {
		return promMetric.GetGauge().GetValue()
	}

	if promMetric.GetCounter() != nil {
		return promMetric.GetCounter().GetValue()
	}

	if promMetric.GetUntyped() != nil {
		return promMetric.GetUntyped().GetValue()
	}

	return 0
}

// findExporterPodOnNode finds an exporter pod running on the specified node
// and returns the pod and the first exposed TCP port.
func (t *Tracker) findExporterPodOnNode(ctx context.Context, nodeName string) (*corev1.Pod, int32, error) {
	if t.client == nil {
		return nil, 0, ErrClientNil
	}

	var pods corev1.PodList

	podListOpts := []crclient.ListOption{
		crclient.InNamespace(t.exporterNamespace),
		crclient.MatchingLabels(t.exporterPodSelector),
	}

	err := t.client.List(ctx, &pods, podListOpts...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list exporter pods: %w", err)
	}

	// Find the first running pod on the target node.
	for i := range pods.Items {
		pod := &pods.Items[i]

		if pod.Status.Phase != corev1.PodRunning {
			continue
		}

		if pod.Spec.NodeName != nodeName {
			continue
		}

		// Extract the first TCP port from the pod's containers.
		port, err := getFirstTCPPort(pod)
		if err != nil {
			return nil, 0, err
		}

		return pod, port, nil
	}

	return nil, 0, fmt.Errorf("%w: %s", ErrNoRunningExporterPod, nodeName)
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
