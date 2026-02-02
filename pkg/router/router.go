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

// Package router provides VRAM-aware model routing across GPU nodes.
package router

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/randomvariable/rocm-envoy-ai-gateway-external-processor/pkg/vram"
)

// Configuration constants.
const (
	modelServerPort     = 8080
	modelQueryTimeout   = 5 * time.Second
	modelLoadTimeout    = 30 * time.Second
	warmModelSyncPeriod = 30 * time.Second
	logVerbosity        = 2
)

// Error definitions for the router package.
var (
	ErrClientNil            = errors.New("client is nil")
	ErrPodHasNoIP           = errors.New("pod has no IP")
	ErrUnexpectedStatusCode = errors.New("unexpected status code")
	ErrNoPodsOnNode         = errors.New("no pods found on node")
	ErrModelLoadFailed      = errors.New("failed to load model")
)

// ModelInfo represents information about a loaded model.
type ModelInfo struct {
	Name      string    `json:"name"`
	NodeName  string    `json:"node_name"`
	PodName   string    `json:"pod_name"`
	PodIP     string    `json:"pod_ip"`
	LoadedAt  time.Time `json:"loaded_at"`
	VRAMUsage int64     `json:"vram_usage"`
}

// Router manages model routing across GPU nodes.
type Router struct {
	client            crclient.Client
	vramTracker       *vram.Tracker
	namespace         string
	podSelector       map[string]string
	modelLoadEndpoint string

	mu     sync.RWMutex
	models map[string]*ModelInfo // model name -> info
	ready  bool
}

// NewRouter creates a new Router instance.
func NewRouter(
	ctx context.Context,
	k8sClient crclient.Client,
	vramTracker *vram.Tracker,
	namespace string,
	podSelector map[string]string,
	modelLoadEndpoint string,
) *Router {
	router := &Router{
		client:            k8sClient,
		vramTracker:       vramTracker,
		namespace:         namespace,
		podSelector:       podSelector,
		modelLoadEndpoint: modelLoadEndpoint,
		models:            make(map[string]*ModelInfo),
		ready:             true,
	}

	// Start background sync of warm models.
	go router.syncWarmModels(ctx)

	return router
}

// IsReady returns whether the router is ready to serve requests.
func (r *Router) IsReady() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.ready
}

// UpdatePodSelector updates the pod selector used for discovering model servers.
func (r *Router) UpdatePodSelector(ctx context.Context, selector map[string]string) {
	r.mu.Lock()
	r.podSelector = selector

	// Copy values to use in goroutine to avoid race.
	namespace := r.namespace
	podSelector := make(map[string]string, len(r.podSelector))
	maps.Copy(podSelector, r.podSelector)

	r.mu.Unlock()

	klog.Infof("Updated pod selector to: %v", selector)

	// Trigger immediate rediscovery with copied values.
	go r.discoverWarmModelsWithSelector(ctx, namespace, podSelector)
}

// UpdateNamespace updates the namespace used for pod discovery.
func (r *Router) UpdateNamespace(namespace string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.namespace = namespace
	klog.Infof("Updated namespace to: %s", namespace)
}

// GetWarmModels returns all currently warm models.
func (r *Router) GetWarmModels() map[string]*ModelInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[string]*ModelInfo, len(r.models))

	for k, v := range r.models {
		result[k] = &ModelInfo{
			Name:      v.Name,
			NodeName:  v.NodeName,
			PodName:   v.PodName,
			PodIP:     v.PodIP,
			LoadedAt:  v.LoadedAt,
			VRAMUsage: v.VRAMUsage,
		}
	}

	return result
}

// GetModelEndpoint returns the endpoint for a model, loading it if necessary.
func (r *Router) GetModelEndpoint(ctx context.Context, modelName string) (string, error) {
	// Check if model is already warm.
	r.mu.RLock()

	if info, ok := r.models[modelName]; ok {
		r.mu.RUnlock()

		return "http://" + net.JoinHostPort(info.PodIP, strconv.Itoa(modelServerPort)), nil
	}

	r.mu.RUnlock()

	// Model not loaded, need to load it on best node.
	endpoint, err := r.loadModelOnBestNode(ctx, modelName)
	if err != nil {
		return "", fmt.Errorf("failed to load model: %w", err)
	}

	return endpoint, nil
}

// syncWarmModels periodically syncs the list of warm models from all nodes.
func (r *Router) syncWarmModels(ctx context.Context) {
	ticker := time.NewTicker(warmModelSyncPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.discoverWarmModels(ctx)
		}
	}
}

// discoverWarmModels queries all model server pods to find loaded models.
// It safely copies namespace and podSelector under lock before proceeding.
func (r *Router) discoverWarmModels(ctx context.Context) {
	if r.client == nil {
		klog.V(logVerbosity).Info("Client is nil, skipping warm model discovery")

		return
	}

	// Copy selector values under lock to avoid race.
	r.mu.RLock()
	namespace := r.namespace
	podSelector := make(map[string]string, len(r.podSelector))
	maps.Copy(podSelector, r.podSelector)

	r.mu.RUnlock()

	r.discoverWarmModelsWithSelector(ctx, namespace, podSelector)
}

// discoverWarmModelsWithSelector queries all model server pods to find loaded models
// using the provided namespace and selector (to avoid race conditions).
func (r *Router) discoverWarmModelsWithSelector(ctx context.Context, namespace string, podSelector map[string]string) {
	if r.client == nil {
		klog.V(logVerbosity).Info("Client is nil, skipping warm model discovery")

		return
	}

	var podList corev1.PodList

	listOpts := []crclient.ListOption{
		crclient.InNamespace(namespace),
		crclient.MatchingLabels(podSelector),
	}

	err := r.client.List(ctx, &podList, listOpts...)
	if err != nil {
		klog.Errorf("Failed to list pods: %v", err)

		return
	}

	warmModels := make(map[string]*ModelInfo)

	for i := range podList.Items {
		pod := &podList.Items[i]

		if pod.Status.Phase != corev1.PodRunning {
			continue
		}

		// Query the pod for loaded models.
		models, err := r.queryPodModels(ctx, pod)
		if err != nil {
			klog.V(logVerbosity).Infof("Failed to query models from pod %s: %v", pod.Name, err)

			continue
		}

		for _, model := range models {
			warmModels[model.Name] = model
		}
	}

	r.mu.Lock()
	r.models = warmModels
	r.mu.Unlock()

	klog.V(logVerbosity).Infof("Discovered %d warm models", len(warmModels))
}

// modelListResponse represents the response from the model server's /v1/models endpoint.
type modelListResponse struct {
	Data []modelData `json:"data"`
}

// modelData represents a single model entry in the model list response.
type modelData struct {
	ID string `json:"id"`
}

// queryPodModels queries a pod for its loaded models.
func (r *Router) queryPodModels(ctx context.Context, pod *corev1.Pod) ([]*ModelInfo, error) {
	if pod.Status.PodIP == "" {
		return nil, ErrPodHasNoIP
	}

	// Query the pod's /v1/models endpoint.
	url := "http://" + net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(modelServerPort)) + "/v1/models"

	client := &http.Client{
		Timeout: modelQueryTimeout,
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to query pod models: %w", err)
	}

	defer func() {
		closeErr := resp.Body.Close()
		if closeErr != nil {
			klog.Errorf("Failed to close response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: %d", ErrUnexpectedStatusCode, resp.StatusCode)
	}

	var response modelListResponse

	err = json.NewDecoder(resp.Body).Decode(&response)
	if err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	models := make([]*ModelInfo, 0, len(response.Data))

	for _, model := range response.Data {
		models = append(models, &ModelInfo{
			Name:     model.ID,
			NodeName: pod.Spec.NodeName,
			PodName:  pod.Name,
			PodIP:    pod.Status.PodIP,
			// LoadedAt is not set during discovery as we don't know the actual load time.
			// It will be set when we explicitly load a model.
		})
	}

	return models, nil
}

// loadModelOnBestNode loads a model on the node with most available VRAM.
func (r *Router) loadModelOnBestNode(ctx context.Context, modelName string) (string, error) {
	// Find node with most available VRAM.
	bestNode, availableVRAM, err := r.vramTracker.GetNodeWithMostAvailableVRAM()
	if err != nil {
		return "", fmt.Errorf("failed to find suitable node: %w", err)
	}

	klog.Infof("Loading model %s on node %s with %d bytes available VRAM", modelName, bestNode, availableVRAM)

	if r.client == nil {
		return "", fmt.Errorf("%w: cannot find pods on node", ErrClientNil)
	}

	// Find a pod on the best node using controller-runtime client.
	var podList corev1.PodList

	selector, err := labels.ValidatedSelectorFromSet(r.podSelector)
	if err != nil {
		return "", fmt.Errorf("failed to create label selector: %w", err)
	}

	listOpts := []crclient.ListOption{
		crclient.InNamespace(r.namespace),
		&crclient.ListOptions{LabelSelector: selector},
		crclient.MatchingFields{"spec.nodeName": bestNode},
	}

	err = r.client.List(ctx, &podList, listOpts...)
	if err != nil {
		return "", fmt.Errorf("failed to find pod on node: %w", err)
	}

	if len(podList.Items) == 0 {
		return "", fmt.Errorf("%w: %s", ErrNoPodsOnNode, bestNode)
	}

	pod := &podList.Items[0]

	if pod.Status.PodIP == "" {
		return "", ErrPodHasNoIP
	}

	// Load model on the pod.
	err = r.loadModelOnPod(ctx, pod, modelName)
	if err != nil {
		return "", err
	}

	// Update our model tracking.
	r.mu.Lock()
	r.models[modelName] = &ModelInfo{
		Name:     modelName,
		NodeName: pod.Spec.NodeName,
		PodName:  pod.Name,
		PodIP:    pod.Status.PodIP,
		LoadedAt: time.Now(),
	}
	r.mu.Unlock()

	return "http://" + net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(modelServerPort)), nil
}

// loadModelOnPod sends a request to load a model on a specific pod.
func (r *Router) loadModelOnPod(ctx context.Context, pod *corev1.Pod, modelName string) error {
	url := "http://" + net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(modelServerPort)) + r.modelLoadEndpoint

	requestBody, err := json.Marshal(map[string]string{
		"model": modelName,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal request body: %w", err)
	}

	client := &http.Client{
		Timeout: modelLoadTimeout,
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(requestBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to load model: %w", err)
	}

	defer func() {
		closeErr := resp.Body.Close()
		if closeErr != nil {
			klog.Errorf("Failed to close response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)

		return fmt.Errorf("%w: status=%d, body=%s", ErrModelLoadFailed, resp.StatusCode, string(body))
	}

	klog.Infof("Successfully loaded model %s on pod %s", modelName, pod.Name)

	return nil
}
