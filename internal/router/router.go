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
	"os"
	"strconv"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/randomvariable/rocm-llamacpp-envoy-ai-gateway-external-processor/internal/vram"
)

// Configuration constants.
const (
	modelServerPort     = 8080
	modelQueryTimeout   = 5 * time.Second
	modelLoadTimeout    = 30 * time.Second
	warmModelSyncPeriod = 30 * time.Second
	leaseDuration       = 60 * time.Second // Lease TTL for model loads
	logVerbosity        = 2
	discoveryTimeout    = 10 * time.Second
)

// Error definitions for the router package.
var (
	ErrClientNil                  = errors.New("client is nil")
	ErrPodHasNoIP                 = errors.New("pod has no IP")
	ErrUnexpectedStatusCode       = errors.New("unexpected status code")
	ErrNoPodsOnNode               = errors.New("no pods found on node")
	ErrModelLoadFailed            = errors.New("failed to load model")
	ErrLeaseAcquisitionFailed     = errors.New("failed to acquire lease")
	ErrModelNotAvailableAfterLoad = errors.New("model still not available after concurrent load completed")
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

// LoadingState tracks a model currently being loaded (Phase 1).
type LoadingState struct {
	ModelName string
	Done      chan error
}

// Router manages model routing across GPU nodes.
type Router struct {
	client            crclient.Client
	vramTracker       *vram.Tracker
	namespace         string
	podSelector       map[string]string
	modelLoadEndpoint string
	replicaName       string // Pod name of this replica (for lease coordination)

	mu     sync.RWMutex
	models map[string]*ModelInfo // model name -> info
	ready  bool

	// Phase 1: Local synchronization
	loadingMu     sync.RWMutex
	loadingModels map[string]*LoadingState // model name -> loading state
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
	// Get pod name from downward API
	podName := os.Getenv("POD_NAME")
	if podName == "" {
		podName = "unknown-replica"
		klog.Warningf("POD_NAME environment variable not set, using %s", podName)
	}

	router := &Router{
		client:            k8sClient,
		vramTracker:       vramTracker,
		namespace:         namespace,
		podSelector:       podSelector,
		modelLoadEndpoint: modelLoadEndpoint,
		replicaName:       podName,
		models:            make(map[string]*ModelInfo),
		ready:             true,
		loadingModels:     make(map[string]*LoadingState),
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
// This method implements both Phase 1 (local synchronization) and Phase 2 (cross-replica coordination).
func (r *Router) GetModelEndpoint(ctx context.Context, modelName string) (string, error) {
	// PHASE 1 CHECK: Is this model already warm?
	r.mu.RLock()

	if info, ok := r.models[modelName]; ok {
		r.mu.RUnlock()

		return "http://" + net.JoinHostPort(info.PodIP, strconv.Itoa(modelServerPort)), nil
	}

	r.mu.RUnlock()

	// PHASE 1: Check if another request on THIS replica is already loading this model.
	r.loadingMu.Lock()

	if loadingState, ok := r.loadingModels[modelName]; ok {
		// Another request is already loading this model on our replica.
		// Wait for it to complete instead of attempting duplicate load.
		r.loadingMu.Unlock()
		klog.V(logVerbosity).Infof("Model %s is already being loaded by another request, waiting...", modelName)

		select {
		case err := <-loadingState.Done:
			if err != nil {
				return "", fmt.Errorf("model loading failed on concurrent request: %w", err)
			}

			// Model loaded, retry endpoint lookup
			r.mu.RLock()
			info, ok := r.models[modelName]
			r.mu.RUnlock()

			if !ok {
				return "", ErrModelNotAvailableAfterLoad
			}

			return "http://" + net.JoinHostPort(info.PodIP, strconv.Itoa(modelServerPort)), nil

		case <-ctx.Done():
			return "", fmt.Errorf("context cancelled while waiting for model load: %w", ctx.Err())
		}
	}

	// Register that THIS replica is starting to load this model (Phase 1).
	loadingState := &LoadingState{
		ModelName: modelName,
		Done:      make(chan error, 1),
	}
	r.loadingModels[modelName] = loadingState
	r.loadingMu.Unlock()

	defer func() {
		// Clean up the loading state when we're done
		r.loadingMu.Lock()
		delete(r.loadingModels, modelName)
		r.loadingMu.Unlock()
	}()

	// PHASE 2: Try to acquire distributed lock (Kubernetes Lease).
	// This ensures only one replica loads this model across all replicas.
	acquired, err := r.tryAcquireLease(ctx, modelName)
	if err != nil {
		loadingState.Done <- fmt.Errorf("failed to acquire lease: %w", err)

		return "", fmt.Errorf("lease acquisition failed: %w", err)
	}

	if !acquired {
		// Another replica already has the lease. Return error and let Envoy retry.
		// The other replica will call discoverWarmModels() after loading, and we'll sync it.
		loadingState.Done <- fmt.Errorf("%w: another replica is loading this model", ErrLeaseAcquisitionFailed)

		return "", fmt.Errorf("%w: another replica is loading this model", ErrLeaseAcquisitionFailed)
	}

	// We acquired the lease! Load the model on the best node.
	klog.Infof("Acquired lease for model %s, starting load on best node", modelName)

	endpoint, err := r.loadModelOnBestNode(ctx, modelName)
	if err != nil {
		loadingState.Done <- fmt.Errorf("failed to load model on best node: %w", err)

		// Try to release the lease on error
		releaseErr := r.releaseLease(ctx, modelName)
		if releaseErr != nil {
			klog.Errorf("Failed to release lease after load failure: %v", releaseErr)
		}

		return "", fmt.Errorf("failed to load model: %w", err)
	}

	// Model loaded successfully. Signal any waiting requests.
	loadingState.Done <- nil

	// Notify the discovery process to sync warm models across replicas.
	// This ensures other replicas find the newly loaded model within seconds.
	// We intentionally use context.Background() to decouple from request context.
	go func() { //nolint:contextcheck // intentionally using fresh context for async discovery
		discoveryCtx, cancel := context.WithTimeout(context.Background(), discoveryTimeout)
		defer cancel()

		r.discoverWarmModels(discoveryCtx)
	}()

	// Try to release the lease now that loading is complete.
	// Other replicas can acquire it if they need to load a different model.
	releaseErr := r.releaseLease(ctx, modelName)
	if releaseErr != nil {
		klog.Errorf("Failed to release lease after successful load: %v", releaseErr)
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

	// Query the pod's /models endpoint.
	url := "http://" + net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(modelServerPort)) + "/models"

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

// tryAcquireLease attempts to acquire a distributed lock via Kubernetes Lease.
// Returns true if we acquired the lease, false if another replica holds it.
// This implements Phase 2 cross-replica coordination.
func (r *Router) tryAcquireLease(ctx context.Context, modelName string) (bool, error) {
	if r.client == nil {
		klog.V(logVerbosity).Info("Client is nil, skipping lease acquisition (Phase 2)")
		// If no client, assume local-only operation. Still acquire "in memory".
		return true, nil
	}

	leaseName := "extproc-model-load-" + modelName

	// Create a Lease for this model load with our replica as the holder.
	replicaNameCopy := r.replicaName
	leaseDurationSeconds := int32(leaseDuration.Seconds())
	acquireTime := metav1.NowMicro()
	renewTime := metav1.NowMicro()

	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      leaseName,
			Namespace: r.namespace,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &replicaNameCopy,
			LeaseDurationSeconds: &leaseDurationSeconds,
			AcquireTime:          &acquireTime,
			RenewTime:            &renewTime,
		},
	}

	// Try to create the lease atomically. If it exists, another replica has it.
	err := r.client.Create(ctx, lease)
	if err == nil {
		// We successfully created the lease - we own this model load.
		klog.V(logVerbosity).Infof("Successfully acquired lease for model %s", modelName)

		return true, nil
	}

	if !apierrors.IsAlreadyExists(err) {
		// Some other error occurred.
		return false, fmt.Errorf("failed to create lease: %w", err)
	}

	// Lease already exists. Check if it's ours or another replica's.
	existingLease := &coordinationv1.Lease{}

	getErr := r.client.Get(ctx, crclient.ObjectKey{Namespace: r.namespace, Name: leaseName}, existingLease)
	if getErr != nil {
		return false, fmt.Errorf("failed to get existing lease: %w", getErr)
	}

	if existingLease.Spec.HolderIdentity != nil && *existingLease.Spec.HolderIdentity == r.replicaName {
		// This is our own lease (shouldn't happen in practice, but handle gracefully).
		klog.V(logVerbosity).Infof("Lease for model %s is already held by us", modelName)

		return true, nil
	}

	// Another replica holds the lease.
	holderID := "unknown"
	if existingLease.Spec.HolderIdentity != nil {
		holderID = *existingLease.Spec.HolderIdentity
	}

	klog.V(logVerbosity).Infof("Lease for model %s is held by replica %s, backing off", modelName, holderID)

	return false, nil
}

// releaseLease removes the distributed lock for a model.
// This allows other replicas to load the model if needed.
func (r *Router) releaseLease(ctx context.Context, modelName string) error {
	if r.client == nil {
		klog.V(logVerbosity).Info("Client is nil, skipping lease release (Phase 2)")

		return nil
	}

	leaseName := "extproc-model-load-" + modelName

	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      leaseName,
			Namespace: r.namespace,
		},
	}

	err := r.client.Delete(ctx, lease)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete lease: %w", err)
	}

	klog.V(logVerbosity).Infof("Released lease for model %s", modelName)

	return nil
}
