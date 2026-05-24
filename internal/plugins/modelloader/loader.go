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

// Package modelloader provides a PreRequest plugin that loads models on-demand
// on llamacpp instances before routing requests.
package modelloader

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"k8s.io/klog/v2"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/plugins"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/requestcontrol"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/scheduling/types"

	"github.com/randomvariable/rocm-llamacpp-envoy-ai-gateway-external-processor/internal/modeltracker"
)

const (
	// ModelLoaderType is the type name for the model loader plugin.
	ModelLoaderType = "model-loader"

	// Default configuration values.
	defaultModelServerPort     = 8080
	defaultModelLoadEndpoint   = "/models/load"
	defaultModelQueryEndpoint  = "/models"
	defaultModelLoadTimeout    = 60 * time.Second
	defaultModelQueryTimeout   = 5 * time.Second
	defaultConcurrentLoadLimit = 2

	// logVerbosity is the klog verbosity level for debug messages.
	logVerbosity = 2
)

// ModelQueryResponse represents the response from the model query endpoint.
type ModelQueryResponse struct {
	Data []ModelInfo `json:"data"`
}

// ModelInfo represents information about a loaded model.
type ModelInfo struct {
	ID     string      `json:"id"`
	Status ModelStatus `json:"status"`
}

// ModelStatus represents the status of a model.
type ModelStatus struct {
	Value string `json:"value"`
}

var (
	// ErrModelLoadFailed indicates that model loading failed with an error status.
	ErrModelLoadFailed = errors.New("model load failed")

	// ErrDepsNotSet indicates that scorer dependencies were not initialized.
	ErrDepsNotSet = errors.New("VRAM scorer dependencies not set - call SetVRAMScorerDeps first")
)

// Config holds the configuration for the ModelLoader plugin.
type Config struct {
	// ModelServerPort is the port on which model servers listen.
	ModelServerPort int `json:"modelServerPort,omitempty"`
	// ModelLoadEndpoint is the API endpoint for loading models.
	ModelLoadEndpoint string `json:"modelLoadEndpoint,omitempty"`
	// ModelQueryEndpoint is the API endpoint for querying loaded models.
	ModelQueryEndpoint string `json:"modelQueryEndpoint,omitempty"`
	// ModelLoadTimeout is the timeout for model loading operations.
	ModelLoadTimeoutSeconds int `json:"modelLoadTimeoutSeconds,omitempty"`
	// ConcurrentLoadLimit limits concurrent model loads per pod.
	ConcurrentLoadLimit int `json:"concurrentLoadLimit,omitempty"`
}

// DefaultConfig returns the default configuration.
var DefaultConfig = Config{
	ModelServerPort:         defaultModelServerPort,
	ModelLoadEndpoint:       defaultModelLoadEndpoint,
	ModelQueryEndpoint:      defaultModelQueryEndpoint,
	ModelLoadTimeoutSeconds: int(defaultModelLoadTimeout.Seconds()),
	ConcurrentLoadLimit:     defaultConcurrentLoadLimit,
}

// Plugin implements on-demand model loading for llamacpp instances.
type Plugin struct {
	typedName plugins.TypedName
	config    Config

	// Track loading state to prevent duplicate loads.
	mu            sync.RWMutex
	loadingModels map[string]chan struct{} // podIP:model -> completion channel
	loadedModels  map[string]time.Time     // podIP:model -> load time

	// HTTP client for model server communication.
	httpClient *http.Client

	// Tracker (optional) receives proactive MarkLoaded notifications
	// the instant a load is initiated/confirmed so the loaded-model
	// filter doesn't race during the 10s poll window. Nil-safe.
	tracker *modeltracker.Tracker
}

// Compile-time interface checks.
var (
	_ plugins.Plugin            = &Plugin{}
	_ requestcontrol.PreRequest = &Plugin{}
)

// ModelLoaderDeps holds optional dependencies for the model-loader.
// The tracker is the link from "cold-load just started" to "filter sees
// pod as warm immediately" without waiting for the next /v1/models poll.
type ModelLoaderDeps struct {
	Tracker *modeltracker.Tracker
}

// Global deps — set before plugin registration, similar to vram-scorer.
var loaderDeps *ModelLoaderDeps

// SetModelLoaderDeps wires the tracker into the modelloader plugin.
// Must be called before RegisterAllPlugins(). Nil tracker is allowed
// for tests/standalone use.
func SetModelLoaderDeps(deps *ModelLoaderDeps) {
	loaderDeps = deps
}

// ModelLoaderFactory creates a new ModelLoader plugin instance.
//
//nolint:ireturn // Required by plugins.FactoryFunc interface signature.
func ModelLoaderFactory(name string, rawParameters json.RawMessage, _ plugins.Handle) (plugins.Plugin, error) {
	config := DefaultConfig

	if len(rawParameters) > 0 {
		err := json.Unmarshal(rawParameters, &config)
		if err != nil {
			return nil, fmt.Errorf("failed to parse model-loader parameters: %w", err)
		}
	}

	p := NewPlugin(name, config)
	if loaderDeps != nil {
		p.tracker = loaderDeps.Tracker
	}

	return p, nil
}

// NewPlugin creates a new ModelLoader plugin with the given configuration.
func NewPlugin(name string, config Config) *Plugin {
	return &Plugin{
		typedName: plugins.TypedName{
			Type: ModelLoaderType,
			Name: name,
		},
		config:        config,
		loadingModels: make(map[string]chan struct{}),
		loadedModels:  make(map[string]time.Time),
		httpClient: &http.Client{
			Timeout: time.Duration(config.ModelLoadTimeoutSeconds) * time.Second,
		},
	}
}

// WithTracker attaches a tracker for tests that need it without going
// through the global deps path.
func (p *Plugin) WithTracker(t *modeltracker.Tracker) *Plugin {
	p.tracker = t

	return p
}

// TypedName returns the type and name of this plugin.
func (p *Plugin) TypedName() plugins.TypedName {
	return p.typedName
}

// PreRequest is called after scheduling but before the request is sent to the backend.
// It ensures the model is loaded on the target pod.
func (p *Plugin) PreRequest(ctx context.Context, request *types.LLMRequest, schedulingResult *types.SchedulingResult) {
	klog.Info("ModelLoader.PreRequest: called")

	// Validate inputs and extract model/pod info.
	modelName, podIP, podKey := p.validateAndExtractPreRequestInfo(request, schedulingResult)
	if modelName == "" || podIP == "" {
		return
	}

	klog.V(logVerbosity).Infof("ModelLoader.PreRequest: checking model %s on pod %s (IP: %s)", modelName, podKey, podIP)

	// Check if model is already loaded or currently loading.
	loadKey := fmt.Sprintf("%s:%s", podIP, modelName)

	if p.handleAlreadyLoadedOrLoading(ctx, loadKey, modelName, podKey, podIP) {
		return
	}

	// Query the model server to check if model is actually loaded.
	if p.isModelLoaded(ctx, podIP, modelName) {
		p.mu.Lock()
		p.loadedModels[loadKey] = time.Now()
		p.mu.Unlock()
		p.markTrackerLoaded(podKey, modelName)
		klog.V(logVerbosity).Infof("ModelLoader.PreRequest: model %s confirmed loaded on pod %s", modelName, podKey)

		return
	}

	// Mark proactively BEFORE the slow HTTP load so concurrent requests
	// in this poll window see the pod as warm and route here instead of
	// triggering redundant cold-loads on the other pod.
	p.markTrackerLoaded(podKey, modelName)

	// Model not loaded - trigger load.
	err := p.loadModel(ctx, podIP, modelName)
	if err != nil {
		klog.Errorf("ModelLoader.PreRequest: failed to load model %s on pod %s: %v", modelName, podKey, err)
		// Retract the optimistic MarkLoaded so the next decision doesn't
		// route to a pod that doesn't actually have the model.
		p.markTrackerUnloaded(podKey, modelName)
		// Continue anyway - let the backend report the error if model is truly not available.
		return
	}

	klog.Infof("ModelLoader.PreRequest: successfully loaded model %s on pod %s", modelName, podKey)
}

// markTrackerLoaded is a nil-safe wrapper around tracker.MarkLoaded.
func (p *Plugin) markTrackerLoaded(podKey, modelName string) {
	if p.tracker == nil || podKey == "" {
		return
	}

	p.tracker.MarkLoaded(podKey, modelName)
}

// markTrackerUnloaded is a nil-safe wrapper around tracker.MarkUnloaded.
func (p *Plugin) markTrackerUnloaded(podKey, modelName string) {
	if p.tracker == nil || podKey == "" {
		return
	}

	p.tracker.MarkUnloaded(podKey, modelName)
}

// ClearLoadedModelsCache clears the loaded models cache.
// Useful for testing or when backends are restarted.
func (p *Plugin) ClearLoadedModelsCache() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.loadedModels = make(map[string]time.Time)
}

// validateAndExtractPreRequestInfo validates the request and scheduling result,
// logging details and returning the model name, pod IP, and pod key
// ("namespace/name", used as the tracker key) if valid.
func (p *Plugin) validateAndExtractPreRequestInfo(
	request *types.LLMRequest,
	schedulingResult *types.SchedulingResult,
) (modelName, podIP, podKey string) {
	if request == nil {
		klog.Info("ModelLoader.PreRequest: request is nil, skipping")

		return "", "", ""
	}

	klog.Infof("ModelLoader.PreRequest: request.TargetModel=%q", request.TargetModel)

	if schedulingResult == nil {
		klog.Info("ModelLoader.PreRequest: schedulingResult is nil, skipping")

		return "", "", ""
	}

	klog.Infof("ModelLoader.PreRequest: schedulingResult.PrimaryProfileName=%q, ProfileResults count=%d",
		schedulingResult.PrimaryProfileName, len(schedulingResult.ProfileResults))

	p.logProfileResults(schedulingResult)

	modelName = request.TargetModel
	if modelName == "" {
		klog.Info("ModelLoader.PreRequest: no target model specified, skipping")

		return "", "", ""
	}

	targetPod := p.getTargetPod(schedulingResult)
	if targetPod == nil {
		klog.Info("ModelLoader.PreRequest: no target pod in scheduling result, skipping (getTargetPod returned nil)")

		return "", "", ""
	}

	podInfo := targetPod.GetPod()
	if podInfo == nil {
		klog.Warning("ModelLoader.PreRequest: targetPod.GetPod() returned nil")

		return "", "", ""
	}

	podIP = podInfo.Address
	if podIP == "" {
		klog.Warningf("ModelLoader.PreRequest: target pod %s has no IP address", podInfo.NamespacedName.String())

		return "", "", ""
	}

	podKey = podInfo.NamespacedName.String()

	return modelName, podIP, podKey
}

// logProfileResults logs details about each profile result for debugging.
func (p *Plugin) logProfileResults(schedulingResult *types.SchedulingResult) {
	for profileName, profileResult := range schedulingResult.ProfileResults {
		if profileResult == nil {
			klog.Infof("ModelLoader.PreRequest: profileResult[%q] is nil", profileName)

			continue
		}

		klog.Infof("ModelLoader.PreRequest: profileResult[%q].TargetPods count=%d",
			profileName, len(profileResult.TargetPods))

		for i, pod := range profileResult.TargetPods {
			if pod == nil {
				klog.Infof("ModelLoader.PreRequest: profileResult[%q].TargetPods[%d] is nil", profileName, i)

				continue
			}

			podInfo := pod.GetPod()
			if podInfo == nil {
				klog.Infof("ModelLoader.PreRequest: profileResult[%q].TargetPods[%d].GetPod() is nil", profileName, i)

				continue
			}

			klog.Infof("ModelLoader.PreRequest: profileResult[%q].TargetPods[%d]: Name=%s, Address=%q",
				profileName, i, podInfo.NamespacedName.String(), podInfo.Address)
		}
	}
}

// handleAlreadyLoadedOrLoading checks if the model is already loaded or loading.
// Returns true if the caller should return early (model handled), false otherwise.
// podKey ("namespace/name") is used to update the tracker; podIP is for logs.
func (p *Plugin) handleAlreadyLoadedOrLoading(ctx context.Context, loadKey, modelName, podKey, podIP string) bool {
	p.mu.RLock()

	if _, loaded := p.loadedModels[loadKey]; loaded {
		p.mu.RUnlock()
		p.markTrackerLoaded(podKey, modelName)
		klog.V(logVerbosity).Infof("ModelLoader.PreRequest: model %s already loaded on pod %s", modelName, podIP)

		return true
	}

	if waitCh, loading := p.loadingModels[loadKey]; loading {
		p.mu.RUnlock()
		klog.V(logVerbosity).Infof("ModelLoader.PreRequest: model %s already loading on pod %s, waiting", modelName, podIP)

		select {
		case <-waitCh:
			klog.V(logVerbosity).Infof("ModelLoader.PreRequest: model %s finished loading on pod %s", modelName, podIP)
		case <-ctx.Done():
			klog.Warning("ModelLoader.PreRequest: context cancelled while waiting for model load")
		}

		return true
	}

	p.mu.RUnlock()

	return false
}

// getTargetPod extracts the target pod from the scheduling result.
//
//nolint:ireturn // Required by scheduling types interface.
func (p *Plugin) getTargetPod(result *types.SchedulingResult) types.Pod {
	if result.ProfileResults == nil {
		klog.Info("ModelLoader.getTargetPod: ProfileResults is nil")

		return nil
	}

	// Log available profile keys.
	var profileKeys []string
	for k := range result.ProfileResults {
		profileKeys = append(profileKeys, k)
	}

	klog.Infof("ModelLoader.getTargetPod: looking for PrimaryProfileName=%q in ProfileResults (keys: %v)",
		result.PrimaryProfileName, profileKeys)

	primaryProfile := result.ProfileResults[result.PrimaryProfileName]
	if primaryProfile == nil {
		klog.Infof("ModelLoader.getTargetPod: primaryProfile for %q is nil", result.PrimaryProfileName)

		return nil
	}

	if len(primaryProfile.TargetPods) == 0 {
		klog.Infof("ModelLoader.getTargetPod: primaryProfile for %q has no TargetPods", result.PrimaryProfileName)

		return nil
	}

	klog.Infof("ModelLoader.getTargetPod: returning first of %d TargetPods", len(primaryProfile.TargetPods))

	return primaryProfile.TargetPods[0]
}

// isModelLoaded queries the model server to check if a model is loaded.
func (p *Plugin) isModelLoaded(ctx context.Context, podIP, modelName string) bool {
	url := "http://" + net.JoinHostPort(podIP, strconv.Itoa(p.config.ModelServerPort)) + p.config.ModelQueryEndpoint

	klog.Infof("ModelLoader.isModelLoaded: querying models at GET %s for model %q", url, modelName)

	queryCtx, cancel := context.WithTimeout(ctx, defaultModelQueryTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(queryCtx, http.MethodGet, url, http.NoBody)
	if err != nil {
		klog.Infof("ModelLoader.isModelLoaded: failed to create query request: %v", err)

		return false
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		klog.Infof("ModelLoader.isModelLoaded: failed to query models on %s: %v", podIP, err)

		return false
	}

	defer func() {
		closeErr := resp.Body.Close()
		if closeErr != nil {
			klog.V(logVerbosity).Infof("ModelLoader.isModelLoaded: failed to close response body: %v", closeErr)
		}
	}()

	// Read body for logging
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		klog.Infof("ModelLoader.isModelLoaded: failed to read response body: %v", err)

		return false
	}

	klog.Infof("ModelLoader.isModelLoaded: response status=%d, body=%s", resp.StatusCode, string(bodyBytes))

	if resp.StatusCode != http.StatusOK {
		klog.Infof("ModelLoader.isModelLoaded: query returned non-OK status %d", resp.StatusCode)

		return false
	}

	var response ModelQueryResponse

	err = json.Unmarshal(bodyBytes, &response)
	if err != nil {
		klog.Infof("ModelLoader.isModelLoaded: failed to decode query response: %v", err)

		return false
	}

	klog.Infof("ModelLoader.isModelLoaded: found %d models in response", len(response.Data))

	for i, model := range response.Data {
		klog.Infof("ModelLoader.isModelLoaded: model[%d].ID=%q, status=%q", i, model.ID, model.Status.Value)

		if model.ID == modelName {
			// Check if the model is actually loaded, not just configured
			if model.Status.Value == "loaded" || model.Status.Value == "running" || model.Status.Value == "" {
				klog.Infof("ModelLoader.isModelLoaded: model %q is loaded (status=%q)", modelName, model.Status.Value)

				return true
			}

			klog.Infof("ModelLoader.isModelLoaded: model %q found but not loaded (status=%q)", modelName, model.Status.Value)

			return false
		}
	}

	klog.Infof("ModelLoader.isModelLoaded: model %q not found in model list", modelName)

	return false
}

// loadModel triggers model loading on a pod.
func (p *Plugin) loadModel(ctx context.Context, podIP, modelName string) error {
	loadKey := fmt.Sprintf("%s:%s", podIP, modelName)

	klog.Infof("ModelLoader.loadModel: starting load for model %q on pod %s", modelName, podIP)

	// Create completion channel and register loading state.
	p.mu.Lock()

	if waitCh, loading := p.loadingModels[loadKey]; loading {
		p.mu.Unlock()
		klog.Infof("ModelLoader.loadModel: another goroutine is already loading %q, waiting", modelName)
		// Another goroutine started loading - wait for it.
		select {
		case <-waitCh:
			klog.Infof("ModelLoader.loadModel: concurrent load of %q completed", modelName)

			return nil
		case <-ctx.Done():
			klog.Infof("ModelLoader.loadModel: context cancelled while waiting for concurrent load")

			return fmt.Errorf("context cancelled while waiting for model load: %w", ctx.Err())
		}
	}

	completionCh := make(chan struct{})
	p.loadingModels[loadKey] = completionCh
	p.mu.Unlock()

	// Ensure cleanup on completion.
	defer func() {
		p.mu.Lock()
		delete(p.loadingModels, loadKey)
		close(completionCh)
		p.mu.Unlock()
	}()

	url := "http://" + net.JoinHostPort(podIP, strconv.Itoa(p.config.ModelServerPort)) + p.config.ModelLoadEndpoint

	requestBody, err := json.Marshal(map[string]string{
		"model": modelName,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal request body: %w", err)
	}

	klog.Infof("ModelLoader.loadModel: POST %s with body: %s", url, string(requestBody))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(requestBody))
	if err != nil {
		klog.Infof("ModelLoader.loadModel: failed to create request: %v", err)

		return fmt.Errorf("failed to create load request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	klog.Infof("ModelLoader.loadModel: sending request to %s", url)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		klog.Infof("ModelLoader.loadModel: request failed: %v", err)

		return fmt.Errorf("model load request failed: %w", err)
	}

	defer func() {
		closeErr := resp.Body.Close()
		if closeErr != nil {
			klog.V(logVerbosity).Infof("ModelLoader.loadModel: failed to close response body: %v", closeErr)
		}
	}()

	// Read response body for logging
	respBody, _ := io.ReadAll(resp.Body)
	klog.Infof("ModelLoader.loadModel: response status=%d, body=%s", resp.StatusCode, string(respBody))

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("%w: status %d: %s", ErrModelLoadFailed, resp.StatusCode, string(respBody))
	}

	// Mark model as loaded.
	p.mu.Lock()
	p.loadedModels[loadKey] = time.Now()
	p.mu.Unlock()

	klog.Infof("ModelLoader.loadModel: successfully loaded model %q on pod %s", modelName, podIP)

	return nil
}
