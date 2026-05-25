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

// Package filter contains custom Filter plugins for the inference scheduler.
package filter

import (
	"context"
	"encoding/json"
	"errors"

	"k8s.io/klog/v2"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/plugins"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/scheduling/framework"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/scheduling/types"

	"github.com/randomvariable/rocm-llamacpp-envoy-ai-gateway-external-processor/internal/modeltracker"
	"github.com/randomvariable/rocm-llamacpp-envoy-ai-gateway-external-processor/internal/podstate"
)

const (
	// LoadedModelFilterType is the type identifier for the loaded-model filter.
	LoadedModelFilterType = "loaded-model-filter"

	// logVerbosity is the klog verbosity level for debug messages.
	logVerbosity = 2
)

// ErrDepsNotSet indicates dependencies were not initialized before
// plugin registration.
var ErrDepsNotSet = errors.New("loaded-model filter dependencies not set - call SetLoadedModelFilterDeps first")

// compile-time interface assertion.
var _ framework.Filter = &LoadedModelFilter{}

// LoadedModelFilter drops pods that do not have the requested model loaded.
//
// Behaviour:
//   - If at least one candidate pod has the request's TargetModel currently
//     loaded (per the modeltracker), only those pods pass through.
//   - If no candidate pod has the model loaded, all input pods pass through
//     unchanged so that the `model-loader` PreRequest plugin can cold-load
//     on whichever pod the downstream scorers pick (typically the one with
//     most free VRAM).
//
// This is a Filter rather than a Scorer because the upstream scoring
// framework clamps per-scorer scores to [0, 1] before applying weights
// (see scheduler_profile.go enforceScoreRange). A Filter expresses the
// hard "warm pods only when any exist" preference without weight gaming.
type LoadedModelFilter struct {
	typedName plugins.TypedName
	tracker   *modeltracker.Tracker
	// podState is an optional shared cache the filter populates from the
	// framework's per-pod metrics so that other components (notably the
	// modeltracker's dedup logic) can read pressure data without having
	// to re-implement the framework's metric refresher. May be nil.
	podState *podstate.Cache
}

// LoadedModelFilterDeps holds the dependencies for the filter.
// Injected at startup before plugin registration.
type LoadedModelFilterDeps struct {
	Tracker  *modeltracker.Tracker
	PodState *podstate.Cache
}

// Global deps - set before plugin registration.
var filterDeps *LoadedModelFilterDeps

// SetLoadedModelFilterDeps sets the dependencies for the filter.
// Must be called before RegisterAllPlugins().
func SetLoadedModelFilterDeps(deps *LoadedModelFilterDeps) {
	filterDeps = deps
}

// LoadedModelFilterFactory creates a new LoadedModelFilter instance.
//
//nolint:ireturn // Required by plugins.FactoryFunc interface signature.
func LoadedModelFilterFactory(
	name string, _ json.RawMessage, _ plugins.Handle,
) (plugins.Plugin, error) {
	if filterDeps == nil {
		return nil, ErrDepsNotSet
	}

	return NewLoadedModelFilter(filterDeps.Tracker).
		WithPodState(filterDeps.PodState).
		WithName(name), nil
}

// NewLoadedModelFilter constructs a filter bound to a tracker.
func NewLoadedModelFilter(tracker *modeltracker.Tracker) *LoadedModelFilter {
	return &LoadedModelFilter{
		typedName: plugins.TypedName{Type: LoadedModelFilterType},
		tracker:   tracker,
	}
}

// WithPodState binds a shared pod-state cache the filter will populate
// from per-pod metrics during Filter calls. nil disables population.
func (f *LoadedModelFilter) WithPodState(cache *podstate.Cache) *LoadedModelFilter {
	f.podState = cache

	return f
}

// WithName sets the plugin instance name.
func (f *LoadedModelFilter) WithName(name string) *LoadedModelFilter {
	f.typedName.Name = name

	return f
}

// TypedName returns the plugin type and name.
func (f *LoadedModelFilter) TypedName() plugins.TypedName {
	return f.typedName
}

// Filter retains only the pods that have request.TargetModel currently
// loaded, falling back to the input pods if none do.
func (f *LoadedModelFilter) Filter(
	_ context.Context,
	_ *types.CycleState,
	request *types.LLMRequest,
	pods []types.Pod,
) []types.Pod {
	if request == nil || request.TargetModel == "" {
		klog.V(logVerbosity).Info(
			"loaded-model-filter: no TargetModel on request, passing through",
		)

		return pods
	}

	if f.tracker == nil {
		klog.Warning(
			"loaded-model-filter: nil tracker, passing through",
		)

		return pods
	}

	model := request.TargetModel

	warm := make([]types.Pod, 0, len(pods))

	for _, pod := range pods {
		info := pod.GetPod()
		if info == nil {
			continue
		}

		key := info.NamespacedName.String()

		// Side effect: feed the shared pod-state cache so the
		// modeltracker can score winners during dedup. Cheap copy of
		// numeric fields; no allocations on the hot path beyond the
		// map write inside the cache itself.
		if f.podState != nil {
			if m := pod.GetMetrics(); m != nil {
				f.podState.Update(podstate.Snapshot{
					PodKey:              key,
					RunningRequestsSize: m.RunningRequestsSize,
					WaitingQueueSize:    m.WaitingQueueSize,
					KVCacheUsagePercent: m.KVCacheUsagePercent,
				})
			}
		}

		if f.tracker.IsLoaded(key, model) {
			warm = append(warm, pod)
		}
	}

	if len(warm) == 0 {
		klog.V(logVerbosity).Infof(
			"loaded-model-filter: model %q not loaded on any of %d pods, "+
				"passing through for cold-load",
			model, len(pods),
		)

		return pods
	}

	klog.V(logVerbosity).Infof(
		"loaded-model-filter: model %q loaded on %d/%d pods, dropping cold pods",
		model, len(warm), len(pods),
	)

	return warm
}
