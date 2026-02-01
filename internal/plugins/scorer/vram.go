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

// Package scorer provides custom scoring plugins for the inference scheduler.
package scorer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/plugins"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/scheduling/framework"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/scheduling/types"

	"github.com/randomvariable/rocm-llamacpp-envoy-ai-gateway-external-processor/internal/vram"
)

const (
	// VRAMScorerType is the type identifier for the VRAM scorer plugin.
	VRAMScorerType = "vram-scorer"

	// DefaultVRAMThresholdGB is the minimum VRAM (in GB) required for a positive score.
	DefaultVRAMThresholdGB = 4

	// logVerbosity is the klog verbosity level for debug messages.
	logVerbosity = 2

	// minScore is the minimum score for pods with low VRAM.
	minScore = 0.1

	// maxScoreRange is the range between min and max scores.
	maxScoreRange = 0.9

	// defaultScore is the score when no max VRAM is available for normalization.
	defaultScore = 0.5
)

// vramScorerParameters holds the configuration for the VRAM scorer.
type vramScorerParameters struct {
	// ThresholdGB is the minimum VRAM (in GB) for a positive score.
	ThresholdGB int `json:"thresholdGB"`
}

// ErrDepsNotSet indicates that scorer dependencies were not initialized.
var ErrDepsNotSet = errors.New("VRAM scorer dependencies not set - call SetVRAMScorerDeps first")

// compile-time type assertion.
var _ framework.Scorer = &VRAMScorer{}

// VRAMScorer scores endpoints based on available VRAM.
// Endpoints with more available VRAM get higher scores.
type VRAMScorer struct {
	typedName   plugins.TypedName
	tracker     *vram.Tracker
	client      crclient.Client
	thresholdGB int
}

// VRAMScorerDeps holds the dependencies for the VRAM scorer.
// These are injected at startup time before plugin registration.
type VRAMScorerDeps struct {
	Tracker *vram.Tracker
	Client  crclient.Client
}

// Global deps - set before plugin registration.
var scorerDeps *VRAMScorerDeps

// SetVRAMScorerDeps sets the dependencies for the VRAM scorer.
// Must be called before RegisterAllPlugins().
func SetVRAMScorerDeps(deps *VRAMScorerDeps) {
	scorerDeps = deps
}

// VRAMScorerFactory creates a new VRAM scorer plugin instance.
//
//nolint:ireturn // Required by plugins.FactoryFunc interface signature.
func VRAMScorerFactory(name string, rawParameters json.RawMessage, _ plugins.Handle) (plugins.Plugin, error) {
	if scorerDeps == nil {
		return nil, ErrDepsNotSet
	}

	params := vramScorerParameters{ThresholdGB: DefaultVRAMThresholdGB}
	if rawParameters != nil {
		err := json.Unmarshal(rawParameters, &params)
		if err != nil {
			return nil, fmt.Errorf("failed to parse %s parameters: %w", VRAMScorerType, err)
		}
	}

	return NewVRAMScorer(scorerDeps.Tracker, scorerDeps.Client, params.ThresholdGB).WithName(name), nil
}

// NewVRAMScorer creates a new VRAM scorer.
func NewVRAMScorer(tracker *vram.Tracker, client crclient.Client, thresholdGB int) *VRAMScorer {
	if thresholdGB <= 0 {
		thresholdGB = DefaultVRAMThresholdGB
	}

	return &VRAMScorer{
		typedName:   plugins.TypedName{Type: VRAMScorerType},
		tracker:     tracker,
		client:      client,
		thresholdGB: thresholdGB,
	}
}

// WithName sets the plugin instance name.
func (s *VRAMScorer) WithName(name string) *VRAMScorer {
	s.typedName.Name = name

	return s
}

// TypedName returns the plugin type and name.
func (s *VRAMScorer) TypedName() plugins.TypedName {
	return s.typedName
}

// Score scores pods based on available VRAM.
// Returns a score between 0 and 1, where 1 indicates maximum available VRAM.
func (s *VRAMScorer) Score(ctx context.Context, _ *types.CycleState, _ *types.LLMRequest, pods []types.Pod) map[types.Pod]float64 {
	scores := make(map[types.Pod]float64, len(pods))

	// Build pod name -> node name mapping.
	podToNode := s.buildPodToNodeMap(ctx, pods)

	// Get VRAM metrics from tracker.
	nodeVRAM := s.tracker.GetMetrics()

	// Calculate max VRAM for normalization.
	var maxAvailable int64
	for _, m := range nodeVRAM {
		if m.AvailableVRAM > maxAvailable {
			maxAvailable = m.AvailableVRAM
		}
	}

	thresholdBytes := int64(s.thresholdGB) * vram.BytesPerGB

	for _, pod := range pods {
		podInfo := pod.GetPod()
		if podInfo == nil {
			scores[pod] = 0

			continue
		}

		// Look up node for this pod.
		podKey := fmt.Sprintf("%s/%s", podInfo.NamespacedName.Namespace, podInfo.NamespacedName.Name)

		nodeName, ok := podToNode[podKey]
		if !ok {
			klog.V(logVerbosity).Infof("No node found for pod %s, scoring 0", podKey)

			scores[pod] = 0

			continue
		}

		// Get VRAM for the node.
		metrics, ok := nodeVRAM[nodeName]
		if !ok {
			klog.V(logVerbosity).Infof("No VRAM metrics for node %s, scoring 0", nodeName)

			scores[pod] = 0

			continue
		}

		// Score based on available VRAM.
		if metrics.AvailableVRAM < thresholdBytes {
			// Below threshold - very low score.
			scores[pod] = minScore

			klog.V(logVerbosity).Infof("Pod %s on node %s has low VRAM (%d GB), scoring %.1f",
				podKey, nodeName, metrics.AvailableVRAM/vram.BytesPerGB, minScore)

			continue
		}

		// Normalize to 0.1-1.0 range based on available VRAM.
		if maxAvailable > 0 {
			// Score between 0.1 and 1.0 based on relative available VRAM.
			score := minScore + maxScoreRange*float64(metrics.AvailableVRAM)/float64(maxAvailable)
			scores[pod] = score
			klog.V(logVerbosity).Infof("Pod %s on node %s: %d GB available, score %.2f",
				podKey, nodeName, metrics.AvailableVRAM/vram.BytesPerGB, score)
		} else {
			scores[pod] = defaultScore
		}
	}

	return scores
}

// buildPodToNodeMap builds a mapping from pod namespace/name to node name.
func (s *VRAMScorer) buildPodToNodeMap(ctx context.Context, pods []types.Pod) map[string]string {
	podToNode := make(map[string]string)

	// Collect unique namespaces to query.
	namespaces := make(map[string]struct{})

	for _, pod := range pods {
		podInfo := pod.GetPod()
		if podInfo != nil {
			namespaces[podInfo.NamespacedName.Namespace] = struct{}{}
		}
	}

	// List pods in each namespace.
	for ns := range namespaces {
		var podList corev1.PodList

		err := s.client.List(ctx, &podList, crclient.InNamespace(ns))
		if err != nil {
			klog.Errorf("Failed to list pods in namespace %s: %v", ns, err)

			continue
		}

		for i := range podList.Items {
			p := &podList.Items[i]
			key := fmt.Sprintf("%s/%s", p.Namespace, p.Name)
			podToNode[key] = p.Spec.NodeName
		}
	}

	return podToNode
}
