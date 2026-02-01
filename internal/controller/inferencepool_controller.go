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

// Package controller provides Kubernetes controllers for the external processor.
package controller

import (
	"context"
	"fmt"

	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	inferencev1 "sigs.k8s.io/gateway-api-inference-extension/api/v1"

	"github.com/randomvariable/rocm-llamacpp-envoy-ai-gateway-external-processor/internal/pool"
)

// InferencePoolReconciler reconciles InferencePool resources.
type InferencePoolReconciler struct {
	crclient.Client

	// PoolManager manages multiple InferencePools with their dedicated routers and trackers.
	PoolManager *pool.Manager
}

// Reconcile handles InferencePool resource changes.
func (r *InferencePoolReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	klog.Infof("Reconciling InferencePool: %s/%s", req.Namespace, req.Name)

	// Fetch the InferencePool.
	inferencePool := &inferencev1.InferencePool{}

	err := r.Get(ctx, req.NamespacedName, inferencePool)
	if err != nil {
		if crclient.IgnoreNotFound(err) != nil {
			klog.Errorf("Failed to get InferencePool: %v", err)

			return reconcile.Result{}, fmt.Errorf("failed to get InferencePool: %w", err)
		}
		// Resource not found - it was deleted.
		klog.Infof("InferencePool %s/%s deleted, removing from pool manager", req.Namespace, req.Name)

		r.PoolManager.DeletePool(pool.PoolKey{
			Namespace: req.Namespace,
			Name:      req.Name,
		})

		return reconcile.Result{}, nil
	}

	// Extract selector from InferencePool spec.
	selector := inferencePool.Spec.Selector.MatchLabels

	// Convert LabelSelector map to string map.
	podSelector := make(map[string]string)

	for k, v := range selector {
		podSelector[string(k)] = string(v)
	}

	// Extract target ports.
	targetPorts := make([]int32, 0, len(inferencePool.Spec.TargetPorts))

	for _, port := range inferencePool.Spec.TargetPorts {
		targetPorts = append(targetPorts, int32(port.Number))
	}

	klog.Infof("InferencePool %s/%s uses pod selector: %v, target ports: %v",
		req.Namespace, req.Name, podSelector, targetPorts)

	// Create or update the pool in the manager.
	// Each InferencePool gets its own dedicated Router and VRAMTracker.
	poolKey := pool.PoolKey{
		Namespace: req.Namespace,
		Name:      req.Name,
	}

	err = r.PoolManager.UpsertPool(ctx, poolKey, podSelector, targetPorts)
	if err != nil {
		klog.Errorf("Failed to upsert pool %s: %v", poolKey, err)

		return reconcile.Result{}, fmt.Errorf("failed to upsert pool: %w", err)
	}

	klog.Infof("Successfully reconciled InferencePool %s/%s (total pools: %d)",
		req.Namespace, req.Name, r.PoolManager.PoolCount())

	return reconcile.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *InferencePoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	err := ctrl.NewControllerManagedBy(mgr).
		For(&inferencev1.InferencePool{}).
		Complete(r)
	if err != nil {
		return fmt.Errorf("failed to setup controller: %w", err)
	}

	return nil
}
