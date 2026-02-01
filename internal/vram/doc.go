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

// Package vram provides VRAM (Video RAM) tracking and metrics for GPU nodes.
//
// This package is designed to be imported and used by other projects that need
// GPU memory-aware scheduling or monitoring capabilities.
//
// # Architecture
//
// The package is built around interfaces to allow flexible integration:
//
//   - [MetricsScraper]: Fetches raw VRAM metrics from endpoints (default: Prometheus format)
//   - [NodeDiscovery]: Discovers nodes and their metric endpoints (default: Kubernetes)
//   - [Tracker]: Coordinates scraping and provides VRAM-aware node selection
//
// # Basic Usage
//
// For simple use cases with Kubernetes and ROCm SMI exporters:
//
//	tracker := vram.NewTracker(
//	    k8sClient,
//	    "default",                              // namespace for model server pods
//	    map[string]string{"app": "model-server"}, // pod selector
//	    map[string]string{"gpu": "true"},         // node selector
//	    "kube-system",                          // exporter namespace
//	    map[string]string{"app": "rocm-exporter"}, // exporter pod selector
//	    30*time.Second,                         // scrape interval
//	)
//
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//	go tracker.Start(ctx)
//
//	// Get node with most available VRAM
//	nodeName, availableVRAM, err := tracker.GetNodeWithMostAvailableVRAM()
//
// # Custom Implementations
//
// To use custom metric sources or discovery mechanisms:
//
//	scraper := &MyCustomScraper{}
//	discovery := &MyCustomDiscovery{}
//	tracker := vram.NewTrackerWithDeps(scraper, discovery, 30*time.Second)
//
// # Metrics Format
//
// The default Prometheus scraper expects these metrics:
//
//   - rocm_memory_total_bytes: Total VRAM in bytes (gauge)
//   - rocm_memory_used_bytes: Used VRAM in bytes (gauge)
//
// Custom metric names can be configured via [PrometheusScraperConfig].
//
// # Thread Safety
//
// All Tracker methods are safe for concurrent use.
package vram
