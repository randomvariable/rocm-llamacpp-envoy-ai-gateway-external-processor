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

// Package main provides the entry point for the VRAM-aware endpoint picker.
// This implementation uses the official gateway-api-inference-extension framework
// with custom VRAM-aware scoring for ROCm GPU workloads.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/gateway-api-inference-extension/cmd/epp/runner"

	"github.com/randomvariable/rocm-llamacpp-envoy-ai-gateway-external-processor/internal/plugins"
	"github.com/randomvariable/rocm-llamacpp-envoy-ai-gateway-external-processor/internal/plugins/scorer"
	pkgversion "github.com/randomvariable/rocm-llamacpp-envoy-ai-gateway-external-processor/internal/version"
	"github.com/randomvariable/rocm-llamacpp-envoy-ai-gateway-external-processor/internal/vram"
)

// Default configuration values.
const (
	defaultExporterNamespace = "kube-system"
	defaultScrapeInterval    = 30 * time.Second
)

func main() {
	// Check for --version before any flag parsing (runner handles this too, but we want our version string).
	for _, arg := range os.Args[1:] {
		if arg == "--version" || arg == "-version" {
			fmt.Println(pkgversion.GetFullVersionString())
			os.Exit(0)
		}
	}

	initConfig()

	klog.Infof("Starting %s version %s (using gateway-api-inference-extension framework)",
		pkgversion.ServiceName, pkgversion.GetVersion())

	ctx := ctrl.SetupSignalHandler()

	// Initialize Kubernetes client for VRAM tracker.
	config := mustGetKubeConfig()
	k8sClient := mustCreateClient(config)

	// Start VRAM tracker.
	tracker := startVRAMTracker(ctx, k8sClient)

	// Set up dependencies for VRAM scorer plugin.
	scorer.SetVRAMScorerDeps(&scorer.VRAMScorerDeps{
		Tracker: tracker,
		Client:  k8sClient,
	})

	// Register custom plugins.
	plugins.RegisterAllPlugins()

	// Run the official EPP framework.
	err := runner.NewRunner().
		WithExecutableName(pkgversion.ServiceName).
		Run(ctx)
	if err != nil {
		klog.Errorf("EPP runner failed: %v", err)
		os.Exit(1)
	}
}

func initConfig() {
	// VRAM tracker configuration.
	viper.SetDefault("namespace", "llm")
	viper.SetDefault("exporter-namespace", defaultExporterNamespace)
	viper.SetDefault("exporter-pod-selector", map[string]string{
		"app": "amd-gpu-metrics-exporter-amdgpu-metrics-exporter",
	})
	viper.SetDefault("node-selector", map[string]string{"kubernetes.io/gpu": "true"})
	viper.SetDefault("scrape-interval", defaultScrapeInterval)

	viper.SetEnvPrefix("EXTPROC")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()
}

func startVRAMTracker(ctx context.Context, k8sClient crclient.Client) *vram.Tracker {
	namespace := viper.GetString("namespace")
	exporterNamespace := viper.GetString("exporter-namespace")
	exporterPodSelector := viper.GetStringMapString("exporter-pod-selector")
	nodeSelector := viper.GetStringMapString("node-selector")
	scrapeInterval := viper.GetDuration("scrape-interval")

	klog.Infof("Starting VRAM tracker: namespace=%s, exporterNamespace=%s, scrapeInterval=%v",
		namespace, exporterNamespace, scrapeInterval)

	tracker := vram.NewTracker(
		k8sClient,
		namespace,
		nil, // Pod selector - will be updated per pool.
		nodeSelector,
		exporterNamespace,
		exporterPodSelector,
		scrapeInterval,
	)

	go tracker.Start(ctx)

	return tracker
}

func mustGetKubeConfig() *rest.Config {
	kubeconfigPath := os.Getenv("KUBECONFIG")

	if kubeconfigPath != "" {
		cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			klog.Fatalf("Failed to build kubeconfig from flags: %v", err)
		}

		return cfg
	}

	cfg, err := rest.InClusterConfig()
	if err != nil {
		klog.Fatalf("Failed to get in-cluster config: %v", err)
	}

	return cfg
}

func mustCreateClient(config *rest.Config) crclient.Client {
	scheme := runtime.NewScheme()

	err := clientgoscheme.AddToScheme(scheme)
	if err != nil {
		klog.Fatalf("Failed to add clientgo scheme: %v", err)
	}

	k8sClient, err := crclient.New(config, crclient.Options{Scheme: scheme})
	if err != nil {
		klog.Fatalf("Failed to create Kubernetes client: %v", err)
	}

	return k8sClient
}
