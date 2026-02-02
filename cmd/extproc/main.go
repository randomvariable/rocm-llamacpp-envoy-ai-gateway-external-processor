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

// Package main provides the entry point for the ROCm Envoy AI Gateway external processor.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	inferencev1 "sigs.k8s.io/gateway-api-inference-extension/api/v1"

	"github.com/randomvariable/rocm-envoy-ai-gateway-external-processor/pkg/controller"
	"github.com/randomvariable/rocm-envoy-ai-gateway-external-processor/pkg/epp"
	"github.com/randomvariable/rocm-envoy-ai-gateway-external-processor/pkg/pool"
	"github.com/randomvariable/rocm-envoy-ai-gateway-external-processor/pkg/telemetry"
	pkgversion "github.com/randomvariable/rocm-envoy-ai-gateway-external-processor/pkg/version"
)

// Default configuration values.
const (
	defaultGRPCPort          = 9001
	defaultMetricsPort       = 9090
	defaultExporterNamespace = "kube-system"
	defaultScrapeInterval    = 30 * time.Second
	defaultMetricInterval    = 10 * time.Second
	telemetryShutdownTimeout = 5 * time.Second
	serverShutdownTimeout    = 10 * time.Second
	readHeaderTimeout        = 5 * time.Second
)

var (
	configFile          = flag.String("config", "", "Path to config file (optional)")
	enableInferencePool = flag.Bool("enable-inference-pool", true, "Enable InferencePool controller")
	showVersion         = flag.Bool("version", false, "Print version information and exit")
)

// appConfig holds all configuration values.
type appConfig struct {
	namespace           string
	exporterNamespace   string
	exporterPodSelector map[string]string
	nodeSelector        map[string]string
	scrapeInterval      time.Duration
	modelLoadEndpoint   string
	grpcPort            int
	metricsPort         int
}

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	if *showVersion {
		fmt.Println(pkgversion.GetFullVersionString())
		os.Exit(0)
	}

	initConfig()

	klog.Infof("Starting %s version %s", pkgversion.ServiceName, pkgversion.GetVersion())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	telemetryProvider := initTelemetry(ctx)
	defer shutdownTelemetry(telemetryProvider)

	config := mustGetKubeConfig()
	k8sClient := mustCreateClient(config)
	appCfg := loadAppConfig()

	// Create a PoolManager to handle multiple InferencePools.
	// Each InferencePool gets its own Router and VRAMTracker.
	poolManager := pool.NewManager(
		k8sClient,
		appCfg.exporterNamespace,
		appCfg.exporterPodSelector,
		appCfg.modelLoadEndpoint,
	)

	mgr := setupControllerManager(config, poolManager)

	genaiMetrics := mustCreateGenAIMetrics()

	grpcServer, listener := setupGRPCServer(ctx, poolManager, genaiMetrics, appCfg.grpcPort)
	metricsServer := setupMetricsServer(poolManager, appCfg.metricsPort)

	startBackgroundServices(ctx, poolManager, mgr, grpcServer, listener, metricsServer, appCfg)
	waitForShutdown(grpcServer, metricsServer)
}

func initTelemetry(ctx context.Context) *telemetry.Provider {
	telemetryCfg := &telemetry.Config{
		OTLPEndpoint:                  viper.GetString("otlp-endpoint"),
		OTLPTracesEnabled:             viper.GetBool("otlp-traces-enabled"),
		OTLPMetricsEnabled:            viper.GetBool("otlp-metrics-enabled"),
		PrometheusEnabled:             viper.GetBool("prometheus-enabled"),
		RuntimeInstrumentationEnabled: viper.GetBool("runtime-instrumentation-enabled"),
		HostInstrumentationEnabled:    viper.GetBool("host-instrumentation-enabled"),
		MetricInterval:                viper.GetDuration("metric-interval"),
	}

	provider, err := telemetry.Initialize(ctx, telemetryCfg)
	if err != nil {
		klog.Fatalf("Failed to initialize telemetry: %v", err)
	}

	return provider
}

func shutdownTelemetry(provider *telemetry.Provider) {
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), telemetryShutdownTimeout)
	defer shutdownCancel()

	err := provider.Shutdown(shutdownCtx)
	if err != nil {
		klog.Errorf("Telemetry shutdown error: %v", err)
	}
}

func mustGetKubeConfig() *rest.Config {
	config, err := getKubeConfig(viper.GetString("kubeconfig"))
	if err != nil {
		klog.Fatalf("Failed to get kubeconfig: %v", err)
	}

	return config
}

func mustCreateClient(config *rest.Config) crclient.Client {
	scheme := setupScheme()

	k8sClient, err := crclient.New(config, crclient.Options{Scheme: scheme})
	if err != nil {
		klog.Fatalf("Failed to create Kubernetes client: %v", err)
	}

	return k8sClient
}

func loadAppConfig() *appConfig {
	return &appConfig{
		namespace:           viper.GetString("namespace"),
		exporterNamespace:   viper.GetString("exporter-namespace"),
		exporterPodSelector: viper.GetStringMapString("exporter-pod-selector"),
		nodeSelector:        viper.GetStringMapString("node-selector"),
		scrapeInterval:      viper.GetDuration("scrape-interval"),
		modelLoadEndpoint:   viper.GetString("model-load-endpoint"),
		grpcPort:            viper.GetInt("grpc-port"),
		metricsPort:         viper.GetInt("metrics-port"),
	}
}

func setupControllerManager(config *rest.Config, poolManager *pool.Manager) ctrl.Manager {
	if !*enableInferencePool {
		return nil
	}

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	mgr, err := ctrl.NewManager(config, ctrl.Options{
		Scheme: setupScheme(),
	})
	if err != nil {
		klog.Fatalf("Failed to create controller manager: %v", err)
	}

	err = (&controller.InferencePoolReconciler{
		Client:      mgr.GetClient(),
		PoolManager: poolManager,
	}).SetupWithManager(mgr)
	if err != nil {
		klog.Fatalf("Failed to setup InferencePool controller: %v", err)
	}

	klog.Info("InferencePool controller enabled")

	return mgr
}

func mustCreateGenAIMetrics() *telemetry.GenAIMetrics {
	meterProvider := otel.GetMeterProvider()
	tracerProvider := otel.GetTracerProvider()

	genaiMetrics, err := telemetry.NewGenAIMetrics(meterProvider, tracerProvider)
	if err != nil {
		klog.Fatalf("Failed to create GenAI metrics: %v", err)
	}

	klog.Info("GenAI metrics initialized")

	return genaiMetrics
}

func setupGRPCServer(ctx context.Context, poolManager *pool.Manager, genaiMetrics *telemetry.GenAIMetrics, grpcPort int) (*grpc.Server, net.Listener) {
	eppServer := epp.NewServer(poolManager, genaiMetrics)

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	eppServer.RegisterServer(grpcServer)

	listenConfig := &net.ListenConfig{}

	listener, err := listenConfig.Listen(ctx, "tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		klog.Fatalf("Failed to listen on gRPC port: %v", err)
	}

	return grpcServer, listener
}

func setupMetricsServer(poolManager *pool.Manager, metricsPort int) *http.Server {
	metricsMux := http.NewServeMux()
	metricsMux.HandleFunc("/healthz", healthHandler)
	metricsMux.HandleFunc("/readyz", readyHandler(poolManager))
	metricsMux.Handle("/metrics", promhttp.Handler())

	instrumentedHandler := otelhttp.NewHandler(metricsMux, "metrics-server",
		otelhttp.WithMessageEvents(otelhttp.ReadEvents, otelhttp.WriteEvents),
	)

	return &http.Server{
		Addr:              fmt.Sprintf(":%d", metricsPort),
		Handler:           instrumentedHandler,
		ReadHeaderTimeout: readHeaderTimeout,
	}
}

func startBackgroundServices(
	ctx context.Context,
	poolManager *pool.Manager,
	mgr ctrl.Manager,
	grpcServer *grpc.Server,
	listener net.Listener,
	metricsServer *http.Server,
	appCfg *appConfig,
) {
	// Note: VRAM trackers are started by the PoolManager when pools are created.
	_ = poolManager // Suppress unused warning - manager is used by controller

	if *enableInferencePool && mgr != nil {
		go func() {
			klog.Info("Starting controller manager")

			err := mgr.Start(ctx)
			if err != nil {
				klog.Fatalf("Controller manager failed: %v", err)
			}
		}()
	}

	go func() {
		klog.Infof("Starting gRPC server for Envoy ext_proc on port %d", appCfg.grpcPort)

		err := grpcServer.Serve(listener)
		if err != nil {
			klog.Fatalf("gRPC server failed: %v", err)
		}
	}()

	go func() {
		klog.Infof("Starting metrics server on port %d", appCfg.metricsPort)

		err := metricsServer.ListenAndServe()

		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			klog.Fatalf("Metrics server failed: %v", err)
		}
	}()
}

func waitForShutdown(grpcServer *grpc.Server, metricsServer *http.Server) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	klog.Info("Shutting down gracefully...")

	grpcServer.GracefulStop()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), serverShutdownTimeout)
	defer shutdownCancel()

	err := metricsServer.Shutdown(shutdownCtx)
	if err != nil {
		klog.Errorf("Metrics server shutdown error: %v", err)
	}

	klog.Info("Shutdown complete")
}

func initConfig() {
	viper.SetDefault("grpc-port", defaultGRPCPort)
	viper.SetDefault("metrics-port", defaultMetricsPort)
	viper.SetDefault("namespace", "llm")
	viper.SetDefault("exporter-namespace", defaultExporterNamespace)
	viper.SetDefault("exporter-pod-selector", map[string]string{
		"app": "amd-gpu-metrics-exporter-amdgpu-metrics-exporter",
	})
	viper.SetDefault("scrape-interval", defaultScrapeInterval)
	viper.SetDefault("model-load-endpoint", "/v1/models/load")

	// Pod selector comes from InferencePool spec.selector - no config needed.
	// Node selector is used for GPU node discovery (optional, can be overridden).
	viper.SetDefault("node-selector", map[string]string{"kubernetes.io/gpu": "true"})

	viper.SetDefault("otlp-endpoint", "")
	viper.SetDefault("otlp-traces-enabled", false)
	viper.SetDefault("otlp-metrics-enabled", false)
	viper.SetDefault("prometheus-enabled", true)
	viper.SetDefault("runtime-instrumentation-enabled", true)
	viper.SetDefault("host-instrumentation-enabled", true)
	viper.SetDefault("metric-interval", defaultMetricInterval)

	viper.SetEnvPrefix("EXTPROC")
	viper.AutomaticEnv()

	if *configFile != "" {
		viper.SetConfigFile(*configFile)

		err := viper.ReadInConfig()
		if err != nil {
			klog.Warningf("Error reading config file: %v", err)
		} else {
			klog.Infof("Using config file: %s", viper.ConfigFileUsed())
		}
	}
}

func getKubeConfig(kubeconfigPath string) (*rest.Config, error) {
	if kubeconfigPath != "" {
		cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			return nil, fmt.Errorf("failed to build kubeconfig from flags: %w", err)
		}

		return cfg, nil
	}

	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in-cluster config: %w", err)
	}

	return cfg, nil
}

func healthHandler(responseWriter http.ResponseWriter, _ *http.Request) {
	responseWriter.WriteHeader(http.StatusOK)

	_, err := responseWriter.Write([]byte("OK"))
	if err != nil {
		klog.Errorf("Failed to write health response: %v", err)
	}
}

func readyHandler(poolManager *pool.Manager) http.HandlerFunc {
	return func(responseWriter http.ResponseWriter, _ *http.Request) {
		if poolManager.IsReady() {
			responseWriter.WriteHeader(http.StatusOK)

			_, err := responseWriter.Write([]byte("Ready"))
			if err != nil {
				klog.Errorf("Failed to write ready response: %v", err)
			}
		} else {
			responseWriter.WriteHeader(http.StatusServiceUnavailable)

			_, err := responseWriter.Write([]byte("Not ready"))
			if err != nil {
				klog.Errorf("Failed to write not ready response: %v", err)
			}
		}
	}
}

func setupScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()

	err := clientgoscheme.AddToScheme(scheme)
	if err != nil {
		klog.Fatalf("Failed to add clientgo scheme: %v", err)
	}

	err = inferencev1.Install(scheme)
	if err != nil {
		klog.Fatalf("Failed to install inference scheme: %v", err)
	}

	return scheme
}
