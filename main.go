package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// NodeState tracks the state of a llama.cpp node
type NodeState struct {
	Hostname        string
	Endpoint        string
	Model           string
	LastHealthCheck time.Time
	IsHealthy       bool
	IsWarm          bool
	VRAMUsed        uint64 // VRAM used in MB
	VRAMTotal       uint64 // Total VRAM in MB
	NodeIP          string // IP address for SSH/exec commands
	mu              sync.RWMutex
}

// MetaRouter manages llama.cpp pods across Kubernetes nodes
type MetaRouter struct {
	nodes              map[string]*NodeState
	modelToNodes       map[string][]string
	mu                 sync.RWMutex
	k8sClient          *kubernetes.Clientset
	Config             *rest.Config // Exported for rocm.go to use
	httpClient         *http.Client
	healthCheckInterval time.Duration
	llamaPort          int
	namespace          string
	useROCmSMI         bool
}

// HealthResponse represents the health check response from llama.cpp
type HealthResponse struct {
	Status string `json:"status"`
	Model  string `json:"model,omitempty"`
}

// CompletionRequest represents an OpenAI-compatible completion request
type CompletionRequest struct {
	Model      string `json:"model"`
	Prompt     string `json:"prompt"`
	MaxTokens  int    `json:"max_tokens,omitempty"`
	Temperature float64 `json:"temperature,omitempty"`
}

// LoadModelRequest represents a model loading request
type LoadModelRequest struct {
	Model string `json:"model"`
}

// StatusResponse represents the router status
type StatusResponse struct {
	Nodes        []*NodeInfo       `json:"nodes"`
	ModelToNodes map[string][]string `json:"model_to_nodes"`
}

// NodeInfo represents node information for status endpoint
type NodeInfo struct {
	Hostname        string    `json:"hostname"`
	Endpoint        string    `json:"endpoint"`
	IsHealthy       bool      `json:"is_healthy"`
	IsWarm          bool      `json:"is_warm"`
	Model           string    `json:"model,omitempty"`
	LastHealthCheck time.Time `json:"last_health_check"`
	VRAMUsed        uint64    `json:"vram_used_mb,omitempty"`
	VRAMTotal       uint64    `json:"vram_total_mb,omitempty"`
	VRAMPercent     float64   `json:"vram_percent,omitempty"`
}

// HealthStatusResponse represents the health endpoint response
type HealthStatusResponse struct {
	Status       string   `json:"status"`
	TotalNodes   int      `json:"total_nodes"`
	HealthyNodes int      `json:"healthy_nodes"`
	WarmNodes    int      `json:"warm_nodes"`
	Models       []string `json:"models"`
}

// NewMetaRouter creates a new meta-router instance
func NewMetaRouter() (*MetaRouter, error) {
	// Initialize Kubernetes client
	var config *rest.Config
	var err error
	
	// Try in-cluster config first
	config, err = rest.InClusterConfig()
	if err != nil {
		// Fall back to kubeconfig
		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			kubeconfig = os.Getenv("HOME") + "/.kube/config"
		}
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create k8s config: %w", err)
		}
	}
	
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create k8s client: %w", err)
	}
	
	// Get configuration from environment
	healthCheckInterval, _ := strconv.Atoi(getEnv("HEALTH_CHECK_INTERVAL", "30"))
	llamaPort, _ := strconv.Atoi(getEnv("LLAMA_PORT", "8080"))
	namespace := getEnv("NAMESPACE", "default")
	useROCmSMI := getEnv("USE_ROCM_SMI", "true") == "true"
	
	router := &MetaRouter{
		nodes:              make(map[string]*NodeState),
		modelToNodes:       make(map[string][]string),
		k8sClient:          clientset,
		Config:             config,
		httpClient:         &http.Client{Timeout: 5 * time.Second},
		healthCheckInterval: time.Duration(healthCheckInterval) * time.Second,
		llamaPort:          llamaPort,
		namespace:          namespace,
		useROCmSMI:         useROCmSMI,
	}
	
	return router, nil
}

// Initialize discovers nodes and starts health check loop
func (r *MetaRouter) Initialize(ctx context.Context) error {
	if err := r.discoverNodes(ctx); err != nil {
		return err
	}
	
	go r.healthCheckLoop(ctx)
	
	return nil
}

// discoverNodes finds all llama.cpp pods in the cluster
func (r *MetaRouter) discoverNodes(ctx context.Context) error {
	log.Println("Discovering llama.cpp nodes...")
	
	pods, err := r.k8sClient.CoreV1().Pods(r.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=llamacpp",
	})
	if err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}
	
	r.mu.Lock()
	defer r.mu.Unlock()
	
	for _, pod := range pods.Items {
		if pod.Status.PodIP != "" && pod.Status.Phase == "Running" {
			hostname := pod.Name
			endpoint := fmt.Sprintf("http://%s:%d", pod.Status.PodIP, r.llamaPort)
			
			r.nodes[hostname] = &NodeState{
				Hostname: hostname,
				Endpoint: endpoint,
				NodeIP:   pod.Status.HostIP,
			}
			log.Printf("Discovered node: %s at %s (host: %s)\n", hostname, endpoint, pod.Status.HostIP)
		}
	}
	
	return nil
}

// checkNodeHealth checks if a node is healthy and what model it has loaded
func (r *MetaRouter) checkNodeHealth(ctx context.Context, node *NodeState) bool {
	req, err := http.NewRequestWithContext(ctx, "GET", node.Endpoint+"/health", nil)
	if err != nil {
		log.Printf("Failed to create health check request for %s: %v\n", node.Hostname, err)
		return false
	}
	
	resp, err := r.httpClient.Do(req)
	if err != nil {
		log.Printf("Health check connection error for %s: %v\n", node.Hostname, err)
		node.mu.Lock()
		node.IsHealthy = false
		node.IsWarm = false
		node.mu.Unlock()
		return false
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		log.Printf("Health check failed for %s: HTTP %d\n", node.Hostname, resp.StatusCode)
		node.mu.Lock()
		node.IsHealthy = false
		node.IsWarm = false
		node.mu.Unlock()
		return false
	}
	
	var health HealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		log.Printf("Failed to decode health response from %s: %v\n", node.Hostname, err)
		node.mu.Lock()
		node.IsHealthy = false
		node.IsWarm = false
		node.mu.Unlock()
		return false
	}
	
	node.mu.Lock()
	node.IsHealthy = true
	node.LastHealthCheck = time.Now()
	
	if health.Model != "" {
		node.Model = health.Model
		node.IsWarm = true
		
		// Update model mapping
		r.mu.Lock()
		if _, exists := r.modelToNodes[health.Model]; !exists {
			r.modelToNodes[health.Model] = []string{}
		}
		// Check if hostname already in list
		found := false
		for _, h := range r.modelToNodes[health.Model] {
			if h == node.Hostname {
				found = true
				break
			}
		}
		if !found {
			r.modelToNodes[health.Model] = append(r.modelToNodes[health.Model], node.Hostname)
		}
		r.mu.Unlock()
	} else {
		node.IsWarm = false
	}
	node.mu.Unlock()
	
	return true
}

// Simple queryROCmSMI stub that calls the real implementation
func (r *MetaRouter) queryROCmSMI(ctx context.Context, node *NodeState) error {
	if !r.useROCmSMI {
		return nil
	}
	
	r.updateNodeVRAM(ctx, node)
	return nil
}

// estimateVRAMFromModel estimates VRAM usage based on loaded model
func estimateVRAMFromModel(model string) uint64 {
	// Rough estimates in MB
	estimates := map[string]uint64{
		"llama-2-7b":  8000,  // ~8GB
		"llama-2-13b": 14000, // ~14GB
		"mistral-7b":  8000,  // ~8GB
	}
	
	if vram, ok := estimates[model]; ok {
		return vram
	}
	
	// Default estimate
	return 8000
}

// healthCheckLoop periodically checks health of all nodes
func (r *MetaRouter) healthCheckLoop(ctx context.Context) {
	ticker := time.NewTicker(r.healthCheckInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			log.Println("Running health checks...")
			
			r.mu.RLock()
			nodes := make([]*NodeState, 0, len(r.nodes))
			for _, node := range r.nodes {
				nodes = append(nodes, node)
			}
			r.mu.RUnlock()
			
			for _, node := range nodes {
				r.checkNodeHealth(ctx, node)
				// Update VRAM info if using ROCm SMI
				if r.useROCmSMI {
					r.queryROCmSMI(ctx, node)
				}
			}
		}
	}
}

// getWarmNode finds a warm node that has the requested model loaded
func (r *MetaRouter) getWarmNode(model string) *NodeState {
	r.mu.RLock()
	hostnames, exists := r.modelToNodes[model]
	r.mu.RUnlock()
	
	if !exists {
		return nil
	}
	
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	for _, hostname := range hostnames {
		node, exists := r.nodes[hostname]
		if !exists {
			continue
		}
		
		node.mu.RLock()
		isHealthy := node.IsHealthy
		isWarm := node.IsWarm
		node.mu.RUnlock()
		
		if isHealthy && isWarm {
			return node
		}
	}
	
	return nil
}

// getAvailableNode finds the healthy node with the least VRAM usage
func (r *MetaRouter) getAvailableNode() *NodeState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	var bestNode *NodeState
	var leastVRAMUsed uint64 = ^uint64(0) // Max uint64
	
	// First pass: try to find a non-warm healthy node with least VRAM
	for _, node := range r.nodes {
		node.mu.RLock()
		isHealthy := node.IsHealthy
		isWarm := node.IsWarm
		vramUsed := node.VRAMUsed
		node.mu.RUnlock()
		
		if isHealthy && !isWarm {
			if r.useROCmSMI && vramUsed < leastVRAMUsed {
				leastVRAMUsed = vramUsed
				bestNode = node
			} else if !r.useROCmSMI {
				// If not using ROCm SMI, return first available
				return node
			}
		}
	}
	
	if bestNode != nil {
		log.Printf("Selected node %s with least VRAM usage: %d MB\n", bestNode.Hostname, leastVRAMUsed)
		return bestNode
	}
	
	// Second pass: if all nodes are warm, find the one with most free VRAM
	leastVRAMUsed = ^uint64(0)
	for _, node := range r.nodes {
		node.mu.RLock()
		isHealthy := node.IsHealthy
		vramUsed := node.VRAMUsed
		node.mu.RUnlock()
		
		if isHealthy {
			if r.useROCmSMI && vramUsed < leastVRAMUsed {
				leastVRAMUsed = vramUsed
				bestNode = node
			} else if !r.useROCmSMI && bestNode == nil {
				bestNode = node
			}
		}
	}
	
	if bestNode != nil && r.useROCmSMI {
		log.Printf("All nodes warm, selected node %s with least VRAM usage: %d MB\n", 
			bestNode.Hostname, leastVRAMUsed)
	}
	
	return bestNode
}

// loadModel loads a model on a specific node
func (r *MetaRouter) loadModel(ctx context.Context, node *NodeState, model string) error {
	log.Printf("Loading model %s on %s\n", model, node.Hostname)
	
	loadReq := LoadModelRequest{Model: model}
	body, err := json.Marshal(loadReq)
	if err != nil {
		return fmt.Errorf("failed to marshal load request: %w", err)
	}
	
	req, err := http.NewRequestWithContext(ctx, "POST", node.Endpoint+"/load", 
		bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create load request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Connection error loading model %s on %s: %v\n", model, node.Hostname, err)
		return err
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		log.Printf("Failed to load model %s on %s: HTTP %d\n", model, node.Hostname, resp.StatusCode)
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	
	node.mu.Lock()
	node.Model = model
	node.IsWarm = true
	// Estimate VRAM usage for the loaded model
	if r.useROCmSMI {
		node.VRAMUsed += estimateVRAMFromModel(model)
	}
	node.mu.Unlock()
	
	r.mu.Lock()
	if _, exists := r.modelToNodes[model]; !exists {
		r.modelToNodes[model] = []string{}
	}
	found := false
	for _, h := range r.modelToNodes[model] {
		if h == node.Hostname {
			found = true
			break
		}
	}
	if !found {
		r.modelToNodes[model] = append(r.modelToNodes[model], node.Hostname)
	}
	r.mu.Unlock()
	
	log.Printf("Successfully loaded %s on %s\n", model, node.Hostname)
	return nil
}

// handleCompletions routes a completion request to an appropriate node
func (r *MetaRouter) handleCompletions(w http.ResponseWriter, req *http.Request) {
	var compReq CompletionRequest
	if err := json.NewDecoder(req.Body).Decode(&compReq); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request: %v", err), http.StatusBadRequest)
		return
	}
	
	model := compReq.Model
	if model == "" {
		model = "default"
	}
	
	// Try to find a warm node with the model
	node := r.getWarmNode(model)
	
	if node == nil {
		// No warm node, try to load model on available node
		log.Printf("No warm node for model %s, attempting to load\n", model)
		node = r.getAvailableNode()
		
		if node == nil {
			http.Error(w, "No available nodes", http.StatusServiceUnavailable)
			return
		}
		
		if err := r.loadModel(req.Context(), node, model); err != nil {
			http.Error(w, fmt.Sprintf("Failed to load model: %v", err), http.StatusServiceUnavailable)
			return
		}
	}
	
	// Forward request to the node
	log.Printf("Routing request to %s for model %s\n", node.Hostname, model)
	
	body, err := json.Marshal(compReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to marshal request: %v", err), http.StatusInternalServerError)
		return
	}
	
	proxyReq, err := http.NewRequestWithContext(req.Context(), "POST", 
		node.Endpoint+"/v1/completions", bytes.NewReader(body))
	if err != nil {
		log.Printf("Error creating proxy request to %s: %v\n", node.Hostname, err)
		http.Error(w, fmt.Sprintf("Failed to create proxy request: %v", err), http.StatusBadGateway)
		return
	}
	
	// Copy headers
	proxyReq.Header = req.Header.Clone()
	proxyReq.Header.Set("Content-Type", "application/json")
	
	client := &http.Client{Timeout: 120 * time.Second}
	proxyResp, err := client.Do(proxyReq)
	if err != nil {
		log.Printf("Error forwarding request to %s: %v\n", node.Hostname, err)
		http.Error(w, fmt.Sprintf("Failed to forward request to %s: %v", node.Hostname, err), 
			http.StatusBadGateway)
		return
	}
	defer proxyResp.Body.Close()
	
	// Copy response headers
	for k, v := range proxyResp.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(proxyResp.StatusCode)
	
	// Copy response body
	if _, err := io.Copy(w, proxyResp.Body); err != nil {
		log.Printf("Error copying response from %s: %v\n", node.Hostname, err)
	}
}

// handleHealth returns health status of the router
func (r *MetaRouter) handleHealth(w http.ResponseWriter, req *http.Request) {
	r.mu.RLock()
	totalNodes := len(r.nodes)
	
	healthyNodes := 0
	warmNodes := 0
	for _, node := range r.nodes {
		node.mu.RLock()
		if node.IsHealthy {
			healthyNodes++
		}
		if node.IsWarm {
			warmNodes++
		}
		node.mu.RUnlock()
	}
	
	models := make([]string, 0, len(r.modelToNodes))
	for model := range r.modelToNodes {
		models = append(models, model)
	}
	r.mu.RUnlock()
	
	status := HealthStatusResponse{
		Status:       "healthy",
		TotalNodes:   totalNodes,
		HealthyNodes: healthyNodes,
		WarmNodes:    warmNodes,
		Models:       models,
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// handleStatus returns detailed status of all nodes
func (r *MetaRouter) handleStatus(w http.ResponseWriter, req *http.Request) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	nodes := make([]*NodeInfo, 0, len(r.nodes))
	for _, node := range r.nodes {
		node.mu.RLock()
		info := &NodeInfo{
			Hostname:        node.Hostname,
			Endpoint:        node.Endpoint,
			IsHealthy:       node.IsHealthy,
			IsWarm:          node.IsWarm,
			Model:           node.Model,
			LastHealthCheck: node.LastHealthCheck,
			VRAMUsed:        node.VRAMUsed,
			VRAMTotal:       node.VRAMTotal,
		}
		if node.VRAMTotal > 0 {
			info.VRAMPercent = float64(node.VRAMUsed) / float64(node.VRAMTotal) * 100
		}
		node.mu.RUnlock()
		nodes = append(nodes, info)
	}
	
	status := StatusResponse{
		Nodes:        nodes,
		ModelToNodes: r.modelToNodes,
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func main() {
	router, err := NewMetaRouter()
	if err != nil {
		log.Fatalf("Failed to create router: %v", err)
	}
	
	ctx := context.Background()
	if err := router.Initialize(ctx); err != nil {
		log.Fatalf("Failed to initialize router: %v", err)
	}
	
	// Setup HTTP routes
	http.HandleFunc("/v1/completions", router.handleCompletions)
	http.HandleFunc("/v1/chat/completions", router.handleCompletions)
	http.HandleFunc("/health", router.handleHealth)
	http.HandleFunc("/status", router.handleStatus)
	
	port := getEnv("PORT", "8000")
	log.Printf("Starting meta-router on port %s\n", port)
	
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
