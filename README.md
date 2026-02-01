# llama.cpp Kubernetes Meta-Router

A meta-router for managing llama.cpp pods across Kubernetes clusters, designed to efficiently route requests to warm nodes with pre-loaded models or dynamically load models on demand.

## Overview

This project addresses scheduling and resource utilization challenges in multi-node GPU clusters by:

- **Intelligent Request Routing**: Routes requests to nodes with already-loaded models (warm nodes) for consistent, fast response times
- **Dynamic Model Loading**: Automatically loads models on available nodes when no warm node exists
- **Efficient Resource Usage**: Uses DaemonSets to run llama.cpp on each GPU node, maximizing utilization of 128GB RAM machines
- **GPU Scheduling**: Designed for AMD GPU clusters (Strix Halo) with proper GPU resource management

## Architecture

```
┌─────────────┐
│   litellm   │
│   (proxy)   │
└──────┬──────┘
       │
       ▼
┌─────────────────┐
│  Meta-Router    │◄─── Health checks, model tracking
│  (Deployment)   │
└─────────────────┘
       │
       ▼
┌──────────────────────────────────────┐
│         Kubernetes Cluster           │
│                                      │
│  ┌────────┐  ┌────────┐  ┌────────┐│
│  │ Node 1 │  │ Node 2 │  │ Node 3 ││
│  │llama.cpp│ │llama.cpp│ │llama.cpp││
│  │+Model A │  │+Model B │  │(ready) ││
│  └────────┘  └────────┘  └────────┘│
└──────────────────────────────────────┘
```

## Features

- **Warm Node Routing**: Prioritizes nodes with models already loaded in memory
- **On-Demand Loading**: Loads models dynamically using llama.cpp router mode
- **VRAM-Aware Scheduling**: Uses ROCm SMI to query GPU VRAM usage and schedule models on the least contended node
- **Health Monitoring**: Continuous health checks to track node and model status
- **LiteLLM Compatible**: Drop-in replacement for direct llama.cpp endpoints
- **High Availability**: Runs multiple router instances for redundancy
- **Resource Aware**: Optimized for high-memory GPU nodes

## Quick Start

### Prerequisites

- Kubernetes cluster with AMD GPU nodes
- AMD GPU device plugin installed
- kubectl configured
- Models stored on each node at `/data/models`

### Deployment

1. **Build the meta-router image**:
   ```bash
   docker build -t meta-router:latest .
   ```

2. **Deploy to Kubernetes**:
   ```bash
   kubectl apply -k k8s/
   ```

3. **Verify deployment**:
   ```bash
   kubectl get pods -l app=llamacpp
   kubectl get pods -l app=meta-router
   ```

4. **Check router status**:
   ```bash
   kubectl port-forward svc/meta-router 8000:8000
   curl http://localhost:8000/status
   ```

### Configuration

Edit `k8s/configmap.yaml` to configure:

- `HEALTH_CHECK_INTERVAL`: How often to check node health (default: 30 seconds)
- `LLAMA_PORT`: Port where llama.cpp is listening (default: 8080)
- `NAMESPACE`: Kubernetes namespace (default: default)
- `USE_ROCM_SMI`: Enable ROCm SMI for VRAM-aware scheduling (default: true)

When `USE_ROCM_SMI` is enabled, the router will:
- Query GPU VRAM usage on each node during health checks
- Select the node with the least VRAM usage when loading new models
- Display VRAM usage in the `/status` endpoint

Edit `k8s/llamacpp-daemonset.yaml` to configure llama.cpp settings:

- GPU layers, context size, memory limits
- Model paths and volumes
- Resource requests and limits

## Usage

### With LiteLLM

Configure LiteLLM to use the meta-router as the backend:

```yaml
model_list:
  - model_name: llama-2-7b
    litellm_params:
      model: openai/llama-2-7b
      api_base: http://meta-router:8000/v1
```

### Direct API Calls

```bash
curl -X POST http://meta-router:8000/v1/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "llama-2-7b",
    "prompt": "Hello, world!",
    "max_tokens": 100
  }'
```

### Monitoring

Check router health and node status:

```bash
# Health check
curl http://meta-router:8000/health

# Detailed status with VRAM usage
curl http://meta-router:8000/status
```

Example status output:
```json
{
  "nodes": [
    {
      "hostname": "llamacpp-abc123",
      "endpoint": "http://10.1.2.3:8080",
      "is_healthy": true,
      "is_warm": true,
      "model": "llama-2-7b",
      "vram_used_mb": 8192,
      "vram_total_mb": 131072,
      "vram_percent": 6.25
    }
  ],
  "model_to_nodes": {
    "llama-2-7b": ["llamacpp-abc123"]
  }
}
```

## Development

### Running Locally

```bash
# Install dependencies
go mod download

# Set environment variables
export NAMESPACE=default
export LLAMA_PORT=8080
export PORT=8000

# Run the router
go run main.go
```

### Testing

Test the router with a local llama.cpp instance:

```bash
# Start llama.cpp server
llama-server --model /path/to/model.gguf --port 8080

# In another terminal, start the router
go run main.go
```

## Troubleshooting

### Pods not scheduling
- Verify AMD GPU device plugin is installed
- Check node labels: `kubectl get nodes --show-labels`
- Ensure nodes have `gpu.amd.com/gpu` label

### Models not loading
- Verify model files exist at `/data/models` on each node
- Check pod logs: `kubectl logs -l app=llamacpp`
- Ensure sufficient memory is available

### Router can't find nodes
- Check RBAC permissions: `kubectl get rolebinding meta-router`
- Verify pods have label `app=llamacpp`
- Check router logs: `kubectl logs -l app=meta-router`

## Community, discussion, contribution, and support

Learn how to engage with the Kubernetes community on the [community page](http://kubernetes.io/community/).

You can reach the maintainers of this project at:

- [Slack](https://slack.k8s.io/)
- [Mailing List](https://groups.google.com/a/kubernetes.io/g/dev)

### Code of conduct

Participation in the Kubernetes community is governed by the [Kubernetes Code of Conduct](code-of-conduct.md).

[owners]: https://git.k8s.io/community/contributors/guide/owners.md
[Creative Commons 4.0]: https://git.k8s.io/website/LICENSE
