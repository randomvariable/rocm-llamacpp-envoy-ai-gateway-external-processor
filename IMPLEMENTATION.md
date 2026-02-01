# Implementation Summary

## Overview

This repository implements a meta-router for managing llama.cpp pods across a Kubernetes cluster, specifically designed for a 3-node Strix Halo cluster with AMD GPUs. The router addresses scheduling issues and resource utilization challenges by intelligently routing requests to warm nodes and using VRAM-aware scheduling for model loading.

## Key Components

### 1. Meta-Router (Go-based)
- **main.go**: Core router logic with request routing, health checking, and node management
- **rocm.go**: ROCm SMI integration for GPU VRAM monitoring via Kubernetes exec API
- Built as a multi-stage Docker image for optimal size

### 2. Kubernetes Manifests

- **DaemonSet** (`k8s/llamacpp-daemonset.yaml`): Ensures llama.cpp runs on each GPU node
- **Deployment** (`k8s/meta-router-deployment.yaml`): Runs 2 replicas of the router for HA
- **Services**: Headless service for llama.cpp pods, ClusterIP for router
- **RBAC** (`k8s/rbac.yaml`): Grants necessary permissions including pods/exec for ROCm SMI
- **ConfigMap** (`k8s/configmap.yaml`): Centralized configuration

### 3. Key Features

#### Warm Node Routing
- Tracks which nodes have models loaded in memory
- Routes requests to warm nodes for immediate response
- Maintains a model-to-nodes mapping for quick lookups

#### VRAM-Aware Scheduling
- Uses ROCm SMI to query GPU VRAM usage on each node
- When loading a new model, selects the node with least VRAM contention
- Falls back to estimation if ROCm SMI is unavailable
- Configurable via `USE_ROCM_SMI` environment variable

#### Health Monitoring
- Continuous health checks every 30 seconds (configurable)
- Tracks node health, warm status, and loaded models
- Updates VRAM usage during health checks

#### LiteLLM Integration
- Compatible API endpoints: `/v1/completions` and `/v1/chat/completions`
- Drop-in replacement for direct llama.cpp backends
- See `LITELLM_INTEGRATION.md` for configuration details

## Architecture

```
┌──────────────┐
│   LiteLLM    │ (Optional proxy layer)
└──────┬───────┘
       │
       ▼
┌──────────────────────────────┐
│   Meta-Router (2 replicas)   │
│   - Health checks             │
│   - ROCm SMI queries          │
│   - Request routing           │
└──────────────┬───────────────┘
               │
       ┌───────┴────────┬────────┐
       ▼                ▼        ▼
┌──────────┐    ┌──────────┐   ┌──────────┐
│  Node 1  │    │  Node 2  │   │  Node 3  │
│ llama.cpp│    │ llama.cpp│   │ llama.cpp│
│ +AMD GPU │    │ +AMD GPU │   │ +AMD GPU │
│ 128GB RAM│    │ 128GB RAM│   │ 128GB RAM│
└──────────┘    └──────────┘   └──────────┘
```

## Configuration

### Environment Variables

Set in `k8s/configmap.yaml`:

- `HEALTH_CHECK_INTERVAL`: Health check interval in seconds (default: 30)
- `LLAMA_PORT`: Port where llama.cpp listens (default: 8080)
- `NAMESPACE`: Kubernetes namespace (default: default)
- `PORT`: Router HTTP port (default: 8000)
- `USE_ROCM_SMI`: Enable VRAM-aware scheduling (default: true)
- `DEFAULT_MODEL`: Default model when none specified (default: llama-2-7b)
- `DEFAULT_VRAM_MB`: Default VRAM size for fallback (default: 131072 = 128GB)

### Model Configuration

Edit the model list in `k8s/configmap.yaml` under the `model-config` ConfigMap. Update VRAM estimates in `main.go` `estimateVRAMFromModel()` to match.

## Deployment

### Prerequisites
- Kubernetes cluster with AMD GPU nodes
- AMD GPU device plugin installed
- kubectl configured
- Models stored on each node at `/data/models`

### Quick Deploy

```bash
# Build and deploy
./deploy.sh

# Or use Make
make build
make deploy
```

### Manual Deploy

```bash
# Build image
docker build -t meta-router:latest .

# Apply manifests
kubectl apply -k k8s/

# Check status
kubectl get pods -l app=meta-router
kubectl get pods -l app=llamacpp
```

## Monitoring

### Status Endpoint

```bash
kubectl port-forward svc/meta-router 8000:8000
curl http://localhost:8000/status
```

Example response:
```json
{
  "nodes": [
    {
      "hostname": "llamacpp-abc",
      "is_healthy": true,
      "is_warm": true,
      "model": "llama-2-7b",
      "vram_used_mb": 8192,
      "vram_total_mb": 131072,
      "vram_percent": 6.25
    }
  ],
  "model_to_nodes": {
    "llama-2-7b": ["llamacpp-abc"]
  }
}
```

### Health Endpoint

```bash
curl http://localhost:8000/health
```

### Logs

```bash
# Router logs
kubectl logs -l app=meta-router -f

# llama.cpp logs
kubectl logs -l app=llamacpp -f
```

## Benefits

1. **Consistent Response Times**: Warm nodes respond immediately without model loading delays
2. **Efficient Resource Usage**: Models stay loaded across cluster, maximizing 128GB RAM utilization
3. **Intelligent Scheduling**: VRAM-aware placement prevents overloading individual nodes
4. **High Availability**: Multiple router replicas and DaemonSet ensure resilience
5. **Simple Integration**: LiteLLM-compatible API for easy adoption

## Troubleshooting

### Pods Not Scheduling
- Verify AMD GPU device plugin: `kubectl get nodes -o yaml | grep amd.com/gpu`
- Check node labels: `kubectl get nodes --show-labels`
- Review DaemonSet status: `kubectl describe ds llamacpp`

### ROCm SMI Errors
- Ensure ROCm tools are available in llama.cpp container
- Check RBAC permissions: `kubectl get role meta-router -o yaml`
- Set `USE_ROCM_SMI=false` to disable and use estimation instead

### Models Not Loading
- Verify model files at `/data/models` on each node
- Check llama.cpp logs for errors
- Ensure sufficient memory available

### Router Can't Find Nodes
- Verify pods have label `app=llamacpp`
- Check router logs for discovery errors
- Ensure RBAC permissions are applied

## Security

- Non-root container user (UID 1000)
- Read-only model mounts
- RBAC with minimal required permissions
- No security vulnerabilities found (CodeQL scan passed)

## Future Enhancements

- Metrics export (Prometheus)
- Grafana dashboards for visualization
- Model pre-warming on startup
- Auto-scaling based on load
- Support for multiple GPUs per node
- Advanced load balancing strategies
