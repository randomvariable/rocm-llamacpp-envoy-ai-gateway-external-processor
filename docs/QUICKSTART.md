# Quick Start Guide

This guide will help you get the external processor up and running quickly with Envoy AI Gateway.

## Prerequisites

- Kubernetes cluster (v1.27+)
- GPU nodes with AMD ROCm support
- Envoy AI Gateway deployed
- kubectl configured to access your cluster
- Docker for building images

## Step 1: Create the Namespace

Create the `llm` namespace for all components:

```bash
kubectl create namespace llm
```

## Step 2: Label Your GPU Nodes

Label your GPU nodes so the DaemonSet knows where to run:

```bash
kubectl label nodes <node-name> kubernetes.io/gpu=true
```

## Step 3: Deploy Model Server DaemonSet

Deploy your model server DaemonSet which will run on all GPU nodes. The model server must support:
- `/v1/models` - List loaded models
- `/v1/models/load` - Load a new model

```bash
kubectl apply -f deployments/daemonset/model-server-daemonset.yaml
```

Verify the pods are running:

```bash
kubectl get pods -n llm -l app=model-server
```

## Step 4: Build and Deploy External Processor

Build the external processor Docker image:

```bash
# Build the image
make docker-build

# Tag for your registry (optional)
docker tag external-processor:latest <your-registry>/external-processor:latest

# Push to registry
docker push <your-registry>/external-processor:latest
```

Update the image in `deployments/extproc/extproc.yaml` if you're using a custom registry.

Deploy the external processor:

```bash
kubectl apply -f deployments/extproc/extproc.yaml
```

Verify the deployment:

```bash
kubectl get pods -n llm -l app=external-processor
kubectl get svc -n llm external-processor
```

## Step 5: Create InferencePool

Create an InferencePool resource to configure routing:

```yaml
apiVersion: inference.networking.k8s.io/v1
kind: InferencePool
metadata:
  name: llm-pool
  namespace: llm
spec:
  selector:
    matchLabels:
      app: model-server
  targetPorts:
    - number: 8080
  endpointPickerRef:
    name: external-processor
    kind: Service
    port: 9001
```

Apply it:

```bash
kubectl apply -f examples/inferencepool.yaml
```

## Step 6: Configure Envoy AI Gateway

Configure your Envoy AI Gateway to use the InferencePool. Create an HTTPRoute:

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: inference-route
  namespace: llm
spec:
  parentRefs:
    - group: gateway.networking.k8s.io
      kind: Gateway
      name: inference-gateway
  rules:
    - matches:
        - path:
            type: PathPrefix
            value: /v1/
      backendRefs:
        - group: inference.networking.k8s.io
          kind: InferencePool
          name: llm-pool
```

## Step 7: Test the Setup

Send a request through the Envoy AI Gateway:

```bash
curl http://<envoy-gateway-address>/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "llama-2-7b",
    "messages": [
      {"role": "system", "content": "You are a helpful assistant."},
      {"role": "user", "content": "Tell me about GPU scheduling."}
    ]
  }'
```

## Step 8: Monitor VRAM Usage

Check VRAM metrics from the external processor:

```bash
kubectl port-forward -n llm svc/external-processor-metrics 9090:9090
curl http://localhost:9090/metrics | grep vram
```

## Troubleshooting

### Pods Not Starting

Check if GPU nodes are labeled:

```bash
kubectl get nodes -l kubernetes.io/gpu=true
```

Check pod events:

```bash
kubectl describe pod -n llm <pod-name>
```

### Models Not Loading

Check external processor logs:

```bash
kubectl logs -n llm -l app=external-processor
```

Check model server pod logs:

```bash
kubectl logs -n llm -l app=model-server
```

### VRAM Metrics Not Showing

Check ROCm SMI exporter logs:

```bash
kubectl logs -n llm -l app=model-server -c rocm-smi-exporter
```

Verify the exporter is accessible from the external processor:

```bash
kubectl exec -n llm -it <external-processor-pod> -- wget -O- http://<node-ip>:9400/metrics
```

## Next Steps

- Configure model storage on GPU nodes
- Set up Prometheus to scrape external processor metrics
- Configure ingress for external access
- Adjust VRAM scrape interval based on your workload
