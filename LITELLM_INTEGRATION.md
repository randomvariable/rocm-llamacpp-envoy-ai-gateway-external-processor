# LiteLLM Configuration for Meta-Router Integration

This guide explains how to configure LiteLLM to work with the meta-router.

## Configuration

Create a `litellm_config.yaml` file:

```yaml
model_list:
  # Llama 2 7B model
  - model_name: llama-2-7b
    litellm_params:
      model: openai/llama-2-7b
      api_base: http://meta-router:8000/v1
      # For external access, use the appropriate service URL
      # api_base: http://meta-router.default.svc.cluster.local:8000/v1

  # Llama 2 13B model
  - model_name: llama-2-13b
    litellm_params:
      model: openai/llama-2-13b
      api_base: http://meta-router:8000/v1

  # Mistral 7B model
  - model_name: mistral-7b
    litellm_params:
      model: openai/mistral-7b
      api_base: http://meta-router:8000/v1

# General settings
general_settings:
  master_key: your_secret_key_here
  database_url: "postgresql://..."  # Optional: for request logging
  
litellm_settings:
  drop_params: true
  success_callback: ["langfuse"]  # Optional: for observability
  failure_callback: ["langfuse"]
```

## Deployment Options

### Option 1: LiteLLM in Kubernetes

Deploy LiteLLM in the same Kubernetes cluster:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: litellm
spec:
  replicas: 2
  selector:
    matchLabels:
      app: litellm
  template:
    metadata:
      labels:
        app: litellm
    spec:
      containers:
      - name: litellm
        image: ghcr.io/berriai/litellm:latest
        ports:
        - containerPort: 4000
        env:
        - name: LITELLM_MASTER_KEY
          valueFrom:
            secretKeyRef:
              name: litellm-secrets
              key: master-key
        volumeMounts:
        - name: config
          mountPath: /app/config.yaml
          subPath: config.yaml
        command: ["litellm", "--config", "/app/config.yaml"]
      volumes:
      - name: config
        configMap:
          name: litellm-config
---
apiVersion: v1
kind: Service
metadata:
  name: litellm
spec:
  selector:
    app: litellm
  ports:
  - port: 4000
    targetPort: 4000
```

### Option 2: LiteLLM External to Cluster

If running LiteLLM outside the cluster, expose meta-router:

```bash
# Using port-forward for testing
kubectl port-forward svc/meta-router 8000:8000

# Or create an Ingress/LoadBalancer
kubectl expose deployment meta-router --type=LoadBalancer --port=8000
```

Then configure LiteLLM with the external URL:

```yaml
model_list:
  - model_name: llama-2-7b
    litellm_params:
      model: openai/llama-2-7b
      api_base: http://your-cluster-ip:8000/v1
```

## Usage Examples

### Using LiteLLM Proxy

```python
import openai

client = openai.OpenAI(
    api_key="your_litellm_key",
    base_url="http://litellm:4000"
)

response = client.completions.create(
    model="llama-2-7b",
    prompt="Explain meta-routing in simple terms:",
    max_tokens=100
)

print(response.choices[0].text)
```

### Direct API Call

```bash
curl -X POST http://litellm:4000/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your_litellm_key" \
  -d '{
    "model": "llama-2-7b",
    "messages": [{"role": "user", "content": "Hello!"}]
  }'
```

## Benefits of Meta-Router with LiteLLM

1. **Consistent Performance**: Requests routed to warm nodes respond instantly
2. **Resource Efficiency**: Models stay loaded across the cluster
3. **High Availability**: Multiple nodes provide redundancy
4. **Simplified Configuration**: Single endpoint for multiple models
5. **Automatic Scaling**: Meta-router handles model loading as needed

## Monitoring

Monitor the meta-router through LiteLLM's dashboard or by querying the status endpoint:

```bash
# Check which nodes have which models loaded
curl http://meta-router:8000/status

# Health check
curl http://meta-router:8000/health
```
