# Examples

This directory contains example configurations for the external processor.

## Files

### inferencepool.yaml

Example InferencePool and HTTPRoute configuration for integrating with Envoy AI Gateway.

Deploy:
```bash
kubectl apply -f inferencepool.yaml
```

This creates:
- An InferencePool resource that configures the external processor to route to model server pods
- An HTTPRoute that routes `/v1/` requests through the InferencePool

### ingress.yaml

Example Ingress configuration for exposing the Envoy AI Gateway externally using nginx-ingress.

Deploy:
```bash
kubectl apply -f ingress.yaml
```

Update the host field to match your domain.

### servicemonitor.yaml

ServiceMonitor resources for Prometheus Operator to scrape metrics from:
- External processor metrics endpoint
- ROCm SMI exporter on model server pods

Deploy:
```bash
kubectl apply -f servicemonitor.yaml
```

Requires Prometheus Operator to be installed in your cluster.

## Integration Examples

All requests go through Envoy AI Gateway, which uses the external processor for VRAM-aware routing.

### Using with Python

```python
import openai

# Point to your Envoy AI Gateway endpoint
client = openai.OpenAI(
    base_url="http://envoy-gateway.llm/v1",
    api_key="not-needed"
)

response = client.chat.completions.create(
    model="llama-2-7b",
    messages=[
        {"role": "user", "content": "Hello!"}
    ]
)

print(response.choices[0].message.content)
```

### Using with curl

```bash
curl http://envoy-gateway.llm/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "llama-2-7b",
    "messages": [{"role": "user", "content": "Hello!"}]
  }'
```

### Using with Node.js

```javascript
const OpenAI = require('openai');

// Point to your Envoy AI Gateway endpoint
const client = new OpenAI({
  baseURL: 'http://envoy-gateway.llm/v1',
  apiKey: 'not-needed'
});

async function main() {
  const completion = await client.chat.completions.create({
    model: 'llama-2-7b',
    messages: [{ role: 'user', content: 'Hello!' }]
  });

  console.log(completion.choices[0].message.content);
}

main();
```
