#!/bin/bash
set -e

echo "=== llama.cpp Kubernetes Meta-Router Deployment ==="

# Check prerequisites
echo "Checking prerequisites..."

if ! command -v kubectl &> /dev/null; then
    echo "Error: kubectl not found. Please install kubectl."
    exit 1
fi

if ! command -v docker &> /dev/null; then
    echo "Error: docker not found. Please install docker."
    exit 1
fi

# Check cluster connectivity
if ! kubectl cluster-info &> /dev/null; then
    echo "Error: Cannot connect to Kubernetes cluster."
    exit 1
fi

echo "✓ Prerequisites met"

# Build Docker image
echo ""
echo "Building meta-router Docker image..."
docker build -t meta-router:latest .
echo "✓ Image built"

# Optional: Load image into kind/minikube if detected
if kubectl get nodes -o jsonpath='{.items[0].status.nodeInfo.containerRuntimeVersion}' | grep -q "containerd://"; then
    echo ""
    echo "Detected local cluster. Loading image..."
    
    # Check if kind is installed and running
    if command -v kind &> /dev/null; then
        if kind get clusters &> /dev/null 2>&1; then
            CLUSTER_NAME=$(kind get clusters | head -n1)
            if [ -n "$CLUSTER_NAME" ]; then
                echo "Loading image into kind cluster: $CLUSTER_NAME"
                kind load docker-image meta-router:latest --name "$CLUSTER_NAME"
            fi
        fi
    fi
    
    # Check if minikube is installed and running
    if command -v minikube &> /dev/null; then
        if minikube status &> /dev/null 2>&1; then
            echo "Loading image into minikube"
            minikube image load meta-router:latest
        fi
    fi
fi

# Deploy to Kubernetes
echo ""
echo "Deploying to Kubernetes..."
kubectl apply -k k8s/
echo "✓ Resources applied"

# Wait for deployment
echo ""
echo "Waiting for pods to be ready..."
kubectl wait --for=condition=ready pod -l app=meta-router --timeout=120s || true
kubectl wait --for=condition=ready pod -l app=llamacpp --timeout=120s || true

# Show status
echo ""
echo "=== Deployment Status ==="
kubectl get pods -l app=meta-router
kubectl get pods -l app=llamacpp
kubectl get svc meta-router llamacpp

echo ""
echo "=== Next Steps ==="
echo "1. Check router status:"
echo "   kubectl port-forward svc/meta-router 8000:8000"
echo "   curl http://localhost:8000/status"
echo ""
echo "2. View logs:"
echo "   kubectl logs -l app=meta-router -f"
echo ""
echo "3. Test with a request:"
echo "   curl -X POST http://localhost:8000/v1/completions \\"
echo "     -H 'Content-Type: application/json' \\"
echo "     -d '{\"model\": \"llama-2-7b\", \"prompt\": \"Hello\", \"max_tokens\": 10}'"
echo ""
echo "Deployment complete!"
