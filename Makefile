.PHONY: help build deploy clean test lint

help:
	@echo "Available targets:"
	@echo "  build       - Build the meta-router Docker image"
	@echo "  deploy      - Deploy to Kubernetes cluster"
	@echo "  clean       - Remove deployed resources"
	@echo "  test        - Run tests (if available)"
	@echo "  lint        - Run Python linter"
	@echo "  status      - Check deployment status"

build:
	docker build -t meta-router:latest .

deploy:
	kubectl apply -k k8s/

clean:
	kubectl delete -k k8s/

status:
	@echo "=== Meta-Router Pods ==="
	kubectl get pods -l app=meta-router
	@echo "\n=== Llama.cpp Pods ==="
	kubectl get pods -l app=llamacpp
	@echo "\n=== Services ==="
	kubectl get svc meta-router llamacpp

logs-router:
	kubectl logs -l app=meta-router --tail=100 -f

logs-llama:
	kubectl logs -l app=llamacpp --tail=100 -f

lint:
	@echo "Running Python linter..."
	@command -v pylint >/dev/null 2>&1 || { echo "pylint not installed. Installing..."; pip install pylint; }
	pylint router.py || true

test:
	@echo "No tests configured yet"

port-forward:
	@echo "Forwarding meta-router to localhost:8000"
	kubectl port-forward svc/meta-router 8000:8000
