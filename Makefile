.PHONY: help build deploy clean test lint

help:
	@echo "Available targets:"
	@echo "  build       - Build the meta-router Docker image"
	@echo "  deploy      - Deploy to Kubernetes cluster"
	@echo "  clean       - Remove deployed resources"
	@echo "  test        - Run tests"
	@echo "  lint        - Run Go linter"
	@echo "  status      - Check deployment status"
	@echo "  run         - Run locally"

build:
	docker build -t meta-router:latest .

build-local:
	go build -o meta-router .

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
	@echo "Running Go linter..."
	@command -v golangci-lint >/dev/null 2>&1 || { echo "golangci-lint not installed"; go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; }
	golangci-lint run || true

test:
	go test -v ./...

run:
	go run main.go

port-forward:
	@echo "Forwarding meta-router to localhost:8000"
	kubectl port-forward svc/meta-router 8000:8000
