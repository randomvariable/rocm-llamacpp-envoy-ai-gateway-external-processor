.PHONY: all build test test-coverage test-coverage-html docker-build docker-push deploy clean lint fix license license-check

# Load environment overrides from .env (not committed)
-include .env
export

# Container image coordinates (override REGISTRY and PROJECT in .env for local builds)
REGISTRY ?= ghcr.io
PROJECT ?= randomvariable
IMAGE_NAME ?= rocm-llamacpp-envoy-ai-gateway-external-processor
IMG ?= $(REGISTRY)/$(PROJECT)/$(IMAGE_NAME)
VERSION ?= 1.0.0

# golangci-lint version
GOLANGCI_LINT_VERSION ?= v2.8.0

# addlicense version
ADDLICENSE_VERSION ?= v1.1.1

# Detect OS and architecture
OS := $(shell uname -s | tr '[:upper:]' '[:lower:]')
ARCH := $(shell uname -m)
ifeq ($(ARCH),x86_64)
	ARCH := amd64
endif
ifeq ($(ARCH),aarch64)
	ARCH := arm64
endif

# Tool binaries
HACK_BIN := $(CURDIR)/hack/bin
GOLANGCI_LINT := $(HACK_BIN)/golangci-lint
ADDLICENSE := $(HACK_BIN)/addlicense

# Version information
GIT_VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "devel")
GIT_COMMIT ?= $(shell git rev-parse HEAD 2>/dev/null || echo "unknown")
GIT_TREE_STATE ?= $(shell if git diff --quiet 2>/dev/null; then echo "clean"; else echo "dirty"; fi)
BUILD_DATE ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

# ldflags for version injection
VERSION_PKG := sigs.k8s.io/release-utils/version
LDFLAGS := -X $(VERSION_PKG).gitVersion=$(GIT_VERSION) \
           -X $(VERSION_PKG).gitCommit=$(GIT_COMMIT) \
           -X $(VERSION_PKG).gitTreeState=$(GIT_TREE_STATE) \
           -X $(VERSION_PKG).buildDate=$(BUILD_DATE)

all: build

# Run tests
test:
	go test -v -race ./...

# Run tests with coverage
test-coverage:
	go test -v -race -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -func=coverage.out

# Run tests with coverage and generate HTML report
test-coverage-html: test-coverage
	go tool cover -html=coverage.out -o coverage.html

# Build the binary
build:
	go build -ldflags "$(LDFLAGS)" -o bin/epp ./cmd/epp

# Build without version injection (for quick dev builds)
build-dev:
	go build -o bin/epp ./cmd/epp

# Run go fmt against code
fmt:
	go fmt ./...

# Run go vet against code
vet:
	go vet ./...

# Download golangci-lint if not present
$(GOLANGCI_LINT):
	@mkdir -p $(HACK_BIN)
	@echo "Downloading golangci-lint $(GOLANGCI_LINT_VERSION)..."
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s -- -b $(HACK_BIN) $(GOLANGCI_LINT_VERSION)

# Run golangci-lint
lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run ./...

# Run golangci-lint with auto-fix
fix: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run --fix ./...

# Download addlicense if not present
$(ADDLICENSE):
	@mkdir -p $(HACK_BIN)
	@echo "Installing addlicense $(ADDLICENSE_VERSION)..."
	@GOBIN=$(HACK_BIN) go install github.com/google/addlicense@$(ADDLICENSE_VERSION)

# Add license headers to all Go files
license: $(ADDLICENSE)
	$(ADDLICENSE) -c "Naadir Jeewa" -l apache -s -v ./

# Check license headers (fails if missing)
license-check: $(ADDLICENSE)
	$(ADDLICENSE) -c "Naadir Jeewa" -l apache -s -check ./

# Build the docker image
docker-build:
	docker build -t $(IMG):$(VERSION) -t $(IMG):latest .

# Push the docker image
docker-push:
	docker push $(IMG):$(VERSION)
	docker push $(IMG):latest

# Deploy to Kubernetes
deploy:
	kubectl apply -f deployments/daemonset/model-server-daemonset.yaml
	kubectl apply -f deployments/epp/epp.yaml

# Undeploy from Kubernetes
undeploy:
	kubectl delete -f deployments/epp/epp.yaml --ignore-not-found=true
	kubectl delete -f deployments/daemonset/model-server-daemonset.yaml --ignore-not-found=true

# Clean up
clean:
	rm -rf bin/
	rm -f coverage.out coverage.html
	go clean

# Run locally (requires kubeconfig)
run:
	go run ./cmd/epp --kubeconfig=${HOME}/.kube/config
