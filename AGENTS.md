# AI Agent Instructions

## Architecture Overview

This is a **Kubernetes External Processor** for Envoy AI Gateway that routes inference requests to GPU nodes based on VRAM availability.

```
InferencePool CRD → Controller → Pool Manager → Router/VRAM Tracker
                                       ↓
Envoy ext_proc ← EPP Server (gRPC) ← picks backend based on VRAM
```

**Key data flow**: Envoy sends gRPC `ext_proc` requests → EPP extracts model name from request body → Router finds/loads model on optimal GPU node → Returns pod endpoint to Envoy.

## Package Structure

| Package | Purpose |
|---------|---------|
| `pkg/controller` | Watches `InferencePool` CRDs, updates pool manager |
| `pkg/pool` | Manages multiple pools with dedicated Router+Tracker per pool |
| `pkg/router` | VRAM-aware backend selection, model loading, warm model tracking |
| `pkg/vram` | Scrapes ROCm SMI exporters for GPU memory metrics |
| `pkg/epp` | Envoy External Processor Protocol gRPC server |
| `pkg/telemetry` | OpenTelemetry + Prometheus metrics (GenAI semantic conventions) |

## Critical Conventions

### Import Alias (Enforced by Linter)
```go
import crclient "sigs.k8s.io/controller-runtime/pkg/client"  // MUST use 'crclient' alias
```

### Error Handling Pattern
Define sentinel errors, wrap with context:
```go
var ErrClientNil = errors.New("client is nil")  // Package-level
return nil, fmt.Errorf("failed to list pods: %w", err)  // Wrap errors
```

### Race-Safe Shared State
When spawning goroutines that read shared state, copy values under lock first:
```go
t.mu.RLock()
namespace := t.namespace                          // Copy under lock
podSelector := make(map[string]string, len(t.podSelector))
maps.Copy(podSelector, t.podSelector)
t.mu.RUnlock()
go func() { /* use copied values */ }()
```

### Test Patterns
- Use `t.Parallel()` in all tests
- Use `t.Cleanup()` instead of `defer` at end of test functions
- Use `t.Helper()` in test helper functions
- Verify all struct fields you initialize (avoid `unusedwrite` linter)

## Developer Commands

```bash
make build          # Build binary with version injection
make test           # Run tests with -race
make lint           # golangci-lint (auto-downloads v2.8.0)
make fix            # Auto-fix linting issues
make test-coverage  # Generate coverage report
```

## External Dependencies

- **Gateway API Inference Extension**: `sigs.k8s.io/gateway-api-inference-extension/api/v1`
  - Use `inferencev1.Install(scheme)` not deprecated `AddToScheme`
- **controller-runtime v0.23.1**: Use `result.IsZero()` not deprecated `result.Requeue`
- **Envoy ext_proc**: `github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3`

## Key Integration Points

1. **InferencePool CRD** → Controller extracts `Spec.Selector.MatchLabels` and `Spec.TargetPorts`
2. **ROCm SMI Exporter** → VRAM tracker scrapes `rocm_smi_gpu_memory_used_bytes`/`total_bytes` on port 9100
3. **Model Server API** → Router calls `/v1/models` (list) and `/v1/models/load` (load model)
4. **Envoy** → EPP Server receives request headers/body via gRPC stream, returns backend override

## Envoy ext_proc Protocol Flow

The EPP server (`pkg/epp/server.go`) handles a bidirectional gRPC stream:

```
Envoy                           EPP Server
  │                                  │
  ├─── RequestHeaders ──────────────►│  Extract x-model-name header
  │◄── ImmediateResponse (skip body) │  OR continue to body
  │                                  │
  ├─── RequestBody ─────────────────►│  Parse JSON, extract "model" field
  │                                  │  Router.RouteRequest(model) → pod IP
  │◄── HeaderMutation ───────────────│  Add x-gateway-destination-endpoint
  │                                  │
  └─── (Envoy routes to pod) ────────┘
```

**Key response types** in `Process()`:
- `ImmediateResponse`: Return error (e.g., model not found)
- `HeaderMutation`: Add/modify headers to control routing
- `CommonResponse.HeaderMutation`: Set `x-gateway-destination-endpoint: <pod-ip>:<port>`

## Debugging & Troubleshooting

### Common Linter Issues

| Issue | Fix |
|-------|-----|
| `client` import alias | Change to `crclient "sigs.k8s.io/controller-runtime/pkg/client"` |
| `maps.Copy` not found | Use Go 1.21+ or add `golang.org/x/exp/maps` |
| `unusedwrite` on struct fields | Add assertions that read those fields in tests |
| `unnecessaryDefer` | Replace `defer x.Close()` before return with `t.Cleanup(x.Close)` |
| Dynamic errors (`err113`) | Define sentinel errors: `var ErrX = errors.New("...")` |
| Inline error handling (`noinlineerr`) | Split `if err := ...; err != nil` into separate lines |

### Race Detection

Run `go test -race ./...` (or `make test`). Common race patterns:

```go
// WRONG: Goroutine reads shared state directly
go func() {
    for _, pod := range t.podSelector {  // Race!
        ...
    }
}()

// CORRECT: Copy under lock before spawning
t.mu.RLock()
selectorCopy := make(map[string]string)
maps.Copy(selectorCopy, t.podSelector)
t.mu.RUnlock()
go func() {
    for _, pod := range selectorCopy {  // Safe
        ...
    }
}()
```

### Deprecated APIs

| Deprecated | Replacement |
|------------|-------------|
| `inferencev1.AddToScheme` | `inferencev1.Install` |
| `result.Requeue` | `result.IsZero()` or `result.RequeueAfter` |

## File Patterns

- All `.go` files require Apache 2.0 license header (run `make license` to add)
- Config: `config/extproc.yaml` (Viper-based)
- K8s manifests: `deployments/{daemonset,extproc}/`
- Examples: `examples/*.yaml`
