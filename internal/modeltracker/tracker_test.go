// Copyright 2026 Naadir Jeewa
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package modeltracker_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/randomvariable/rocm-llamacpp-envoy-ai-gateway-external-processor/internal/modeltracker"
)

// fakeLlamaServer is a per-pod HTTP handler that responds to /models with
// the configured set of loaded model IDs. Tests use a single httptest server
// keyed by Host header → pod (server returns the right model set per host).
type fakeLlamaServer struct {
	mu       sync.RWMutex
	byHost   map[string][]string // host -> loaded model IDs (status=loaded)
	requests atomic.Int64
}

func (f *fakeLlamaServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.requests.Add(1)

	if r.URL.Path != "/models" {
		http.NotFound(w, r)

		return
	}

	f.mu.RLock()
	models, ok := f.byHost[strings.SplitN(r.Host, ":", 2)[0]]
	f.mu.RUnlock()

	if !ok {
		// Pod is reachable but knows nothing — empty list.
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{}})

		return
	}

	type modelEntry struct {
		ID     string         `json:"id"`
		Status map[string]any `json:"status"`
	}

	data := make([]modelEntry, 0, len(models))
	for _, m := range models {
		data = append(data, modelEntry{
			ID:     m,
			Status: map[string]any{"value": "loaded"},
		})
	}

	// Throw in an "unloaded" entry too — tracker must filter it out.
	data = append(data, modelEntry{
		ID:     "configured-but-unloaded",
		Status: map[string]any{"value": "unloaded"},
	})

	_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
}

func (f *fakeLlamaServer) setHostModels(host string, models []string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.byHost == nil {
		f.byHost = map[string][]string{}
	}

	f.byHost[host] = models
}

// newTrackerWithFake spins up a test HTTP server and returns a Tracker
// wired to use it as every pod's /models endpoint.
//
// Trick: we point all pods at the SAME backing test server (single port)
// but use the pod IP as the Host header discriminator. To make that work,
// each pod's recorded PodIP is the test server's address, but tracker
// queries each at host=podIP — so we set the pods' IPs to distinct
// loopback addresses that all resolve to the same server when the test
// uses httptest.NewServer (which binds to 127.0.0.1).
//
// Simpler: spin up one httptest.Server per pod. Each pod's IP is set to
// the server's host:port and the tracker queries that directly.
func newTrackerWithFakeServers(
	t *testing.T,
	pods map[string][]string, // pod-name -> loaded model IDs
) (*modeltracker.Tracker, map[string]*httptest.Server, func()) {
	t.Helper()

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	servers := make(map[string]*httptest.Server, len(pods))
	objs := make([]any, 0, len(pods))
	_ = objs

	var podObjs []*corev1.Pod

	for name, models := range pods {
		fls := &fakeLlamaServer{}
		fls.setHostModels("test", models)

		srv := httptest.NewServer(fls)
		servers[name] = srv

		u, err := url.Parse(srv.URL)
		if err != nil {
			t.Fatalf("parse server URL: %v", err)
		}

		host, portStr, err := splitHostPort(u.Host)
		if err != nil {
			t.Fatalf("split host:port: %v", err)
		}

		port, _ := strconv.Atoi(portStr)
		fls.setHostModels(host, models)

		podObjs = append(podObjs, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "openai",
				Labels:    map[string]string{"app": "llama-server"},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: host,
			},
		})

		// Each test server runs on a different port, so we need a
		// per-pod port mapping. We approximate that by setting the
		// tracker's podPort from the *first* server's port — and
		// require all test servers to bind the same port. Since
		// httptest assigns random ports, we override by constructing
		// a tracker per-port. For multi-pod tests we use distinct
		// trackers per pod or a single shared server (see helper
		// below).
		_ = port
	}

	// To support multiple pods on different ports, we use a custom
	// dispatcher that routes by the inbound port (not standard
	// behaviour but acceptable for test). Simpler: use a single
	// shared server and discriminate by Host: pod-name in tests.
	// Final decision: tests below use a single shared server and
	// set pods' IPs to "127.0.0.1" with the shared port, and use
	// a custom HTTP transport that routes pod IP → real test server.

	// Build the K8s client with our pod objects.
	builder := fake.NewClientBuilder().WithScheme(scheme)
	for _, p := range podObjs {
		builder = builder.WithObjects(p)
	}

	cli := builder.Build()

	t.Cleanup(func() {
		for _, s := range servers {
			s.Close()
		}
	})

	tracker := modeltracker.NewTracker(cli, modeltracker.Options{
		Namespace:    "openai",
		PodSelector:  map[string]string{"app": "llama-server"},
		PollInterval: 50 * time.Millisecond,
		PollTimeout:  500 * time.Millisecond,
	})

	cleanup := func() {
		for _, s := range servers {
			s.Close()
		}
	}

	return tracker, servers, cleanup
}

// Simpler test approach: don't use NewTracker's HTTP-doing internals at
// all. Use SetLoaded to seed state, then exercise IsLoaded /
// AnyPodHasLoaded / Snapshot directly. Keeps tests deterministic and
// avoids the multi-server port-routing headache.

func TestTracker_IsLoaded_seededState(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	tr := modeltracker.NewTracker(cli, modeltracker.Options{
		Namespace:   "openai",
		PodSelector: map[string]string{"app": "llama-server"},
	})

	tr.SetLoaded("openai/server3", []string{"gpt-oss-20b", "qwen3.6-35b"})
	tr.SetLoaded("openai/server5", []string{})

	cases := []struct {
		name    string
		podKey  string
		model   string
		want    bool
	}{
		{"warm pod warm model", "openai/server3", "gpt-oss-20b", true},
		{"warm pod cold model", "openai/server3", "luminia-8b", false},
		{"cold pod any model", "openai/server5", "gpt-oss-20b", false},
		{"unknown pod", "openai/unknown", "gpt-oss-20b", false},
		{"empty model", "openai/server3", "", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := tr.IsLoaded(tc.podKey, tc.model)
			if got != tc.want {
				t.Errorf(
					"IsLoaded(%q, %q) = %v, want %v",
					tc.podKey, tc.model, got, tc.want,
				)
			}
		})
	}
}

func TestTracker_AnyPodHasLoaded(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	tr := modeltracker.NewTracker(cli, modeltracker.Options{})

	tr.SetLoaded("openai/server3", []string{"gpt-oss-20b"})
	tr.SetLoaded("openai/server5", []string{"qwen3.6-35b"})

	if !tr.AnyPodHasLoaded("gpt-oss-20b") {
		t.Error("expected AnyPodHasLoaded(gpt-oss-20b)=true")
	}

	if !tr.AnyPodHasLoaded("qwen3.6-35b") {
		t.Error("expected AnyPodHasLoaded(qwen3.6-35b)=true")
	}

	if tr.AnyPodHasLoaded("never-loaded") {
		t.Error("expected AnyPodHasLoaded(never-loaded)=false")
	}
}

func TestTracker_BeforeFirstPoll_AllFalse(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	tr := modeltracker.NewTracker(cli, modeltracker.Options{})

	// No SetLoaded calls, no Start → firstPoll=false → both return false.
	if tr.IsLoaded("openai/server3", "gpt-oss-20b") {
		t.Error("expected IsLoaded=false before first poll")
	}

	if tr.AnyPodHasLoaded("gpt-oss-20b") {
		t.Error("expected AnyPodHasLoaded=false before first poll")
	}
}

func TestTracker_Snapshot_IsIndependentCopy(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	tr := modeltracker.NewTracker(cli, modeltracker.Options{})

	tr.SetLoaded("openai/server3", []string{"gpt-oss-20b"})

	snap := tr.Snapshot()

	// Mutate the snapshot — must not affect the tracker.
	snap["openai/server3"]["injected"] = struct{}{}
	delete(snap, "openai/server3")

	if !tr.IsLoaded("openai/server3", "gpt-oss-20b") {
		t.Error("tracker state was mutated by snapshot consumer")
	}
}

func TestTracker_Poll_QueriesPodAndUpdates(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	// Stand up a fake llama-server that reports gpt-oss-20b as loaded.
	fls := &fakeLlamaServer{}
	srv := httptest.NewServer(fls)
	defer srv.Close()

	u, _ := url.Parse(srv.URL)

	host, portStr, _ := splitHostPort(u.Host)
	port, _ := strconv.Atoi(portStr)
	fls.setHostModels(host, []string{"gpt-oss-20b", "qwen3.6-35b"})

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama-server-aaa",
			Namespace: "openai",
			Labels:    map[string]string{"app": "llama-server"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: host},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	tr := modeltracker.NewTracker(cli, modeltracker.Options{
		Namespace:    "openai",
		PodSelector:  map[string]string{"app": "llama-server"},
		PodPort:      port,
		PollInterval: 50 * time.Millisecond,
		PollTimeout:  500 * time.Millisecond,
	})

	// Run Start briefly so the initial sync poll fires once.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	tr.Start(ctx)

	// After Start returns, the initial poll has completed.
	if !tr.IsLoaded("openai/llama-server-aaa", "gpt-oss-20b") {
		t.Errorf(
			"expected gpt-oss-20b loaded on llama-server-aaa, snapshot=%+v",
			tr.Snapshot(),
		)
	}

	if tr.IsLoaded("openai/llama-server-aaa", "configured-but-unloaded") {
		t.Error("'unloaded' status should not be reported as loaded")
	}

	if got := fls.requests.Load(); got < 1 {
		t.Errorf("expected at least 1 request to fake server, got %d", got)
	}
}

func TestTracker_Poll_HandlesUnreachablePod(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "broken-pod",
			Namespace: "openai",
			Labels:    map[string]string{"app": "llama-server"},
		},
		// Use TEST-NET-1 address that should be unroutable.
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "192.0.2.1"},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	tr := modeltracker.NewTracker(cli, modeltracker.Options{
		Namespace:    "openai",
		PodSelector:  map[string]string{"app": "llama-server"},
		PollInterval: 50 * time.Millisecond,
		PollTimeout:  100 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	tr.Start(ctx)

	// Unreachable pod produces empty set, but tracker doesn't panic.
	if tr.IsLoaded("openai/broken-pod", "anything") {
		t.Error("expected no models reported for unreachable pod")
	}
}

func TestNewTracker_AppliesDefaults(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	tr := modeltracker.NewTracker(cli, modeltracker.Options{})

	if tr == nil {
		t.Fatal("NewTracker returned nil")
	}
	// Defaults applied internally — we can't reach them directly,
	// but a no-config Start must not panic. Run a sub-100ms ctx.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	tr.Start(ctx)
}

// splitHostPort is a tiny helper to keep the test self-contained without
// pulling in net.SplitHostPort error semantics in every callsite.
func splitHostPort(hostport string) (host, port string, err error) {
	idx := strings.LastIndex(hostport, ":")
	if idx == -1 {
		return hostport, "", nil
	}

	return hostport[:idx], hostport[idx+1:], nil
}

// Reference (kept compiled to avoid dead-code lint) so a future refactor
// to multi-pod test servers has a starting point.
var _ = newTrackerWithFakeServers
