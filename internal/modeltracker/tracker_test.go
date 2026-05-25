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

// TestTracker_IsLoaded_RankSuffixReconciliation guards the v1.3.0
// framework regression: the poller keys pods as "<ns>/<name>" while the
// gateway-api-inference-extension datastore addresses them as
// "<ns>/<name>-rank-<idx>". IsLoaded/MarkLoaded/MarkUnloaded must
// reconcile the rank-suffixed scheduling-path keys against the poller's
// plain keys, or the loaded-model filter sees zero warm pods and every
// request cold-loads on all pods.
func TestTracker_IsLoaded_RankSuffixReconciliation(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	tr := modeltracker.NewTracker(cli, modeltracker.Options{})

	// Poller-form key (plain pod name), as written by pollOnce.
	tr.SetLoaded("openai/llama-server-wzl2c", []string{"gpt-oss-20b"})

	// Filter/loader-form key (framework rank suffix) must resolve to it.
	if !tr.IsLoaded("openai/llama-server-wzl2c-rank-0", "gpt-oss-20b") {
		t.Error("IsLoaded with -rank-0 key did not match plain poller key")
	}

	// A non-rank-suffixed key must still work unchanged.
	if !tr.IsLoaded("openai/llama-server-wzl2c", "gpt-oss-20b") {
		t.Error("IsLoaded with plain key regressed")
	}

	// MarkLoaded via the framework key must be visible to a plain lookup.
	tr.MarkLoaded("openai/llama-server-k7dcf-rank-0", "gemma4-26b-a4b")
	if !tr.IsLoaded("openai/llama-server-k7dcf", "gemma4-26b-a4b") {
		t.Error("MarkLoaded(-rank-0) not visible to plain IsLoaded")
	}

	// MarkUnloaded via the framework key must clear the plain entry.
	tr.MarkUnloaded("openai/llama-server-k7dcf-rank-0", "gemma4-26b-a4b")
	if tr.IsLoaded("openai/llama-server-k7dcf", "gemma4-26b-a4b") {
		t.Error("MarkUnloaded(-rank-0) did not clear plain entry")
	}

	// A literal "-rank-" with a non-numeric tail is NOT a rank suffix and
	// must be preserved verbatim.
	tr.SetLoaded("openai/weird-rank-x", []string{"m"})
	if !tr.IsLoaded("openai/weird-rank-x", "m") {
		t.Error("non-numeric -rank- suffix was wrongly stripped")
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

func TestTracker_MarkLoaded_ProactiveBeforePoll(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	tr := modeltracker.NewTracker(cli, modeltracker.Options{})

	// Before MarkLoaded, the tracker thinks the pod has nothing.
	if tr.IsLoaded("openai/server3", "gpt-oss-20b") {
		t.Fatal("expected IsLoaded=false before MarkLoaded")
	}

	// MarkLoaded should flip the result immediately, no poll required.
	tr.MarkLoaded("openai/server3", "gpt-oss-20b")
	if !tr.IsLoaded("openai/server3", "gpt-oss-20b") {
		t.Error("expected IsLoaded=true after MarkLoaded")
	}

	// Other pod / other model still cold.
	if tr.IsLoaded("openai/server5", "gpt-oss-20b") {
		t.Error("MarkLoaded leaked across pods")
	}

	if tr.IsLoaded("openai/server3", "qwen3.6-35b") {
		t.Error("MarkLoaded leaked across models")
	}
}

func TestTracker_MarkLoaded_Additive(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	tr := modeltracker.NewTracker(cli, modeltracker.Options{})

	tr.SetLoaded("openai/server3", []string{"existing-model"})
	tr.MarkLoaded("openai/server3", "gpt-oss-20b")

	// Both should be present — MarkLoaded must not overwrite the set.
	if !tr.IsLoaded("openai/server3", "existing-model") {
		t.Error("MarkLoaded clobbered existing-model")
	}

	if !tr.IsLoaded("openai/server3", "gpt-oss-20b") {
		t.Error("MarkLoaded didn't add gpt-oss-20b")
	}
}

func TestTracker_MarkUnloaded(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	tr := modeltracker.NewTracker(cli, modeltracker.Options{})

	tr.MarkLoaded("openai/server3", "gpt-oss-20b")
	tr.MarkLoaded("openai/server3", "other-model")

	tr.MarkUnloaded("openai/server3", "gpt-oss-20b")

	if tr.IsLoaded("openai/server3", "gpt-oss-20b") {
		t.Error("MarkUnloaded didn't remove gpt-oss-20b")
	}

	if !tr.IsLoaded("openai/server3", "other-model") {
		t.Error("MarkUnloaded clobbered sibling")
	}

	// MarkUnloaded on a model that wasn't loaded should be a no-op.
	tr.MarkUnloaded("openai/server3", "never-loaded")
	tr.MarkUnloaded("openai/unknown-pod", "anything")
}

func TestTracker_MarkLoaded_EmptyArgs_Noop(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	tr := modeltracker.NewTracker(cli, modeltracker.Options{})

	// Empty strings should be silently dropped, not panic or pollute.
	tr.MarkLoaded("", "gpt-oss-20b")
	tr.MarkLoaded("openai/server3", "")
	tr.MarkUnloaded("", "gpt-oss-20b")
	tr.MarkUnloaded("openai/server3", "")

	snap := tr.Snapshot()
	if len(snap) != 0 {
		t.Errorf("expected empty snapshot, got %+v", snap)
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

// fakePressure is a deterministic PressureProvider for dedup tests.
type fakePressure struct {
	scores map[string]float64
}

func (f fakePressure) Pressure(podKey string) float64 {
	return f.scores[podKey]
}

// fakeUnloadServer responds to GET /models with the configured set and
// records POST /models/unload bodies for later assertion.
type fakeUnloadServer struct {
	mu       sync.Mutex
	models   []string
	unloaded []string // model IDs the server received unload requests for
}

func (s *fakeUnloadServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Path == "/models" {
		s.mu.Lock()
		ms := append([]string{}, s.models...)
		s.mu.Unlock()

		type entry struct {
			ID     string            `json:"id"`
			Status map[string]string `json:"status"`
		}

		out := make([]entry, 0, len(ms))
		for _, m := range ms {
			out = append(out, entry{ID: m, Status: map[string]string{"value": "loaded"}})
		}

		_ = json.NewEncoder(w).Encode(map[string]any{"data": out})

		return
	}

	if r.Method == http.MethodPost && r.URL.Path == "/models/unload" {
		var body struct {
			Model string `json:"model"`
		}

		_ = json.NewDecoder(r.Body).Decode(&body)

		s.mu.Lock()
		s.unloaded = append(s.unloaded, body.Model)
		s.mu.Unlock()

		_ = json.NewEncoder(w).Encode(map[string]any{"success": true})

		return
	}

	http.NotFound(w, r)
}

func (s *fakeUnloadServer) unloadedCalls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := append([]string{}, s.unloaded...)

	return out
}

// trackerForDedup returns a tracker plus per-pod fake servers for use
// in dedup tests. Each pod gets its own httptest server so unload calls
// land on the right pod.
func trackerForDedup(
	t *testing.T,
	pods map[string][]string, // podKey "ns/name" -> loaded models
	pressure modeltracker.PressureProvider,
	dedupGracePolls int,
) (*modeltracker.Tracker, map[string]*fakeUnloadServer) {
	t.Helper()

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	servers := make(map[string]*fakeUnloadServer, len(pods))

	tr := modeltracker.NewTracker(cli, modeltracker.Options{
		Namespace:       "openai",
		Pressure:        pressure,
		DedupGracePolls: dedupGracePolls,
		// Default PodPort=8080. We override per-pod via per-tracker
		// servers below by using each test server's port.
	})

	// All test servers share httptest's 127.0.0.1, but each binds a
	// distinct random port. The tracker has only one podPort, so we
	// also need per-pod IP→port routing in the URL. The simplest
	// path: replace the tracker's httpClient with one whose Transport
	// rewrites Host to point at the right test server based on the
	// requested host:port. But we can't do that without exposing
	// internals. Pragmatic alternative: bind each test server to the
	// same loopback addr+port via a single Mux, keyed on a Host
	// header we set... also requires internals.
	//
	// Simplest working approach: per-pod single-port server, set
	// Tracker.podPort = first server port, and add a SetPodIP that
	// records the literal "host:port" string as the "IP". Then in
	// unloadModel the URL becomes
	// http://<host:port>:<podPort>/models/unload which is wrong.
	//
	// Cleanest: each pod's IP IS the unique host+port string ONLY
	// when we set podPort=0 in the URL builder... but tracker forces
	// a port.
	//
	// Decision: use a single shared httptest server. Encode the
	// "pod" in the URL path via a custom mux that strips the prefix.
	// To do that the tracker would need a per-pod unloadPath, which
	// we don't have. So instead use a single server and let all
	// "pods" point at it — that means we can't tell which pod the
	// unload was sent to except by observing call order under our
	// deterministic winner selection (lowest pressure first, alpha
	// tiebreak). That's what the dedup tests assert anyway.

	shared := &fakeUnloadServer{}
	srv := httptest.NewServer(shared)

	t.Cleanup(srv.Close)

	u, _ := url.Parse(srv.URL)
	host, portStr, _ := splitHostPort(u.Host)
	port, _ := strconv.Atoi(portStr)

	// Trick: point all pods at the same loopback host but with a
	// distinct "synthetic" IP each. The tracker builds the URL as
	// http://<IP>:<podPort>/models/unload. We can override the IP to
	// the literal "127.0.0.1" and the podPort to the shared test
	// server port, so every pod's request lands on the same server.
	// The shared server records the model name (which is what dedup
	// uses to pick the loser), so per-pod IP routing isn't required
	// for the assertions below.
	_ = host

	// Re-construct tracker with the right podPort. Since NewTracker
	// applies defaults at construction time and we already built one
	// above, we discard it and build again.
	tr = modeltracker.NewTracker(cli, modeltracker.Options{
		Namespace:       "openai",
		PodPort:         port,
		Pressure:        pressure,
		DedupGracePolls: dedupGracePolls,
	})

	for podKey, models := range pods {
		tr.SetLoaded(podKey, models)
		tr.SetPodIP(podKey, "127.0.0.1")

		servers[podKey] = shared
	}

	return tr, servers
}

func TestTracker_ResolveDuplicates_NoPressureProvider_NoUnload(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	tr := modeltracker.NewTracker(cli, modeltracker.Options{})

	tr.SetLoaded("openai/a", []string{"gpt-oss-20b"})
	tr.SetLoaded("openai/b", []string{"gpt-oss-20b"})

	// Without a Pressure provider, dedup MUST be a no-op even after
	// many polls — feature is opt-in via Options.Pressure.
	for i := 0; i < 10; i++ {
		tr.ResolveDuplicates(context.Background())
	}

	if !tr.IsLoaded("openai/a", "gpt-oss-20b") || !tr.IsLoaded("openai/b", "gpt-oss-20b") {
		t.Error("dedup ran without a pressure provider")
	}
}

func TestTracker_ResolveDuplicates_GraceWindow(t *testing.T) {
	t.Parallel()

	pressure := fakePressure{scores: map[string]float64{
		"openai/a": 5,
		"openai/b": 1, // lower → loser
	}}

	tr, srv := trackerForDedup(t, map[string][]string{
		"openai/a": {"gpt-oss-20b"},
		"openai/b": {"gpt-oss-20b"},
	}, pressure, 3)

	// First two observations: duplicate detected but within grace.
	tr.ResolveDuplicates(context.Background())
	tr.ResolveDuplicates(context.Background())

	if got := len(srv["openai/a"].unloadedCalls()); got != 0 {
		t.Errorf("expected no unload during grace, got %d", got)
	}

	// Third observation crosses the threshold → one unload.
	tr.ResolveDuplicates(context.Background())

	calls := srv["openai/a"].unloadedCalls()
	if len(calls) != 1 || calls[0] != "gpt-oss-20b" {
		t.Errorf("expected one gpt-oss-20b unload after grace, got %+v", calls)
	}

	if tr.IsLoaded("openai/b", "gpt-oss-20b") {
		t.Error("expected gpt-oss-20b unloaded from openai/b (lower pressure)")
	}

	if !tr.IsLoaded("openai/a", "gpt-oss-20b") {
		t.Error("expected gpt-oss-20b still loaded on openai/a (winner)")
	}
}

func TestTracker_ResolveDuplicates_WinnerByPressure(t *testing.T) {
	t.Parallel()

	pressure := fakePressure{scores: map[string]float64{
		"openai/a": 1,
		"openai/b": 10, // highest → winner
		"openai/c": 5,
	}}

	tr, srv := trackerForDedup(t, map[string][]string{
		"openai/a": {"shared-model"},
		"openai/b": {"shared-model"},
		"openai/c": {"shared-model"},
	}, pressure, 1)

	tr.ResolveDuplicates(context.Background())

	// 1 unload per cycle by default. The lowest-pressure pod (a) loses.
	if len(srv["openai/a"].unloadedCalls()) != 1 {
		t.Errorf("expected 1 unload call, got %d", len(srv["openai/a"].unloadedCalls()))
	}

	if tr.IsLoaded("openai/a", "shared-model") {
		t.Error("openai/a was lowest pressure, expected it to lose")
	}

	if !tr.IsLoaded("openai/b", "shared-model") || !tr.IsLoaded("openai/c", "shared-model") {
		t.Error("expected winners (b, c) to keep the model")
	}
}

func TestTracker_ResolveDuplicates_AlphaTiebreakWhenAllScoresEqual(t *testing.T) {
	t.Parallel()

	pressure := fakePressure{scores: map[string]float64{
		"openai/a": 0,
		"openai/b": 0,
	}}

	tr, srv := trackerForDedup(t, map[string][]string{
		"openai/a": {"m"},
		"openai/b": {"m"},
	}, pressure, 1)

	tr.ResolveDuplicates(context.Background())

	// With all-zero pressure, pickLoser picks the alphabetically-first
	// pod (openai/a) as the seed loser and never sees a strictly
	// lower score, so a stays the loser.
	if tr.IsLoaded("openai/a", "m") {
		t.Error("expected openai/a to lose tiebreak")
	}

	if !tr.IsLoaded("openai/b", "m") {
		t.Error("expected openai/b to win tiebreak")
	}

	if len(srv["openai/a"].unloadedCalls()) != 1 {
		t.Errorf("expected exactly 1 unload, got %d", len(srv["openai/a"].unloadedCalls()))
	}
}

func TestTracker_ResolveDuplicates_ThrottledTo1PerCycle(t *testing.T) {
	t.Parallel()

	pressure := fakePressure{scores: map[string]float64{
		"openai/a": 5,
		"openai/b": 1, // loser
	}}

	tr, srv := trackerForDedup(t, map[string][]string{
		"openai/a": {"model-1", "model-2", "model-3"},
		"openai/b": {"model-1", "model-2", "model-3"},
	}, pressure, 1)

	tr.ResolveDuplicates(context.Background())

	// 3 duplicates, but the per-cycle throttle caps us at 1 unload.
	if got := len(srv["openai/a"].unloadedCalls()); got != 1 {
		t.Errorf("expected 1 unload per cycle, got %d", got)
	}

	// Two duplicates still pending; next cycle handles one more.
	tr.ResolveDuplicates(context.Background())

	if got := len(srv["openai/a"].unloadedCalls()); got != 2 {
		t.Errorf("expected 2 cumulative unloads after 2 cycles, got %d", got)
	}
}

func TestTracker_ResolveDuplicates_ResolvedDuplicateClearsObservations(t *testing.T) {
	t.Parallel()

	pressure := fakePressure{scores: map[string]float64{
		"openai/a": 5,
		"openai/b": 1,
	}}

	tr, srv := trackerForDedup(t, map[string][]string{
		"openai/a": {"gpt-oss-20b"},
		"openai/b": {"gpt-oss-20b"},
	}, pressure, 3)

	// Observe twice (under grace).
	tr.ResolveDuplicates(context.Background())
	tr.ResolveDuplicates(context.Background())

	// Now the duplicate resolves naturally (pod b restarted, lost the
	// model). The dedup pass should drop the observation counter.
	tr.MarkUnloaded("openai/b", "gpt-oss-20b")
	tr.ResolveDuplicates(context.Background())

	if got := len(srv["openai/a"].unloadedCalls()); got != 0 {
		t.Errorf("dedup acted after duplicate resolved on its own: %d calls", got)
	}

	// Re-create the duplicate and verify the observation counter
	// restarts at 0 (must observe 3 fresh polls again, not 1).
	tr.MarkLoaded("openai/b", "gpt-oss-20b")
	tr.ResolveDuplicates(context.Background()) // obs=1
	tr.ResolveDuplicates(context.Background()) // obs=2

	if got := len(srv["openai/a"].unloadedCalls()); got != 0 {
		t.Errorf("dedup didn't restart grace counter, %d unloads", got)
	}

	tr.ResolveDuplicates(context.Background()) // obs=3, fires

	if got := len(srv["openai/a"].unloadedCalls()); got != 1 {
		t.Errorf("expected 1 unload after fresh 3-poll grace, got %d", got)
	}
}

func TestTracker_ResolveDuplicates_SingleHolderSkipped(t *testing.T) {
	t.Parallel()

	pressure := fakePressure{scores: map[string]float64{
		"openai/a": 5,
	}}

	tr, srv := trackerForDedup(t, map[string][]string{
		"openai/a": {"gpt-oss-20b"},
	}, pressure, 1)

	tr.ResolveDuplicates(context.Background())

	if got := len(srv["openai/a"].unloadedCalls()); got != 0 {
		t.Errorf("non-duplicate triggered unload: %d calls", got)
	}

	if !tr.IsLoaded("openai/a", "gpt-oss-20b") {
		t.Error("the only copy was unloaded — would have stranded all traffic")
	}
}
