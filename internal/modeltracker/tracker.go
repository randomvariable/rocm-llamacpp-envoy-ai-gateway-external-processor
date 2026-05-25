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

// Package modeltracker maintains a per-pod view of which models are currently
// loaded on each llama-server instance.
//
// llama-server's /v1/models endpoint reports every configured model along with
// a status.value field. Models marked "loaded" or "running" have a live child
// process serving requests; "unloaded" models are configured but cold.
//
// The tracker polls each pod's /v1/models periodically and exposes IsLoaded
// for the scheduling filter to consult before routing requests.
package modeltracker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// DefaultPollInterval is how often each pod's /v1/models is queried.
	// Short enough to react to new loads quickly; long enough to keep
	// per-request scheduling decisions effectively cache-hit.
	DefaultPollInterval = 10 * time.Second

	// DefaultPollTimeout bounds each individual /v1/models HTTP call.
	DefaultPollTimeout = 5 * time.Second

	// DefaultModelServerPort is the port llama-server listens on inside
	// the pod. Matches InferencePool.spec.targetPortNumber.
	DefaultModelServerPort = 8080

	// DefaultModelQueryPath is llama-server's loaded-model query endpoint.
	// (The "/v1/" prefix variant is the same; llama-server accepts both.)
	DefaultModelQueryPath = "/models"

	// DefaultUnloadPath is llama-server's router-mode unload endpoint.
	// POST {"model": "<id>"} → {"success": true}.
	DefaultUnloadPath = "/models/unload"

	// DefaultDedupGracePolls is how many consecutive polls a duplicate
	// must be observed for before we act. At the default 10s poll,
	// this works out to ~2 minutes, enough that:
	//   - transient races (concurrent cold-load on two pods that both
	//     win the modelloader's MarkLoaded in the same window) drain;
	//   - an in-flight startup-verify call on the candidate loser pod
	//     has had time to either finish (releasing the slot so unload
	//     doesn't race with the request) or fail (so it doesn't get
	//     immediately re-served by autoload);
	//   - hindsight-api restart churn settles before we second-guess.
	// At 30s we observed the same model being unloaded then auto-
	// reloaded multiple times in 2 minutes because the dedup window
	// fired before hindsight's restart-driven verify churn drained.
	DefaultDedupGracePolls = 12

	// MaxUnloadsPerCycle bounds how many duplicate-evictions we issue
	// per poll cycle to avoid simultaneous reclaim across many models
	// thrashing the affected pod.
	MaxUnloadsPerCycle = 1

	// logVerbosity is the klog verbosity level for debug messages.
	logVerbosity = 2
)

// ErrNotStarted indicates IsLoaded was called before the first poll
// completed. Callers should treat as "loaded status unknown" — never
// drop pods based on this.
var ErrNotStarted = errors.New("model tracker has not completed an initial poll")

// modelResponse mirrors the relevant subset of llama-server's /v1/models
// response. Other fields are ignored.
type modelResponse struct {
	Data []struct {
		ID     string `json:"id"`
		Status struct {
			Value string `json:"value"`
		} `json:"status"`
	} `json:"data"`
}

// PodModelSet is the per-pod set of currently loaded model IDs.
type PodModelSet = map[string]struct{}

// PressureProvider returns a per-pod load score used to pick which copy
// of a duplicated model to keep during dedup. Higher = more loaded =
// keep the model here. Returning 0 for unknown pods is fine.
type PressureProvider interface {
	Pressure(podKey string) float64
}

// Tracker periodically scans llama-server pods and records which models
// are currently loaded on each.
type Tracker struct {
	client       crclient.Client
	httpClient   *http.Client
	namespace    string
	podSelector  map[string]string
	podPort      int
	queryPath    string
	unloadPath   string
	pollInterval time.Duration

	pressure        PressureProvider
	dedupGracePolls int

	mu sync.RWMutex
	// loaded is keyed by pod IP — the same identifier the framework hands
	// scheduling-path callers as the endpoint Address. Keying by IP (not
	// namespace/name) keeps the poller and the filter/loader in one
	// keyspace immune to framework endpoint-naming changes (e.g. GIE
	// v1.3.0's "<ns>/<name>-rank-<idx>").
	loaded map[string]PodModelSet // pod IP -> loaded model IDs
	podIP  map[string]string      // tracker key -> routable pod IP, refreshed each poll
	// dupObs counts how many consecutive polls a model has appeared on
	// ≥2 pods. Reset to 0 on either dedup action or on the model
	// returning to a single pod naturally.
	dupObs    map[string]int
	firstPoll bool // true once an initial poll has run
}

// Options configures a Tracker. All fields have sensible defaults.
type Options struct {
	// Namespace is the K8s namespace to scan for llama-server pods.
	Namespace string
	// PodSelector matches llama-server pods (e.g. {"app": "llama-server"}).
	PodSelector map[string]string
	// PodPort is the model-server HTTP port (default 8080).
	PodPort int
	// QueryPath is the model-listing endpoint (default "/models").
	QueryPath string
	// UnloadPath is the router-mode unload endpoint (default "/models/unload").
	UnloadPath string
	// PollInterval is how often each pod is queried (default 10s).
	PollInterval time.Duration
	// PollTimeout bounds each HTTP call (default 5s). Lower than the
	// poll interval so a hung pod can't block the cycle.
	PollTimeout time.Duration
	// Pressure scores pods for dedup winner selection. nil disables
	// dedup entirely (the tracker still observes duplicates but never
	// triggers an unload — useful for unit tests).
	Pressure PressureProvider
	// DedupGracePolls overrides DefaultDedupGracePolls. 0 = default.
	DedupGracePolls int
}

// NewTracker constructs a Tracker. Caller invokes Start to begin polling.
func NewTracker(client crclient.Client, opts Options) *Tracker {
	if opts.PodPort == 0 {
		opts.PodPort = DefaultModelServerPort
	}

	if opts.QueryPath == "" {
		opts.QueryPath = DefaultModelQueryPath
	}

	if opts.PollInterval == 0 {
		opts.PollInterval = DefaultPollInterval
	}

	if opts.PollTimeout == 0 {
		opts.PollTimeout = DefaultPollTimeout
	}

	if opts.UnloadPath == "" {
		opts.UnloadPath = DefaultUnloadPath
	}

	if opts.DedupGracePolls == 0 {
		opts.DedupGracePolls = DefaultDedupGracePolls
	}

	return &Tracker{
		client:          client,
		httpClient:      &http.Client{Timeout: opts.PollTimeout},
		namespace:       opts.Namespace,
		podSelector:     opts.PodSelector,
		podPort:         opts.PodPort,
		queryPath:       opts.QueryPath,
		unloadPath:      opts.UnloadPath,
		pollInterval:    opts.PollInterval,
		pressure:        opts.Pressure,
		dedupGracePolls: opts.DedupGracePolls,
		loaded:          make(map[string]PodModelSet),
		podIP:           make(map[string]string),
		dupObs:          make(map[string]int),
	}
}

// Start begins the polling loop. Runs until ctx is cancelled.
// Performs an initial synchronous poll before the ticker so IsLoaded
// returns meaningful data immediately after Start returns.
func (t *Tracker) Start(ctx context.Context) {
	t.pollOnce(ctx)

	ticker := time.NewTicker(t.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			klog.Info("Model tracker stopping")

			return
		case <-ticker.C:
			t.pollOnce(ctx)
		}
	}
}

// IsLoaded returns whether `model` is currently loaded on the pod
// identified by `podKey`, which is the pod IP (the framework endpoint
// Address) — the keyspace the poller writes. See the `loaded` field.
//
// Returns false if:
//   - the tracker has never completed a poll
//   - the pod is unknown to the tracker
//   - the model is configured but unloaded on that pod
func (t *Tracker) IsLoaded(podKey, model string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if !t.firstPoll {
		return false
	}

	set, ok := t.loaded[podKey]
	if !ok {
		return false
	}

	_, loaded := set[model]

	return loaded
}

// AnyPodHasLoaded reports whether at least one tracked pod has `model`
// loaded. Used by the filter to decide whether to drop cold pods or
// fall through to cold-load.
func (t *Tracker) AnyPodHasLoaded(model string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if !t.firstPoll {
		return false
	}

	for _, set := range t.loaded {
		if _, ok := set[model]; ok {
			return true
		}
	}

	return false
}

// Snapshot returns a copy of the per-pod loaded model sets for
// observability and tests.
func (t *Tracker) Snapshot() map[string]PodModelSet {
	t.mu.RLock()
	defer t.mu.RUnlock()

	out := make(map[string]PodModelSet, len(t.loaded))
	for podKey, set := range t.loaded {
		cp := make(PodModelSet, len(set))
		for k := range set {
			cp[k] = struct{}{}
		}

		out[podKey] = cp
	}

	return out
}

// SetLoaded is exposed for tests to seed the loaded map without polling.
func (t *Tracker) SetLoaded(podKey string, models []string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	set := make(PodModelSet, len(models))
	for _, m := range models {
		set[m] = struct{}{}
	}

	t.loaded[podKey] = set
	t.firstPoll = true
}

// SetPodIP is exposed for tests to seed the pod→IP map used by dedup
// to route the unload HTTP call.
func (t *Tracker) SetPodIP(podKey, ip string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.podIP == nil {
		t.podIP = make(map[string]string)
	}

	t.podIP[podKey] = ip
}

// ResolveDuplicates is the public entry point to the dedup pass.
// Production code calls it once per poll cycle from pollOnce; tests
// call it directly after seeding state.
func (t *Tracker) ResolveDuplicates(ctx context.Context) {
	t.resolveDuplicates(ctx)
}

// MarkLoaded marks `model` as loaded on `podKey` immediately, without
// waiting for the next poll. The next poll will overwrite this state
// with the actual /v1/models response. This is the proactive in-band
// update path used by the modelloader plugin: when a cold-load is
// triggered on a specific pod for a specific model, all subsequent
// filter decisions for the same model within the poll window must see
// that pod as warm, or the picker races and triggers redundant
// cold-loads on other pods. (Caught 2026-05-24: Hindsight's 4-pod
// startup verify fired 4 concurrent gpt-oss-20b calls; both
// llama-server pods cold-loaded the same model.)
func (t *Tracker) MarkLoaded(podKey, model string) {
	if podKey == "" || model == "" {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	set, ok := t.loaded[podKey]
	if !ok {
		set = make(PodModelSet)
		t.loaded[podKey] = set
	}

	set[model] = struct{}{}
	t.firstPoll = true
}

// MarkUnloaded is the inverse of MarkLoaded, used when a cold-load
// attempt fails. Without it the in-band mark would persist until the
// next poll and route traffic to a pod that doesn't actually have the
// model loaded.
func (t *Tracker) MarkUnloaded(podKey, model string) {
	if podKey == "" || model == "" {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	set, ok := t.loaded[podKey]
	if !ok {
		return
	}

	delete(set, model)
}

// pollOnce lists the matching pods, queries each, and atomically
// updates the loaded-model map.
func (t *Tracker) pollOnce(ctx context.Context) {
	pods, err := t.listPods(ctx)
	if err != nil {
		klog.Errorf("modeltracker: failed to list pods: %v", err)

		return
	}

	next := make(map[string]PodModelSet, len(pods))
	nextIPs := make(map[string]string, len(pods))

	for i := range pods {
		pod := &pods[i]
		if pod.Status.PodIP == "" {
			continue
		}

		// Key by pod IP, NOT namespace/name. The scheduling-path callers
		// (loaded-model filter, modelloader) identify a pod by the
		// framework's endpoint Address (the pod IP); keying here by IP
		// keeps both sides in one keyspace with zero string-format
		// coupling. Earlier name-based keying broke when GIE v1.3.0 began
		// suffixing endpoint names "<ns>/<name>-rank-<idx>". podIP maps
		// the tracker key to the routable IP for unloads (identity here,
		// but the indirection lets tests use synthetic keys).
		key := pod.Status.PodIP
		nextIPs[key] = pod.Status.PodIP

		set, qErr := t.queryPod(ctx, pod.Status.PodIP)
		if qErr != nil {
			klog.V(logVerbosity).Infof(
				"modeltracker: pod %s/%s (%s) query failed: %v",
				pod.Namespace, pod.Name, pod.Status.PodIP, qErr,
			)

			// Preserve last-known state on transient failure so a flaky
			// pod doesn't get all its routing dropped mid-cycle.
			t.mu.RLock()
			prev, ok := t.loaded[key]
			t.mu.RUnlock()

			if ok {
				next[key] = prev
			}

			continue
		}

		next[key] = set
	}

	t.mu.Lock()
	t.loaded = next
	t.podIP = nextIPs
	t.firstPoll = true
	t.mu.Unlock()

	klog.V(logVerbosity).Infof(
		"modeltracker: poll complete, tracking %d pods", len(next),
	)

	t.resolveDuplicates(ctx)
}

func (t *Tracker) listPods(ctx context.Context) ([]corev1.Pod, error) {
	var list corev1.PodList

	listOpts := []crclient.ListOption{}
	if t.namespace != "" {
		listOpts = append(listOpts, crclient.InNamespace(t.namespace))
	}

	if len(t.podSelector) > 0 {
		listOpts = append(
			listOpts, crclient.MatchingLabels(t.podSelector),
		)
	}

	err := t.client.List(ctx, &list, listOpts...)
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}

	return list.Items, nil
}

func (t *Tracker) queryPod(
	ctx context.Context, podIP string,
) (PodModelSet, error) {
	url := "http://" + net.JoinHostPort(
		podIP, strconv.Itoa(t.podPort),
	) + t.queryPath

	req, err := http.NewRequestWithContext(
		ctx, http.MethodGet, url, http.NoBody,
	)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}

	defer func() {
		closeErr := resp.Body.Close()
		if closeErr != nil {
			klog.V(logVerbosity).Infof(
				"modeltracker: close body: %v", closeErr,
			)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"unexpected status %d", resp.StatusCode,
		)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	var parsed modelResponse

	err = json.Unmarshal(body, &parsed)
	if err != nil {
		return nil, fmt.Errorf("decode body: %w", err)
	}

	set := make(PodModelSet)

	for _, m := range parsed.Data {
		// llama-server reports "loaded" or "running" for warm models.
		// Treat empty status as warm too: some llama-server builds
		// (older) omit the status field for active models.
		switch m.Status.Value {
		case "loaded", "running", "":
			set[m.ID] = struct{}{}
		}
	}

	return set, nil
}

// resolveDuplicates inspects the just-updated loaded map for models
// present on ≥2 pods and, after a grace period of consecutive
// observations, unloads the model from the lowest-pressure pod.
//
// At most MaxUnloadsPerCycle unloads are issued per call to avoid
// reclaim stampedes. Observations are tracked per model: a single
// dedup action satisfies the contract for one model per cycle but
// other models with mature observations get their turn on subsequent
// polls.
//
// Skips entirely when:
//   - dedup is disabled (no pressure provider configured)
//   - no model appears on more than one pod
//   - the only candidate pod is one whose pressure score is unknown
//     AND there is no deterministic tiebreak available
//
// Safety: only ever unloads from a pod when at least one *other* pod
// still has the model loaded — the dedup criterion itself guarantees
// this (a model isn't a duplicate unless ≥2 pods have it). Combined
// with MarkUnloaded on success, the in-memory view converges before
// the next poll.
func (t *Tracker) resolveDuplicates(ctx context.Context) {
	if t.pressure == nil {
		return
	}

	// Snapshot under read lock; modifications happen via MarkUnloaded.
	t.mu.RLock()

	if !t.firstPoll {
		t.mu.RUnlock()

		return
	}

	// model -> pods that currently have it loaded.
	dup := make(map[string][]string)
	for pk, set := range t.loaded {
		for model := range set {
			dup[model] = append(dup[model], pk)
		}
	}

	// Snapshot the IP map so unloads outside the lock have a stable
	// view even if the next poll runs concurrently.
	ipSnap := make(map[string]string, len(t.podIP))
	for k, v := range t.podIP {
		ipSnap[k] = v
	}
	t.mu.RUnlock()

	// Phase 1: update observation counters; collect ripe duplicates.
	t.mu.Lock()

	type ripe struct {
		model   string
		holders []string
	}

	var ripeList []ripe

	resolvedModels := make([]string, 0)

	for model := range t.dupObs {
		// If a previously-observed duplicate has resolved on its own
		// (one pod evicted, k8s rolled, etc.), drop the counter.
		if len(dup[model]) < 2 {
			resolvedModels = append(resolvedModels, model)
		}
	}

	for _, m := range resolvedModels {
		delete(t.dupObs, m)
	}

	for model, holders := range dup {
		if len(holders) < 2 {
			continue
		}

		t.dupObs[model]++

		if t.dupObs[model] >= t.dedupGracePolls {
			ripeList = append(ripeList, ripe{model: model, holders: holders})
		}
	}
	t.mu.Unlock()

	if len(ripeList) == 0 {
		return
	}

	// Deterministic ordering across cycles so the per-cycle throttle
	// doesn't starve any one model when several are ripe.
	sort.Slice(ripeList, func(i, j int) bool {
		return ripeList[i].model < ripeList[j].model
	})

	issued := 0
	for _, r := range ripeList {
		if issued >= MaxUnloadsPerCycle {
			break
		}

		loserKey := t.pickLoser(r.holders)
		if loserKey == "" {
			continue
		}

		loserIP, ok := ipSnap[loserKey]
		if !ok || loserIP == "" {
			klog.V(logVerbosity).Infof(
				"modeltracker: dedup skip %q on %s: no IP",
				r.model, loserKey,
			)

			continue
		}

		uErr := t.unloadModel(ctx, loserIP, r.model)
		if uErr != nil {
			klog.Errorf(
				"modeltracker: dedup unload %q on %s (%s) failed: %v",
				r.model, loserKey, loserIP, uErr,
			)

			continue
		}

		t.MarkUnloaded(loserKey, r.model)

		t.mu.Lock()
		delete(t.dupObs, r.model)
		t.mu.Unlock()

		klog.Infof(
			"modeltracker: dedup unloaded %q from %s (held by %d pods)",
			r.model, loserKey, len(r.holders),
		)

		issued++
	}
}

// pickLoser returns the pod from `holders` with the lowest pressure
// score; ties broken by alphabetical podKey for determinism. Returns
// "" when holders is empty.
func (t *Tracker) pickLoser(holders []string) string {
	if len(holders) == 0 {
		return ""
	}

	// Stable order before scoring so tiebreak is deterministic.
	sorted := make([]string, len(holders))
	copy(sorted, holders)
	sort.Strings(sorted)

	loser := sorted[0]
	loserScore := t.pressure.Pressure(loser)

	for _, pk := range sorted[1:] {
		s := t.pressure.Pressure(pk)
		if s < loserScore {
			loser = pk
			loserScore = s
		}
	}

	return loser
}

// unloadModel sends POST /models/unload {"model": "<id>"} to podIP.
// Returns nil on HTTP 200 with {"success": true}; error otherwise.
func (t *Tracker) unloadModel(
	ctx context.Context, podIP, model string,
) error {
	url := "http://" + net.JoinHostPort(
		podIP, strconv.Itoa(t.podPort),
	) + t.unloadPath

	body, err := json.Marshal(map[string]string{"model": model})
	if err != nil {
		return fmt.Errorf("marshal body: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, url, bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}

	defer func() {
		closeErr := resp.Body.Close()
		if closeErr != nil {
			klog.V(logVerbosity).Infof(
				"modeltracker: unload close body: %v", closeErr,
			)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unload %s: status %d", model, resp.StatusCode)
	}

	var parsed struct {
		Success bool `json:"success"`
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}

	jErr := json.Unmarshal(respBody, &parsed)
	if jErr != nil {
		return fmt.Errorf("decode body: %w", jErr)
	}

	if !parsed.Success {
		return fmt.Errorf("unload %s: server reported success=false", model)
	}

	return nil
}
