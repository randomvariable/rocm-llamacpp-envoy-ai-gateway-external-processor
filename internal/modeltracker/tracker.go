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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
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

// Tracker periodically scans llama-server pods and records which models
// are currently loaded on each.
type Tracker struct {
	client       crclient.Client
	httpClient   *http.Client
	namespace    string
	podSelector  map[string]string
	podPort      int
	queryPath    string
	pollInterval time.Duration

	mu        sync.RWMutex
	loaded    map[string]PodModelSet // podKey "namespace/name" -> loaded model IDs
	firstPoll bool                   // true once an initial poll has run
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
	// PollInterval is how often each pod is queried (default 10s).
	PollInterval time.Duration
	// PollTimeout bounds each HTTP call (default 5s). Lower than the
	// poll interval so a hung pod can't block the cycle.
	PollTimeout time.Duration
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

	return &Tracker{
		client:       client,
		httpClient:   &http.Client{Timeout: opts.PollTimeout},
		namespace:    opts.Namespace,
		podSelector:  opts.PodSelector,
		podPort:      opts.PodPort,
		queryPath:    opts.QueryPath,
		pollInterval: opts.PollInterval,
		loaded:       make(map[string]PodModelSet),
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
// identified by `podKey` (format: "namespace/name").
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

// pollOnce lists the matching pods, queries each, and atomically
// updates the loaded-model map.
func (t *Tracker) pollOnce(ctx context.Context) {
	pods, err := t.listPods(ctx)
	if err != nil {
		klog.Errorf("modeltracker: failed to list pods: %v", err)

		return
	}

	next := make(map[string]PodModelSet, len(pods))
	for i := range pods {
		pod := &pods[i]
		if pod.Status.PodIP == "" {
			continue
		}

		set, qErr := t.queryPod(ctx, pod.Status.PodIP)
		if qErr != nil {
			klog.V(logVerbosity).Infof(
				"modeltracker: pod %s/%s (%s) query failed: %v",
				pod.Namespace, pod.Name, pod.Status.PodIP, qErr,
			)

			// Preserve last-known state on transient failure so a flaky
			// pod doesn't get all its routing dropped mid-cycle.
			t.mu.RLock()
			prev, ok := t.loaded[podKey(pod)]
			t.mu.RUnlock()

			if ok {
				next[podKey(pod)] = prev
			}

			continue
		}

		next[podKey(pod)] = set
	}

	t.mu.Lock()
	t.loaded = next
	t.firstPoll = true
	t.mu.Unlock()

	klog.V(logVerbosity).Infof(
		"modeltracker: poll complete, tracking %d pods", len(next),
	)
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

func podKey(p *corev1.Pod) string {
	return p.Namespace + "/" + p.Name
}
