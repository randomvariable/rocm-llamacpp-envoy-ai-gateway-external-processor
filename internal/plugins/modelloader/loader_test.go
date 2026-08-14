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

package modelloader

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/backend"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/scheduling/types"

	"github.com/randomvariable/rocm-llamacpp-envoy-ai-gateway-external-processor/internal/modeltracker"
)

const (
	testModelName = "gpt-oss-20b"
	testPodIP     = "127.0.0.1"
	testPodKey    = "openai/llama-server-rank-0"
)

type scriptedModelServer struct {
	mu              sync.Mutex
	loadCalls       int
	modelLoaded     bool
	loadDelay       time.Duration
	loadStatusCode  int
	queryStatusCode int
}

func newScriptedModelServer() *scriptedModelServer {
	return &scriptedModelServer{
		loadStatusCode:  http.StatusOK,
		queryStatusCode: http.StatusOK,
	}
}

func (s *scriptedModelServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/models/load", func(w http.ResponseWriter, _ *http.Request) {
		if s.loadDelay > 0 {
			time.Sleep(s.loadDelay)
		}

		s.mu.Lock()
		s.loadCalls++
		statusCode := s.loadStatusCode
		s.modelLoaded = statusCode == http.StatusOK || statusCode == http.StatusCreated || statusCode == http.StatusAccepted
		s.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		_, _ = w.Write([]byte(`{"success":true}`))
	})
	mux.HandleFunc("/models", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		loaded := s.modelLoaded
		statusCode := s.queryStatusCode
		s.mu.Unlock()

		status := "unloaded"
		if loaded {
			status = "loaded"
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		_, _ = w.Write([]byte(fmt.Sprintf(`{"data":[{"id":%q,"status":{"value":%q}}]}`, testModelName, status)))
	})

	return mux
}

func newTestPlugin(t *testing.T, server *httptest.Server) *Plugin {
	t.Helper()

	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse test server URL: %v", err)
	}

	port, err := strconv.Atoi(serverURL.Port())
	if err != nil {
		t.Fatalf("parse test server port: %v", err)
	}

	config := DefaultConfig
	config.ModelServerPort = port
	config.ModelLoadTimeoutSeconds = 5

	return NewPlugin("model-loader", config)
}

func newSchedulingResult() *types.SchedulingResult {
	pod := &types.PodMetrics{
		Pod: &backend.Pod{
			NamespacedName: k8stypes.NamespacedName{Namespace: "openai", Name: testPodKey},
			Address:        testPodIP,
		},
	}

	return &types.SchedulingResult{
		PrimaryProfileName: "default",
		ProfileResults: map[string]*types.ProfileRunResult{
			"default": {
				TargetPods: []types.Pod{pod},
			},
		},
	}
}

func newLLMRequest() *types.LLMRequest {
	return &types.LLMRequest{TargetModel: testModelName}
}

func TestLoadModelConcurrentFailureIsVisibleToAllWaiters(t *testing.T) {
	t.Parallel()

	serverState := newScriptedModelServer()
	serverState.loadDelay = 50 * time.Millisecond
	serverState.loadStatusCode = http.StatusInternalServerError

	server := httptest.NewServer(serverState.handler())
	t.Cleanup(server.Close)

	plugin := newTestPlugin(t, server)

	primaryErr := make(chan error, 1)
	go func() {
		primaryErr <- plugin.loadModel(context.Background(), testPodIP, testModelName)
	}()

	// Ensure the primary request owns the in-flight load before waiters join.
	time.Sleep(10 * time.Millisecond)

	const waiterCount = 3
	waiterErrors := make(chan error, waiterCount)
	for range waiterCount {
		go func() {
			waiterErrors <- plugin.loadModel(context.Background(), testPodIP, testModelName)
		}()
	}

	for range waiterCount {
		if err := <-waiterErrors; err == nil {
			t.Fatal("expected every waiter to observe the failed load")
		}
	}

	if err := <-primaryErr; err == nil {
		t.Fatal("expected primary load to fail")
	}

	serverState.mu.Lock()
	loadCalls := serverState.loadCalls
	serverState.mu.Unlock()

	if loadCalls != 1 {
		t.Fatalf("expected failed concurrent load to be deduplicated once, got %d load calls", loadCalls)
	}

	plugin.mu.RLock()
	_, loading := plugin.loadingModels[testPodIP+":"+testModelName]
	_, loaded := plugin.loadedModels[testPodIP+":"+testModelName]
	plugin.mu.RUnlock()

	if loading {
		t.Fatal("expected loading state to be cleaned up")
	}

	if loaded {
		t.Fatal("expected failed load not to be marked loaded")
	}
}

func TestModelLoaderFactoryWiresUnloadObserver(t *testing.T) {
	t.Parallel()

	tracker := modeltracker.NewTracker(nil, modeltracker.Options{})
	SetModelLoaderDeps(&ModelLoaderDeps{Tracker: tracker})
	t.Cleanup(func() { SetModelLoaderDeps(nil) })

	pluginInterface, err := ModelLoaderFactory("model-loader", nil, nil)
	if err != nil {
		t.Fatalf("ModelLoaderFactory returned error: %v", err)
	}

	plugin, ok := pluginInterface.(*Plugin)
	if !ok {
		t.Fatalf("expected *Plugin, got %T", pluginInterface)
	}

	loadKey := testPodIP + ":" + testModelName
	plugin.mu.Lock()
	plugin.loadedModels[loadKey] = time.Now()
	plugin.mu.Unlock()

	tracker.MarkLoaded(testPodIP, testModelName)
	tracker.MarkUnloaded(testPodIP, testModelName)

	plugin.mu.RLock()
	_, loaded := plugin.loadedModels[loadKey]
	plugin.mu.RUnlock()

	if loaded {
		t.Fatal("expected factory-wired unload observer to clear cached loaded model")
	}
}

func TestInvalidateLoadedModelClearsCachedEntry(t *testing.T) {
	t.Parallel()

	serverState := newScriptedModelServer()
	server := httptest.NewServer(serverState.handler())
	t.Cleanup(server.Close)

	plugin := newTestPlugin(t, server)
	loadKey := testPodIP + ":" + testModelName

	plugin.mu.Lock()
	plugin.loadedModels[loadKey] = time.Now()
	plugin.mu.Unlock()

	plugin.InvalidateLoadedModel(testPodIP, testModelName)

	plugin.mu.RLock()
	_, loaded := plugin.loadedModels[loadKey]
	plugin.mu.RUnlock()

	if loaded {
		t.Fatal("expected dedup unload invalidation to clear cached loaded model")
	}
}

func TestPreRequestConcurrentColdLoadSuccessWaits(t *testing.T) {
	t.Parallel()

	serverState := newScriptedModelServer()
	serverState.loadDelay = 50 * time.Millisecond

	server := httptest.NewServer(serverState.handler())
	t.Cleanup(server.Close)

	plugin := newTestPlugin(t, server)
	schedulingResult := newSchedulingResult()

	var waitGroup sync.WaitGroup
	waitGroup.Add(2)

	go func() {
		defer waitGroup.Done()
		plugin.PreRequest(context.Background(), newLLMRequest(), schedulingResult)
	}()

	time.Sleep(10 * time.Millisecond)

	go func() {
		defer waitGroup.Done()
		plugin.PreRequest(context.Background(), newLLMRequest(), schedulingResult)
	}()

	waitGroup.Wait()

	serverState.mu.Lock()
	loadCalls := serverState.loadCalls
	serverState.mu.Unlock()

	if loadCalls != 1 {
		t.Fatalf("expected concurrent successful load to be deduplicated, got %d load calls", loadCalls)
	}

	plugin.mu.RLock()
	_, loading := plugin.loadingModels[testPodIP+":"+testModelName]
	_, loaded := plugin.loadedModels[testPodIP+":"+testModelName]
	plugin.mu.RUnlock()

	if loading {
		t.Fatal("expected loading state to be cleaned up")
	}

	if !loaded {
		t.Fatal("expected successful load to be marked loaded")
	}
}
