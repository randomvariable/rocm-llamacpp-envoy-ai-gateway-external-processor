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

package filter_test

import (
	"context"
	"errors"
	"sort"
	"testing"

	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/backend"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/scheduling/types"

	"github.com/randomvariable/rocm-llamacpp-envoy-ai-gateway-external-processor/internal/modeltracker"
	pluginfilter "github.com/randomvariable/rocm-llamacpp-envoy-ai-gateway-external-processor/internal/plugins/filter"
)

func mkPod(namespace, name string) types.Pod {
	return &types.PodMetrics{
		Pod: &backend.Pod{
			NamespacedName: k8stypes.NamespacedName{
				Namespace: namespace,
				Name:      name,
			},
			Address: "10.0.0.1",
		},
	}
}

func newTrackerSeeded(t *testing.T, seed map[string][]string) *modeltracker.Tracker {
	t.Helper()

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	cli := crfake.NewClientBuilder().WithScheme(scheme).Build()
	tr := modeltracker.NewTracker(cli, modeltracker.Options{})

	for key, models := range seed {
		tr.SetLoaded(key, models)
	}

	return tr
}

func podNames(pods []types.Pod) []string {
	out := make([]string, 0, len(pods))
	for _, p := range pods {
		out = append(out, p.GetPod().NamespacedName.String())
	}

	sort.Strings(out)

	return out
}

func TestLoadedModelFilter_DropsColdPodsWhenWarmExists(t *testing.T) {
	t.Parallel()

	tr := newTrackerSeeded(t, map[string][]string{
		"openai/server3": {"gpt-oss-20b", "qwen3.6-35b"},
		"openai/server5": {"luminia-8b"},
	})
	f := pluginfilter.NewLoadedModelFilter(tr)

	pods := []types.Pod{
		mkPod("openai", "server3"),
		mkPod("openai", "server5"),
	}

	got := f.Filter(
		context.Background(), nil,
		&types.LLMRequest{TargetModel: "gpt-oss-20b"},
		pods,
	)

	want := []string{"openai/server3"}
	if diff := podNames(got); !equalStrSlice(diff, want) {
		t.Errorf("Filter dropped cold pods incorrectly. got=%v want=%v",
			diff, want)
	}
}

func TestLoadedModelFilter_PassesThroughWhenNoWarmPod(t *testing.T) {
	t.Parallel()

	tr := newTrackerSeeded(t, map[string][]string{
		"openai/server3": {"luminia-8b"},
		"openai/server5": {"noromaid-7b"},
	})
	f := pluginfilter.NewLoadedModelFilter(tr)

	pods := []types.Pod{
		mkPod("openai", "server3"),
		mkPod("openai", "server5"),
	}

	got := f.Filter(
		context.Background(), nil,
		&types.LLMRequest{TargetModel: "gpt-oss-20b"},
		pods,
	)

	// No pod has gpt-oss-20b loaded → fall through to all input pods
	// so model-loader can cold-load.
	want := []string{"openai/server3", "openai/server5"}
	if diff := podNames(got); !equalStrSlice(diff, want) {
		t.Errorf("Filter dropped pods when no warm pod existed. got=%v want=%v",
			diff, want)
	}
}

func TestLoadedModelFilter_KeepsAllWhenAllWarm(t *testing.T) {
	t.Parallel()

	tr := newTrackerSeeded(t, map[string][]string{
		"openai/server3": {"gpt-oss-20b"},
		"openai/server5": {"gpt-oss-20b"},
	})
	f := pluginfilter.NewLoadedModelFilter(tr)

	pods := []types.Pod{
		mkPod("openai", "server3"),
		mkPod("openai", "server5"),
	}

	got := f.Filter(
		context.Background(), nil,
		&types.LLMRequest{TargetModel: "gpt-oss-20b"},
		pods,
	)

	want := []string{"openai/server3", "openai/server5"}
	if diff := podNames(got); !equalStrSlice(diff, want) {
		t.Errorf("Filter dropped warm pods. got=%v want=%v", diff, want)
	}
}

func TestLoadedModelFilter_NoTargetModel_PassThrough(t *testing.T) {
	t.Parallel()

	tr := newTrackerSeeded(t, map[string][]string{
		"openai/server3": {"gpt-oss-20b"},
	})
	f := pluginfilter.NewLoadedModelFilter(tr)

	pods := []types.Pod{mkPod("openai", "server3"), mkPod("openai", "server5")}

	// Empty TargetModel → pass through (no signal to filter on).
	got := f.Filter(
		context.Background(), nil, &types.LLMRequest{}, pods,
	)
	if len(got) != len(pods) {
		t.Errorf("expected pass-through, got %d pods (want %d)",
			len(got), len(pods))
	}

	// Nil request → pass through.
	got = f.Filter(context.Background(), nil, nil, pods)
	if len(got) != len(pods) {
		t.Errorf("expected pass-through for nil request, got %d pods",
			len(got))
	}
}

func TestLoadedModelFilter_BeforeFirstPoll_PassThrough(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	cli := crfake.NewClientBuilder().WithScheme(scheme).Build()
	// Tracker exists but firstPoll=false → IsLoaded returns false for all
	// → filter sees no warm pods → falls through to all pods.
	tr := modeltracker.NewTracker(cli, modeltracker.Options{})

	f := pluginfilter.NewLoadedModelFilter(tr)
	pods := []types.Pod{mkPod("openai", "server3"), mkPod("openai", "server5")}

	got := f.Filter(
		context.Background(), nil,
		&types.LLMRequest{TargetModel: "gpt-oss-20b"},
		pods,
	)

	if len(got) != len(pods) {
		t.Errorf(
			"expected pass-through before first poll, got %d pods (want %d)",
			len(got), len(pods),
		)
	}
}

func TestLoadedModelFilterFactory_ErrorsWithoutDeps(t *testing.T) {
	t.Parallel()

	// Reset global deps.
	pluginfilter.SetLoadedModelFilterDeps(nil)

	_, err := pluginfilter.LoadedModelFilterFactory("test", nil, nil)
	if !errors.Is(err, pluginfilter.ErrDepsNotSet) {
		t.Errorf("expected ErrDepsNotSet, got %v", err)
	}
}

func TestLoadedModelFilterFactory_BuildsWithDeps(t *testing.T) {
	t.Parallel()

	tr := newTrackerSeeded(t, map[string][]string{
		"openai/server3": {"gpt-oss-20b"},
	})
	pluginfilter.SetLoadedModelFilterDeps(
		&pluginfilter.LoadedModelFilterDeps{Tracker: tr},
	)

	t.Cleanup(func() { pluginfilter.SetLoadedModelFilterDeps(nil) })

	p, err := pluginfilter.LoadedModelFilterFactory("my-filter", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if p == nil {
		t.Fatal("Factory returned nil plugin")
	}

	if p.TypedName().Name != "my-filter" {
		t.Errorf("expected name=my-filter, got %q", p.TypedName().Name)
	}

	if p.TypedName().Type != pluginfilter.LoadedModelFilterType {
		t.Errorf("expected type=%q, got %q",
			pluginfilter.LoadedModelFilterType, p.TypedName().Type)
	}
}

func equalStrSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
