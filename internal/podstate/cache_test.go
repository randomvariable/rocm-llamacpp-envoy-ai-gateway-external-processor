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

package podstate

import "testing"

func TestCache_PressureUnknownPodIsZero(t *testing.T) {
	c := NewCache()
	if got := c.Pressure("openai/llama-server-x"); got != 0 {
		t.Errorf("expected 0 for unknown pod, got %v", got)
	}
}

func TestCache_PressureSumsRunningAndWaiting(t *testing.T) {
	c := NewCache()
	c.Update(Snapshot{
		PodKey:              "openai/llama-server-a",
		RunningRequestsSize: 3,
		WaitingQueueSize:    2,
	})

	if got := c.Pressure("openai/llama-server-a"); got != 5 {
		t.Errorf("expected 5, got %v", got)
	}
}

func TestCache_UpdateOverwrites(t *testing.T) {
	c := NewCache()
	c.Update(Snapshot{PodKey: "openai/p", RunningRequestsSize: 10})
	c.Update(Snapshot{PodKey: "openai/p", RunningRequestsSize: 1})

	if got := c.Pressure("openai/p"); got != 1 {
		t.Errorf("expected last-write 1, got %v", got)
	}
}

func TestCache_UpdateEmptyKeyIsNoop(t *testing.T) {
	c := NewCache()
	c.Update(Snapshot{PodKey: "", RunningRequestsSize: 99})

	if _, ok := c.Get(""); ok {
		t.Error("empty PodKey should not be stored")
	}
}

func TestCache_NilReceiverIsSafe(t *testing.T) {
	var c *Cache
	c.Update(Snapshot{PodKey: "x", RunningRequestsSize: 1})

	if got := c.Pressure("x"); got != 0 {
		t.Errorf("nil cache should return 0 pressure, got %v", got)
	}

	if _, ok := c.Get("x"); ok {
		t.Error("nil cache should not have anything stored")
	}
}
