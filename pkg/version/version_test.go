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

package version

import (
	"runtime/debug"
	"testing"
)

func TestServiceName(t *testing.T) {
	t.Parallel()

	expected := "rocm-envoy-ai-gateway-extproc"
	if ServiceName != expected {
		t.Errorf("ServiceName = %q, want %q", ServiceName, expected)
	}
}

func TestGetVersion(t *testing.T) {
	t.Parallel()

	version := GetVersion()

	// Version should not be empty
	if version == "" {
		t.Error("GetVersion should not return empty string")
	}

	// In test environment, it might return "devel" or a version from build info
	t.Logf("GetVersion returned: %s", version)
}

func TestShortRevisionLength(t *testing.T) {
	t.Parallel()

	if shortRevisionLength != 7 {
		t.Errorf("shortRevisionLength = %d, want 7", shortRevisionLength)
	}
}

func TestDevelVersion(t *testing.T) {
	t.Parallel()

	if develVersion != "devel" {
		t.Errorf("develVersion = %q, want %q", develVersion, "devel")
	}
}

func TestGetRevisionFromSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		settings []debug.BuildSetting
		want     string
	}{
		{
			name: "revision found",
			settings: []debug.BuildSetting{
				{Key: "vcs.revision", Value: "abc123def456"},
			},
			want: "abc123def456",
		},
		{
			name: "revision not found",
			settings: []debug.BuildSetting{
				{Key: "vcs.modified", Value: "false"},
			},
			want: "",
		},
		{
			name:     "empty settings",
			settings: []debug.BuildSetting{},
			want:     "",
		},
		{
			name:     "nil settings",
			settings: nil,
			want:     "",
		},
		{
			name: "revision among multiple settings",
			settings: []debug.BuildSetting{
				{Key: "foo", Value: "bar"},
				{Key: "vcs.revision", Value: "1234567890abcdef"},
				{Key: "vcs.modified", Value: "true"},
			},
			want: "1234567890abcdef",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := getRevisionFromSettings(tt.settings)
			if got != tt.want {
				t.Errorf("getRevisionFromSettings() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIsDirtyFromSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		settings []debug.BuildSetting
		want     bool
	}{
		{
			name:     "modified is true",
			settings: []debug.BuildSetting{{Key: "vcs.modified", Value: "true"}},
			want:     true,
		},
		{
			name:     "modified is false",
			settings: []debug.BuildSetting{{Key: "vcs.modified", Value: "false"}},
			want:     false,
		},
		{
			name:     "modified key missing",
			settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abc"}},
			want:     false,
		},
		{
			name:     "empty settings",
			settings: []debug.BuildSetting{},
			want:     false,
		},
		{
			name:     "nil settings",
			settings: nil,
			want:     false,
		},
		{
			name:     "unexpected value",
			settings: []debug.BuildSetting{{Key: "vcs.modified", Value: "yes"}},
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := isDirtyFromSettings(tt.settings)
			if got != tt.want {
				t.Errorf("isDirtyFromSettings() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetGitCommit(t *testing.T) {
	t.Parallel()

	commit := GetGitCommit()
	if commit == "" {
		t.Error("GetGitCommit should not return empty string")
	}
}

func TestIsDirty(t *testing.T) {
	t.Parallel()

	// Just verify no panic.
	_ = IsDirty()
}

func TestGetFullVersionString(t *testing.T) {
	t.Parallel()

	fullVersion := GetFullVersionString()
	if fullVersion == "" {
		t.Error("GetFullVersionString should not return empty string")
	}
}
