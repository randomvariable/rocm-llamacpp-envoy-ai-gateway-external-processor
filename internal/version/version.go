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

// Package version provides build-time version information.
// Version information is injected at build time via ldflags.
package version

import (
	"runtime/debug"

	"sigs.k8s.io/release-utils/version"
)

const (
	// ServiceName is the name of the service for telemetry.
	ServiceName = "rocm-llamacpp-envoy-ai-gateway-extproc"

	// shortRevisionLength is the length of the abbreviated git revision.
	shortRevisionLength = 7

	// develVersion is the fallback version when no version info is available.
	develVersion = "devel"
)

// GetVersion returns the version information.
// If version was not set via ldflags, it attempts to get version from build info.
func GetVersion() string {
	ver := version.GetVersionInfo().GitVersion

	if ver != "" && ver != develVersion {
		return ver
	}

	// Fallback: try to get version from build info (go modules).
	return getVersionFromBuildInfo()
}

// getVersionFromBuildInfo extracts version from go build info.
func getVersionFromBuildInfo() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return develVersion
	}

	rev := getRevisionFromSettings(info.Settings)
	if rev == "" {
		return develVersion
	}

	if len(rev) > shortRevisionLength {
		rev = rev[:shortRevisionLength]
	}

	if isDirtyFromSettings(info.Settings) {
		return rev + "-dirty"
	}

	return rev
}

// getRevisionFromSettings extracts the vcs.revision from build settings.
func getRevisionFromSettings(settings []debug.BuildSetting) string {
	for _, setting := range settings {
		if setting.Key == "vcs.revision" {
			return setting.Value
		}
	}

	return ""
}

// isDirtyFromSettings checks if vcs.modified is true in build settings.
func isDirtyFromSettings(settings []debug.BuildSetting) bool {
	for _, setting := range settings {
		if setting.Key == "vcs.modified" {
			return setting.Value == "true"
		}
	}

	return false
}

// GetGitCommit returns the git commit SHA.
func GetGitCommit() string {
	commit := version.GetVersionInfo().GitCommit

	if commit != "" {
		return commit
	}

	// Fallback to build info.
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				return setting.Value
			}
		}
	}

	return "unknown"
}

// IsDirty returns true if the build was from a dirty (modified) tree.
func IsDirty() bool {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.modified" {
				return setting.Value == "true"
			}
		}
	}

	return false
}

// GetFullVersionString returns a detailed version string for logging.
func GetFullVersionString() string {
	info := version.GetVersionInfo()

	return info.String()
}
