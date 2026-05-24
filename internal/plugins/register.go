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

// Package plugins provides custom plugins for the inference scheduler.
package plugins

import (
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/plugins"

	"github.com/randomvariable/rocm-llamacpp-envoy-ai-gateway-external-processor/internal/plugins/filter"
	"github.com/randomvariable/rocm-llamacpp-envoy-ai-gateway-external-processor/internal/plugins/modelloader"
	"github.com/randomvariable/rocm-llamacpp-envoy-ai-gateway-external-processor/internal/plugins/scorer"
)

// RegisterAllPlugins registers the factory functions of all custom plugins.
// This should be called in main() before runner.NewRunner().Run().
func RegisterAllPlugins() {
	// Register scheduling plugins.
	plugins.Register(filter.LoadedModelFilterType, filter.LoadedModelFilterFactory)
	plugins.Register(scorer.VRAMScorerType, scorer.VRAMScorerFactory)

	// Register request control plugins.
	plugins.Register(modelloader.ModelLoaderType, modelloader.ModelLoaderFactory)
}
