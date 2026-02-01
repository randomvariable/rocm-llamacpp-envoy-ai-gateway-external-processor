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

package epp

import (
	"testing"

	envoy_config_core_v3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoy_service_ext_proc_v3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/protobuf/types/known/structpb"
)

// FuzzExtractModelFromBody verifies that extractModelFromBody never panics
// regardless of input, and that when it returns a non-empty string, the
// string is a valid model name from a JSON "model" field.
func FuzzExtractModelFromBody(f *testing.F) {
	// Seed corpus with representative inputs.
	f.Add([]byte(`{"model": "gpt-4"}`))
	f.Add([]byte(`{"model": ""}`))
	f.Add([]byte(`{"model": null}`))
	f.Add([]byte(`{"model": 42}`))
	f.Add([]byte(`{"model": true}`))
	f.Add([]byte(`{"model": ["a"]}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`not json`))
	f.Add([]byte(``))
	f.Add([]byte(`{"model": "a/b/c"}`))
	f.Add([]byte{0xff, 0xfe})

	server := &Server{}

	f.Fuzz(func(_ *testing.T, body []byte) {
		// Must never panic.
		_ = server.extractModelFromBody(body)
	})
}

// FuzzExtractPoolKeyFromMetadata verifies that extractPoolKeyFromMetadata
// never panics for arbitrary metadata string values, and that when it returns
// a non-nil PoolKey, Namespace and Name are populated from the first two
// slash-separated segments.
func FuzzExtractPoolKeyFromMetadata(f *testing.F) {
	// Seed corpus with representative metadata values.
	f.Add("default/vllm-pool/vllm-epp/9002/duplex/false")
	f.Add("ns/name")
	f.Add("only-one")
	f.Add("")
	f.Add("/")
	f.Add("//")
	f.Add("a/b/c/d/e/f")
	f.Add("\x00/\xff")

	f.Fuzz(func(_ *testing.T, poolValue string) {
		req := &envoy_service_ext_proc_v3.ProcessingRequest{
			MetadataContext: &envoy_config_core_v3.Metadata{
				FilterMetadata: map[string]*structpb.Struct{
					metadataNamespace: {
						Fields: map[string]*structpb.Value{
							metadataInferencePoolKey: structpb.NewStringValue(poolValue),
						},
					},
				},
			},
		}

		// Must never panic.
		_ = extractPoolKeyFromMetadata(req)
	})
}
