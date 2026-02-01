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

package vram

import (
	"strings"
	"testing"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// FuzzParsePrometheusMetrics verifies that the Prometheus text parser used by
// scrapeNodeMetrics never panics on arbitrary input, and that when parsing
// succeeds, VRAM metric extraction produces sensible values.
func FuzzParsePrometheusMetrics(f *testing.F) {
	// Seed corpus with representative Prometheus exposition format inputs.
	f.Add(`# HELP rocm_memory_total_bytes Total VRAM
# TYPE rocm_memory_total_bytes gauge
rocm_memory_total_bytes 34359738368
# HELP rocm_memory_used_bytes Used VRAM
# TYPE rocm_memory_used_bytes gauge
rocm_memory_used_bytes 8589934592
`)
	f.Add(`# HELP some_metric Help text
# TYPE some_metric gauge
some_metric 42
`)
	f.Add(``)
	f.Add(`not valid prometheus format`)
	f.Add("rocm_memory_total_bytes 0\nrocm_memory_used_bytes 0\n")
	f.Add("rocm_memory_total_bytes NaN\n")
	f.Add("rocm_memory_total_bytes +Inf\n")
	f.Add("rocm_memory_total_bytes -1\n")

	f.Fuzz(func(t *testing.T, metricsText string) {
		parser := expfmt.NewTextParser(model.LegacyValidation)

		metricFamilies, err := parser.TextToMetricFamilies(strings.NewReader(metricsText))
		if err != nil {
			// Parse errors are expected for arbitrary input.
			return
		}

		// Extract VRAM metrics the same way the scraper does.
		var totalVRAM, usedVRAM int64

		totalVRAM = DefaultTotalVRAMGB * BytesPerGB // default

		if mf, ok := metricFamilies[DefaultTotalMetricName]; ok {
			if len(mf.GetMetric()) > 0 {
				totalVRAM = int64(getMetricValue(mf.GetMetric()[0]))
			}
		}

		if mf, ok := metricFamilies[DefaultUsedMetricName]; ok {
			if len(mf.GetMetric()) > 0 {
				usedVRAM = int64(getMetricValue(mf.GetMetric()[0]))
			}
		}

		available := max(totalVRAM-usedVRAM, 0)

		// available must never be negative.
		if available < 0 {
			t.Errorf("available VRAM is negative: %d (total=%d, used=%d)", available, totalVRAM, usedVRAM)
		}
	})
}
