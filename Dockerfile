# syntax=docker/dockerfile:1.7
# Copyright 2026 Naadir Jeewa
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0

# Build stage
FROM golang:1.25-alpine AS builder

# Build arguments for version information
ARG VERSION=devel
ARG GIT_COMMIT=unknown
ARG GIT_TREE_STATE=clean
ARG BUILD_DATE

WORKDIR /workspace

# Copy go mod files (separate layer for better caching)
COPY go.mod go.sum ./

# Download dependencies (cached unless go.mod/go.sum change)
RUN --mount=type=cache,target=/go/pkg/mod \
  --mount=type=cache,target=/root/.cache/go-build \
  go mod download

# Copy source code
COPY cmd/ cmd/
COPY pkg/ pkg/

# Build with version injection (cached unless source changes)
RUN --mount=type=cache,target=/go/pkg/mod \
  --mount=type=cache,target=/root/.cache/go-build \
  CGO_ENABLED=0 GOOS=linux go build \
  -ldflags "-X sigs.k8s.io/release-utils/version.gitVersion=${VERSION} \
  -X sigs.k8s.io/release-utils/version.gitCommit=${GIT_COMMIT} \
  -X sigs.k8s.io/release-utils/version.gitTreeState=${GIT_TREE_STATE} \
  -X sigs.k8s.io/release-utils/version.buildDate=${BUILD_DATE}" \
  -o extproc ./cmd/extproc

# Runtime stage
FROM gcr.io/distroless/static:nonroot

WORKDIR /

COPY --from=builder /workspace/extproc .

USER 65532:65532

ENTRYPOINT ["/extproc"]
