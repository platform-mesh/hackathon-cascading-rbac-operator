#!/usr/bin/env bash
# Copyright The Platform Mesh Authors.
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

# setup.sh provisions the demo scenario for the cascading-rbac-operator against a
# running kcp:
#   - registers the fleet API (APIResourceSchema + APIExport) in root
#   - builds the workspace hierarchy root:org -> team -> squad
#   - binds the fleet API into root:org and creates the demo Cascade there
#
# The fleet.platform-mesh.io APIExportEndpointSlice is created automatically by
# kcp, so it is not applied here.
#
# Prerequisites: kubectl, the kcp kubectl-ws plugin, and KUBECONFIG pointing at a
# running kcp (defaults to ./.kcp/admin.kubeconfig from the repo root).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export KUBECONFIG="${KUBECONFIG:-$REPO_ROOT/.kcp/admin.kubeconfig}"

echo "Using KUBECONFIG=$KUBECONFIG"

# ensure_child creates a child workspace under the current workspace unless it
# already exists, then enters it. This keeps the script idempotent across re-runs
# regardless of the existing workspace's type.
ensure_child() {
  local name="$1"
  if ! kubectl ws use "$name" >/dev/null 2>&1; then
    kubectl ws create "$name" --enter
  fi
}

echo "==> Registering fleet API in root"
kubectl ws use :root
kubectl apply -f "$REPO_ROOT/config/resources/apiresourceschema-cascades.fleet.platform-mesh.io.yaml"
kubectl apply -f "$REPO_ROOT/config/resources/apiexport-fleet.platform-mesh.io.yaml"

echo "==> Creating workspace hierarchy root:org -> team -> squad"
kubectl ws use :root
ensure_child org
ensure_child team
ensure_child squad

echo "==> Binding fleet API and creating the demo Cascade in root:org"
kubectl ws use :root:org
kubectl apply -f "$REPO_ROOT/example/apibinding.yaml"
kubectl wait --for=jsonpath='{.status.phase}'=Bound apibinding/fleet.platform-mesh.io --timeout=30s
kubectl apply -f "$REPO_ROOT/example/cascade.yaml"

kubectl ws use :root

cat <<EOF

Setup complete.

Next steps:
  1. In one terminal, run the operator:
       export KUBECONFIG="$KUBECONFIG"
       go run .

  2. In another terminal, create a workspace covered by the demo Cascade and
     watch the operator log "Triggered cascade through workspace":
       export KUBECONFIG="$KUBECONFIG"
       kubectl ws use :root:org
       kubectl ws create team2

     Creating a workspace outside the Cascade's reach (e.g. more than maxDepth
     levels below root:org) instead logs "No cascades cover this workspace".
EOF
