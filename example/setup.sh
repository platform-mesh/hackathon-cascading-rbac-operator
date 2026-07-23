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
# running Platform Mesh kcp. It mirrors the "provider workspace" layout the
# operator uses in the portal (see the repo README):
#   - deploys the fleet provider stack (APIResourceSchema + APIExport + bind RBAC
#     + a self-binding) into the provider workspace
#     root:orgs:e2e:cascade-provider
#   - builds a consumer hierarchy consumer -> team -> squad, binds the fleet
#     API into cascade-demo, and creates the referenced ClusterRole + demo Cascade
#     there
#
# The fleet.platform-mesh.io APIExportEndpointSlice is created automatically by
# kcp next to the APIExport, so it is not applied here.
#
# Assumptions: a Platform Mesh kcp where root:orgs:e2e exists (the e2e org). The
# provider path is configurable below if your setup differs.
#
# Prerequisites: kubectl, the kcp kubectl-ws plugin, and KUBECONFIG pointing at a
# running kcp (defaults to ./.kcp/admin.kubeconfig from the repo root).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export KUBECONFIG="${KUBECONFIG:-$REPO_ROOT/.kcp/admin.kubeconfig}"

ORG="${ORG:-root:orgs:e2e}"
PROVIDER="${PROVIDER:-cascade-provider}"
CONSUMER="${CONSUMER:-consumer}"
PROVIDER_WS="${PROVIDER_WS:-$ORG:$PROVIDER}"
CONSUMER_WS="${CONSUMER_WS:-$ORG:$CONSUMER}"

echo "Using KUBECONFIG=$KUBECONFIG"
echo "Provider workspace: $PROVIDER_WS"
echo "Consumer workspace: $CONSUMER_WS"

# ensure_child creates a child workspace under the current workspace unless it
# already exists, then enters it. This keeps the script idempotent across re-runs.
ensure_child() {
  local name="$1"
  if ! kubectl ws use "$name" >/dev/null 2>&1; then
    kubectl ws create "$name" --enter
  fi
}

echo "==> Deploying the fleet provider stack into $PROVIDER_WS"
kubectl ws use ":$ORG"
ensure_child "$PROVIDER"
kubectl apply -f "$REPO_ROOT/config/resources/apiresourceschema-cascades.fleet.platform-mesh.io.yaml"
kubectl apply -f "$REPO_ROOT/config/resources/apiexport-fleet.platform-mesh.io.yaml"
kubectl apply -f "$REPO_ROOT/config/resources/rbac-apiexport-bind.fleet.platform-mesh.io.yaml"
kubectl apply -f "$REPO_ROOT/config/resources/apibinding-fleet.platform-mesh.io.yaml"

echo "==> Building consumer hierarchy $CONSUMER_WS -> team -> squad"
kubectl ws use ":$ORG"
ensure_child "$CONSUMER"
ensure_child team
ensure_child squad

echo "==> Binding fleet API and creating the ClusterRole + demo Cascade in $CONSUMER_WS"
kubectl ws use ":$CONSUMER_WS"
kubectl apply -f "$REPO_ROOT/example/apibinding.yaml"
kubectl wait --for=jsonpath='{.status.phase}'=Bound apibinding/fleet.platform-mesh.io --timeout=30s
kubectl apply -f "$REPO_ROOT/example/clusterrole.yaml"
kubectl apply -f "$REPO_ROOT/example/cascade.yaml"

kubectl ws use :root

cat <<EOF

Setup complete.

Next steps:
  1. In one terminal, run the operator (point the cascade provider at the
     provider workspace; tenancy stays on root):
       export KUBECONFIG="$KUBECONFIG"
       go run . --cascade-endpoint "$PROVIDER_WS/fleet.platform-mesh.io"

     The operator applies the example-configmap-viewer ClusterRole into
     $CONSUMER_WS:team and :team:squad (maxDepth 2), logging "Cascaded object
     applied".

  2. In another terminal, create a new workspace covered by the Cascade and watch
     the operator log "Triggered cascade through workspace", then apply the role
     into it:
       export KUBECONFIG="$KUBECONFIG"
       kubectl ws use ":$CONSUMER_WS"
       kubectl ws create team2

     Creating a workspace deeper than maxDepth levels below $CONSUMER_WS instead
     logs "No cascades cover this workspace".
EOF
