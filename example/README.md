# Cascading RBAC operator — example

A runnable demo of the cascading-rbac-operator against a Platform Mesh kcp, using
the same **provider workspace** layout the operator uses in the portal (see the
[repo README](../README.md#deployment-layout)).

## What it demonstrates

A `Cascade` created in a consumer workspace (`cascade-demo`) copies a referenced
resource — here the `example-configmap-viewer` ClusterRole — into that
workspace's descendants, down to `spec.maxDepth` levels, via server-side apply.
When a new workspace is later created within that reach, the workspace controller
triggers the covering `Cascade` to re-reconcile, and the operator applies the
role into the new workspace too.

## Prerequisites

- A running Platform Mesh kcp where the `root:orgs:e2e` org exists, with its admin
  kubeconfig at `../.kcp/admin.kubeconfig` (relative to the repo root), or
  `KUBECONFIG` exported to point elsewhere. Override `ORG`/`PROVIDER_WS`/
  `CONSUMER_WS` env vars if your paths differ.
- `kubectl` with the kcp `kubectl-ws` plugin.
- Go, to run the operator via `go run .`.

## Files

- `setup.sh` — deploys the fleet provider stack (`config/resources`) into the
  provider workspace `root:orgs:e2e:cascade-provider`, builds the consumer
  hierarchy `cascade-demo -> team -> squad`, binds the fleet API into
  `cascade-demo`, and creates the `example-configmap-viewer` ClusterRole and the
  demo `Cascade` there.
- `apibinding.yaml` — binds the `fleet.platform-mesh.io` APIExport (from the
  provider workspace) into a consumer workspace.
- `cascade.yaml` — the demo `Cascade` (`maxDepth: 2`, referencing
  `example-configmap-viewer`).

## Run it

```sh
# from the repo root
./example/setup.sh
```

Then follow the printed instructions. The operator must point its cascade
provider at the provider workspace (tenancy stays on `root`):

```sh
export KUBECONFIG="$PWD/.kcp/admin.kubeconfig"
go run . --cascade-endpoint root:orgs:e2e:cascade-provider/fleet.platform-mesh.io
```

The operator applies the ClusterRole into `cascade-demo:team` and `:team:squad`,
logging `Cascaded object applied`. In another terminal, create a covered
workspace and watch it get the role:

```sh
export KUBECONFIG="$PWD/.kcp/admin.kubeconfig"
kubectl ws use :root:orgs:e2e:cascade-demo
kubectl ws create team2   # covered -> "Triggered cascade through workspace" -> role applied
```

Creating a workspace deeper than `maxDepth` levels below `cascade-demo` instead
logs `No cascades cover this workspace`.

> This example uses the CLI/operator directly. For the portal integration
> (marketplace tile, read-only ClusterRoles view, Cascade create form), see the
> `config/ui` resources and the [Portal UI](../README.md#portal-ui-contentconfiguration--providermetadata)
> section of the repo README.
