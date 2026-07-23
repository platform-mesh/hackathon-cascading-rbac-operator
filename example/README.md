# TODO remove AI slop explanations


# Cascading RBAC operator — example

This folder contains a runnable demo of the cascading-rbac-operator against a
local kcp.

## What it demonstrates

A `Cascade` in a parent workspace (`root:org`) covers its descendant workspaces
down to `spec.maxDepth` levels. When a new workspace is created within that
reach, the operator triggers a reconcile of the covering `Cascade` and logs the
affected child workspaces.

## Prerequisites

- A running kcp with its admin kubeconfig at `../.kcp/admin.kubeconfig` (relative
  to the repo root), or `KUBECONFIG` exported to point elsewhere.
- `kubectl` with the kcp `kubectl-ws` plugin.
- Go, to run the operator via `go run .`.

## Files

- `setup.sh` — registers the fleet API in `root`, builds the
  `root:org -> team -> squad` workspace hierarchy, binds the fleet API into
  `root:org`, and creates the demo `Cascade`.
- `apibinding.yaml` — binds the `fleet.platform-mesh.io` APIExport into a
  workspace.
- `cascade.yaml` — the demo `Cascade` (`maxDepth: 2`).

## Run it

```sh
# from the repo root
./example/setup.sh
```

Then follow the printed instructions: run the operator with `go run .` and create
a workspace under `root:org` to observe the trigger.

```sh
export KUBECONFIG="$PWD/.kcp/admin.kubeconfig"
go run .
```

In another terminal:

```sh
export KUBECONFIG="$PWD/.kcp/admin.kubeconfig"
kubectl ws use :root:org
kubectl ws create team2   # covered -> operator logs "Triggered cascade through workspace"
```

Creating a workspace deeper than `maxDepth` levels below `root:org` instead logs
`No cascades cover this workspace`.
