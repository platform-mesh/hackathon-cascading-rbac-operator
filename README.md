# Hackathon Cascading RBAC Operator

This repository contains content from a Platform Mesh Hackathon. It is not production ready. Consider it a proof of concept.

## Running the operator

With `KUBECONFIG` or `--kubeconfig` pointing to the root workspace. The operator
runs two providers, each watching an `APIExportEndpointSlice` in a workspace it
resolves from a flag of the form `<workspace-path>/<endpointslice-name>`:

- `--tenancy-endpoint` (default `root/tenancy.kcp.io`) — the workspaces API.
- `--cascade-endpoint` (default `root/fleet.platform-mesh.io`) — the fleet
  (cascades) API. Point this at the provider workspace that hosts the fleet
  APIExport (see *Deployment layout*).

```sh
kcp start
export KUBECONFIG=".kcp/admin.kubeconfig"

# fleet API deployed in the cascade-provider workspace (this repo's layout):
go run . --cascade-endpoint root:orgs:e2e:cascade-provider/fleet.platform-mesh.io
```

For a runnable end-to-end demo (provider stack + a consumer hierarchy + a
Cascade), see [`example/`](example/README.md).

## Creating CRDs & ARS

```sh
# Generate DeepCopy
go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.21.0 object:headerFile=./hack/boilerplate/boilerplate.go.txt paths=./apis/fleet/v1alpha1

# Generate CRDs
go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.21.0 crd:headerFile=./hack/boilerplate/boilerplate.yaml.txt paths=./apis/fleet/... output:crd:artifacts:config=config/crd/bases

# Generate ARS
go run github.com/kcp-dev/sdk/cmd/apigen@v0.32.3 --input-dir ./config/crd/bases --output-dir ./config/resources --header-file ./hack/boilerplate/boilerplate.yaml.txt
```

## Deployment layout

The fleet API and its marketplace/UI content are deployed as a **provider**,
into a dedicated provider workspace — `root:orgs:e2e:cascade-provider` in the
e2e setup. This mirrors the other providers (e.g. `s3-provider`) and is required
for the portal to behave correctly: the `contentconfigurations` virtual
workspace always merges configs from the *system* workspace
(`platform-mesh-system`) into every account, so a `content-for` view placed there
would show for everyone. Hosting it in a provider workspace lets the portal gate
it to accounts that actually bound the export.

Two workspaces are involved:

| Workspace | Resources |
| --- | --- |
| `root:orgs:e2e:cascade-provider` (provider) | `config/resources/` (ARS, APIExport, endpoint slice, self-binding, bind RBAC) + the provider-scoped `config/ui/` (`contentconfiguration-cascades.yaml`, `providermetadata-fleet.platform-mesh.io.yaml`) |
| `root:platform-mesh-system` (system) | the global `contentconfiguration-clusterroles.yaml` only |

`config/resources/` contains:

- `apiresourceschema-cascades…` + `apiexport-fleet…` — the `Cascade` API. The
  APIExport carries `ui.platform-mesh.io/content-for: fleet.platform-mesh.io`,
  which (a) lets the Marketplace virtual workspace join it with the
  `ProviderMetadata` of the same name into a `MarketplaceEntry`, and (b) scopes
  the `cascades-ui` view to accounts that bind the export.
- `rbac-apiexport-bind…` — ClusterRole + ClusterRoleBinding granting the `bind`
  verb on the APIExport to `system:authenticated`, so it can be installed from
  the marketplace (without it, binding fails with "no permission to bind").
- `apibinding-fleet…` — a self-binding of the export in the provider workspace.
  It makes `Cascade` authorable there and, crucially, provides the ≥1 binding an
  `APIExportEndpointSlice` needs before it publishes its virtual-workspace URL.
- `apiexportendpointslice-fleet…` — the slice whose URL the operator connects to.

```sh
# provider workspace: the Cascade API + marketplace/UI content
kubectl ws use root:orgs:e2e:cascade-provider
kubectl apply -f config/resources/
kubectl apply -f config/ui/contentconfiguration-cascades.yaml
kubectl apply -f config/ui/providermetadata-fleet.platform-mesh.io.yaml

# system workspace: the global read-only ClusterRoles view
kubectl ws use root:platform-mesh-system
kubectl apply -f config/ui/contentconfiguration-clusterroles.yaml
```

> The operator's fleet provider watches for the `fleet.platform-mesh.io`
> `APIExportEndpointSlice` in the workspace given by `--cascade-endpoint`
> (`root:orgs:e2e:cascade-provider/fleet.platform-mesh.io` for this layout); the
> tenancy provider watches the `tenancy.kcp.io` slice in `root` via
> `--tenancy-endpoint`. kcp auto-creates the slice next to the APIExport. If you
> relocate the provider stack, update `--cascade-endpoint` to match.

## Portal UI (ContentConfiguration & ProviderMetadata)

`config/ui/` surfaces this operator in the portal using the built-in generic web
components (`generic-list-view`) — no separate micro-frontend is shipped. Each
`ContentConfiguration` declares a `resourceDefinition` (GVK, scope, and the
list/detail/create fields); the `ProviderMetadata` advertises the export in the
marketplace.

- `contentconfiguration-clusterroles.yaml` — a **read-only** ClusterRoles view.
  Only the `ui.platform-mesh.io/entity: core_platform-mesh_io_account` label (no
  `content-for`), so it appears on every account page like the built-in
  Namespaces node. Read-only because only `listView`/`detailView` are declared
  (no `createView`, no row `actions`). This is the payoff view: the ClusterRole
  cascaded into a child workspace shows up here. Lives in `platform-mesh-system`.
- `contentconfiguration-cascades.yaml` — a list **and create** view for
  `Cascade`, labelled `content-for: fleet.platform-mesh.io` so it appears only in
  accounts that bound the export. Lives in the provider workspace.
- `providermetadata-fleet.platform-mesh.io.yaml` — the marketplace entry (name ==
  APIExport name). Lives in the provider workspace, next to the export.

> GraphQL identifiers are the underscored form of the API group
> (`fleet.platform-mesh.io` → `fleet_platform_mesh_io`).

### End-to-end demo flow

1. Apply the manifests as in *Deployment layout* above.
2. In the portal marketplace, **install** the *Cascading RBAC (Hackathon)*
   provider into a consumer account — this creates an APIBinding to
   `fleet.platform-mesh.io` and activates that account's **Cascades** view.
3. Open the account's **Cascades** view and **Create** a Cascade (e.g. referencing
   the `hackathon-test-role` ClusterRole, `maxDepth: 1`).
4. With the operator running, it cascades the ClusterRole into descendant
   workspaces; open the **Cluster Roles** view there to see the copy.

## Support, Feedback, Contributing

This project is open to feature requests/suggestions, bug reports etc. via [GitHub issues](https://github.com/platform-mesh/<your-project>/issues). Contribution and feedback are encouraged and always welcome. For more information about how to contribute, the project structure, as well as additional contribution information, see our [Contribution Guidelines](CONTRIBUTING.md).

## Security / Disclosure

If you find any bug that may be a security problem, please follow our instructions at [in our security policy](https://github.com/platform-mesh/<your-project>/security/policy) on how to report it. Please do not create GitHub issues for security-related doubts or problems.

## Code of Conduct

Please refer to our [Code of Conduct](https://github.com/platform-mesh/.github/blob/main/CODE_OF_CONDUCT.md) for information on the expected conduct for contributing to Platform Mesh.

<p align="center"><img alt="Bundesministerium für Wirtschaft und Energie (BMWE)-EU funding logo" src="https://apeirora.eu/assets/img/BMWK-EU.png" width="400"/></p>
