# Hackathon Cascading RBAC Operator

This repository contains content from a Platform Mesh Hackathon. It is not production ready. Consider it a proof of concept.

## Running the operator

With `KUBECONFIG` or `--kubeconfig` pointing to root workspace. So for example

```sh
kcp start
export KUBECONFIG=".kcp/admin.kubeconfig"

go run main.go
```

## Creating CRDs & ARS

```sh
# Generate DeepCopy
go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.21.0 object:headerFile=./hack/boilerplate/boilerplate.go.txt paths=./apis/cascade/v1alpha1

# Generate CRDs
go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.21.0 crd:headerFile=./hack/boilerplate/boilerplate.yaml.txt paths=./apis/cascade/... output:crd:artifacts:config=config/crd/bases

# Generate ARS
go run github.com/kcp-dev/sdk/cmd/apigen@v0.32.3 --input-dir ./config/crd/bases --output-dir ./config/resources --header-file ./hack/boilerplate/boilerplate.yaml.txt
```

## Support, Feedback, Contributing

This project is open to feature requests/suggestions, bug reports etc. via [GitHub issues](https://github.com/platform-mesh/<your-project>/issues). Contribution and feedback are encouraged and always welcome. For more information about how to contribute, the project structure, as well as additional contribution information, see our [Contribution Guidelines](CONTRIBUTING.md).

## Security / Disclosure

If you find any bug that may be a security problem, please follow our instructions at [in our security policy](https://github.com/platform-mesh/<your-project>/security/policy) on how to report it. Please do not create GitHub issues for security-related doubts or problems.

## Code of Conduct

Please refer to our [Code of Conduct](https://github.com/platform-mesh/.github/blob/main/CODE_OF_CONDUCT.md) for information on the expected conduct for contributing to Platform Mesh.

<p align="center"><img alt="Bundesministerium für Wirtschaft und Energie (BMWE)-EU funding logo" src="https://apeirora.eu/assets/img/BMWK-EU.png" width="400"/></p>
