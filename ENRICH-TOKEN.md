# Configure Keycloak to add group memberships

This guide covers how to obtain a JWT from keycloak which contains the
groups claim with users group memberships.

## Configuring KeyCloak

For now there is no predefined scope in keycloak to map to the users groups.
Therefore one needs to do the following:

* Login to keycloak master realm using the keycloak-admin credentials
* switching the realm to the organization (e.g. "e2e")
* Manage -> Client Scopes -> Create client scope
  * Settings:
    * Name: "groups"
    * Description: "OpenID Connect scope for group memberships to the token"
    * Type: "default"
    * Protocol: "OpenID Connect"
    * Include in token scope: "On"
    * Include in OpenID Provider Metadata: "On"
  * Mappers:
    * Add mapper -> by Configuration -> "Group Membership"
      * Name: "groups"
      * Token Claim Name: "groups"
      * Full group path: "On"
      * Add to ID token: "On"
      * Add to access token: "On"
      * keep the rest default (doesn't matter)
* Manage -> Clients
  * Select the client with name "kubectl" (that is the one that gets added in the downloadable kubeconfig)
    * Client scopes -> Add client scope
      * Select newly created "groups" and keep Assigned type "Default"

## Obtaining the kubeconfig

To obtain the kubeconfig with the oidc-login flow one can just go to the Platform Mesh portal, select/create an account and then press "Download kubeconfig" in the dashboard

## Obtaining the JWT

To now obtain the token containing the groups claim one just have to use the downloaded kubeconfig with the oidc-login plugin.

```
# Validate you have the plugin for kubectl - look for "kubectl-oidc_login"
kubectl plugin list

# Login using the downloaded kubeconfig
KUBECONFIG=<path to kubeconfig> kubectl api-resources

# View the JWT
cat ~/.kube/cache/oidc-login/<tokenfile> | jq

# You can now decode it using b64 --decode or an online decoder like jwt.io
cat ~/.kube/cache/oidc-login/<tokenfile> | jq -r '.id_token | split(".")[1] | @base64d' | jq
```

## NOTE about WorkspaceAuthenticationConfiguration

Platform Mesh does create a WorkspaceAuthenticationConfiguration resource for every organization since it is using workspace-based authentication to support multiple realms.
The default configuration does look as the following:

```yaml
...
spec:
  jwt:
  - claimMappings:
      groups:
        claim: groups
        prefix: ""
      uid: {}
      username:
        expression: claims.email
    claimValidationRules:
    - expression: claims.?email_verified.orValue(true) == true || claims.?email_verified.orValue(true)
        == false
      message: Allowing both verified and unverified emails
...
```

So we have to note that it will respect the `groups` claim **BUT will remove the default ```oidc:``` prefix** and just spit out the groups - we need to respect that for our `ClusterRoleBindings`.
