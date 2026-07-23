/*
Copyright The Platform Mesh Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mcbuilder "sigs.k8s.io/multicluster-runtime/pkg/builder"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"

	"github.com/kcp-dev/logicalcluster/v3"
	apisv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	"github.com/kcp-dev/sdk/apis/core"
	corev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	tenancyv1alpha1 "github.com/kcp-dev/sdk/apis/tenancy/v1alpha1"

	fleetv1alpha1 "github.com/platform-mesh/hackathon-cascading-rbac-operator/apis/fleet/v1alpha1"
	"github.com/platform-mesh/hackathon-cascading-rbac-operator/cascade"

	"github.com/kcp-dev/multicluster-provider/apiexport"
)

func init() {
	runtime.Must(tenancyv1alpha1.AddToScheme(scheme.Scheme))
	runtime.Must(apisv1alpha1.AddToScheme(scheme.Scheme))
	runtime.Must(corev1alpha1.AddToScheme(scheme.Scheme))
	runtime.Must(fleetv1alpha1.AddToScheme(scheme.Scheme))
}

// reconcileRequestedAtAnnotation is touched on a Cascade to trigger a reconcile
// via the api-server when a covered workspace is created.
const reconcileRequestedAtAnnotation = "fleet.platform-mesh.io/reconcile-requested-at"

const (
	// fieldManager is the server-side-apply field owner used when writing
	// cascaded copies into descendant workspaces.
	fieldManager = "cascade-operator"

	// cascadeOwnerLabel is stamped on every cascaded copy so the copies can be
	// identified (and later cleaned up) by the Cascade that produced them.
	cascadeOwnerLabel = "cascade.fleet.platform-mesh.io/owner"
)

func main() {
	if err := run(); err != nil {
		fmt.Printf("Error running manager: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	log.SetLogger(zap.New(zap.UseDevMode(false)))

	ctx := signals.SetupSignalHandler()
	entryLog := log.Log.WithName("entrypoint")

	// The tenancy API (workspaces) and the fleet API (cascades) are served by
	// separate APIExports, potentially residing in different workspaces. Each
	// flag has the form "<workspace-path>/<endpointslice-name>", e.g.
	// "root:orgs:e2e:cascade-provider/fleet.platform-mesh.io".
	var tenancyEndpoint, cascadeEndpoint string
	pflag.StringVar(&tenancyEndpoint, "tenancy-endpoint", "root/tenancy.kcp.io", "APIExportEndpointSlice for the tenancy API (workspaces), as <workspace-path>/<endpointslice-name>")
	pflag.StringVar(&cascadeEndpoint, "cascade-endpoint", "root/fleet.platform-mesh.io", "APIExportEndpointSlice for the fleet API (cascades), as <workspace-path>/<endpointslice-name>")
	pflag.Parse()

	cfg := ctrl.GetConfigOrDie()

	tenancyCfg, tenancySlice, err := configForEndpoint(cfg, tenancyEndpoint)
	if err != nil {
		return fmt.Errorf("invalid --tenancy-endpoint: %w", err)
	}
	entryLog.Info("Setting up tenancy provider", "endpoint", tenancyEndpoint)
	tenancyProvider, err := apiexport.New(tenancyCfg, tenancySlice, apiexport.Options{})
	if err != nil {
		return fmt.Errorf("unable to construct tenancy cluster provider: %w", err)
	}
	wsMgr, err := mcmanager.New(cfg, tenancyProvider, manager.Options{})
	if err != nil {
		return fmt.Errorf("unable to set up tenancy manager: %w", err)
	}

	cascadeCfg, cascadeSlice, err := configForEndpoint(cfg, cascadeEndpoint)
	if err != nil {
		return fmt.Errorf("invalid --cascade-endpoint: %w", err)
	}
	entryLog.Info("Setting up cascade provider", "endpoint", cascadeEndpoint)
	cascadeProvider, err := apiexport.New(cascadeCfg, cascadeSlice, apiexport.Options{})
	if err != nil {
		return fmt.Errorf("unable to construct cascade cluster provider: %w", err)
	}
	cascadeMgr, err := mcmanager.New(cfg, cascadeProvider, manager.Options{
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		return fmt.Errorf("unable to set up cascade manager: %w", err)
	}

	// cache records each Cascade's reach (its own workspace path + maxDepth). It
	// is populated by the cascade reconciler and consulted by the workspace
	// reconciler to decide whether a newly created workspace is covered by a
	// cascade living in one of its ancestors.
	cache := cascade.NewCache()

	if err := mcbuilder.ControllerManagedBy(wsMgr).
		Named("kcp-workspace-controller").
		For(&tenancyv1alpha1.Workspace{}).
		WithEventFilter(predicate.Funcs{
			// only trigger on workspace creation
			CreateFunc:  func(event.CreateEvent) bool { return true },
			UpdateFunc:  func(event.UpdateEvent) bool { return false },
			DeleteFunc:  func(event.DeleteEvent) bool { return false },
			GenericFunc: func(event.GenericEvent) bool { return false },
		}).
		Complete(mcreconcile.Func(reconcileWorkspace(wsMgr, cascadeMgr, cache))); err != nil {
		return fmt.Errorf("failed to build workspace controller: %w", err)
	}

	if err := mcbuilder.ControllerManagedBy(cascadeMgr).
		Named("cascade-controller").
		For(&fleetv1alpha1.Cascade{}).
		Complete(mcreconcile.Func(reconcileCascade(cascadeMgr, cache, cfg))); err != nil {
		return fmt.Errorf("failed to build cascade controller: %w", err)
	}

	// Run both managers concurrently; if either stops, the shared context is
	// cancelled and the other stops too.
	entryLog.Info("Starting managers")
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return wsMgr.Start(ctx) })
	g.Go(func() error { return cascadeMgr.Start(ctx) })
	if err := g.Wait(); err != nil {
		return fmt.Errorf("unable to run managers: %w", err)
	}

	return nil
}

// reconcileWorkspace triggers the covering Cascade(s) whenever a workspace is
// created. It resolves the new workspace's path, asks the cache which Cascades
// reach it, and patches each of those Cascades so their reconciler re-runs and
// (re-)applies the cascaded object into the new workspace.
func reconcileWorkspace(mgr mcmanager.Manager, cascadeMgr mcmanager.Manager, cache *cascade.Cache) mcreconcile.Func {
	return func(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
		log := log.FromContext(ctx).WithValues("cluster", req.ClusterName, "workspace", req.Name)

		cl, err := mgr.GetCluster(ctx, req.ClusterName)
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("failed to get cluster: %w", err)
		}

		ws := &tenancyv1alpha1.Workspace{}
		if err := cl.GetClient().Get(ctx, req.NamespacedName, ws); err != nil {
			if apierrors.IsNotFound(err) {
				log.Info("Workspace deleted")
				return reconcile.Result{}, nil
			}
			return reconcile.Result{}, fmt.Errorf("failed to get workspace: %w", err)
		}

		if !ws.DeletionTimestamp.IsZero() {
			log.Info("Workspace deleting")
			return reconcile.Result{}, nil
		}

		// A freshly created workspace is not scheduled onto a shard yet, so it
		// cannot be cascaded into and triggering the covering Cascade now would be
		// a no-op that never re-fires (the controller only watches creation).
		// Requeue until the workspace is ready, then trigger.
		if !workspaceReady(ws) {
			log.Info("Workspace not ready yet, requeueing", "phase", ws.Status.Phase)
			return reconcile.Result{}, fmt.Errorf("workspace %q not ready yet (phase %q, cluster %q)", ws.Name, ws.Status.Phase, ws.Spec.Cluster)
		}

		// The workspace's own path is embedded in its spec.URL
		// (".../clusters/<path>"), so we read it directly instead of making an
		// extra call to resolve the parent's path from its LogicalCluster.
		childPath, err := fullClusterPathFromURL(ws.Spec.URL)
		if err != nil {
			log.Info("Workspace URL not populated yet, requeueing", "err", err.Error())
			return reconcile.Result{}, err
		}

		log.Info("Workspace present", "path", childPath, "phase", ws.Status.Phase)

		matches := cache.Match(childPath)
		if len(matches) == 0 {
			log.Info("No cascades cover this workspace", "path", childPath)
			return reconcile.Result{}, nil
		}

		// Trigger each covering Cascade's reconciler by patching a timestamp
		// annotation. We patch on the cascade's own logical cluster, reached via
		// the cascade manager's per-cluster client.
		for _, e := range matches {
			cascadeCl, err := cascadeMgr.GetCluster(ctx, e.Hash)
			if err != nil {
				log.Info("Cascade cluster not engaged, skipping trigger", "cascade", e.Name, "cascadeCluster", e.Hash, "err", err.Error())
				continue
			}
			c := &fleetv1alpha1.Cascade{}
			if err := cascadeCl.GetClient().Get(ctx, types.NamespacedName{Name: e.Name}, c); err != nil {
				if apierrors.IsNotFound(err) {
					cache.Delete(e.Hash, e.Name)
					continue
				}
				log.Info("Failed to get cascade for trigger", "cascade", e.Name, "err", err.Error())
				continue
			}
			patch := client.MergeFrom(c.DeepCopy())
			if c.Annotations == nil {
				c.Annotations = map[string]string{}
			}
			c.Annotations[reconcileRequestedAtAnnotation] = time.Now().UTC().Format(time.RFC3339Nano)
			if err := cascadeCl.GetClient().Patch(ctx, c, patch); err != nil {
				log.Info("Failed to patch cascade", "cascade", e.Name, "err", err.Error())
				continue
			}
			log.Info("Triggered cascade through workspace", "cascade", e.Name, "cascadePath", e.Path, "path", childPath)
		}

		return reconcile.Result{}, nil
	}
}

// reconcileCascade records the Cascade's reach in the cache and cascades the
// referenced object into descendant workspaces down to spec.MaxDepth levels via
// server-side apply.
func reconcileCascade(mgr mcmanager.Manager, cache *cascade.Cache, adminCfg *rest.Config) mcreconcile.Func {
	return func(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
		log := log.FromContext(ctx).WithValues("cluster", req.ClusterName, "cascade", req.NamespacedName)

		cl, err := mgr.GetCluster(ctx, req.ClusterName)
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("failed to get cluster: %w", err)
		}

		casc := &fleetv1alpha1.Cascade{}
		if err := cl.GetClient().Get(ctx, req.NamespacedName, casc); err != nil {
			if apierrors.IsNotFound(err) {
				log.Info("Cascade deleted")
				cache.Delete(req.ClusterName, req.Name)
				return reconcile.Result{}, nil
			}
			return reconcile.Result{}, fmt.Errorf("failed to get cascade: %w", err)
		}

		if !casc.DeletionTimestamp.IsZero() {
			log.Info("Cascade deleting", "cascade", casc.Name)
			cache.Delete(req.ClusterName, req.Name)
			return reconcile.Result{}, nil
		}

		path, err := fullClusterPathFromHash(ctx, adminCfg, string(req.ClusterName))
		if err != nil {
			log.Info("Cascade path not resolvable yet, requeueing", "cascade", casc.Name, "err", err.Error())
			return reconcile.Result{}, err
		}

		maxDepth := max(casc.Spec.MaxDepth, 1)

		cache.Upsert(cascade.Entry{
			Hash:     req.ClusterName,
			Name:     casc.Name,
			Path:     path,
			MaxDepth: maxDepth,
		})

		log.Info("Reconciling cascade", "cascade", casc.Name, "path", path, "maxDepth", maxDepth, "gvk", casc.Spec.GVK, "target", casc.Spec.Name)

		// The referenced resource is an arbitrary GVK that cannot be exposed
		// through the fleet APIExport virtual workspace (it cannot be added as a
		// permissionClaim), so it is read directly from the Cascade's own kcp
		// workspace using the admin kubeconfig the binary was started with.
		srcClient, err := scopedClient(adminCfg, string(req.ClusterName))
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("failed to build source workspace client: %w", err)
		}

		gvk := schema.GroupVersionKind{
			Group:   casc.Spec.GVK.Group,
			Version: casc.Spec.GVK.Version,
			Kind:    casc.Spec.GVK.Kind,
		}
		referenced := &unstructured.Unstructured{}
		referenced.SetGroupVersionKind(gvk)
		key := client.ObjectKey{Namespace: casc.Spec.Namespace, Name: casc.Spec.Name}
		if err := srcClient.Get(ctx, key, referenced); err != nil {
			if apierrors.IsNotFound(err) {
				log.Info("Referenced resource not found", "gvk", gvk, "key", key)
				return reconcile.Result{}, nil
			}
			return reconcile.Result{}, fmt.Errorf("failed to get referenced resource %s %s: %w", gvk, key, err)
		}

		// Cascade the referenced object into descendant workspaces, down to
		// maxDepth levels below this (the source) workspace.
		template := sanitizeForApply(referenced, casc.Name)
		if err := cascadeObject(ctx, adminCfg, string(req.ClusterName), template, 0, maxDepth); err != nil {
			return reconcile.Result{}, fmt.Errorf("failed to cascade object: %w", err)
		}

		return reconcile.Result{}, nil
	}
}

// configForEndpoint parses an endpoint flag of the form
// "<workspace-path>/<endpointslice-name>" and returns a rest.Config scoped to
// that workspace path together with the endpointslice name. If no "/" is
// present the whole value is treated as the endpointslice name and the base
// config is used unscoped.
func configForEndpoint(base *rest.Config, endpoint string) (*rest.Config, string, error) {
	path, sliceName, found := strings.Cut(endpoint, "/")
	if !found {
		return base, endpoint, nil
	}
	if path == "" || sliceName == "" {
		return nil, "", fmt.Errorf("expected <workspace-path>/<endpointslice-name>, got %q", endpoint)
	}

	cfg := rest.CopyConfig(base)
	if idx := strings.Index(cfg.Host, "/clusters/"); idx != -1 {
		cfg.Host = cfg.Host[:idx]
	}
	cfg.Host += logicalcluster.NewPath(path).RequestPath()
	return cfg, sliceName, nil
}

// fullClusterPathFromURL calculates the full workspace path from a workspace url.
// This is a quick and dirty hack, so we don't have to look up the logicalcluster
func fullClusterPathFromURL(url string) (string, error) {
	_, path, found := strings.Cut(url, "/clusters/")
	if !found || path == "" {
		return "", fmt.Errorf("no /clusters/ segment in workspace URL %q", url)
	}
	if i := strings.IndexByte(path, '/'); i != -1 {
		path = path[:i]
	}
	return path, nil
}

// fullClusterPathFromHash calculates the full workspace path from a logical cluster hash
func fullClusterPathFromHash(ctx context.Context, adminCfg *rest.Config, hash string) (string, error) {
	cl, err := scopedClient(adminCfg, hash)
	if err != nil {
		return "", fmt.Errorf("failed to build client for logical cluster %q: %w", hash, err)
	}
	lc := &corev1alpha1.LogicalCluster{}
	if err := cl.Get(ctx, types.NamespacedName{Name: corev1alpha1.LogicalClusterName}, lc); err != nil {
		return "", fmt.Errorf("failed to get LogicalCluster singleton: %w", err)
	}
	if p := lc.Annotations[core.LogicalClusterPathAnnotationKey]; p != "" {
		return p, nil
	}
	// The root logical cluster carries no kcp.io/path annotation because its
	// cluster name ("root") is already its path. Any other cluster without the
	// annotation only has a hash, which is not a usable path yet, so requeue.
	if name := logicalcluster.From(lc); name == core.RootCluster {
		return name.Path().String(), nil
	}
	return "", fmt.Errorf("LogicalCluster %q has no %s annotation yet", logicalcluster.From(lc), core.LogicalClusterPathAnnotationKey)
}

// scopedClient builds a client against a specific logical cluster, identified by
// its hash or full path.
func scopedClient(baseCfg *rest.Config, path string) (client.Client, error) {
	cfg := rest.CopyConfig(baseCfg)
	if idx := strings.Index(cfg.Host, "/clusters/"); idx != -1 {
		cfg.Host = cfg.Host[:idx]
	}
	cfg.Host += logicalcluster.NewPath(path).RequestPath()
	return client.New(cfg, client.Options{Scheme: scheme.Scheme})
}

// sanitizeForApply returns a copy of obj suitable for server-side applying into
// another workspace: identity and spec/data are kept, but server-populated and
// source-cluster-specific fields are dropped, and the ownership label is set.
func sanitizeForApply(obj *unstructured.Unstructured, owner string) *unstructured.Unstructured {
	desired := obj.DeepCopy()

	for _, f := range [][]string{
		{"metadata", "resourceVersion"},
		{"metadata", "uid"},
		{"metadata", "generation"},
		{"metadata", "creationTimestamp"},
		{"metadata", "managedFields"},
		{"metadata", "ownerReferences"},
		{"metadata", "selfLink"},
		{"status"},
	} {
		unstructured.RemoveNestedField(desired.Object, f...)
	}

	// Drop the source workspace's own annotations that must not be copied.
	annotations := desired.GetAnnotations()
	delete(annotations, "kcp.io/cluster")
	delete(annotations, "kubectl.kubernetes.io/last-applied-configuration")
	if len(annotations) == 0 {
		annotations = nil
	}
	desired.SetAnnotations(annotations)

	labels := desired.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[cascadeOwnerLabel] = owner
	desired.SetLabels(labels)

	return desired
}

// workspaceReady reports whether a workspace has been scheduled onto a shard and
// is therefore safe to act on: its logical cluster (spec.cluster) is assigned and
// its phase is Ready. Both the workspace reconciler (before triggering a Cascade)
// and the cascade walk (before applying into a descendant) gate on this.
func workspaceReady(ws *tenancyv1alpha1.Workspace) bool {
	return ws.Status.Phase == corev1alpha1.LogicalClusterPhaseReady && ws.Spec.Cluster != ""
}

// cascadeObject walks the descendant workspaces of clusterName depth-first and
// server-side-applies template into each one, down to maxDepth levels below the
// Cascade's own workspace. depth is the number of levels already descended
// (0 == the Cascade's workspace, which is the source and is not written to).
//
// It is best-effort: child workspaces that are not Ready are skipped, and errors
// are collected rather than aborting the walk, so one broken branch does not
// stop the others.
func cascadeObject(ctx context.Context, adminCfg *rest.Config, clusterName string, template *unstructured.Unstructured, depth, maxDepth int32) error {
	logger := log.FromContext(ctx)

	c, err := scopedClient(adminCfg, clusterName)
	if err != nil {
		return fmt.Errorf("failed to build client for cluster %q: %w", clusterName, err)
	}

	// Apply into this workspace, unless it is the source (depth 0).
	if depth > 0 {
		if err := applyObject(ctx, c, template); err != nil {
			return err
		}
	}

	// Stop descending once the deepest requested level has been applied.
	if depth >= maxDepth {
		return nil
	}

	workspaces := &tenancyv1alpha1.WorkspaceList{}
	if err := c.List(ctx, workspaces); err != nil {
		return fmt.Errorf("failed to list workspaces in cluster %q: %w", clusterName, err)
	}

	var errs []error
	for i := range workspaces.Items {
		ws := &workspaces.Items[i]
		if !workspaceReady(ws) {
			logger.Info("Skipping workspace that is not ready", "workspace", ws.Name, "phase", ws.Status.Phase)
			continue
		}

		childCtx := log.IntoContext(ctx, logger.WithValues("targetCluster", ws.Spec.Cluster, "workspace", ws.Name, "depth", depth+1))
		if err := cascadeObject(childCtx, adminCfg, ws.Spec.Cluster, template, depth+1, maxDepth); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// applyObject server-side-applies a fresh copy of template using the cascade
// field manager.
func applyObject(ctx context.Context, c client.Client, template *unstructured.Unstructured) error {
	desired := template.DeepCopy()
	ac := client.ApplyConfigurationFromUnstructured(desired)
	if err := c.Apply(ctx, ac, client.FieldOwner(fieldManager), client.ForceOwnership); err != nil {
		return fmt.Errorf("failed to apply %s %q: %w", desired.GetKind(), desired.GetName(), err)
	}

	log.FromContext(ctx).Info("Cascaded object applied", "kind", desired.GetKind(), "name", desired.GetName())
	return nil
}
