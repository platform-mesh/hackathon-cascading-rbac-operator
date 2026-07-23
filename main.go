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
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-logr/logr"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	// "root:subworkspace/tenancy.kcp.io".
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

		// because we pass in the cascadeMgr, we can just create a client on the cascade
		// logicalcluster to patch cascade and trigger the other reconcile loop
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

		// Log the currently affected child workspaces down to maxDepth levels.
		logAffectedChildren(ctx, log, adminCfg, path, string(req.ClusterName), int(maxDepth))

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
// its hash or full path
func scopedClient(baseCfg *rest.Config, path string) (client.Client, error) {
	cfg := rest.CopyConfig(baseCfg)
	if idx := strings.Index(cfg.Host, "/clusters/"); idx != -1 {
		cfg.Host = cfg.Host[:idx]
	}
	cfg.Host += logicalcluster.NewPath(path).RequestPath()
	return client.New(cfg, client.Options{Scheme: scheme.Scheme})
}

// logAffectedChildren walks the workspace subtree beneath the cascade's path,
// down to maxDepth levels, logging each descendant's path and name. Each level
// is listed through a client built from adminCfg scoped to that logical cluster,
// because the APIExport-scoped provider clients do not serve Workspace objects.
func logAffectedChildren(ctx context.Context, log logr.Logger, adminCfg *rest.Config, rootPath, rootHash string, maxDepth int) {
	type level struct {
		path  string
		hash  string
		depth int
	}

	queue := []level{{path: rootPath, hash: rootHash, depth: 0}}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]

		cl, err := scopedClient(adminCfg, cur.hash)
		if err != nil {
			log.Info("Failed to build client for logical cluster", "path", cur.path, "err", err.Error())
			continue
		}
		children := &tenancyv1alpha1.WorkspaceList{}
		if err := cl.List(ctx, children); err != nil {
			log.Info("Failed to list child workspaces", "path", cur.path, "err", err.Error())
			continue
		}
		for i := range children.Items {
			ws := &children.Items[i]
			childPath := logicalcluster.NewPath(cur.path).Join(ws.Name).String()
			log.Info("Affected child workspace", "cascadePath", rootPath, "childPath", childPath, "workspace", ws.Name)

			if cur.depth+1 >= maxDepth || ws.Spec.Cluster == "" {
				continue
			}
			queue = append(queue, level{path: childPath, hash: ws.Spec.Cluster, depth: cur.depth + 1})
		}
	}
}
