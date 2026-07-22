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

	"golang.org/x/sync/errgroup"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mcbuilder "sigs.k8s.io/multicluster-runtime/pkg/builder"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	apisv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	corev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	tenancyv1alpha1 "github.com/kcp-dev/sdk/apis/tenancy/v1alpha1"

	fleetv1alpha1 "github.com/platform-mesh/hackathon-cascading-rbac-operator/apis/fleet/v1alpha1"

	"github.com/kcp-dev/multicluster-provider/apiexport"
)

// tenancyEndpointSliceName is the name of the APIExportEndpointSlice for the tenancy
// API. It lives in the root workspace and is always called "tenancy.kcp.io".
const tenancyEndpointSliceName = "tenancy.kcp.io"

// fleetEndpointSliceName is the name of the APIExportEndpointSlice for the fleet
// API. It lives in the root workspace and is always called "fleet.platform-mesh.io".
const fleetEndpointSliceName = "fleet.platform-mesh.io"

func init() {
	runtime.Must(tenancyv1alpha1.AddToScheme(scheme.Scheme))
	runtime.Must(apisv1alpha1.AddToScheme(scheme.Scheme))
	runtime.Must(fleetv1alpha1.AddToScheme(scheme.Scheme))
}

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

	cfg := ctrl.GetConfigOrDie()

	// Setup the kcp apiexport provider. This makes workspaces available as
	// clusters to the multicluster manager but does not engage them yet.
	entryLog.Info("Setting up provider", "endpointslice", tenancyEndpointSliceName)
	provider, err := apiexport.New(cfg, tenancyEndpointSliceName, apiexport.Options{})
	if err != nil {
		return fmt.Errorf("unable to construct cluster provider: %w", err)
	}

	// Setup the kcp apiexport provider. This makes workspaces available as
	// clusters to the multicluster manager but does not engage them yet.
	entryLog.Info("Setting up provider", "endpointslice", fleetEndpointSliceName)
	fleetProvider, err := apiexport.New(cfg, fleetEndpointSliceName, apiexport.Options{})
	if err != nil {
		return fmt.Errorf("unable to construct cluster provider: %w", err)
	}

	entryLog.Info("Setting up manager for workspace controller")
	mgr, err := mcmanager.New(cfg, provider, manager.Options{})
	if err != nil {
		return fmt.Errorf("unable to set up overall controller manager: %w", err)
	}

	entryLog.Info("Setting up manager for cascade controller")
	// Distinct metrics bind address: both managers run concurrently and the
	// default (:8080) would otherwise collide with mgr's metrics server.
	fleetMgr, err := mcmanager.New(cfg, fleetProvider, manager.Options{
		Metrics: metricsserver.Options{BindAddress: ":8081"},
	})
	if err != nil {
		return fmt.Errorf("unable to set up fleet controller manager: %w", err)
	}

	// Setup the controllers
	if err := mcbuilder.ControllerManagedBy(mgr).
		Named("kcp-workspace-controller").
		For(&tenancyv1alpha1.Workspace{}).
		Complete(mcreconcile.Func(reconcileWorkspace(mgr))); err != nil {
		return fmt.Errorf("failed to build controller: %w", err)
	}

	if err := mcbuilder.ControllerManagedBy(fleetMgr).
		Named("kcp-fleet-controller").
		For(&fleetv1alpha1.Cascade{}).
		Complete(mcreconcile.Func(reconcileCascade(fleetMgr, cfg))); err != nil {
		return fmt.Errorf("failed to build cascade controller: %w", err)
	}

	// Manager.Start blocks until the context is cancelled, so the two managers
	// must run concurrently. errgroup.WithContext ties them together: if either
	// manager stops (error or shutdown), the derived context cancels the other.
	entryLog.Info("Starting managers")
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := mgr.Start(ctx); err != nil {
			return fmt.Errorf("unable to run manager: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		if err := fleetMgr.Start(ctx); err != nil {
			return fmt.Errorf("unable to run fleet manager: %w", err)
		}
		return nil
	})

	return g.Wait()
}

func reconcileWorkspace(mgr mcmanager.Manager) mcreconcile.Func {
	return func(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
		log := log.FromContext(ctx).WithValues("cluster", req.ClusterName, "name", req.Name)

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

		log.Info("Workspace present", "phase", ws.Status.Phase)
		return reconcile.Result{}, nil
	}
}
func reconcileCascade(mgr mcmanager.Manager, adminCfg *rest.Config) mcreconcile.Func {
	return func(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
		log := log.FromContext(ctx).WithValues("cluster", req.ClusterName, "name", req.Name)

		cl, err := mgr.GetCluster(ctx, req.ClusterName)
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("failed to get cluster: %w", err)
		}

		cascade := &fleetv1alpha1.Cascade{}
		if err := cl.GetClient().Get(ctx, req.NamespacedName, cascade); err != nil {
			if apierrors.IsNotFound(err) {
				log.Info("Cascade deleted")
				return reconcile.Result{}, nil
			}
			return reconcile.Result{}, fmt.Errorf("failed to get cascade: %w", err)
		}

		// The referenced resource is an arbitrary GVK that cannot be exposed
		// through the fleet APIExport virtual workspace (it cannot be added as a
		// permissionClaim), so it is read directly from the Cascade's own kcp
		// workspace using the admin kubeconfig the binary was started with.
		wsClient, err := clusterClient(adminCfg, string(req.ClusterName))
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("failed to build workspace client: %w", err)
		}

		gvk := schema.GroupVersionKind{
			Group:   cascade.Spec.GVK.Group,
			Version: cascade.Spec.GVK.Version,
			Kind:    cascade.Spec.GVK.Kind,
		}
		referenced := &unstructured.Unstructured{}
		referenced.SetGroupVersionKind(gvk)
		key := client.ObjectKey{Namespace: cascade.Spec.Namespace, Name: cascade.Spec.Name}
		if err := wsClient.Get(ctx, key, referenced); err != nil {
			if apierrors.IsNotFound(err) {
				log.Info("Referenced resource not found", "gvk", gvk, "key", key)
				return reconcile.Result{}, nil
			}
			return reconcile.Result{}, fmt.Errorf("failed to get referenced resource %s %s: %w", gvk, key, err)
		}

		log.Info("Referenced resource found",
			"gvk", gvk,
			"name", referenced.GetName(),
			"namespace", referenced.GetNamespace(),
			"uid", referenced.GetUID(),
			"resourceVersion", referenced.GetResourceVersion(),
		)

		// Cascade the referenced object into descendant workspaces, down to
		// spec.MaxDepth levels below this (the source) workspace.
		if cascade.Spec.MaxDepth < 1 {
			log.Info("maxDepth < 1, nothing to cascade", "maxDepth", cascade.Spec.MaxDepth)
			return reconcile.Result{}, nil
		}

		template := sanitizeForApply(referenced, cascade.Name)
		if err := cascadeObject(ctx, adminCfg, string(req.ClusterName), template, 0, cascade.Spec.MaxDepth); err != nil {
			return reconcile.Result{}, fmt.Errorf("failed to cascade object: %w", err)
		}

		return reconcile.Result{}, nil
	}
}

// baseHost returns the kcp base URL from a possibly workspace-scoped host by
// stripping any trailing "/clusters/<name>" segment, so that a specific
// workspace path can be appended.
func baseHost(host string) string {
	if i := strings.Index(host, "/clusters/"); i != -1 {
		return host[:i]
	}
	return host
}

const (
	// fieldManager is the server-side-apply field owner used when writing
	// cascaded copies into descendant workspaces.
	fieldManager = "cascade-operator"

	// cascadeOwnerLabel is stamped on every cascaded copy so the copies can be
	// identified (and later cleaned up) by the Cascade that produced them.
	cascadeOwnerLabel = "cascade.fleet.platform-mesh.io/owner"
)

// clusterClient returns a controller-runtime client scoped to the given kcp
// logical cluster, built from the admin config the binary was started with.
func clusterClient(adminCfg *rest.Config, clusterName string) (client.Client, error) {
	cfg := rest.CopyConfig(adminCfg)
	cfg.Host = baseHost(cfg.Host) + "/clusters/" + clusterName
	return client.New(cfg, client.Options{})
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

	c, err := clusterClient(adminCfg, clusterName)
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
		if ws.Status.Phase != corev1alpha1.LogicalClusterPhaseReady || ws.Spec.Cluster == "" {
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
