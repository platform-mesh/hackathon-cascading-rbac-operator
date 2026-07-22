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
		wsCfg := rest.CopyConfig(adminCfg)
		wsCfg.Host = baseHost(wsCfg.Host) + "/clusters/" + string(req.ClusterName)

		wsClient, err := client.New(wsCfg, client.Options{})
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

		// TODO: cascade the referenced object to child workspaces down to
		// spec.MaxDepth. Child workspaces are identified by Workspace objects.

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
