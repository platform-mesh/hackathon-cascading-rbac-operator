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

	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mcbuilder "sigs.k8s.io/multicluster-runtime/pkg/builder"
	mchandler "sigs.k8s.io/multicluster-runtime/pkg/handler"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	apisv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	tenancyv1alpha1 "github.com/kcp-dev/sdk/apis/tenancy/v1alpha1"

	"github.com/kcp-dev/multicluster-provider/apiexport"
)

// endpointSliceName is the name of the APIExportEndpointSlice for the tenancy
// API. It lives in the root workspace and is always called "tenancy.kcp.io".
const endpointSliceName = "tenancy.kcp.io"

func init() {
	runtime.Must(tenancyv1alpha1.AddToScheme(scheme.Scheme))
	runtime.Must(apisv1alpha1.AddToScheme(scheme.Scheme))
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
	entryLog.Info("Setting up provider", "endpointslice", endpointSliceName)
	provider, err := apiexport.New(cfg, endpointSliceName, apiexport.Options{})
	if err != nil {
		return fmt.Errorf("unable to construct cluster provider: %w", err)
	}

	entryLog.Info("Setting up manager")
	mgr, err := mcmanager.New(cfg, provider, manager.Options{})
	if err != nil {
		return fmt.Errorf("unable to set up overall controller manager: %w", err)
	}

	if err := mcbuilder.ControllerManagedBy(mgr).
		Named("kcp-workspace-controller").
		Watches(&tenancyv1alpha1.Workspace{}, workspaceEventHandler()).
		Complete(mcreconcile.Func(
			func(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
				return reconcile.Result{}, nil
			},
		)); err != nil {
		return fmt.Errorf("failed to build controller: %w", err)
	}

	entryLog.Info("Starting manager")
	if err := mgr.Start(ctx); err != nil {
		return fmt.Errorf("unable to run manager: %w", err)
	}

	return nil
}

// workspaceEventHandler returns a multicluster event handler that natively
// distinguishes create, update and delete events for Workspaces and logs a
// line for each.
func workspaceEventHandler() mchandler.TypedEventHandlerFunc[client.Object, mcreconcile.Request] {
	l := log.Log.WithName("kcp-workspace-controller")
	return mchandler.Lift(handler.Funcs{
		CreateFunc: func(ctx context.Context, evt event.CreateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			l.Info("Workspace created", "name", evt.Object.GetName())
		},
		UpdateFunc: func(ctx context.Context, evt event.UpdateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			l.Info("Workspace updated", "name", evt.ObjectNew.GetName())
		},
		DeleteFunc: func(ctx context.Context, evt event.DeleteEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			l.Info("Workspace deleted", "name", evt.Object.GetName())
		},
	})
}
