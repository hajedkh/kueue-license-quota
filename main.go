package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
)

type config struct {
	promURL      string
	clusterQueue string
	flavor       string
	resourceName string
	freeQuery    string
	headroomQuery string
	apiVersion   string
	interval     time.Duration
	staleAfter   time.Duration
	kubeconfig   string
}

type reconciler struct {
	cfg  config
	prom promv1.API
	dyn  dynamic.Interface
	gvr  schema.GroupVersionResource
	log  *slog.Logger
	last int64
}

func main() {
	var c config
	flag.StringVar(&c.promURL, "prometheus-url", "http://prometheus.monitoring:9090", "Prometheus base URL")
	flag.StringVar(&c.clusterQueue, "cluster-queue", "licensed-compute", "ClusterQueue to manage")
	flag.StringVar(&c.flavor, "flavor", "default", "ResourceFlavor name inside the ClusterQueue")
	flag.StringVar(&c.resourceName, "resource", "licenses.example.com/fep", "resource whose nominalQuota is owned by this reconciler")
	flag.StringVar(&c.freeQuery, "free-query", `license_tokens_free{feature="fep"}`, "PromQL returning free tokens on the license server")
	flag.StringVar(&c.headroomQuery, "headroom-query", `quantile_over_time(0.99, license_tokens_used_external{feature="fep"}[30d])`, "PromQL returning headroom to reserve")
	flag.StringVar(&c.apiVersion, "kueue-api-version", "v1beta1", "kueue.x-k8s.io API version served by the cluster (v1beta1 or v1beta2)")
	flag.DurationVar(&c.interval, "interval", 30*time.Second, "reconcile interval")
	flag.DurationVar(&c.staleAfter, "stale-after", 2*time.Minute, "treat Prometheus data older than this as missing (fail-safe: quota 0)")
	flag.StringVar(&c.kubeconfig, "kubeconfig", "", "path to kubeconfig (empty = in-cluster)")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	restCfg, err := loadRestConfig(c.kubeconfig)
	if err != nil {
		log.Error("kubeconfig", "err", err)
		os.Exit(1)
	}
	dyn, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		log.Error("dynamic client", "err", err)
		os.Exit(1)
	}
	pc, err := promapi.NewClient(promapi.Config{Address: c.promURL})
	if err != nil {
		log.Error("prometheus client", "err", err)
		os.Exit(1)
	}

	r := &reconciler{
		cfg:  c,
		prom: promv1.NewAPI(pc),
		dyn:  dyn,
		gvr:  schema.GroupVersionResource{Group: "kueue.x-k8s.io", Version: c.apiVersion, Resource: "clusterqueues"},
		log:  log,
		last: -1,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Info("starting", "clusterQueue", c.clusterQueue, "resource", c.resourceName, "interval", c.interval.String())
	t := time.NewTicker(c.interval)
	defer t.Stop()
	r.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			log.Info("stopping")
			return
		case <-t.C:
			r.tick(ctx)
		}
	}
}

func (r *reconciler) tick(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	free, freeOK := r.scalar(ctx, r.cfg.freeQuery)
	headroom, hrOK := r.scalar(ctx, r.cfg.headroomQuery)
	if !hrOK {
		headroom = 0
	}

	held, err := r.heldByAdmitted(ctx)
	if err != nil {
		r.log.Error("read ClusterQueue usage", "err", err)
		return
	}

	var nominal int64
	if !freeOK {
		nominal = 0
		r.log.Warn("no fresh license data, fail-safe quota 0")
	} else {
		nominal = int64(math.Max(0, math.Floor(free+held-headroom)))
	}

	if nominal == r.last {
		return
	}
	if err := r.writeQuota(ctx, nominal); err != nil {
		r.log.Error("write nominalQuota", "err", err)
		return
	}
	r.last = nominal
	r.log.Info("nominalQuota updated", "free", free, "held", held, "headroom", headroom, "nominal", nominal)
}

// scalar runs an instant query and returns the first sample if it is fresh.
func (r *reconciler) scalar(ctx context.Context, q string) (float64, bool) {
	val, _, err := r.prom.Query(ctx, q, time.Now())
	if err != nil {
		r.log.Error("prometheus query", "query", q, "err", err)
		return 0, false
	}
	vec, ok := val.(model.Vector)
	if !ok || len(vec) == 0 {
		return 0, false
	}
	s := vec[0]
	if time.Since(s.Timestamp.Time()) > r.cfg.staleAfter {
		return 0, false
	}
	if math.IsNaN(float64(s.Value)) {
		return 0, false
	}
	return float64(s.Value), true
}

// heldByAdmitted sums the resource across status.flavorsUsage of the ClusterQueue.
func (r *reconciler) heldByAdmitted(ctx context.Context) (float64, error) {
	cq, err := r.dyn.Resource(r.gvr).Get(ctx, r.cfg.clusterQueue, metav1.GetOptions{})
	if err != nil {
		return 0, err
	}
	usage, _, _ := unstructured.NestedSlice(cq.Object, "status", "flavorsUsage")
	var total float64
	for _, f := range usage {
		fm, _ := f.(map[string]interface{})
		res, _, _ := unstructured.NestedSlice(fm, "resources")
		for _, x := range res {
			xm, _ := x.(map[string]interface{})
			if xm["name"] != r.cfg.resourceName {
				continue
			}
			if s, ok := xm["total"].(string); ok {
				if q, err := resource.ParseQuantity(s); err == nil {
					total += q.AsApproximateFloat64()
				}
			}
		}
	}
	return total, nil
}

// writeQuota updates only nominalQuota of the managed resource, with conflict retry.
func (r *reconciler) writeQuota(ctx context.Context, nominal int64) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		cq, err := r.dyn.Resource(r.gvr).Get(ctx, r.cfg.clusterQueue, metav1.GetOptions{})
		if err != nil {
			return err
		}
		groups, found, _ := unstructured.NestedSlice(cq.Object, "spec", "resourceGroups")
		if !found {
			return fmt.Errorf("ClusterQueue %s has no spec.resourceGroups", r.cfg.clusterQueue)
		}
		changed := false
		for gi, g := range groups {
			gm, _ := g.(map[string]interface{})
			flavors, _, _ := unstructured.NestedSlice(gm, "flavors")
			for fi, f := range flavors {
				fm, _ := f.(map[string]interface{})
				if fm["name"] != r.cfg.flavor {
					continue
				}
				res, _, _ := unstructured.NestedSlice(fm, "resources")
				for ri, x := range res {
					xm, _ := x.(map[string]interface{})
					if xm["name"] != r.cfg.resourceName {
						continue
					}
					xm["nominalQuota"] = fmt.Sprintf("%d", nominal)
					res[ri] = xm
					changed = true
				}
				fm["resources"] = res
				flavors[fi] = fm
			}
			gm["flavors"] = flavors
			groups[gi] = gm
		}
		if !changed {
			return fmt.Errorf("resource %s in flavor %s not found in ClusterQueue %s", r.cfg.resourceName, r.cfg.flavor, r.cfg.clusterQueue)
		}
		if err := unstructured.SetNestedSlice(cq.Object, groups, "spec", "resourceGroups"); err != nil {
			return err
		}
		_, err = r.dyn.Resource(r.gvr).Update(ctx, cq, metav1.UpdateOptions{FieldManager: "license-quota-reconciler"})
		if apierrors.IsConflict(err) {
			return err
		}
		return err
	})
}

func loadRestConfig(path string) (*rest.Config, error) {
	if path != "" {
		return clientcmd.BuildConfigFromFlags("", path)
	}
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	return clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
}
