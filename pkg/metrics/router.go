package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	PromMetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

func newStatsRouterServiceHandler() *StatsRouterServiceHandler {
	handler := &StatsRouterServiceHandler{
		statNumReplicas: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "num_replicas_of_router",
			}, []string{"type", "namespace"},
		),
		statUpStatus: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "up_status_router",
			}, []string{"errReason", "namespace"},
		),
	}

	PromMetrics.Registry.MustRegister(handler.statNumReplicas, handler.statUpStatus)
	return handler
}

type StatsRouterServiceHandler struct {
	statNumReplicas *prometheus.GaugeVec
	statUpStatus    *prometheus.GaugeVec
}

func (t *StatsRouterServiceHandler) SetNumReplicas(numReplicas, numReadyReplicas, numUpdatedReplicas int64, namespace string) {
	t.statNumReplicas.WithLabelValues("replicas", namespace).Set(float64(numReplicas))
	t.statNumReplicas.WithLabelValues("readyReplicas", namespace).Set(float64(numReadyReplicas))
	t.statNumReplicas.WithLabelValues("updatedReplicas", namespace).Set(float64(numUpdatedReplicas))
}

func (t *StatsRouterServiceHandler) SetUpStatus(upStatus bool, errReason, namespace string) {
	hasUp := float64(0)
	if upStatus {
		hasUp = 1
	}

	t.statUpStatus.Reset()
	t.statUpStatus.WithLabelValues(
		errReason,
		namespace,
	).Set(hasUp)
}
