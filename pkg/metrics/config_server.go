package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	PromMetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

func newStatsConfigServiceHandler() *StatsConfigServiceHandler {
	handler := &StatsConfigServiceHandler{
		statNumReplicas: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "num_replicas_of_config_server",
			}, []string{"replicaSetId", "type", "namespace"},
		),
		statUpStatus: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "up_status_config_server",
			}, []string{"replicaSetId", "errReason", "namespace"},
		),
	}

	PromMetrics.Registry.MustRegister(handler.statNumReplicas, handler.statUpStatus)
	return handler
}

type StatsConfigServiceHandler struct {
	statNumReplicas *prometheus.GaugeVec
	statUpStatus    *prometheus.GaugeVec
}

func (t *StatsConfigServiceHandler) SetNumReplicas(numReplicas, numReadyReplicas, numUpdatedReplicas int64, replicaSetId, namespace string) {
	t.statNumReplicas.WithLabelValues(replicaSetId, "replicas", namespace).Set(float64(numReplicas))
	t.statNumReplicas.WithLabelValues(replicaSetId, "readyReplicas", namespace).Set(float64(numReadyReplicas))
	t.statNumReplicas.WithLabelValues(replicaSetId, "updatedReplicas", namespace).Set(float64(numUpdatedReplicas))
}

func (t *StatsConfigServiceHandler) SetUpStatus(upStatus bool, errReason, replicaSetId, namespace string) {
	hasUp := float64(0)
	if upStatus {
		hasUp = 1
	}

	t.statUpStatus.Reset()
	t.statUpStatus.WithLabelValues(
		replicaSetId,
		errReason,
		namespace,
	).Set(hasUp)
}
