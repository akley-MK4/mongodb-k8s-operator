package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	PromMetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

func newStatsDataReplicasetServiceHandler() *StatsDataReplicasetServiceHandler {
	handler := &StatsDataReplicasetServiceHandler{
		statNumReplicas: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "num_replicas_of_datareplicaset",
			},
			[]string{"replicaSetId", "type", "namespace"},
		),
		statUpStatus: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "up_status_datareplicaset",
			},
			[]string{"replicaSetId", "errReason", "namespace"},
		),
	}

	PromMetrics.Registry.MustRegister(handler.statNumReplicas, handler.statUpStatus)
	return handler
}

type StatsDataReplicasetServiceHandler struct {
	statNumReplicas *prometheus.GaugeVec
	statUpStatus    *prometheus.GaugeVec
}

func (t *StatsDataReplicasetServiceHandler) SetNumReplicas(numReplicas, numReadyReplicas, numUpdatedReplicas int64, replicaSetId, namespace string) {
	t.statNumReplicas.WithLabelValues(replicaSetId, "replicas", namespace).Set(float64(numReplicas))
	t.statNumReplicas.WithLabelValues(replicaSetId, "readyReplicas", namespace).Set(float64(numReadyReplicas))
	t.statNumReplicas.WithLabelValues(replicaSetId, "updatedReplicas", namespace).Set(float64(numUpdatedReplicas))
}

func (t *StatsDataReplicasetServiceHandler) SetUpStatus(upStatus bool, errReason, replicaSetId, namespace string) {
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
