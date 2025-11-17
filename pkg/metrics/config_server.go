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
			}, []string{"replicaSetId", "namespace"},
		),
		statNumReadyReplicas: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "num_ready_replicas_of_config_server",
			}, []string{"replicaSetId", "namespace"},
		),
		statNumUpdatedReplicas: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "num_updated_replicas_of_config_server",
			}, []string{"replicaSetId", "namespace"},
		),
		statUpStatus: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "up_status_config_server",
			}, []string{"replicaSetId", "namespace", "errReason"},
		),
	}

	PromMetrics.Registry.MustRegister(handler.statNumReplicas, handler.statNumReadyReplicas, handler.statNumUpdatedReplicas, handler.statUpStatus)
	return handler
}

type StatsConfigServiceHandler struct {
	statNumReplicas        *prometheus.GaugeVec
	statNumReadyReplicas   *prometheus.GaugeVec
	statNumUpdatedReplicas *prometheus.GaugeVec
	statUpStatus           *prometheus.GaugeVec
}

func (t *StatsConfigServiceHandler) SetNumReplicas(numReplicas int64, replicaSetId, namespace string) {
	t.statNumReplicas.WithLabelValues(replicaSetId, namespace).Set(float64(numReplicas))
}

func (t *StatsConfigServiceHandler) SetNumReadyReplicas(numReplicas int64, replicaSetId, namespace string) {
	t.statNumReadyReplicas.WithLabelValues(replicaSetId, namespace).Set(float64(numReplicas))
}

func (t *StatsConfigServiceHandler) SetNumUpdatedReplicas(numReplicas int64, replicaSetId, namespace string) {
	t.statNumUpdatedReplicas.WithLabelValues(replicaSetId, namespace).Set(float64(numReplicas))
}

func (t *StatsConfigServiceHandler) SetUpStatus(upStatus bool, errReason, replicaSetId, namespace string) {
	hasUp := float64(0)
	if upStatus {
		hasUp = 1
	}

	t.statUpStatus.Reset()
	t.statUpStatus.WithLabelValues(
		replicaSetId,
		namespace,
		errReason,
	).Set(hasUp)
}
