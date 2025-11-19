package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	PromMetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	statsNumMgoPodReplicasHandler = newStatsNumMgoPodReplicasHandler()
)

func GetStatsNumMgoPodReplicasHandler() *StatsNumMgoPodReplicasHandler {
	return statsNumMgoPodReplicasHandler
}

func newStatsNumMgoPodReplicasHandler() *StatsNumMgoPodReplicasHandler {
	handler := &StatsNumMgoPodReplicasHandler{
		gaugeVec: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "num_mgo_pod_replicas",
			},
			[]string{"mgoComponent", "replicaSetId", "numType", "ns"},
		),
	}

	PromMetrics.Registry.MustRegister(handler.gaugeVec)
	return handler
}

type StatsNumMgoPodReplicasHandler struct {
	gaugeVec *prometheus.GaugeVec
}

func (t *StatsNumMgoPodReplicasHandler) Set(numReplicas, numReadyReplicas, numUpdatedReplicas int64, mgoComponent, replicaSetId, ns string) {
	t.gaugeVec.WithLabelValues(mgoComponent, replicaSetId, "replicas", ns).Set(float64(numReplicas))
	t.gaugeVec.WithLabelValues(mgoComponent, replicaSetId, "readyReplicas", ns).Set(float64(numReadyReplicas))
	t.gaugeVec.WithLabelValues(mgoComponent, replicaSetId, "updatedReplicas", ns).Set(float64(numUpdatedReplicas))
}
