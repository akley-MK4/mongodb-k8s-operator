package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	PromMetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

func newStatsConfigServiceHandler() *StatsConfigServiceHandler {
	handler := &StatsConfigServiceHandler{
		stats: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "config_service_info",
				Help: "Record the status information of config services",
			}, []string{"replicaSetId", "numReplicas", "numReadyReplicas", "numUpdatedReplicas", "status"},
		),
	}

	PromMetrics.Registry.MustRegister(handler.stats)
	return handler
}

type StatsConfigServiceHandler struct {
	stats *prometheus.GaugeVec
}

func (t *StatsConfigServiceHandler) Update(replicaSetId string, numReplicas, numReadyReplicas, numUpdatedReplicas int64, initialized bool) {
	status := "uninitialized"
	if initialized {
		status = "initialized"
	}

	t.stats.Reset()
	t.stats.WithLabelValues(
		replicaSetId,
		strconv.FormatInt(numReplicas, 10),
		strconv.FormatInt(numReadyReplicas, 10),
		strconv.FormatInt(numUpdatedReplicas, 10),
		status,
	).SetToCurrentTime()
}
