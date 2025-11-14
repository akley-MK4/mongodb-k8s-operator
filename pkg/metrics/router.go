package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	PromMetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

func newStatsRouterServiceHandler() *StatsRouterServiceHandler {
	handler := &StatsRouterServiceHandler{
		stats: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "router_service_info",
				Help: "Record the status information of routing services",
			}, []string{"numReplicas", "numReadyReplicas", "numUpdatedReplicas", "status"},
		),
	}

	PromMetrics.Registry.MustRegister(handler.stats)
	return handler
}

type StatsRouterServiceHandler struct {
	stats *prometheus.GaugeVec
}

func (t *StatsRouterServiceHandler) Update(numReplicas, numReadyReplicas, numUpdatedReplicas int64, initialized bool) {
	t.stats.Reset()
	status := "uninitialized"
	if initialized {
		status = "initialized"
	}
	t.stats.WithLabelValues(
		strconv.FormatInt(numReplicas, 10),
		strconv.FormatInt(numReadyReplicas, 10),
		strconv.FormatInt(numUpdatedReplicas, 10),
		status,
	).SetToCurrentTime()
}
