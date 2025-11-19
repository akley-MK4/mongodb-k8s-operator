package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	PromMetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	statsMgoComponentStateHandler = newStatsMgoComponentStateHandler()
)

func GetStatsMgoComponentStateHandler() *StatsMgoComponentStateHandler {
	return statsMgoComponentStateHandler
}

func newStatsMgoComponentStateHandler() *StatsMgoComponentStateHandler {
	handler := &StatsMgoComponentStateHandler{
		gaugeVec: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "num_mgo_component_state",
			},
			[]string{"mgoComponent", "replicaSetId", "ns"},
		),
	}

	PromMetrics.Registry.MustRegister(handler.gaugeVec)
	return handler
}

type StatsMgoComponentStateHandler struct {
	gaugeVec *prometheus.GaugeVec
}

func (t *StatsMgoComponentStateHandler) Set(mgoComponent, replicaSetId, ns string, up bool) {
	hasUp := float64(0)
	if up {
		hasUp = 1
	}
	t.gaugeVec.WithLabelValues(mgoComponent, replicaSetId, ns).Set(hasUp)
}

func (t *StatsMgoComponentStateHandler) Delete(mgoComponent, replicaSetId, ns string) {
	t.gaugeVec.DeleteLabelValues(mgoComponent, replicaSetId, ns)
}
