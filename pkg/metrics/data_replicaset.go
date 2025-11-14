package metrics

import (
	"strconv"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	PromMetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

type StatsDataReplicasetServiceRecord struct {
	replicaSetId       string
	numReplicas        int64
	numReadyReplicas   int64
	numUpdatedReplicas int64
	initialized        bool
	addedShard         bool
}

func newStatsDataReplicasetServiceHandler() *StatsDataReplicasetServiceHandler {
	handler := &StatsDataReplicasetServiceHandler{
		stats: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "datareplicaset_service_info",
				Help: "Record the status information of datareplicaset services",
			},
			[]string{"replicaSetId", "numReplicas", "numReadyReplicas", "numUpdatedReplicas", "status"},
		),
	}

	PromMetrics.Registry.MustRegister(handler.stats)
	return handler
}

type StatsDataReplicasetServiceHandler struct {
	stats   *prometheus.GaugeVec
	records sync.Map
}

func (t *StatsDataReplicasetServiceHandler) refreshStats() bool {
	t.stats.Reset()
	t.records.Range(func(key, value any) bool {
		rd := value.(*StatsDataReplicasetServiceRecord)
		status := "uninitialized"
		if rd.initialized {
			status = "initialized"
		}
		if rd.addedShard {
			status = "addedShard"
		}

		t.stats.WithLabelValues(
			rd.replicaSetId,
			strconv.FormatInt(rd.numReplicas, 10),
			strconv.FormatInt(rd.numReadyReplicas, 10),
			strconv.FormatInt(rd.numUpdatedReplicas, 10),
			status,
		).SetToCurrentTime()

		return true
	})

	return true
}

func (t *StatsDataReplicasetServiceHandler) NewStatsDataReplicasetServiceRecord(replicaSetId string) {
	if _, loaded := t.records.LoadOrStore(replicaSetId, &StatsDataReplicasetServiceRecord{replicaSetId: replicaSetId}); !loaded {
		t.refreshStats()
	}
}

func (t *StatsDataReplicasetServiceHandler) SetNumReplicas(replicaSetId string, numReplicas, numReadyReplicas, numUpdatedReplicas int64) {
	updated := false

	iRecord, exists := t.records.Load(replicaSetId)
	if !exists {
		return
	}
	record := iRecord.(*StatsDataReplicasetServiceRecord)

	if record.numReplicas != numReplicas {
		record.numReplicas = numReplicas
		updated = true
	}

	if record.numReadyReplicas != numReadyReplicas {
		record.numReadyReplicas = numReadyReplicas
		updated = true
	}

	if record.numUpdatedReplicas != numUpdatedReplicas {
		record.numUpdatedReplicas = numUpdatedReplicas
		updated = true
	}

	if updated {
		t.refreshStats()
		return
	}

}

func (t *StatsDataReplicasetServiceHandler) SetInitializedStatus(replicaSetId string, initialized bool) {
	iRecord, exists := t.records.Load(replicaSetId)
	if !exists {
		return
	}

	record := iRecord.(*StatsDataReplicasetServiceRecord)
	if record.initialized != initialized {
		record.initialized = initialized
		t.refreshStats()
	}
}

func (t *StatsDataReplicasetServiceHandler) SetAddedShardStatus(replicaSetId string, addedShardStatus bool) {
	iRecord, exists := t.records.Load(replicaSetId)
	if !exists {
		return
	}

	record := iRecord.(*StatsDataReplicasetServiceRecord)
	if record.addedShard != addedShardStatus {
		record.addedShard = addedShardStatus
		t.refreshStats()
	}
}
