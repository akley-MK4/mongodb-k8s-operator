package metrics

var (
	statsRouterServiceHandler         = newStatsRouterServiceHandler()
	statsConfigServiceHandler         = newStatsConfigServiceHandler()
	statsDataReplicasetServiceHandler = newStatsDataReplicasetServiceHandler()
)

func GetStatsRouterServiceHandler() *StatsRouterServiceHandler {
	return statsRouterServiceHandler
}

func GetStatsConfigServiceHandler() *StatsConfigServiceHandler {
	return statsConfigServiceHandler
}

func GetStatsDataReplicasetServiceHandler() *StatsDataReplicasetServiceHandler {
	return statsDataReplicasetServiceHandler
}
