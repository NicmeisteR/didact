package core

// Player match aggregates
type PlayerMatchAggregates struct {
	Map      *string
	Leader   *string
	Matches  int64
	Wins     int64
	MMR      float64
	CSR      int64
	Duration float64
}

// Relation statistics
type RelationStats struct {
	Name   string
	Tuples int64
}

// Task statistics
type TaskStats struct {
	Status TaskStatus
	Tuples int64
}

// System Status
type SystemStats struct {
	DBSize    string
	Relations []*RelationStats
	Tasks     []*TaskStats
}
