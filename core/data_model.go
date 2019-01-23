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
