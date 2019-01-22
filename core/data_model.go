package core

// Player stats
type PlayerTeamStats struct {
	Team     *string
	Map      *string
	Leader   *string
	Matches  int64
	Wins     int64
	MMR      float64
	CSR      int64
	Duration float64
}
