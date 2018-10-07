package core

import "time"

type AnnotatedPlayerTeam struct {
	teamID    int
	player1ID int
	player2ID int
	player3ID int
	player1GT string
	player2GT string
	player3GT string
	matches   int
	wins      int
	duration  time.Duration
}

type AnnotatedPlayerMatch struct {
	matchID        int64
	matchUUID      string
	playlistUUID   string
	matchStartDate time.Time
	matchDuration  time.Duration
	matchOutcome   int
	team1ID        int
	team2ID        int
	player1ID      int
	player2ID      int
	player3ID      int
	player4ID      int
	player5ID      int
	player6ID      int
	player1GT      string
	player2GT      string
	player3GT      string
	player4GT      string
	player5GT      string
	player6GT      string
	mapName        string
	leaderName     string
	gameMode       string
	teamSize       string
	ranking        string
	platform       string
}
