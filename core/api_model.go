package core

// -------------------------------------------------------------------------------------------------------------
// Common
// -------------------------------------------------------------------------------------------------------------

// An ISO date
type ISODate struct {
	Value string `json:"ISO8601Date"`
}

// A player id
type PlayerId struct {
	Gamertag *string `json:"Gamertag"`
}

// A player CSR record
type PlayerCSR struct {
	Designation                 *int64 `json:"Designation"`
	MeasurementMatchesRemaining *int64 `json:"MeasurementMatchesRemaining"`
	PercentToNextTier           *int64 `json:"PercentToNextTier"`
	Rank                        *int64 `json:"Rank"`
	Raw                         *int64 `json:"Raw"`
	Tier                        *int64 `json:"Tier"`
}

// A player MMR record
type PlayerMMR struct {
	LastModifiedDate ISODate `json:"LastModifiedDate"`
	Rating           float64 `json:"Rating"`
	Variance         float64 `json:"Variance"`
}

// A player rating progress
type PlayerRatingProgress struct {
	PreviousCsr PlayerCSR `json:"PreviousCsr"`
	PreviousMmr PlayerMMR `json:"PreviousMmr"`
	UpdatedCsr  PlayerCSR `json:"UpdatedCsr"`
	UpdatedMmr  PlayerMMR `json:"UpdatedMmr"`
}

// A player xp progress
type PlayerXPProgress struct {
	ChallengesXP    int `json:"ChallengesXP"`
	GameplayXP      int `json:"GameplayXP"`
	PreviousTotalXP int `json:"PreviousTotalXP"`
	UpdatedTotalXP  int `json:"UpdatedTotalXP"`
}

// -------------------------------------------------------------------------------------------------------------
// Player stats summary
// https://developer.haloapi.com/docs/services/58acdf27e2f7f71ad0dad84b/operations/Halo-Wars-2-Player-Stats-Summary
// -------------------------------------------------------------------------------------------------------------

type PlayerStatsSummaryType int

const (
	CustomSkirmishSinglePlayerSummary PlayerStatsSummaryType = iota
	CustomSkirmishMultiplayerSummary
	CustomNonSkirmishSummary
	MatchmakingSocialPlaylistSummary
	MatchmakingRankedPlaylistSummary
)

type PlayerStatsLeaderSummary struct {
	TotalTimePlayed       string `json:"TotalTimePlayed"`
	TotalMatchesStarted   int    `json:"TotalMatchesStarted"`
	TotalMatchesCompleted int    `json:"TotalMatchesCompleted"`
	TotalMatchesWon       int    `json:"TotalMatchesWon"`
	TotalMatchesLost      int    `json:"TotalMatchesLost"`
	TotalLeaderPowerCasts int    `json:"TotalLeaderPowersCast"`
}

type PlayerStatsSummary struct {
	PlaylistId             *string                             `json:"PlaylistId"`
	PlaylistClassification *int                                `json:"PlaylistClassification"`
	HighestCSR             PlayerCSR                           `json:"HighestCsr"`
	TotalTimePlayed        string                              `json:"TotalTimePlayed"`
	TotalMatchesStarted    int                                 `json:"TotalMatchesStarted"`
	TotalMatchesCompleted  int                                 `json:"TotalMatchesCompleted"`
	TotalMatchesWon        int                                 `json:"TotalMatchesWon"`
	TotalMatchesLost       int                                 `json:"TotalMatchesLost"`
	TotalPointCaptures     int                                 `json:"TotalPointCaptures"`
	TotalUnitsBuilt        int                                 `json:"TotalUnitsBuilt"`
	TotalUnitsLost         int                                 `json:"TotalUnitsLost"`
	TotalUnitsDestroyed    int                                 `json:"TotalUnitsDestroyed"`
	TotalCardPlays         int                                 `json:"TotalCardPlays"`
	HighestWave            int                                 `json:"HighestWaveCompleted"`
	LeaderStats            map[string]PlayerStatsLeaderSummary `json:"LeaderStats"`
	GameMode               *int                                `json:"GameMode"`
}

type PlayerStatsSkirmish struct {
	SinglePlayerStats     PlayerStatsSummary   `json:"SinglePlayerStats"`
	MultiplayerStats      PlayerStatsSummary   `json:"MultiplayerStats"`
	SinglePlayerModeStats []PlayerStatsSummary `json:"SinglePlayerModeStats"`
	MultiplayerModeStats  []PlayerStatsSummary `json:"MultiplayerModeStats"`
}

type PlayerStatsCustom struct {
	SkirmishStats   PlayerStatsSkirmish  `json:"SkirmishStats"`
	CustomStats     PlayerStatsSummary   `json:"CustomStats"`
	CustomModeStats []PlayerStatsSummary `json:"CustomModeStats"`
}

type PlayerStatsMatchmaking struct {
	SocialPlaylistStats []PlayerStatsSummary `json:"SocialPlaylistStats"`
	RankedPlaylistStats []PlayerStatsSummary `json:"RankedPlaylistStats"`
	SocialModeStats     []PlayerStatsSummary `json:"SocialModeStats"`
	RankedModeStats     []PlayerStatsSummary `json:"RankedModeStats"`
}

type PlayerStats struct {
	CustomSummary      PlayerStatsCustom      `json:"CustomSummary"`
	MatchmakingSummary PlayerStatsMatchmaking `json:"MatchmakingSummary"`
}

// -------------------------------------------------------------------------------------------------------------
// Player match history
// https://developer.haloapi.com/docs/services/58acdf27e2f7f71ad0dad84b/operations/58acdf28e2f7f70db4854b46?
// -------------------------------------------------------------------------------------------------------------

// A player match
type PlayerMatch struct {
	GameMode             int                            `json:"GameMode"`
	LeaderId             int                            `json:"LeaderId"`
	MapId                string                         `json:"MapId"`
	MatchType            int                            `json:"MatchType"`
	MatchId              string                         `json:"MatchId"`
	MatchStartDate       ISODate                        `json:"MatchStartDate"`
	PlayerCompletedMatch bool                           `json:"PlayerCompletedMatch"`
	PlayerIndex          int                            `json:"PlayerIndex"`
	PlayerMatchDuration  string                         `json:"PlayerMatchDuration"`
	PlayerMatchOutcome   int                            `json:"PlayerMatchOutcome"`
	PlaylistId           *string                        `json:"PlaylistId"`
	RatingProgress       PlayerRatingProgress           `json:"RatingProgress"`
	SeasonId             *string                        `json:"SeasonId"`
	Teams                map[int]map[string]interface{} `json:"Teams"`
	TeamId               int                            `json:"TeamId"`
	TeamPlayerIndex      int                            `json:"TeamPlayerIndex"`
	XPProgress           PlayerXPProgress               `json:"XPProgress"`
}

// A player match history
type PlayerMatchHistory struct {
	Count       int           `json:"Count"`
	ResultCount int           `json:"ResultCount"`
	Results     []PlayerMatch `json:"Results"`
}

// -------------------------------------------------------------------------------------------------------------
// Match result
// https://developer.haloapi.com/docs/services/58acdf27e2f7f71ad0dad84b/operations/58acdf28e2f7f70db4854b44
// -------------------------------------------------------------------------------------------------------------

// A player point stats
type MatchPlayerPointStats struct {
	TimesCaptured int `json:"TimesCaptured"`
}

// Unit stats of a match player
type MatchPlayerUnitStats struct {
	TotalBuilt     int `json:"TotalBuilt"`
	TotalLost      int `json:"TotalLost"`
	TotalDestroyed int `json:"TotalDestroyed"`
}

// Card stats of a match player
type MatchPlayerCardStats struct {
	TotalPlays int `json:"TotalPlays"`
}

// Wave stats of a match player
type MatchPlayerWaveStats struct {
	WaveDuration string `json:"WaveDuration"`
}

// Leader power stats of a match player
type MatchPlayerLeaderPowerStats struct {
	TimesCast int `json:"TimesCast"`
}

// A match team
type MatchTeam struct {
	TeamSize       int `json:"TeamSize"`
	MatchOutcome   int `json:"MatchOutcome"`
	ObjectiveScore int `json:"ObjectiveScore"`
}

// A match player
type MatchPlayer struct {
	IsHuman              bool                                   `json:"IsHuman"`
	PlayerId             PlayerId                               `json:"HumanPlayerId"`
	ComputerPlayerId     *int                                   `json:"ComputerPlayerId"`
	ComputerDifficulty   *int                                   `json:"ComputerDifficulty"`
	TeamId               *int                                   `json:"TeamId"`
	TeamPlayerIndex      *int                                   `json:"TeamPlayerIndex"`
	LeaderId             *int                                   `json:"LeaderId"`
	PlayerCompletedMatch *bool                                  `json:"PlayerCompletedMatch"`
	TimeInMatch          *string                                `json:"TimeInMatch"`
	PlayerMatchOutcome   *int                                   `json:"PlayerMatchOutcome"`
	PointStats           map[string]MatchPlayerPointStats       `json:"PointStats"`
	UnitStats            map[string]MatchPlayerUnitStats        `json:"UnitStats"`
	CardStats            map[string]MatchPlayerCardStats        `json:"CardStats"`
	WaveStats            map[int]MatchPlayerWaveStats           `json:"WaveStats"`
	LeaderPowerStats     map[string]MatchPlayerLeaderPowerStats `json:"LeaderPowerStats"`
	XPProgress           PlayerXPProgress                       `json:"XPProgress"`
	RatingProgress       PlayerRatingProgress                   `json:"RatingProgress"`
}

// A match
type Match struct {
	MatchId          string              `json:"MatchId"`
	MatchType        int                 `json:"MatchType"`
	GameMode         int                 `json:"GameMode"`
	SeasonId         *string             `json:"SeasonId"`
	PlaylistId       *string             `json:"PlaylistId"`
	MapId            string              `json:"MapId"`
	IsMatchComplete  bool                `json:"IsMatchComplete"`
	MatchEndReason   *int                `json:"MatchEndReason"`
	VictoryCondition *int                `json:"VictoryCondition"`
	MatchStartDate   ISODate             `json:"MatchStartDate"`
	MatchDuration    *string             `json:"MatchDuration"`
	Teams            map[int]MatchTeam   `json:"Teams"`
	Players          map[int]MatchPlayer `json:"Players"`
}

// -------------------------------------------------------------------------------------------------------------
// Match events
// https://developer.haloapi.com/docs/services/58acdf27e2f7f71ad0dad84b/operations/58acdf28e2f7f70db4854b43
// -------------------------------------------------------------------------------------------------------------

// A match event
type MatchEvent struct {
	EventName                  string `json:"EventName"`
	TimeSinceStartMilliseconds int    `json:"TimeSinceStartMilliseconds"`
}

// A match event location
type MatchEventLocation struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
	Z float64 `json:"z"`
}

// The match event: BuildingConstructionQueued
type MatchEventBuildingConstructionQueued struct {
	MatchEvent
	PlayerIndex        int                `json:"PlayerIndex"`
	BuildingId         string             `json:"BuildingId"`
	InstanceId         int                `json:"InstanceId"`
	Location           MatchEventLocation `json:"Location"`
	SupplyCost         int                `json:"SupplyCost"`
	EnergyCost         int                `json:"EnergyCost"`
	QueueLength        int                `json:"QueueLength"`
	ProvidedByScenario bool               `json:"ProvidedByScenario"`
}

// The match event: BuildingConstructionCompleted
type MatchEventBuildingConstructionCompleted struct {
	MatchEvent
	PlayerIndex int `json:"PlayerIndex"`
	InstanceId  int `json:"InstanceId"`
}

// The match event: BuildingRecycled
type MatchEventBuildingRecycled struct {
	MatchEvent
	PlayerIndex        int  `json:"PlayerIndex"`
	InstanceId         int  `json:"InstanceId"`
	SupplyEarned       int  `json:"SupplyEarned"`
	EnergyEarned       int  `json:"EnergyEarned"`
	ProvidedByScenario bool `json:"ProvidedByScenario"`
}

// The match event: BuildingUpgraded
type MatchEventBuildingUpgraded struct {
	MatchEvent
	PlayerIndex   int    `json:"PlayerIndex"`
	NewBuildingId string `json:"NewBuildingId"`
	InstanceId    int    `json:"InstanceId"`
	SupplyCost    int    `json:"SupplyCost"`
	EnergyCost    int    `json:"EnergyCost"`
}

// The match event: CardCycled
type MatchEventCardCycled struct {
	MatchEvent
	PlayerIndex int `json:"PlayerIndex"`
	ManaCost    int `json:"ManaCost"`
}

// The match event: CardPlayed
type MatchEventCardPlayed struct {
	MatchEvent
	PlayerIndex    int                `json:"PlayerIndex"`
	CardId         string             `json:"CardId"`
	InstanceId     int                `json:"InstanceId"`
	ManaCost       int                `json:"ManaCost"`
	TargetLocation MatchEventLocation `json:"TargetLocation"`
	SpawnAtBase    bool               `json:"SpawnAtBase"`
}

type MatchEventCombatStats struct {
	AttacksLanded int `json:"AttacksLanded"`
}

type MatchEventDeathParticipant struct {
	Count       int                           `json:"Count"`
	CombatStats map[int]MatchEventCombatStats `json:"CombatStats"`
}

type MatchEventDeathParticipants struct {
	ObjectParticipants map[string]MatchEventDeathParticipant `json:"ObjectParticipants"`
}

// The match event: Death
type MatchEventDeath struct {
	MatchEvent
	VictimPlayerIndex  int                                 `json:"VictimPlayerIndex"`
	VictimObjectTypeId string                              `json:"VictimObjectTypeId"`
	VictimInstanceId   int                                 `json:"VictimInstanceId"`
	VictimLocation     MatchEventLocation                  `json:"VictimLocation"`
	IsSuicide          bool                                `json:"IsSuicide"`
	Participants       map[int]MatchEventDeathParticipants `json:"Participants"`
}

// The match event: WaveCompleted
type MatchEventFirefightWaveCompleted struct {
	MatchEvent
	WaveNumber               int `json:"WaveNumber"`
	WaveDurationMilliseconds int `json:"WaveDurationMilliseconds"`
}

// The match event: WaveSpawned
type MatchEventFirefightWaveSpawned struct {
	MatchEvent
	WaveNumber       int   `json:"WaveNumber"`
	InstancesSpawned []int `json:"InstancesSpawned"`
}

// The match event: WaveStarted
type MatchEventFirefightWaveStarted struct {
	MatchEvent
	WaveNumber int `json:"WaveNumber"`
}

// The match event: LeaderPowerCast
type MatchEventLeaderPowerCast struct {
	MatchEvent
	PlayerIndex    int                `json:"PlayerIndex"`
	PowerId        string             `json:"PowerId"`
	InstanceId     int                `json:"InstanceId"`
	TargetLocation MatchEventLocation `json:"TargetLocation"`
	SupplyCost     int                `json:"SupplyCost"`
	EnergyCost     int                `json:"EnergyCost"`
}

// The match event: LeaderPowerUnlocked
type MatchEventLeaderPowerUnlocked struct {
	MatchEvent
	PlayerIndex        int    `json:"PlayerIndex"`
	PowerId            string `json:"PowerId"`
	CommandPointCost   int    `json:"CommandPointCost"`
	ProvidedByScenario bool   `json:"ProvidedByScenario"`
}

// The match event: ManaOrbCollected
type MatchEventManaOrbCollected struct {
	MatchEvent
	PlayerIndex         int                `json:"PlayerIndex"`
	CollectorInstanceId int                `json:"CollectorInstanceId"`
	ManaRateIncrease    int                `json:"ManaRateIncrease"`
	Location            MatchEventLocation `json:"Location"`
}

type MatchEventMatchEndTeamState struct {
	ObjectiveScore int `json:"ObjectiveScore"`
	MatchOutcome   int `json:"MatchOutcome"`
}

type MatchEventMatchEndPlayerState struct {
	PersonalScore int `json:"PersonalScore"`
}

// The match event: MatchEnd
type MatchEventMatchEnd struct {
	MatchEvent
	MatchEndReason             int                                   `json:"MatchEndReason"`
	VictoryCondition           int                                   `json:"VictoryCondition"`
	ActivePlaytimeMilliseconds int                                   `json:"ActivePlaytimeMilliseconds"`
	TeamState                  map[int]MatchEventMatchEndTeamState   `json:"TeamState"`
	PlayerState                map[int]MatchEventMatchEndPlayerState `json:"PlayerState"`
}

// The match event: MatchStart
type MatchEventMatchStart struct {
	MatchEvent
	MatchId          string `json:"MatchId"`
	GameMode         int    `json:"GameMode"`
	MatchType        int    `json:"MatchType"`
	MapId            string `json:"MapId"`
	PlaylistId       string `json:"PlaylistId"`
	TeamSize         int    `json:"TeamSize"`
	IsDefaultRuleSet bool   `json:"IsDefaultRuleSet"`
}

// The match event: PlayerEliminated
type MatchEventPlayerEliminated struct {
	MatchEvent
	PlayerIndex int `json:"PlayerIndex"`
}

// The match event: PlayerJoinedMatch
type MatchEventPlayerJoinedMatch struct {
	MatchEvent
	PlayerIndex        int      `json:"PlayerIndex"`
	PlayerType         int      `json:"PlayerType"`
	HumanPlayerId      PlayerId `json:"HumanPlayerId"`
	ComputerPlayerId   int      `json:"ComputerPlayerId"`
	ComputerDifficulty int      `json:"ComputerDifficulty"`
	TeamId             int      `json:"TeamId"`
	LeaderId           int      `json:"LeaderId"`
}

// The match event: PlayerLeftMatch
type MatchEventPlayerLeftMatch struct {
	MatchEvent
	PlayerIndex int `json:"PlayerIndex"`
}

// The match event: PointCaptured
type MatchEventPointCaptured struct {
	MatchEvent
	PlayerIndex        int                `json:"PlayerIndex"`
	InstanceId         int                `json:"InstanceId"`
	CapturerInstanceId int                `json:"CapturerInstanceId"`
	CapturerLocation   MatchEventLocation `json:"CapturerLocation"`
	NewOwningTeamId    int                `json:"NewOwningTeamId"`
}

// The match event: PointCreated
type MatchEventPointCreated struct {
	MatchEvent
	PointId    string             `json:"PointId"`
	InstanceId int                `json:"InstanceId"`
	Location   MatchEventLocation `json:"Location"`
}

// The match event: PointStatusChange
type MatchEventPointStatusChange struct {
	MatchEvent
	InstanceId   int `json:"InstanceId"`
	Status       int `json:"Status"`
	OwningTeamId int `json:"OwningTeamId"`
}

type MatchEventPlayerResources struct {
	Supply             int     `json:"Supply"`
	Energy             int     `json:"Energy"`
	Population         int     `json:"Population"`
	PopulationCap      int     `json:"PopulationCap"`
	TechLevel          int     `json:"TechLevel"`
	CommandPoints      int     `json:"CommandPoints"`
	Mana               int     `json:"Mana"`
	ManaRate           float64 `json:"ManaRate"`
	TotalSupply        float64 `json:"TotalSupply"`
	TotalEnergy        float64 `json:"TotalEnergy"`
	TotalMana          float64 `json:"TotalMana"`
	TotalCommandPoints int     `json:"TotalCommandPoints"`
	CommandXP          int     `json:"CommandXP"`
}

// The match event: ResourceHeartbeat
type MatchEventResourceHeartbeat struct {
	MatchEvent
	PlayerResources map[int]MatchEventPlayerResources `json:"PlayerResources"`
}

// The match event: ResourceTransferred
type MatchEventResourceTransferred struct {
	MatchEvent
	SendingPlayerIndex   int `json:"SendingPlayerIndex"`
	ReceivingPlayerIndex int `json:"ReceivingPlayerIndex"`
	SupplyCost           int `json:"SupplyCost"`
	EnergyCost           int `json:"EnergyCost"`
	SupplyEarned         int `json:"SupplyEarned"`
	EnergyEarned         int `json:"EnergyEarned"`
}

// The match event: TechResearched
type MatchEventTechResearched struct {
	MatchEvent
	PlayerIndex          int    `json:"PlayerIndex"`
	TechId               string `json:"TechId"`
	ResearcherInstanceId int    `json:"ResearcherInstanceId"`
	SupplyCost           int    `json:"SupplyCost"`
	EnergyCost           int    `json:"EnergyCost"`
	ProvidedByScenario   bool   `json:"ProvidedByScenario"`
}

// The match event: UnitControlTransferred
type MatchEventUnitControlTransferred struct {
	MatchEvent
	OldPlayerIndex     int                `json:"OldPlayerIndex"`
	NewPlayerIndex     int                `json:"NewPlayerIndex"`
	SquadId            string             `json:"SquadId"`
	InstanceId         int                `json:"InstanceId"`
	CapturerInstanceId int                `json:"CapturerInstanceId"`
	Location           MatchEventLocation `json:"Location"`
	PopulationCost     int                `json:"PopulationCost"`
}

// The match event: UnitPromoted
type MatchEventUnitPromoted struct {
	MatchEvent
	PlayerIndex int    `json:"PlayerIndex"`
	SquadId     string `json:"SquadId"`
	InstanceId  int    `json:"InstanceId"`
}

// The match event: UnitTrained
type MatchEventUnitTrained struct {
	MatchEvent
	PlayerIndex        int                `json:"PlayerIndex"`
	SquadId            string             `json:"SquadId"`
	InstanceId         int                `json:"InstanceId"`
	CreatorInstanceId  int                `json:"CreatorInstanceId"`
	SpawnLocation      MatchEventLocation `json:"SpawnLocation"`
	SupplyCost         int                `json:"SupplyCost"`
	EnergyCost         int                `json:"EnergyCost"`
	PopulationCost     int                `json:"PopulationCost"`
	IsClone            bool               `json:"IsClone"`
	ProvidedByScenario bool               `json:"ProvidedByScenario"`
}

type MatchEvents struct {
	isCompleteSet bool

	BuildingConstructionQueued    []*MatchEventBuildingConstructionQueued
	BuildingConstructionCompleted []*MatchEventBuildingConstructionCompleted
	BuildingRecycled              []*MatchEventBuildingRecycled
	BuildingUpgraded              []*MatchEventBuildingUpgraded
	CardCycled                    []*MatchEventCardCycled
	CardPlayed                    []*MatchEventCardPlayed
	Death                         []*MatchEventDeath
	FirefightWaveCompleted        []*MatchEventFirefightWaveCompleted
	FirefightWaveSpawned          []*MatchEventFirefightWaveSpawned
	FirefightWaveStarted          []*MatchEventFirefightWaveStarted
	LeaderPowerCast               []*MatchEventLeaderPowerCast
	LeaderPowerUnlocked           []*MatchEventLeaderPowerUnlocked
	ManaOrbCollected              []*MatchEventManaOrbCollected
	MatchEnd                      []*MatchEventMatchEnd
	MatchStart                    []*MatchEventMatchStart
	PlayerEliminated              []*MatchEventPlayerEliminated
	PlayerJoined                  []*MatchEventPlayerJoinedMatch
	PlayerLeft                    []*MatchEventPlayerLeftMatch
	PointCaptured                 []*MatchEventPointCaptured
	PointCreated                  []*MatchEventPointCreated
	PointStatusChange             []*MatchEventPointStatusChange
	ResourceHeartbeat             []*MatchEventResourceHeartbeat
	ResourceTransferred           []*MatchEventResourceTransferred
	TechResearched                []*MatchEventTechResearched
	UnitControlTransferred        []*MatchEventUnitControlTransferred
	UnitPromoted                  []*MatchEventUnitPromoted
	UnitTrained                   []*MatchEventUnitTrained
}
