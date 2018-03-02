package core

import (
	"context"
	"database/sql"
	"encoding/json"
	_ "github.com/lib/pq"
	"log"
	"strings"
)

type DataStore struct {
	config *Config
	db     *sql.DB
}

func NewDataStore(config *Config) (*DataStore, error) {
	db, err := sql.Open("postgres", config.Database.GetConnectionString(false))
	if err != nil {
		return nil, err
	}

	ds := new(DataStore)
	ds.config = config
	ds.db = db
	return ds, nil
}

// -------------------------------------------------------------------------------------------------------------
// Player ID
// -------------------------------------------------------------------------------------------------------------

// Query player stats
func (ds *DataStore) getPlayerID(gamertag string) (int, error) {
	// Query row
	row := ds.db.QueryRow(`
		SELECT p_id
		FROM player
		WHERE p_gamertag = $1
	`, gamertag)

	// Scan row
	var playerID int
	err := row.Scan(&playerID)

	// No rows?
	if err == sql.ErrNoRows {
		return 0, err
	}

	// Different error?
	if err != nil {
		return 0, err
	}

	return playerID, nil
}

// -------------------------------------------------------------------------------------------------------------
// Match History
// -------------------------------------------------------------------------------------------------------------

// Insert player match
func (ds *DataStore) storePlayerMatch(playerId int, match *PlayerMatch) *sql.Row {
	playlistId := sql.NullString{}
	if match.PlaylistId != nil && len(*match.PlaylistId) > 0 {
		playlistId.String = *match.PlaylistId
		playlistId.Valid = true
	}
	seasonId := sql.NullString{}
	if match.SeasonId != nil && len(*match.SeasonId) > 0 {
		seasonId.String = *match.SeasonId
		seasonId.Valid = true
	}
	statement := `
		INSERT INTO match_history (
			mh_player_id,

			mh_match_uuid,
			mh_match_type,
			mh_game_mode,
			mh_season_uuid,
			mh_playlist_uuid,

			mh_map_id,
			mh_match_start_date,

			mh_player_match_duration,
			mh_leader_id,
			mh_player_completed_match,
			mh_player_match_outcome
		)
		VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			$11, $12
		)
		ON CONFLICT DO NOTHING
		RETURNING mh_match_uuid
	`
	return ds.db.QueryRow(
		statement,

		playerId,

		match.MatchId,
		match.MatchType,
		match.GameMode,
		seasonId,
		playlistId,

		match.MapId,
		match.MatchStartDate.Value,

		sqlDuration(match.PlayerMatchDuration),
		match.LeaderId,
		match.PlayerCompletedMatch,
		match.PlayerMatchOutcome,
	)
}

// Query player stats
func (ds *DataStore) playerNeedsHistoryScan(playerId int, stats *PlayerStats) (bool, int, error) {
	// Query row
	row := ds.db.QueryRow(`
		SELECT
			sum(ps_matches_started) AS cnt
		FROM player, player_stats
		WHERE p_id = ps_player_id
		AND p_id = $1
		AND ps_game_mode IS NULL
	`, playerId)

	// Scan row
	var knownMatchCount sql.NullInt64
	err := row.Scan(&knownMatchCount)

	// No rows?
	if err == sql.ErrNoRows {
		return false, 0, nil
	}

	// Different error?
	if err != nil {
		return false, 0, err
	}

	// Check if there are new games
	newMatchCount := 0
	newMatchCount += stats.CustomSummary.CustomStats.TotalMatchesStarted
	newMatchCount += stats.CustomSummary.SkirmishStats.MultiplayerStats.TotalMatchesStarted
	newMatchCount += stats.CustomSummary.SkirmishStats.SinglePlayerStats.TotalMatchesStarted
	for _, s := range stats.MatchmakingSummary.RankedPlaylistStats {
		newMatchCount += s.TotalMatchesStarted
	}
	for _, s := range stats.MatchmakingSummary.SocialPlaylistStats {
		newMatchCount += s.TotalMatchesStarted
	}

	return (newMatchCount != int(knownMatchCount.Int64)), newMatchCount, nil
}

// -------------------------------------------------------------------------------------------------------------
// Match
// -------------------------------------------------------------------------------------------------------------

// Check if the match exists
func (ds *DataStore) matchExists(matchUUID string) bool {
	row := ds.db.QueryRow(`
		SELECT m_id FROM match WHERE m_match_uuid = $1
	`, matchUUID)
	var matchId string
	switch err := row.Scan(&matchId); err {
	case sql.ErrNoRows:
		return false
	default:
		return true
	}
}

// Insert a match
func (ds *DataStore) storeMatch(match *Match) error {
	// Create transaction
	ctx := context.Background()
	txOpts := new(sql.TxOptions)
	tx, err := ds.db.BeginTx(ctx, txOpts)

	defer func() {
		if err == nil {
			tx.Commit()
			return
		}
		tx.Rollback()
	}()

	// Delete old match
	_, err = tx.Exec(`
		DELETE FROM match WHERE m_match_uuid = $1
	`, match.MatchId)

	if err != nil {
		return err
	}

	// Get map uuids
	mapUUID, ok := MapUUIDs[match.MapId]
	if !ok {
		log.Printf("Unknown map: %s\n", mapUUID)
		return ErrMetadataIncomplete
	}

	// Create new match
	result := tx.QueryRow(`
		INSERT INTO match (
			m_match_uuid,
			m_match_type,
			m_game_mode,
			m_season_uuid,
			m_playlist_uuid,
			m_map_uuid,
			m_is_complete,
			m_end_reason,
			m_victory_condition,
			m_start_date,
			m_duration
		)
		VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
		)
		ON CONFLICT DO NOTHING
		RETURNING m_id
	`,
		match.MatchId,
		match.MatchType,
		match.GameMode,
		match.SeasonId,
		match.PlaylistId,
		mapUUID,
		match.IsMatchComplete,
		match.MatchEndReason,
		match.VictoryCondition,
		match.MatchStartDate.Value,
		match.MatchDuration,
	)
	var matchId int
	err = result.Scan(&matchId)
	if err != nil {
		return err
	}

	// Insert teams
	for teamId, team := range match.Teams {
		_, err = tx.Exec(`
			INSERT INTO match_team (
				mt_match_id,
				mt_team_id,

				mt_team_size,
				mt_match_outcome,
				mt_objective_score
			)
			VALUES (
				$1, $2, $3, $4, $5
			)
			ON CONFLICT DO NOTHING
		`, matchId, teamId, team.TeamSize, team.MatchOutcome, team.ObjectiveScore)

		if err != nil {
			return err
		}
	}

	// Insert players
	for playerIdx, player := range match.Players {
		_, err = tx.Exec(`
			INSERT INTO match_player (
				mp_match_id,
				mp_player_idx,

				mp_is_human,
				mp_gamertag,
				mp_computer_id,
				mp_computer_difficulty,
				mp_team_id,
				mp_team_player_index,
				mp_leader_id,
				mp_completed_match,
				mp_time_in_match,
				mp_match_outcome,

				mp_xp_challenges,
				mp_xp_gameplay,
				mp_xp_total_prev,
				mp_xp_total_new,

				mp_csr_prev_designation,
				mp_csr_prev_mm_remaining,
				mp_csr_prev_percent_tier,
				mp_csr_prev_rank,
				mp_csr_prev_raw,
				mp_csr_prev_tier,

				mp_csr_new_designation,
				mp_csr_new_mm_remaining,
				mp_csr_new_percent_tier,
				mp_csr_new_rank,
				mp_csr_new_raw,
				mp_csr_new_tier,

				mp_mmr_prev_rating,
				mp_mmr_prev_variance,
				mp_mmr_new_rating,
				mp_mmr_new_variance
			)
			VALUES (
				$1, $2,
				$3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
				$13, $14, $15, $16,
				$17, $18, $19, $20, $21, $22,
				$23, $24, $25, $26, $27, $28,
				$29, $30, $31, $32
			)
		`,
			matchId,
			playerIdx,

			player.IsHuman,
			player.PlayerId.Gamertag,
			player.ComputerPlayerId,
			player.ComputerDifficulty,
			player.TeamId,
			player.TeamPlayerIndex,
			player.LeaderId,
			player.PlayerCompletedMatch,
			player.TimeInMatch,
			player.PlayerMatchOutcome,

			player.XPProgress.ChallengesXP,
			player.XPProgress.GameplayXP,
			player.XPProgress.PreviousTotalXP,
			player.XPProgress.UpdatedTotalXP,

			player.RatingProgress.PreviousCsr.Designation,
			player.RatingProgress.PreviousCsr.MeasurementMatchesRemaining,
			player.RatingProgress.PreviousCsr.PercentToNextTier,
			player.RatingProgress.PreviousCsr.Rank,
			player.RatingProgress.PreviousCsr.Raw,
			player.RatingProgress.PreviousCsr.Tier,

			player.RatingProgress.UpdatedCsr.Designation,
			player.RatingProgress.UpdatedCsr.MeasurementMatchesRemaining,
			player.RatingProgress.UpdatedCsr.PercentToNextTier,
			player.RatingProgress.UpdatedCsr.Rank,
			player.RatingProgress.UpdatedCsr.Raw,
			player.RatingProgress.UpdatedCsr.Tier,

			player.RatingProgress.PreviousMmr.Rating,
			player.RatingProgress.PreviousMmr.Variance,
			player.RatingProgress.UpdatedMmr.Rating,
			player.RatingProgress.UpdatedMmr.Variance,
		)

		if err != nil {
			return err
		}

		// Insert point stats
		for pointName, pointStats := range player.PointStats {
			// Insert capture point
			result := ds.db.QueryRow(`
				INSERT INTO map_capture_point (
					mcp_map_uuid,
					mcp_point_name
				)
				VALUES (
					$1, $2
				)
				ON CONFLICT (mcp_map_uuid, mcp_point_name)
				DO UPDATE SET
					mcp_id = map_capture_point.mcp_id
				RETURNING mcp_id
			`,
				mapUUID,
				pointName,
			)
			var pointId int
			err = result.Scan(&pointId)
			if err != nil {
				log.Printf("Failed to insert point '%s': %v", pointName, err)
				return err
			}

			// Insert point stats
			_, err = tx.Exec(`
				INSERT INTO match_player_point (
					mpp_match_id,
					mpp_player_idx,
					mpp_point_id,

					mpp_times_captured
				)
				VALUES (
					$1, $2, $3,
					$4
				)
			`,
				matchId,
				playerIdx,
				pointId,

				pointStats.TimesCaptured,
			)

			if err != nil {
				return err
			}
		}

		// Insert unit stats
		for unitName, unitStats := range player.UnitStats {
			unitUUID, ok := GameObjectUUIDs[strings.ToLower(unitName)]
			if !ok {
				log.Printf("Unknown unit: %s\n", unitName)
				return ErrMetadataIncomplete
			}
			_, err = tx.Exec(`
				INSERT INTO match_player_unit (
					mpu_match_id,
					mpu_player_idx,
					mpu_unit_uuid,

					mpu_total_built,
					mpu_total_lost,
					mpu_total_destroyed
				)
				VALUES (
					$1, $2, $3,
					$4, $5, $6
				)
			`,
				matchId,
				playerIdx,
				unitUUID,

				unitStats.TotalBuilt,
				unitStats.TotalLost,
				unitStats.TotalDestroyed,
			)

			if err != nil {
				return err
			}
		}

		// Insert card stats
		for cardUUID, cardStats := range player.CardStats {
			_, err = tx.Exec(`
				INSERT INTO match_player_card (
					mpc_match_id,
					mpc_player_idx,
					mpc_card_uuid,

					mpc_total_plays
				)
				VALUES (
					$1, $2, $3,
					$4
				)
			`,
				matchId,
				playerIdx,
				cardUUID,

				cardStats.TotalPlays,
			)
			if err != nil {
				return err
			}
		}

		// Insert wave stats
		for waveNumber, waveStats := range player.WaveStats {
			_, err = tx.Exec(`
				INSERT INTO match_player_wave (
					mpw_match_id,
					mpw_player_idx,
					mpw_wave_id,

					mpw_duration
				)
				VALUES (
					$1, $2, $3,
					$4
				)
			`,
				matchId,
				playerIdx,
				waveNumber,

				waveStats.WaveDuration,
			)

			if err != nil {
				return err
			}
		}

		// Insert leader power stats
		for leaderPowerName, leaderPowerStats := range player.LeaderPowerStats {
			leaderPowerUUID, ok := LeaderPowerUUIDs[leaderPowerName]
			if !ok {
				log.Printf("Unknown leader power: %s\n", leaderPowerName)
				return ErrMetadataIncomplete
			}
			_, err = tx.Exec(`
				INSERT INTO match_player_leader_power (
					mplp_match_id,
					mplp_player_idx,
					mplp_leader_power_uuid,

					mplp_times_cast
				)
				VALUES (
					$1, $2, $3,
					$4
				)
			`,
				matchId,
				playerIdx,
				leaderPowerUUID,

				leaderPowerStats.TimesCast,
			)

			if err != nil {
				return err
			}
		}
	}

	// Store players
	for _, player := range match.Players {
		if player.PlayerId.Gamertag == nil || *player.PlayerId.Gamertag == "" {
			continue
		}
		_, err = tx.Exec(`SELECT didact_upsert_player($1)`, player.PlayerId.Gamertag)
		if err != nil {
			return err
		}
	}

	// Store team encounter
	_, err = tx.Exec(`SELECT didact_store_team_encounter($1)`, matchId)
	return err
}

// -------------------------------------------------------------------------------------------------------------
// Match Events
// -------------------------------------------------------------------------------------------------------------

// Insert match events
func (ds *DataStore) storeMatchEvents(matchId int, matchEvents *MatchEvents) error {
	// Create transaction
	ctx := context.Background()
	txOpts := new(sql.TxOptions)
	tx, err := ds.db.BeginTx(ctx, txOpts)

	defer func() {
		if err == nil {
			tx.Commit()
			return
		}
		tx.Rollback()
	}()

	// Delete old match events
	_, err = tx.Exec(`
		DELETE FROM match_events WHERE me_match_id = $1
	`, matchId)

	if err != nil {
		return err
	}

	buildingQueued, _ := json.Marshal(matchEvents.BuildingConstructionQueued)
	buildingCompleted, _ := json.Marshal(matchEvents.BuildingConstructionCompleted)
	buildingRecycled, _ := json.Marshal(matchEvents.BuildingRecycled)
	buildingUpgraded, _ := json.Marshal(matchEvents.BuildingUpgraded)
	cardCycled, _ := json.Marshal(matchEvents.CardCycled)
	cardPlayed, _ := json.Marshal(matchEvents.CardPlayed)
	death, _ := json.Marshal(matchEvents.Death)
	firefightWaveCompleted, _ := json.Marshal(matchEvents.FirefightWaveCompleted)
	firefightWaveSpawned, _ := json.Marshal(matchEvents.FirefightWaveSpawned)
	firefightWaveStarted, _ := json.Marshal(matchEvents.FirefightWaveStarted)
	leaderPowerCast, _ := json.Marshal(matchEvents.LeaderPowerCast)
	leaderPowerUnlocked, _ := json.Marshal(matchEvents.LeaderPowerUnlocked)
	manaOrbCollected, _ := json.Marshal(matchEvents.ManaOrbCollected)
	matchEnd, _ := json.Marshal(matchEvents.MatchEnd)
	matchStart, _ := json.Marshal(matchEvents.MatchStart)
	playerEliminated, _ := json.Marshal(matchEvents.PlayerEliminated)
	playerJoined, _ := json.Marshal(matchEvents.PlayerJoined)
	playerLeft, _ := json.Marshal(matchEvents.PlayerLeft)
	pointCaptured, _ := json.Marshal(matchEvents.PointCaptured)
	pointCreated, _ := json.Marshal(matchEvents.PointCreated)
	pointStatusChange, _ := json.Marshal(matchEvents.PointStatusChange)
	resourceHeartbeat, _ := json.Marshal(matchEvents.ResourceHeartbeat)
	resourceTransferred, _ := json.Marshal(matchEvents.ResourceTransferred)
	techResearched, _ := json.Marshal(matchEvents.TechResearched)
	unitControlTransferred, _ := json.Marshal(matchEvents.UnitControlTransferred)
	unitPromoted, _ := json.Marshal(matchEvents.UnitPromoted)
	unitTrained, _ := json.Marshal(matchEvents.UnitTrained)

	// Store new match events
	_, err = tx.Exec(`
		INSERT INTO match_events (
			me_match_id,
			me_complete_set,
			me_building_queued,
			me_building_completed,
			me_building_recycled,
			me_building_upgraded,
			me_card_cycled,
			me_card_played,
			me_death,
			me_firefight_wave_completed,
			me_firefight_wave_spawned,
			me_firefight_wave_started,
			me_leader_power_cast,
			me_leader_power_unlocked,
			me_mana_orb_collected,
			me_match_end,
			me_match_start,
			me_player_eliminated,
			me_player_joined,
			me_player_left,
			me_point_captured,
			me_point_created,
			me_point_status_change,
			me_resource_heartbeat,
			me_resource_transferred,
			me_tech_researched,
			me_unit_control_transferred,
			me_unit_promoted,
			me_unit_trained
		)
		VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
			$21, $22, $23, $24, $25, $26, $27, $28, $29
		)
		ON CONFLICT DO NOTHING
		RETURNING me_match_id
	`,
		matchId,
		matchEvents.isCompleteSet,
		buildingQueued,
		buildingCompleted,
		buildingRecycled,
		buildingUpgraded,
		cardCycled,
		cardPlayed,
		death,
		firefightWaveCompleted,
		firefightWaveSpawned,
		firefightWaveStarted,
		leaderPowerCast,
		leaderPowerUnlocked,
		manaOrbCollected,
		matchEnd,
		matchStart,
		playerEliminated,
		playerJoined,
		playerLeft,
		pointCaptured,
		pointCreated,
		pointStatusChange,
		resourceHeartbeat,
		resourceTransferred,
		techResearched,
		unitControlTransferred,
		unitPromoted,
		unitTrained,
	)
	if err != nil {
		return err
	}

	return nil
}

// -------------------------------------------------------------------------------------------------------------
// Player Stats
// -------------------------------------------------------------------------------------------------------------

// Insert player stats
func (ds *DataStore) storePlayerStats(playerId int, stats *PlayerStats) (err error) {
	// Create transaction
	ctx := context.Background()
	txOpts := new(sql.TxOptions)
	tx, err := ds.db.BeginTx(ctx, txOpts)

	defer func() {
		if err == nil {
			tx.Commit()
			return
		}
		tx.Rollback()
	}()

	// Insert leader stats
	insertLeaderStats := func(statsId int, leaderId int, leaderSummary *PlayerStatsLeaderSummary, errors *[]error) {
		_, err = tx.Exec(`
			INSERT INTO player_leader_stats (
				pls_stats_id,
				pls_leader_id,

				pls_time_played,
				pls_matches_started,
				pls_matches_completed,
				pls_matches_won,
				pls_matches_lost,
				pls_leader_power_casts
			)
			VALUES (
				$1, $2,
				$3, $4, $5, $6, $7, $8
			)
			ON CONFLICT DO NOTHING
		`,
			statsId,
			leaderId,
			sqlDuration(leaderSummary.TotalTimePlayed),
			leaderSummary.TotalMatchesStarted,
			leaderSummary.TotalMatchesCompleted,
			leaderSummary.TotalMatchesWon,
			leaderSummary.TotalMatchesLost,
			leaderSummary.TotalLeaderPowerCasts,
		)

		if err != nil {
			*errors = append(*errors, err)
			return
		}
	}

	// Insert stats
	insertStats := func(summaryType PlayerStatsSummaryType, summary *PlayerStatsSummary, errors *[]error) {
		result := tx.QueryRow(`
		INSERT INTO player_stats (
			ps_player_id,
			ps_summary_type,
			ps_game_mode,

			ps_playlist_uuid,
			ps_playlist_classification,

			ps_csr_designation,
			ps_csr_mm_remaining,
			ps_csr_percent_tier,
			ps_csr_rank,
			ps_csr_raw,
			ps_csr_tier,

			ps_time_played,
			ps_matches_started,
			ps_matches_completed,
			ps_matches_won,
			ps_matches_lost,
			ps_point_captures,
			ps_units_built,
			ps_units_lost,
			ps_units_destroyed,
			ps_card_plays,
			ps_highest_wave
		)
		VALUES (
			$1, $2, $3,
			$4, $5,
			$6, $7, $8, $9, $10, $11,
			$12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22
		)
		ON CONFLICT DO NOTHING
		RETURNING ps_id
	`,
			playerId,
			summaryType,
			summary.GameMode,
			summary.PlaylistId,
			summary.PlaylistClassification,
			summary.HighestCSR.Designation,
			summary.HighestCSR.MeasurementMatchesRemaining,
			summary.HighestCSR.PercentToNextTier,
			summary.HighestCSR.Rank,
			summary.HighestCSR.Raw,
			summary.HighestCSR.Tier,
			sqlDuration(summary.TotalTimePlayed),
			summary.TotalMatchesStarted,
			summary.TotalMatchesCompleted,
			summary.TotalMatchesWon,
			summary.TotalMatchesLost,
			summary.TotalPointCaptures,
			summary.TotalUnitsBuilt,
			summary.TotalUnitsLost,
			summary.TotalUnitsDestroyed,
			summary.TotalCardPlays,
			summary.HighestWave,
		)

		// Scan result
		var statsId int
		err := result.Scan(&statsId)
		if err != nil {
			*errors = append(*errors, err)
			return
		}

		// Insert leader stats
		for leader, leaderSummary := range summary.LeaderStats {
			insertLeaderStats(statsId, leaderIds[leader], &leaderSummary, errors)
		}
	}

	// Delete old summaries
	_, err = tx.Exec(`
		DELETE FROM player_stats WHERE ps_player_id = $1
	`, playerId)
	if err != nil {
		return err
	}

	// Insertion errors
	var errors []error

	// Custom skirmish singleplayer games
	insertStats(CustomSkirmishSinglePlayerSummary, &stats.CustomSummary.SkirmishStats.SinglePlayerStats, &errors)
	for _, x := range stats.CustomSummary.SkirmishStats.SinglePlayerModeStats {
		insertStats(CustomSkirmishSinglePlayerSummary, &x, &errors)
	}

	// Custom skirmish multiplayer games
	insertStats(CustomSkirmishMultiplayerSummary, &stats.CustomSummary.SkirmishStats.MultiplayerStats, &errors)
	for _, x := range stats.CustomSummary.SkirmishStats.MultiplayerModeStats {
		insertStats(CustomSkirmishMultiplayerSummary, &x, &errors)
	}

	// Custom non-skirmish games
	insertStats(CustomNonSkirmishSummary, &stats.CustomSummary.CustomStats, &errors)
	for _, x := range stats.CustomSummary.CustomModeStats {
		insertStats(CustomNonSkirmishSummary, &x, &errors)
	}

	// Matchmade social games
	for _, x := range stats.MatchmakingSummary.SocialPlaylistStats {
		insertStats(MatchmakingSocialPlaylistSummary, &x, &errors)
	}
	for _, x := range stats.MatchmakingSummary.SocialModeStats {
		insertStats(MatchmakingSocialPlaylistSummary, &x, &errors)
	}

	// Matchmade ranked games
	for _, x := range stats.MatchmakingSummary.RankedPlaylistStats {
		insertStats(MatchmakingRankedPlaylistSummary, &x, &errors)
	}
	for _, x := range stats.MatchmakingSummary.RankedModeStats {
		insertStats(MatchmakingRankedPlaylistSummary, &x, &errors)
	}

	// Print errors
	for _, err = range errors {
		log.Println(err)
	}

	// Return first
	if len(errors) > 0 {
		return errors[0]
	} else {
		return nil
	}
}
