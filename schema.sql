-- ----------------------------------------------------------------------------
-- TASK
-- ----------------------------------------------------------------------------

-- A task. This serves as task queue for the crawler.
CREATE TABLE task (
    t_id        SERIAL NOT NULL,
    t_status    INTEGER NOT NULL,
    t_priority  INTEGER NOT NULL,
    t_updated   TIMESTAMP NOT NULL,
    t_type      INTEGER NOT NULL,
    t_data      JSONB NOT NULL,

    PRIMARY KEY (t_id)
);

CREATE INDEX task_queue_idx
    ON task(t_status DESC, t_priority DESC, t_updated ASC);

-- ----------------------------------------------------------------------------
-- META DATA
-- ----------------------------------------------------------------------------

-- Playlist metadata.
CREATE TABLE meta_playlist (
    mpl_uuid        UUID NOT NULL,
    mpl_game_mode   VARCHAR(255) NOT NULL,
    mpl_team_size   VARCHAR(255) NOT NULL,
    mpl_ranking     VARCHAR(255) NOT NULL,
    mpl_platform    VARCHAR(255) NOT NULL,
    PRIMARY KEY (mpl_uuid)
);

CREATE UNIQUE INDEX meta_playlist_uuid_idx
    ON meta_playlist(mpl_uuid);

-- Leader metadata.
CREATE TABLE meta_leader (
    ml_id       INTEGER NOT NULL,
    ml_name     VARCHAR(255) NOT NULL,
    ml_faction  VARCHAR(40) NOT NULL
    PRIMARY KEY (ml_id)
);

CREATE UNIQUE INDEX meta_leader_id_idx
    ON meta_leader(ml_id);

-- Map metadata.
CREATE TABLE meta_map (
    mm_uuid     UUID NOT NULL,
    mm_name     VARCHAR(255) NOT NULL,
    PRIMARY KEY (mm_uuid)
);

CREATE UNIQUE INDEX meta_map_uuid_idx
    ON meta_map(mm_uuid);

-- Object metadata.
CREATE TABLE meta_object (
    mo_id       VARCHAR(255) NOT NULL,
    mo_name     VARCHAR(255),
    mo_uuid     UUID UNIQUE,
    PRIMARY KEY(mo_id)
);

-- ----------------------------------------------------------------------------
-- PLAYER
-- ----------------------------------------------------------------------------

-- A player.
CREATE TABLE player (
    p_id        SERIAL NOT NULL,
    p_gamertag  VARCHAR(255) NOT NULL,

    PRIMARY KEY (p_id)
);

CREATE UNIQUE INDEX player_gamertag_idx
    ON player(p_gamertag varchar_pattern_ops);

CREATE INDEX player_gamertag_gin_idx
    ON player
    USING GIN(p_gamertag gin_trgm_ops);

-- ----------------------------------------------------------------------------
-- MATCH HISTORY
-- ----------------------------------------------------------------------------

-- A player match history.
-- Our primary table to crawl match results.
CREATE TABLE match_history (
    mh_id                       SERIAL NOT NULL,
    mh_player_id                INTEGER NOT NULL,

    mh_match_uuid               UUID NOT NULL,
    mh_match_type               INTEGER NOT NULL,
    mh_game_mode                INTEGER NOT NULL,
    mh_season_uuid              UUID,
    mh_playlist_uuid            UUID,

    mh_map_id                   VARCHAR(255) NOT NULL,
    mh_match_start_date         TIMESTAMP NOT NULL,

    mh_player_match_duration    INTERVAL NOT NULL,
    mh_leader_id                INTEGER NOT NULL,
    mh_player_completed_match   BOOLEAN NOT NULL,
    mh_player_match_outcome     INTEGER NOT NULL,

    FOREIGN KEY (mh_player_id) REFERENCES player(p_id),
    PRIMARY KEY (mh_id, mh_player_id)
);

CREATE INDEX match_history_player_id_idx
    ON match_history(mh_player_id);

CREATE UNIQUE INDEX match_history_player_match_idx
    ON match_history(mh_player_id, mh_match_uuid);

CREATE INDEX match_history_player_match_start_idx
    ON match_history(mh_player_id, mh_match_start_date);

CREATE INDEX match_history_match_uuid_idx
    ON match_history(mh_match_uuid);

-- ----------------------------------------------------------------------------
-- PLAYER STATS
-- ----------------------------------------------------------------------------

-- Player statistics.
-- This table is used by the crawler to discover stale match histories.
CREATE TABLE player_stats (
    ps_id                       SERIAL NOT NULL,

    ps_player_id                INTEGER NOT NULL,
    ps_summary_type             INTEGER NOT NULL,

    ps_game_mode                INTEGER,
    ps_playlist_uuid            UUID,
    ps_playlist_classification  INTEGER,

    ps_csr_designation          INTEGER,
    ps_csr_mm_remaining         INTEGER,
    ps_csr_percent_tier         INTEGER,
    ps_csr_rank                 INTEGER,
    ps_csr_raw                  INTEGER,
    ps_csr_tier                 INTEGER,

    ps_time_played              INTERVAL NOT NULL,
    ps_matches_started          INTEGER NOT NULL,
    ps_matches_completed        INTEGER NOT NULL,
    ps_matches_won              INTEGER NOT NULL,
    ps_matches_lost             INTEGER NOT NULL,
    ps_point_captures           INTEGER NOT NULL,
    ps_units_built              INTEGER NOT NULL,
    ps_units_lost               INTEGER NOT NULL,
    ps_units_destroyed          INTEGER NOT NULL,
    ps_card_plays               INTEGER NOT NULL,
    ps_highest_wave             INTEGER NOT NULL,

    FOREIGN KEY (ps_player_id)
        REFERENCES player(p_id)
        ON DELETE CASCADE,
    PRIMARY KEY (ps_id)
);

CREATE INDEX player_stats_player_id_idx
    ON player_stats(ps_player_id);

-- Player leader statistics
CREATE TABLE player_leader_stats (
    pls_stats_id            INTEGER NOT NULL,
    pls_leader_id           INTEGER NOT NULL,

    pls_time_played         INTERVAL NOT NULL,
    pls_matches_started     INTEGER NOT NULL,
    pls_matches_completed   INTEGER NOT NULL,
    pls_matches_won         INTEGER NOT NULL,
    pls_matches_lost        INTEGER NOT NULL,
    pls_leader_power_casts  INTEGER NOT NULL,

    FOREIGN KEY (pls_stats_id)
        REFERENCES player_stats(ps_id)
        ON DELETE CASCADE,
    PRIMARY KEY (pls_stats_id, pls_leader_id)
);

CREATE INDEX player_leader_stats_id_idx
    ON player_leader_stats(pls_stats_id);

-- ----------------------------------------------------------------------------
-- MAP CAPTURE POINTS
-- ----------------------------------------------------------------------------

-- A capture point
CREATE TABLE map_capture_point (
    mcp_id          SERIAL NOT NULL,

    mcp_map_uuid    UUID NOT NULL,
    mcp_point_name  VARCHAR(255) NOT NULL,

    PRIMARY KEY (mcp_id)
);

CREATE UNIQUE INDEX map_capture_point_point_name_idx
    ON map_capture_point(mcp_map_uuid, mcp_point_name);

-- ----------------------------------------------------------------------------
-- MATCH
-- ----------------------------------------------------------------------------

-- The match is probably the most interesting table.
-- We use a star schema to link additional match information from the API.
CREATE TABLE match (
    m_id                SERIAL NOT NULL,

    m_match_uuid        UUID NOT NULL,
    m_match_type        INTEGER NOT NULL,
    m_game_mode         INTEGER NOT NULL,
    m_season_uuid       UUID,
    m_playlist_uuid     UUID,
    m_map_uuid          UUID NOT NULL,
    m_is_complete       BOOLEAN,
    m_end_reason        INTEGER,
    m_victory_condition INTEGER,
    m_start_date        TIMESTAMP NOT NULL,
    m_duration          INTERVAL,

    PRIMARY KEY(m_id)
);

CREATE UNIQUE INDEX match_match_uuid_idx
    ON match(m_match_uuid);

CREATE INDEX match_start_date_idx
    ON match(m_start_date);

-- A match team
CREATE table match_team (
    mt_match_id         INTEGER NOT NULL,
    mt_team_id          INTEGER NOT NULL,

    mt_team_size        INTEGER NOT NULL,
    mt_match_outcome    INTEGER NOT NULL,
    mt_objective_score  INTEGER NOT NULL,

    FOREIGN KEY (mt_match_id)
        REFERENCES match(m_id)
        ON DELETE CASCADE,
    PRIMARY KEY (mt_match_id, mt_team_id)
);

-- A match player.
-- When crawling matches, we don't have a player yet.
-- The match_player therefore has no foreign key to the player table.
-- (Having this FK would make many things simpler though...)
CREATE TABLE match_player (
    mp_match_id                 INTEGER NOT NULL,
    mp_player_idx               INTEGER NOT NULL,

    mp_is_human                 BOOLEAN NOT NULL,
    mp_gamertag                 VARCHAR(255),
    mp_computer_id              INTEGER,
    mp_computer_difficulty      INTEGER,
    mp_team_id                  INTEGER,
    mp_team_player_index        INTEGER,
    mp_leader_id                INTEGER,
    mp_completed_match          BOOLEAN,
    mp_time_in_match            INTERVAL,
    mp_match_outcome            INTEGER,

    mp_xp_challenges            INTEGER,
    mp_xp_gameplay              INTEGER,
    mp_xp_total_prev            INTEGER,
    mp_xp_total_new             INTEGER,

    mp_csr_prev_designation     INTEGER,
    mp_csr_prev_mm_remaining    INTEGER,
    mp_csr_prev_percent_tier    INTEGER,
    mp_csr_prev_rank            INTEGER,
    mp_csr_prev_raw             INTEGER,
    mp_csr_prev_tier            INTEGER,

    mp_csr_new_designation      INTEGER,
    mp_csr_new_mm_remaining     INTEGER,
    mp_csr_new_percent_tier     INTEGER,
    mp_csr_new_rank             INTEGER,
    mp_csr_new_raw              INTEGER,
    mp_csr_new_tier             INTEGER,

    mp_mmr_prev_rating          DOUBLE PRECISION,
    mp_mmr_prev_variance        DOUBLE PRECISION,
    mp_mmr_new_rating           DOUBLE PRECISION,
    mp_mmr_new_variance         DOUBLE PRECISION,

    FOREIGN KEY (mp_match_id)
        REFERENCES match(m_id)
        ON DELETE CASCADE,
    PRIMARY KEY (mp_match_id, mp_player_idx)
);

CREATE INDEX match_player_match_idx
    ON match_player
    USING BTREE(mp_match_id);

CREATE INDEX match_player_gamertag_idx
    ON match_player(mp_gamertag);

-- Capture point info
CREATE TABLE match_player_point (
    mpp_match_id    INTEGER NOT NULL,
    mpp_player_idx  INTEGER NOT NULL,
    mpp_point_id    INTEGER NOT NULL,

    mpp_times_captured  INTEGER NOT NULL,

    FOREIGN KEY (mpp_match_id)
        REFERENCES match(m_id)
        ON DELETE CASCADE,
    PRIMARY KEY (mpp_match_id, mpp_player_idx, mpp_point_id)
);

CREATE INDEX match_player_point_match_idx
    ON match_player_point(mpp_match_id, mpp_player_idx);

-- Unit info
CREATE TABLE match_player_unit (
    mpu_match_id            INTEGER NOT NULL,
    mpu_player_idx          INTEGER NOT NULL,
    mpu_unit_uuid           UUID NOT NULL,

    mpu_total_built         INTEGER NOT NULL,
    mpu_total_lost          INTEGER NOT NULL,
    mpu_total_destroyed     INTEGER NOT NULL,

    FOREIGN KEY (mpu_match_id)
        REFERENCES match(m_id)
        ON DELETE CASCADE,
    PRIMARY KEY (mpu_match_id, mpu_player_idx, mpu_unit_uuid)
);

CREATE INDEX match_player_unit_match_idx
    ON match_player_unit(mpu_match_id, mpu_player_idx);

-- Car info
CREATE TABLE match_player_card (
    mpc_match_id            INTEGER NOT NULL,
    mpc_player_idx          INTEGER NOT NULL,
    mpc_card_uuid           UUID NOT NULL,

    mpc_total_plays         INTEGER NOT NULL,

    FOREIGN KEY (mpc_match_id)
        REFERENCES match(m_id)
        ON DELETE CASCADE,
    PRIMARY KEY (mpc_match_id, mpc_player_idx, mpc_card_uuid)
);

CREATE INDEX match_player_card_match_idx
    ON match_player_card(mpc_match_id, mpc_player_idx);

-- Wave info
CREATE TABLE match_player_wave (
    mpw_match_id            INTEGER NOT NULL,
    mpw_player_idx          INTEGER NOT NULL,
    mpw_wave_id             INTEGER NOT NULL,

    mpw_duration            INTERVAL NOT NULL,

    FOREIGN KEY (mpw_match_id)
        REFERENCES match(m_id)
        ON DELETE CASCADE,
    PRIMARY KEY (mpw_match_id, mpw_player_idx, mpw_wave_id)
);

CREATE INDEX match_player_wave_match_idx
    ON match_player_wave(mpw_match_id, mpw_player_idx);

-- Leader power
CREATE TABLE match_player_leader_power (
    mplp_match_id           INTEGER NOT NULL,
    mplp_player_idx         INTEGER NOT NULL,
    mplp_leader_power_uuid  UUID NOT NULL,

    mplp_times_cast         INTEGER NOT NULL,

    FOREIGN KEY (mplp_match_id)
        REFERENCES match(m_id)
        ON DELETE CASCADE,
    PRIMARY KEY (mplp_match_id, mplp_player_idx, mplp_leader_power_uuid)
);

CREATE INDEX match_player_leader_power_match_idx
    ON match_player_leader_power(mplp_match_id, mplp_player_idx);

-- ----------------------------------------------------------------------------
-- MATCH EVENTS
-- ----------------------------------------------------------------------------

-- We store the match events as raw jsonb as there are a lot of them
CREATE TABLE match_events (
    me_match_id           INTEGER NOT NULL,
    me_complete_set       BOOLEAN NOT NULL,

    me_building_queued              JSONB,
    me_building_completed           JSONB,
    me_building_recycled            JSONB,
    me_building_upgraded            JSONB,
    me_card_cycled                  JSONB,
    me_card_played                  JSONB,
    me_death                        JSONB,
    me_firefight_wave_completed     JSONB,
    me_firefight_wave_spawned       JSONB,
    me_firefight_wave_started       JSONB,
    me_leader_power_cast            JSONB,
    me_leader_power_unlocked        JSONB,
    me_mana_orb_collected           JSONB,
    me_match_end                    JSONB,
    me_match_start                  JSONB,
    me_player_eliminated            JSONB,
    me_player_joined                JSONB,
    me_player_left                  JSONB,
    me_point_captured               JSONB,
    me_point_created                JSONB,
    me_point_status_change          JSONB,
    me_resource_heartbeat           JSONB,
    me_resource_transferred         JSONB,
    me_tech_researched              JSONB,
    me_unit_control_transferred     JSONB,
    me_unit_promoted                JSONB,
    me_unit_trained                 JSONB,

    FOREIGN KEY (me_match_id)
        REFERENCES match(m_id)
        ON DELETE CASCADE,
    PRIMARY KEY (me_match_id)
);

-- ----------------------------------------------------------------------------
-- TEAM
-- ----------------------------------------------------------------------------

-- The team encounter table is maintained by the crawler and allows a retreival of
-- recent games without scanning the matches of all team members.
-- It has a lot of indexes as it is the main table for match discovery.
CREATE TABLE team_encounter (
    te_match_id         INTEGER NOT NULL,

    te_t1_p1_id         INTEGER,
    te_t1_p2_id         INTEGER,
    te_t1_p3_id         INTEGER,

    te_t2_p1_id         INTEGER,
    te_t2_p2_id         INTEGER,
    te_t2_p3_id         INTEGER,

    te_t1_p1_pos        INTEGER,
    te_t1_p2_pos        INTEGER,
    te_t1_p3_pos        INTEGER,

    te_t2_p1_pos        INTEGER,
    te_t2_p2_pos        INTEGER,
    te_t2_p3_pos        INTEGER,

    te_start_date       TIMESTAMP NOT NULL,
    te_duration         INTERVAL,
    te_match_outcome    INTEGER NOT NULL,

    te_map_uuid         UUID NOT NULL,
    te_match_uuid       UUID NOT NULL,
    te_playlist_uuid    UUID,
    te_season_uuid      UUID,

    te_mmr_min          DOUBLE PRECISION,
    te_mmr_max          DOUBLE PRECISION,
    te_mmr_avg_diff     DOUBLE PRECISION,

    FOREIGN KEY (te_match_id)
        REFERENCES match(m_id)
        ON DELETE CASCADE,

    PRIMARY KEY (te_match_id)
);

CREATE INDEX team_encounter_t1_idx
    ON team_encounter
    USING BTREE(te_t1_p1_id, te_t2_p1_id, te_t3_p1_id, te_start_date);

CREATE INDEX team_encounter_t2_idx
    ON team_encounter
    USING BTREE(te_t2_p1_id, te_t2_p1_id, te_t2_p1_id, te_start_date);

CREATE INDEX team_encounter_t1_t2_idx
    ON team_encounter
    USING BTREE(
        te_t1_p1_id, te_t2_p1_id, te_t3_p1_id,
        te_t2_p1_id, te_t2_p1_id, te_t2_p1_id,
        te_start_date);

CREATE INDEX team_encounter_t1_p1_idx
    ON team_encounter
    USING BTREE(te_t1_p1_id, te_start_date);

CREATE INDEX team_encounter_t1_p2_idx
    ON team_encounter
    USING BTREE(te_t1_p2_id, te_start_date);

CREATE INDEX team_encounter_t1_p3_idx
    ON team_encounter
    USING BTREE(te_t1_p3_id, te_start_date);

CREATE INDEX team_encounter_t2_p1_idx
    ON team_encounter
    USING BTREE(te_t2_p1_id, te_start_date);

CREATE INDEX team_encounter_t2_p2_idx
    ON team_encounter
    USING BTREE(te_t2_p2_id, te_start_date);

CREATE INDEX team_encounter_t2_p3_idx
    ON team_encounter
    USING BTREE(te_t2_p3_id, te_start_date);

CREATE INDEX team_encounter_start_idx
    ON team_encounter
    USING BTREE(te_start_date);
