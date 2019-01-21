-- ----------------------------------------------------------------------------
-- UTILS
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION table_privileges(IN target_table VARCHAR)
RETURNS TABLE(
    grantee         information_schema.sql_identifier,
    privilege_type  information_schema.character_data
) AS $$
    BEGIN
        RETURN QUERY
        SELECT
            g.grantee AS grantee,
            g.privilege_type AS privilege_type
        FROM information_schema.role_table_grants g
        WHERE g.table_name = target_table;
    END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION encode_uri(input text)
  RETURNS text
  IMMUTABLE STRICT
AS $$
    DECLARE
    parsed text;
    safePattern text;
    BEGIN
    safePattern = 'a-zA-Z0-9_~/\-\.';
    IF input ~ ('[^' || safePattern || ']') THEN
        SELECT STRING_AGG(fragment, '')
        INTO parsed
        FROM (
            SELECT prefix || encoded AS fragment
            FROM (
                SELECT COALESCE(match[1], '') AS prefix,
                    COALESCE('%' || encode(match[2]::bytea, 'hex'), '') AS encoded
                FROM (
                SELECT regexp_matches(
                    input,
                    '([' || safePattern || ']*)([^' || safePattern || '])?',
                    'g') AS match
                ) matches
            ) parsed
        ) fragments;
        RETURN parsed;
    ELSE
        RETURN input;
    END IF;
    END
$$ LANGUAGE plpgsql;

-- ----------------------------------------------------------------------------
-- UTILS
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION didact_schedule_scan()
RETURNS INTEGER AS $$
    DECLARE
        t TIMESTAMP := clock_timestamp();
    BEGIN
        IF (SELECT COUNT(*) FROM task) > 0 THEN
            RAISE NOTICE 'Crawler is not idle.';
        ELSE
            PERFORM didact_init_active_player_stat_scan(INTERVAL '30 days');
        END IF;
        RAISE NOTICE 'Duration=%', clock_timestamp() - t;
        RETURN 1;
    END
$$ LANGUAGE plpgsql;

-- ----------------------------------------------------------------------------
-- PLAYER
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION didact_upsert_player(gamertag VARCHAR)
RETURNS INTEGER AS $$
    WITH existing_player_ AS (
        SELECT p_id
        FROM player
        WHERE p_gamertag = gamertag
    ), inserted_player_ AS (
        INSERT INTO player(p_gamertag)
        SELECT gamertag
        ON CONFLICT DO NOTHING
        RETURNING p_id
    )
    SELECT p_id FROM existing_player_
    UNION ALL
    SELECT p_id FROM inserted_player_
    LIMIT 1;
$$ LANGUAGE sql;

-- ----------------------------------------------------------------------------
-- TEAM
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION didact_match_teams(match_id INTEGER)
RETURNS TABLE(
    team_id INTEGER,
    player_1 INTEGER,
    player_2 INTEGER,
    player_3 INTEGER,
    match_outcome INTEGER
) AS $$
    BEGIN
        RETURN QUERY
        WITH x AS (
            SELECT
                p_id AS player_id,
                mp_match_id AS match_id,
                mp_player_idx AS player_idx,
                mp_team_id AS team_id,
                mt_team_size AS team_size,
                mt_match_outcome AS team_outcome,
                rank() over (partition by mp_match_id, mp_team_id order by p_id asc) as rank
            FROM match_player, match_team, player
            WHERE mp_match_id = match_id
            AND mt_match_id = match_id
            AND mt_team_id = mp_team_id
            AND mp_gamertag = p_gamertag
        )
        SELECT
            m1.team_id AS team_id,
            m1.player_id AS player_1,
            COALESCE(m2.player_id, 0) AS player_2,
            COALESCE(m3.player_id, 0) AS player_3,
            m1.team_outcome AS match_outcome
        FROM x m1
            LEFT OUTER JOIN x m2
                ON m1.match_id = m2.match_id
                AND m1.team_id = m2.team_id
                AND m2.rank = 2
            LEFT OUTER JOIN x m3
                ON m1.match_id = m3.match_id
                AND m1.team_id = m3.team_id
                AND m3.rank = 3
        WHERE m1.rank = 1;
    END
$$ LANGUAGE plpgsql;

-- ----------------------------------------------------------------------------
-- TEAM ENCOUNTER
-- ----------------------------------------------------------------------------

-- Bulk load team encounter.
-- This will attempt to insert the encounters of all existing matches.
-- (~ 2 hours)
CREATE OR REPLACE FUNCTION didact_bulkload_team_encounters()
RETURNS VOID AS $$
    DECLARE
        t TIMESTAMP := clock_timestamp();
    BEGIN
        RAISE NOTICE 'Bulk loading team encounters';

        WITH x_ AS (
            SELECT
                p_id AS player_id,
                mp_match_id AS match_id,
                mp_player_idx AS player_idx,
                mp_team_id AS team_id,
                rank() over (partition by mp_match_id, mp_team_id order by p_id asc) as rank
            FROM match_player, player
            WHERE mp_gamertag = p_gamertag
        ), t_ AS (
            SELECT
                m1.match_id AS match_id,
                m1.team_id AS team_id,
                m1.player_id AS p1_id,
                COALESCE(m2.player_id, 0) AS p2_id,
                COALESCE(m3.player_id, 0) AS p3_id
            FROM x_ m1
                LEFT OUTER JOIN x_ m2
                    ON m1.match_id = m2.match_id
                    AND m1.team_id = m2.team_id
                    AND m2.rank = 2
                LEFT OUTER JOIN x_ m3
                    ON m1.match_id = m3.match_id
                    AND m1.team_id = m3.team_id
                    AND m3.rank = 3
            WHERE m1.rank = 1
        )
        INSERT INTO team_encounter(
                te_match_id,
                te_t1_p1_id,
                te_t1_p2_id,
                te_t1_p3_id,
                te_t2_p1_id,
                te_t2_p2_id,
                te_t2_p3_id,
                te_start_date,
                te_duration,
                te_match_outcome,
                te_map_uuid,
                te_match_uuid,
                te_playlist_uuid,
                te_season_uuid
        )
        SELECT
                m.m_id,
                t1.p1_id, t1.p2_id, t1.p3_id,
                t2.p1_id, t2.p2_id, t2.p3_id,
                m.m_start_date,
                m.m_duration,
                mt.mt_match_outcome,
                m.m_map_uuid,
                m.m_match_uuid,
                m.m_playlist_uuid,
                m.m_season_uuid
        FROM t_ t1, t_ t2, match m, match_team mt
        WHERE t1.match_id = t2.match_id
        -- The < predicate prevents the nested loop join (estimates are far off here)
        AND t1.team_id < t2.team_id
        AND t1.match_id = m.m_id
        AND mt.mt_match_id = m.m_id
        AND mt.mt_team_id = 1
        ON CONFLICT DO NOTHING;

        RAISE NOTICE 'Duration=%', clock_timestamp() - t;
        END
$$ LANGUAGE plpgsql;

-- Add missing team encounters.
CREATE OR REPLACE FUNCTION didact_sync_team_encounters()
RETURNS VOID AS $$
    DECLARE
        t TIMESTAMP := clock_timestamp();
    BEGIN
        RAISE NOTICE 'Synchronizing team encounters';

        WITH x_ AS (
            SELECT
                p_id AS player_id,
                mp_match_id AS match_id,
                mp_player_idx AS player_idx,
                mp_team_id AS team_id,
                rank() over (partition by mp_match_id, mp_team_id order by p_id asc) as rank
            FROM match_player, player
            WHERE mp_gamertag = p_gamertag
        ), t_ AS (
            SELECT
                m1.match_id AS match_id,
                m1.team_id AS team_id,
                m1.player_id AS p1_id,
                COALESCE(m2.player_id, 0) AS p2_id,
                COALESCE(m3.player_id, 0) AS p3_id
            FROM x_ m1
                LEFT OUTER JOIN x_ m2
                    ON m1.match_id = m2.match_id
                    AND m1.team_id = m2.team_id
                    AND m2.rank = 2
                LEFT OUTER JOIN x_ m3
                    ON m1.match_id = m3.match_id
                    AND m1.team_id = m3.team_id
                    AND m3.rank = 3
            WHERE m1.rank = 1
        ), tdiff_ AS (
            SELECT t.*
            FROM t_ t
                LEFT OUTER JOIN team_encounter te
                ON te.te_match_id = t.match_id
            WHERE te.te_t1_p1_id IS NULL
        )
        INSERT INTO team_encounter(
                te_match_id,
                te_t1_p1_id,
                te_t1_p2_id,
                te_t1_p3_id,
                te_t2_p1_id,
                te_t2_p2_id,
                te_t2_p3_id,
                te_start_date,
                te_duration,
                te_match_outcome,
                te_map_uuid,
                te_match_uuid,
                te_playlist_uuid,
                te_season_uuid
        )
        SELECT
                m.m_id,
                t1.p1_id, t1.p2_id, t1.p3_id,
                t2.p1_id, t2.p2_id, t2.p3_id,
                m.m_start_date,
                m.m_duration,
                mt.mt_match_outcome,
                m.m_map_uuid,
                m.m_match_uuid,
                m.m_playlist_uuid,
                m.m_season_uuid
        FROM tdiff_ t1, tdiff_ t2, match m, match_team mt
        WHERE t1.match_id = t2.match_id
        -- The < predicate prevents the nested loop join (estimates are far off here)
        AND t1.team_id < t2.team_id
        AND t1.match_id = m.m_id
        AND mt.mt_match_id = m.m_id
        AND mt.mt_team_id = 1
        ON CONFLICT DO NOTHING;

        RAISE NOTICE 'Duration=%', clock_timestamp() - t;
        END
$$ LANGUAGE plpgsql;

-- ----------------------------------------------------------------------------
-- TEAM ENCOUNTER POINTS
-- ----------------------------------------------------------------------------

-- Compute all team encounter points.
CREATE OR REPLACE FUNCTION didact_recompute_team_encounter_points()
RETURNS VOID AS $$
    DECLARE
        t TIMESTAMP := clock_timestamp();
    BEGIN
        RAISE NOTICE 'Bulk loading team encounter points';

        INSERT INTO team_encounter_points(tep_match_id, tep_value)
        SELECT
            te.te_match_id AS match_id,
            (abs(r1.r_value - r2.r_value) * 100)::INTEGER AS value
        FROM team_encounter te, team_dsr r1, team_dsr r2
        WHERE te.te_t1_p1_id = r1.r_p1_id
        AND te.te_t1_p2_id = r1.r_p2_id
        AND te.te_t1_p3_id = r1.r_p3_id
        AND te.te_t2_p1_id = r2.r_p1_id
        AND te.te_t2_p2_id = r2.r_p2_id
        AND te.te_t2_p3_id = r2.r_p3_id
        ON CONFLICT(tep_match_id) DO UPDATE SET tep_value = EXCLUDED.tep_value;

        RAISE NOTICE 'Duration=%', clock_timestamp() - t;
        END
$$ LANGUAGE plpgsql;

-- Compute team encounter points for a match.
CREATE OR REPLACE FUNCTION didact_compute_team_encounter_points(match_id INTEGER)
RETURNS VOID AS $$
    DECLARE
        t TIMESTAMP := clock_timestamp();
    BEGIN
        RAISE NOTICE 'Computing team encounter points of match %', match_id;

        INSERT INTO team_encounter_points(tep_match_id, tep_value)
        SELECT
            te.te_match_id AS match_id,
            (abs(r1.r_value - r2.r_value) * 100)::INTEGER AS value
        FROM team_encounter te, team_dsr r1, team_dsr r2
        WHERE te.te_match_id = match_id
        AND te.te_t1_p1_id = r1.r_p1_id
        AND te.te_t1_p2_id = r1.r_p2_id
        AND te.te_t1_p3_id = r1.r_p3_id
        AND te.te_t2_p1_id = r2.r_p1_id
        AND te.te_t2_p2_id = r2.r_p2_id
        AND te.te_t2_p3_id = r2.r_p3_id
        ON CONFLICT(tep_match_id) DO UPDATE SET tep_value = EXCLUDED.tep_value;

        RAISE NOTICE 'Duration=%', clock_timestamp() - t;
        END
$$ LANGUAGE plpgsql;

-- ----------------------------------------------------------------------------
-- TASKS
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION didact_init_player_stat_scan()
RETURNS INTEGER AS $$
    DECLARE
        t TIMESTAMP := clock_timestamp();
    BEGIN
        RAISE NOTICE 'Initiate player statistic scans';
        INSERT INTO task (t_type, t_updated, t_status, t_priority, t_data)
            SELECT
                1,      -- TaskPlayerStatsUpdate
                now(),  -- Updated
                0,      -- TaskQueued
                0,      -- Priority
                json_build_object(
                    'PlayerID', p_id,
                    'Gamertag', p_gamertag
                )
            FROM player;
        RAISE NOTICE 'Duration=%', clock_timestamp() - t;
        RETURN 1;
    END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION didact_init_team_encounter_insert(match_id INTEGER)
RETURNS INTEGER AS $$
    DECLARE
        t TIMESTAMP := clock_timestamp();
    BEGIN
        RAISE NOTICE 'Initiate team encounter insert';
        INSERT INTO task (t_type, t_updated, t_status, t_priority, t_data)
            SELECT
                4,      -- TaskTeamEncounterInsert
                now(),  -- Updated
                0,      -- TaskQueued
                0,      -- Priority
                json_build_object(
                    'MatchID', match_id
                );
        RAISE NOTICE 'Duration=%', clock_timestamp() - t;
        RETURN 1;
    END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION didact_init_active_player_stat_scan(i INTERVAL)
RETURNS INTEGER AS $$
    DECLARE
        t TIMESTAMP := clock_timestamp();
    BEGIN
        RAISE NOTICE 'Initiate player statistic scans';
        INSERT INTO task (t_type, t_updated, t_status, t_priority, t_data)
            SELECT
                1,      -- TaskPlayerStatsUpdate
                now(),  -- Updated
                0,      -- TaskQueued
                0,      -- Priority
                json_build_object(
                    'PlayerID', p_id,
                    'Gamertag', p_gamertag
                )
            FROM
                player,
                (
                    SELECT DISTINCT mp_gamertag AS mp_gamertag
                    FROM match, match_player
                    WHERE m_id = mp_match_id
                    AND m_start_date > (now() - i)
                ) recent_gamertags
            WHERE p_gamertag = mp_gamertag;
        RAISE NOTICE 'Duration=%', clock_timestamp() - t;
        RETURN 1;
    END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION didact_init_match_update()
RETURNS INTEGER AS $$
    DECLARE
        t TIMESTAMP := clock_timestamp();
    BEGIN
        RAISE NOTICE 'Initiate match updates';
        INSERT INTO task (t_type, t_updated, t_status, t_priority, t_data)
            SELECT
                2,      -- TaskMatchResultUpdate
                now(),  -- Updated
                0,      -- TaskQueued
                20,      -- Priority
                json_build_object(
                    'MatchUUID', m.match_uuid
                )
            FROM
                (
                    SELECT DISTINCT(mh_match_uuid) as match_uuid
                    FROM
                        match_history LEFT OUTER JOIN match
                        ON mh_match_uuid = m_match_uuid
                    WHERE m_id IS NULL
                ) m;
        RAISE NOTICE 'Duration=%', clock_timestamp() - t;
        RETURN 1;
    END
$$ LANGUAGE plpgsql;

-- ----------------------------------------------------------------------------
-- REFRESH ALL MATERIALIZED VIEWS
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION didact_refresh_all_materialized_views(schema_arg TEXT DEFAULT 'public')
RETURNS INT AS $$
    DECLARE
        r RECORD;
    BEGIN
        RAISE NOTICE 'Refreshing materialized view in schema %', schema_arg;
        FOR r IN SELECT matviewname FROM pg_matviews WHERE schemaname = schema_arg
        LOOP
            RAISE NOTICE 'Refreshing %.%', schema_arg, r.matviewname;
            EXECUTE 'REFRESH MATERIALIZED VIEW ' || schema_arg || '.' || r.matviewname;
        END LOOP;
        RETURN 1;
    END 
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION didact_refresh_all_materialized_views_concurrently(schema_arg TEXT DEFAULT 'public')
RETURNS INT AS $$
    DECLARE
        r RECORD;
    BEGIN
        RAISE NOTICE 'Refreshing materialized view in schema %', schema_arg;
        FOR r IN SELECT matviewname FROM pg_matviews WHERE schemaname = schema_arg
        LOOP
            RAISE NOTICE 'Refreshing %.%', schema_arg, r.matviewname;
            EXECUTE 'REFRESH MATERIALIZED VIEW CONCURRENTLY ' || schema_arg || '.' || r.matviewname;
        END LOOP;

        RETURN 1;
    END
$$ LANGUAGE plpgsql;

-- ----------------------------------------------------------------------------
-- MATCH EVENTS
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION didact_match_events_build_order(IN match_id INTEGER)
RETURNS TABLE (
    tag VARCHAR,
    time_since_start INTEGER,
    player_idx INTEGER,
    instance INTEGER,
    target VARCHAR,
    supply INTEGER,
    energy INTEGER
) AS $$
    DECLARE
        building_queued_events JSONB;
        building_completed_events JSONB;
        building_upgraded_events JSONB;
        building_recycled_events JSONB;
        tech_researched_events JSONB;
        unit_trained_events JSONB;
    BEGIN
        SELECT me_building_queued, me_building_completed, me_building_upgraded, me_building_recycled, me_tech_researched, me_unit_trained
        INTO building_queued_events, building_completed_events, building_upgraded_events, building_recycled_events, tech_researched_events, unit_trained_events
            FROM match_events
            WHERE me_match_id = match_id
            LIMIT 1;

        -- Building Construction
        RETURN QUERY
            SELECT
                'building_construction'::VARCHAR,
                c."TimeSinceStartMilliseconds",
                q."PlayerIndex",
                q."InstanceId",
                q."BuildingId",
                q."SupplyCost",
                q."EnergyCost"
            FROM
                jsonb_to_recordset(building_queued_events) AS q(
                    "TimeSinceStartMilliseconds" INTEGER,
                    "PlayerIndex" INTEGER,
                    "InstanceId" INTEGER,
                    "BuildingId" VARCHAR,
                    "SupplyCost" INTEGER,
                    "EnergyCost" INTEGER
                ),
                jsonb_to_recordset(building_completed_events) AS c(
                    "TimeSinceStartMilliseconds" INTEGER,
                    "InstanceId" INTEGER
                )
            WHERE
                q."InstanceId" = c."InstanceId";

        -- Building Upgrade
        RETURN QUERY
            SELECT
                'building_upgrade'::VARCHAR,
                u."TimeSinceStartMilliseconds",
                u."PlayerIndex",
                u."InstanceId",
                u."NewBuildingId",
                u."SupplyCost",
                u."EnergyCost"
            FROM
                jsonb_to_recordset(building_upgraded_events) AS u(
                    "TimeSinceStartMilliseconds" INTEGER,
                    "PlayerIndex" INTEGER,
                    "InstanceId" INTEGER,
                    "NewBuildingId" VARCHAR,
                    "SupplyCost" INTEGER,
                    "EnergyCost" INTEGER
                );

        -- Building Recycled
        RETURN QUERY
            SELECT
                'building_recycled'::VARCHAR,
                r."TimeSinceStartMilliseconds",
                r."PlayerIndex",
                r."InstanceId",
                ''::VARCHAR,
                -r."SupplyEarned",
                -r."EnergyEarned"
            FROM
                jsonb_to_recordset(building_recycled_events) AS r(
                    "TimeSinceStartMilliseconds" INTEGER,
                    "PlayerIndex" INTEGER,
                    "InstanceId" INTEGER,
                    "SupplyEarned" INTEGER,
                    "EnergyEarned" INTEGER
                );

        -- Tech Researched
        RETURN QUERY
            SELECT
                'tech_researched'::VARCHAR,
                t."TimeSinceStartMilliseconds",
                t."PlayerIndex",
                t."ResearcherInstanceId",
                t."TechId",
                t."SupplyCost",
                t."EnergyCost"
            FROM
                jsonb_to_recordset(tech_researched_events) AS t(
                    "TimeSinceStartMilliseconds" INTEGER,
                    "PlayerIndex" INTEGER,
                    "ResearcherInstanceId" INTEGER,
                    "TechId" VARCHAR,
                    "SupplyCost" INTEGER,
                    "EnergyCost" INTEGER
                );

        -- Unit Trained
        RETURN QUERY
            SELECT
                'unit_trained'::VARCHAR,
                t."TimeSinceStartMilliseconds",
                t."PlayerIndex",
                t."InstanceId",
                t."SquadId",
                t."SupplyCost",
                t."EnergyCost"
            FROM
                jsonb_to_recordset(unit_trained_events) AS t(
                    "TimeSinceStartMilliseconds" INTEGER,
                    "PlayerIndex" INTEGER,
                    "InstanceId" INTEGER,
                    "SquadId" VARCHAR,
                    "SupplyCost" INTEGER,
                    "EnergyCost" INTEGER
                );
    END
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION didact_match_events_resource_heartbeats(IN match_id INTEGER)
RETURNS TABLE (
    time_since_start INTEGER,
    player_idx INTEGER,
    supply INTEGER,
    energy INTEGER,
    population INTEGER,
    population_cap INTEGER,
    tech_level INTEGER,
    command_points INTEGER,
    total_supply DECIMAL,
    total_energy DECIMAL,
    total_command_points DECIMAL,
    command_xp INTEGER
) AS $$
    WITH heartbeats AS (
        SELECT jsonb_array_elements(me_resource_heartbeat) heartbeat
        FROM match_events
        WHERE me_match_id = match_id
    )
    SELECT
        (h.heartbeat->>'TimeSinceStartMilliseconds')::INTEGER AS time_since_start,
        j.key::INTEGER AS player_idx,
        r."Supply" AS supply,
        r."Energy" AS energy,
        r."Population" AS population,
        r."PopulationCap" AS population_cap,
        r."TechLevel" AS tech_level,
        r."CommandPoints" AS command_points,
        r."TotalSupply" AS total_supply,
        r."TotalEnergy" AS total_energy,
        r."TotalCommandPoints" AS total_command_points,
        r."CommandXP" AS command_xp
    FROM
        heartbeats h,
        jsonb_each(h.heartbeat->'PlayerResources') j,
        jsonb_to_record(j.value) r(
            "Supply" INTEGER,
            "Energy" INTEGER,
            "Population" INTEGER,
            "PopulationCap" INTEGER,
            "TechLevel" INTEGER,
            "CommandPoints" INTEGER,
            "TotalSupply" DECIMAL,
            "TotalEnergy" DECIMAL,
            "TotalCommandPoints" DECIMAL,
            "CommandXP" INTEGER
        )
$$ LANGUAGE sql STABLE;

-- ----------------------------------------------------------------------------
-- PLAYER MATCHES
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION didact_player_matches(IN player_id INTEGER, IN target_interval INTERVAL)
RETURNS TABLE (
    match_id INTEGER,
    t1_p1_id INTEGER,
    t1_p2_id INTEGER,
    t1_p3_id INTEGER,
    t2_p1_id INTEGER,
    t2_p2_id INTEGER,
    t2_p3_id INTEGER,
    start_date TIMESTAMP,
    map_uuid UUID,
    match_uuid UUID,
    playlist_uuid UUID,
    is_win BOOLEAN,
    team_size INTEGER
) AS $$
	WITH matches1 AS (
		SELECT
			te_match_id,
			te_t1_p1_id,
			te_t1_p2_id,
			te_t1_p3_id,
			te_t2_p1_id,
			te_t2_p2_id,
			te_t2_p3_id,
			te_start_date,
			te_map_uuid,
			te_match_uuid,
			te_playlist_uuid,
			(CASE WHEN te_match_outcome = 1 THEN true ELSE false END),
			1 + GREATEST((te_t1_p2_id = 0)::int + (te_t1_p3_id = 0)::int, (te_t2_p2_id = 0)::int + (te_t2_p3_id = 0)::int) as team_size
		FROM team_encounter
		WHERE (
			te_t1_p1_id = player_id
			OR te_t1_p2_id = player_id
			OR te_t1_p3_id = player_id
		)
		AND te_start_date > (NOW() - INTERVAL target_interval)
	), matches2 AS (
		SELECT
			te_match_id,
			te_t2_p1_id,
			te_t2_p2_id,
			te_t2_p3_id,
			te_t1_p1_id,
			te_t1_p2_id,
			te_t1_p3_id,
			te_start_date,
			te_map_uuid,
			te_match_uuid,
			te_playlist_uuid,
			(CASE WHEN te_match_outcome = 2 THEN true ELSE false END),
			1 + GREATEST((te_t1_p2_id = 0)::int + (te_t1_p3_id = 0)::int, (te_t2_p2_id = 0)::int + (te_t2_p3_id = 0)::int)
		FROM team_encounter
		WHERE (
			te_t2_p1_id = player_id
			OR te_t2_p2_id = player_id
			OR te_t2_p3_id = player_id
		)
		AND te_start_date > (NOW() - target_interval)
	), matches AS (
		SELECT * FROM matches1
		UNION ALL
		SELECT * FROM matches2
	)
	SELECT *
	FROM matches
	ORDER BY start_date DESC
$$ LANGUAGE sql STABLE;
