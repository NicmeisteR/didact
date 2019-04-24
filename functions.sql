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

CREATE OR REPLACE FUNCTION didact_schedule_scan(IN d INTERVAL)
RETURNS INTEGER AS $$
    DECLARE
        t TIMESTAMP := clock_timestamp();
    BEGIN
        IF (SELECT COUNT(*) FROM task WHERE t_status = 0) > 0 THEN
            RAISE NOTICE 'Crawler is not idle.';
        ELSIF d IS NULL THEN
            PERFORM didact_init_player_stat_scan();
        ELSE
            PERFORM didact_init_active_player_stat_scan(d);
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
                mp_player_idx AS player_pos,
                mp_team_id AS team_id,
                mp_mmr_new_rating AS mmr,
                rank() over (partition by mp_match_id, mp_team_id order by p_id asc) as rank
            FROM match_player, player
            WHERE mp_gamertag = p_gamertag
        ), t_ AS (
            SELECT
                m1.match_id AS match_id,
                m1.team_id AS team_id,
                m1.player_id AS p1_id,
                m2.player_id AS p2_id,
                m3.player_id AS p3_id,
                m1.player_pos AS p1_pos,
                m2.player_pos AS p2_pos,
                m3.player_pos AS p3_pos,
                LEAST(m1.mmr, COALESCE(m2.mmr, m1.mmr), COALESCE(m3.mmr, m1.mmr)) AS mmr_min,
                GREATEST(m1.mmr, COALESCE(m2.mmr, m1.mmr), COALESCE(m3.mmr, m1.mmr)) AS mmr_max,
                (m1.mmr + COALESCE(m2.mmr, 0.0) + COALESCE(m3.mmr, 0.0)) AS mmr_sum,
                (
                    (m1.player_id IS NOT NULL)::INTEGER +
                    (m2.player_id IS NOT NULL)::INTEGER +
                    (m3.player_id IS NOT NULL)::INTEGER
                ) team_size
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
                te_t1_p1_pos,
                te_t1_p2_pos,
                te_t1_p3_pos,
                te_t2_p1_pos,
                te_t2_p2_pos,
                te_t2_p3_pos,
                te_start_date,
                te_duration,
                te_match_outcome,
                te_map_uuid,
                te_match_uuid,
                te_playlist_uuid,
                te_season_uuid,
                te_mmr_min,
                te_mmr_max,
                te_mmr_avg_diff
        )
        SELECT
                m.m_id,
                t1.p1_id, t1.p2_id, t1.p3_id,
                t2.p1_id, t2.p2_id, t2.p3_id,
                t1.p1_pos, t1.p2_pos, t1.p3_pos,
                t2.p1_pos, t2.p2_pos, t2.p3_pos,
                m.m_start_date,
                m.m_duration,
                mt.mt_match_outcome,
                m.m_map_uuid,
                m.m_match_uuid,
                m.m_playlist_uuid,
                m.m_season_uuid,
                LEAST(t1.mmr_min, t2.mmr_min),
                GREATEST(t1.mmr_max, t2.mmr_max),
                ABS((t1.mmr_sum / CAST(t1.team_size AS DOUBLE PRECISION)) - (t2.mmr_sum / CAST(t2.team_size AS DOUBLE PRECISION)))
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

CREATE OR REPLACE FUNCTION didact_find_team_encounters(team1 character varying[], team2 character varying[], interval_ interval)
RETURNS TABLE (
  match_id         INTEGER,
  t1_p1_id         INTEGER,
  t1_p2_id         INTEGER,
  t1_p3_id         INTEGER,
  t2_p1_id         INTEGER,
  t2_p2_id         INTEGER,
  t2_p3_id         INTEGER,
  t1_p1_pos        INTEGER,
  t1_p2_pos        INTEGER,
  t1_p3_pos        INTEGER,
  t2_p1_pos        INTEGER,
  t2_p2_pos        INTEGER,
  t2_p3_pos        INTEGER,
  start_date       TIMESTAMP,
  duration         INTERVAL,
  outcome          INTEGER,
  map_uuid         UUID,
  match_uuid       UUID,
  playlist_uuid    UUID,
  season_uuid      UUID,
  mmr_min          DOUBLE PRECISION,
  mmr_max          DOUBLE PRECISION,
  mmr_avg_diff     DOUBLE PRECISION,
  swapped          BOOLEAN
) AS $$
    DECLARE
        league_id INTEGER := 0;
        player_id INTEGER := 0;
        gamertag VARCHAR := '';
        team1_gts VARCHAR[] := Array[]::VARCHAR[];
        team2_gts VARCHAR[] := Array[]::VARCHAR[];
        team1_ids INTEGER[] := Array[]::INTEGER[];
        team2_ids INTEGER[] := Array[]::INTEGER[];
    BEGIN
        -- Remove empty gamertags
        SELECT COALESCE(array_agg(x), array[]::varchar[])
            INTO team1_gts FROM unnest(team1) AS x WHERE x <> '' AND x <> '-' AND x <> '_';
        SELECT COALESCE(array_agg(x), array[]::varchar[])
            INTO team2_gts FROM unnest(team2) AS x WHERE x <> '' AND x <> '-' AND x <> '_';

        -- Get player ids of team 1
        FOREACH gamertag IN ARRAY team1_gts
        LOOP
            SELECT p_id INTO player_id
                FROM player
                WHERE p_gamertag ILIKE gamertag;
            IF NOT FOUND THEN
                RAISE NOTICE 'Could not find player %', gamertag;
                RETURN;
            END IF;
            team1_ids := array_append(team1_ids, player_id);
        END LOOP;

        -- Get player ids of team 2
        FOREACH gamertag IN ARRAY team2_gts
        LOOP
            SELECT p_id INTO player_id
                FROM player
                WHERE p_gamertag ILIKE gamertag;
            IF NOT FOUND THEN
                RAISE NOTICE 'Could not find player %', gamertag;
                RETURN;
            END IF;
            team2_ids := array_append(team2_ids, player_id);
        END LOOP;

        -- Sort player ids
        SELECT array_agg(x) INTO team1_ids FROM (SELECT unnest(team1_ids) AS x ORDER BY x) d;
        SELECT array_agg(x) INTO team2_ids FROM (SELECT unnest(team2_ids) AS x ORDER BY x) d;

        -- Team 2 size invalid?
        IF array_length(team2_ids, 1) > 0 AND array_length(team2_ids, 1) <> array_length(team1_ids, 1) THEN
            RAISE NOTICE 'Size of team 2 invalid';
            RETURN;
        END IF;

        IF array_length(team2_ids, 1) IS NULL THEN
            -- Versus everyone

            -- Pad team ids
            team1_ids := array_append(team1_ids, NULL);
            team1_ids := array_append(team1_ids, NULL);
            team1_ids := array_append(team1_ids, NULL);
            team2_ids := array_append(team2_ids, NULL);
            team2_ids := array_append(team2_ids, NULL);
            team2_ids := array_append(team2_ids, NULL);

            -- Everywhere
            RETURN QUERY
            (
                SELECT *, FALSE FROM team_encounter
                    WHERE te_start_date > NOW() - interval_
                    AND te_t1_p1_id = team1_ids[1]
                    AND (te_t1_p2_id = team1_ids[2] OR (te_t1_p2_id IS NULL AND team1_ids[2] IS NULL))
                    AND (te_t1_p3_id = team1_ids[3] OR (te_t1_p3_id IS NULL AND team1_ids[3] IS NULL))
                UNION ALL
                SELECT *, TRUE FROM team_encounter
                    WHERE te_start_date > NOW() - interval_
                    AND te_t2_p1_id = team1_ids[1]
                    AND (te_t2_p2_id = team1_ids[2] OR (te_t2_p2_id IS NULL AND team1_ids[2] IS NULL))
                    AND (te_t2_p3_id = team1_ids[3] OR (te_t2_p3_id IS NULL AND team1_ids[3] IS NULL))
            );
        ELSE
            -- Versus team

            -- Pad team ids
            team1_ids := array_append(team1_ids, NULL);
            team1_ids := array_append(team1_ids, NULL);
            team1_ids := array_append(team1_ids, NULL);
            team2_ids := array_append(team2_ids, NULL);
            team2_ids := array_append(team2_ids, NULL);
            team2_ids := array_append(team2_ids, NULL);

            RAISE NOTICE 'Querying matches of team [%, %, %] vs team [%, %, %]',
                team1_ids[1], team1_ids[2], team1_ids[3],
                team2_ids[1], team2_ids[2], team2_ids[3];

            -- Run query
            RETURN QUERY
            (
                SELECT *, FALSE FROM team_encounter
                    WHERE te_start_date > NOW() - interval_
                    AND te_t1_p1_id = team1_ids[1]
                    AND (te_t1_p2_id = team1_ids[2] OR (te_t1_p2_id IS NULL AND team1_ids[2] IS NULL))
                    AND (te_t1_p3_id = team1_ids[3] OR (te_t1_p3_id IS NULL AND team1_ids[3] IS NULL))
                    AND (te_t2_p1_id = team2_ids[1] OR (te_t2_p1_id IS NULL AND team2_ids[1] IS NULL))
                    AND (te_t2_p2_id = team2_ids[2] OR (te_t2_p2_id IS NULL AND team2_ids[2] IS NULL))
                    AND (te_t2_p3_id = team2_ids[3] OR (te_t2_p3_id IS NULL AND team2_ids[3] IS NULL))
                UNION ALL
                SELECT *, TRUE FROM team_encounter
                    WHERE te_start_date > NOW() - interval_
                    AND te_t1_p1_id = team2_ids[1]
                    AND (te_t1_p2_id = team2_ids[2] OR (te_t1_p2_id is NULL AND team2_ids[2] IS NULL))
                    AND (te_t1_p3_id = team2_ids[3] OR (te_t1_p3_id is NULL AND team2_ids[3] IS NULL))
                    AND (te_t2_p1_id = team1_ids[1] OR (te_t2_p1_id is NULL AND team1_ids[1] IS NULL))
                    AND (te_t2_p2_id = team1_ids[2] OR (te_t2_p2_id is NULL AND team1_ids[2] IS NULL))
                    AND (te_t2_p3_id = team1_ids[3] OR (te_t2_p3_id is NULL AND team1_ids[3] IS NULL))
            );
        END IF;
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
    duration INTERVAL,
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
            te_duration,
			te_map_uuid,
			te_match_uuid,
			te_playlist_uuid,
			(CASE WHEN te_match_outcome = 1 THEN true ELSE false END),
			((te_t1_p1_id IS NOT NULL)::int + (te_t1_p2_id IS NOT NULL)::int + (te_t1_p3_id IS NOT NULL)::int)
		FROM team_encounter
		WHERE (
			te_t1_p1_id = player_id
			OR te_t1_p2_id = player_id
			OR te_t1_p3_id = player_id
		)
		AND te_start_date > (NOW() - target_interval)
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
            te_duration,
			te_map_uuid,
			te_match_uuid,
			te_playlist_uuid,
			(CASE WHEN te_match_outcome = 2 THEN true ELSE false END),
			((te_t2_p1_id IS NOT NULL)::int + (te_t2_p2_id IS NOT NULL)::int + (te_t2_p3_id IS NOT NULL)::int)
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
	ORDER BY te_start_date DESC
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION didact_player_match_data(IN player_id INTEGER, IN player_name VARCHAR, IN target_interval INTERVAL)
RETURNS TABLE (
    match_id INTEGER,
    match_uuid UUID,
    start_date TIMESTAMP,
    duration INTERVAL,
    map_name VARCHAR,
    is_win BOOLEAN,
    team_size INTEGER,
    p11_gamertag VARCHAR,
    p12_gamertag VARCHAR,
    p13_gamertag VARCHAR,
    p21_gamertag VARCHAR,
    p22_gamertag VARCHAR,
    p23_gamertag VARCHAR,
    csr_prev INTEGER,
    csr_new INTEGER,
    mmr_prev DOUBLE PRECISION,
    mmr_new DOUBLE PRECISION,
    leader_name VARCHAR,
    playlist_mode VARCHAR,
    playlist_ranking VARCHAR
) AS $$
    WITH matches AS (
		SELECT *
		FROM didact_player_matches(player_id, target_interval)
		LIMIT 10000
	)
	SELECT
		m.match_id,
		m.match_uuid,
		m.start_date,
		m.duration,
		mm.mm_name,
		m.is_win,
		m.team_size,
		p11.p_gamertag,
		p12.p_gamertag,
		p13.p_gamertag,
		p21.p_gamertag,
		p22.p_gamertag,
		p23.p_gamertag,
		(
			CASE
				WHEN mp_csr_prev_designation < 6
				THEN (
					((mp_csr_prev_designation - 1) * 300) +
					(mp_csr_prev_tier * 50) +
					(mp_csr_prev_percent_tier / 100.0) * 50
				)::INTEGER
				ELSE mp_csr_prev_raw
			END
		) AS csr_prev,
		(
			CASE
				WHEN mp_csr_new_designation < 6
				THEN (
					((mp_csr_new_designation - 1) * 300) +
					(mp_csr_new_tier * 50) +
					(mp_csr_new_percent_tier / 100.0) * 50
				)::INTEGER
				ELSE mp_csr_new_raw
			END
		) AS csr_new,
		mp.mp_mmr_prev_rating,
		mp.mp_mmr_new_rating,
		ml.ml_name,
        mpl.mpl_game_mode,
        mpl.mpl_ranking
	FROM matches m
	LEFT OUTER JOIN player p11 ON m.t1_p1_id = p11.p_id
	LEFT OUTER JOIN player p12 ON m.t1_p2_id = p12.p_id
	LEFT OUTER JOIN player p13 ON m.t1_p3_id = p13.p_id
	LEFT OUTER JOIN player p21 ON m.t2_p1_id = p21.p_id
	LEFT OUTER JOIN player p22 ON m.t2_p2_id = p22.p_id
	LEFT OUTER JOIN player p23 ON m.t2_p3_id = p23.p_id
	LEFT OUTER JOIN meta_map mm ON mm.mm_uuid::UUID = m.map_uuid
	CROSS JOIN LATERAL (SELECT * FROM match_player mp WHERE mp.mp_match_id = m.match_id AND mp.mp_gamertag = player_name LIMIT 1) mp
	LEFT OUTER JOIN meta_leader ml ON ml.ml_id = mp.mp_leader_id
    LEFT OUTER JOIN meta_playlist mpl ON mpl.mpl_uuid::UUID = m.playlist_uuid
	ORDER BY m.start_date DESC
$$ LANGUAGE sql STABLE;
