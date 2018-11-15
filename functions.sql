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
    team_idx INTEGER,
    player_1 INTEGER,
    player_2 INTEGER,
    player_3 INTEGER
) AS $$
    BEGIN
        RETURN QUERY
        WITH x AS (
            SELECT
                p_id AS player_id,
                mp_match_id AS match_id,
                mp_player_idx AS player_idx,
                mp_team_id AS team_id,
                rank() over (partition by mp_match_id, mp_team_id order by p_id asc) as rank
            FROM match_player, player
            WHERE mp_gamertag = p_gamertag
            AND mp_match_id = match_id
        )
        SELECT
            m1.team_id AS team_idx,
            COALESCE(m1.player_id, 0) AS player_1,
            COALESCE(m2.player_id, 0) AS player_2,
            COALESCE(m3.player_id, 0) AS player_3
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

CREATE OR REPLACE FUNCTION didact_store_team_encounter(match_id INTEGER)
RETURNS VOID AS $$
    BEGIN
        WITH match_teams_ AS (
            SELECT *
            FROM didact_match_teams(match_id)
            LIMIT 2
        ), inserted_teams_ AS (
            INSERT INTO team(t_p1_id, t_p2_id, t_p3_id)
            SELECT player_1, player_2, player_3
            FROM match_teams_
            ON CONFLICT DO NOTHING
            RETURNING t_id, t_p1_id, t_p2_id, t_p3_id
        ), team_ids_ AS (
            SELECT
                match_id AS mid,
                mt.team_idx AS team_idx,
                COALESCE(it.t_id, st.t_id, 0) AS team_id
            FROM match_teams_ mt
                LEFT OUTER JOIN inserted_teams_ it
                    ON it.t_p1_id = mt.player_1
                    AND it.t_p2_id = mt.player_2
                    AND it.t_p3_id = mt.player_3
                LEFT OUTER JOIN team st
                    ON st.t_p1_id = mt.player_1
                    AND st.t_p2_id = mt.player_2
                    AND st.t_p3_id = mt.player_3
            LIMIT 2
        )
        INSERT INTO team_encounter(
            te_t1_id,
            te_t2_id,
            te_match_id,
            te_start_date,
            te_duration,
            te_match_outcome,
            te_map_uuid,
            te_match_uuid,
            te_playlist_uuid,
            te_season_uuid
        )
        SELECT
            t1.team_id,
            t2.team_id,
            m.m_id,
            m.m_start_date,
            m.m_duration,
            mt.mt_match_outcome,
            m.m_map_uuid,
            m.m_match_uuid,
            m.m_playlist_uuid,
            m.m_season_uuid
        FROM match m, match_team mt, team_ids_ t1, team_ids_ t2
        WHERE m.m_id = match_id
        AND mt.mt_match_id = match_id
        AND mt.mt_team_id = 1
        AND t1.mid = match_id
        AND t2.mid = match_id
        AND t1.team_idx = 1
        AND t2.team_idx = 2
        ON CONFLICT DO NOTHING;
    END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION didact_store_team_encounters()
RETURNS VOID AS $$
  DECLARE
      t TIMESTAMP := clock_timestamp();
  BEGIN
      RAISE NOTICE 'Storing team encounters';
      WITH match_player_ AS (
          SELECT
              p_id AS player_id,
              mp_match_id AS match_id,
              mp_player_idx AS player_idx,
              mp_team_id AS team_id,
              rank() over (partition by mp_match_id, mp_team_id order by p_id asc) as rank
          FROM match_player
            LEFT OUTER JOIN team_encounter ON mp_match_id = te_match_id
            INNER JOIN player ON mp_gamertag = p_gamertag
            WHERE te_match_id IS NULL
      ), match_teams_ AS (
          SELECT
              m1.match_id AS match_id,
              m1.team_id AS team_idx,
              COALESCE(m1.player_id, 0) AS player_1,
              COALESCE(m2.player_id, 0) AS player_2,
              COALESCE(m3.player_id, 0) AS player_3
          FROM match_player_ m1
              LEFT OUTER JOIN match_player_ m2
                  ON m1.match_id = m2.match_id
                  AND m1.team_id = m2.team_id
                  AND m2.rank = 2
              LEFT OUTER JOIN match_player_ m3
                  ON m1.match_id = m3.match_id
                  AND m1.team_id = m3.team_id
                  AND m3.rank = 3
          WHERE m1.rank = 1
      ), inserted_teams_ AS (
          INSERT INTO team(t_p1_id, t_p2_id, t_p3_id)
          SELECT player_1, player_2, player_3
          FROM match_teams_
          ON CONFLICT DO NOTHING
          RETURNING t_id, t_p1_id, t_p2_id, t_p3_id
      ), team_ids_ AS (
          SELECT
              mt.match_id AS mid,
              mt.team_idx AS team_idx,
              COALESCE(it.t_id, st.t_id, 0) AS team_id
          FROM match_teams_ mt
              LEFT OUTER JOIN inserted_teams_ it
                  ON it.t_p1_id = mt.player_1
                  AND it.t_p2_id = mt.player_2
                  AND it.t_p3_id = mt.player_3
              LEFT OUTER JOIN team st
                  ON st.t_p1_id = mt.player_1
                  AND st.t_p2_id = mt.player_2
                  AND st.t_p3_id = mt.player_3
      )
      INSERT INTO team_encounter(
          te_t1_id,
          te_t2_id,
          te_match_id,
          te_start_date,
          te_duration,
          te_match_outcome,
          te_map_uuid,
          te_match_uuid,
          te_playlist_uuid,
          te_season_uuid
      )
      SELECT
          t1.team_id,
          t2.team_id,
          m.m_id,
          m.m_start_date,
          m.m_duration,
          mt.mt_match_outcome,
          m.m_map_uuid,
          m.m_match_uuid,
          m.m_playlist_uuid,
          m.m_season_uuid
      FROM match m, match_team mt, team_ids_ t1, team_ids_ t2
      WHERE m.m_id = mt.mt_match_id
      AND mt.mt_team_id = 1
      AND t1.mid = m.m_id
      AND t2.mid = m.m_id
      AND t1.team_idx = 1
      AND t2.team_idx = 2
      ON CONFLICT DO NOTHING;
      RAISE NOTICE 'Duration=%', clock_timestamp() - t;
    END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION didact_store_player_team_encounters(player_id INTEGER)
RETURNS VOID AS $$
  DECLARE
      t TIMESTAMP := clock_timestamp();
  BEGIN
      RAISE NOTICE 'Storing player team encounters';
      WITH match_player_ AS (
          SELECT
              p_id AS player_id,
              mp_match_id AS match_id,
              mp_player_idx AS player_idx,
              mp_team_id AS team_id,
              rank() over (partition by mp_match_id, mp_team_id order by p_id asc) as rank
          FROM match_player
            LEFT OUTER JOIN team_encounter ON mp_match_id = te_match_id
            INNER JOIN player ON mp_gamertag = p_gamertag
          WHERE te_match_id IS NULL
          AND p_id = player_id
      ), match_teams_ AS (
          SELECT
              m1.match_id AS match_id,
              m1.team_id AS team_idx,
              COALESCE(m1.player_id, 0) AS player_1,
              COALESCE(m2.player_id, 0) AS player_2,
              COALESCE(m3.player_id, 0) AS player_3
          FROM match_player_ m1
              LEFT OUTER JOIN match_player_ m2
                  ON m1.match_id = m2.match_id
                  AND m1.team_id = m2.team_id
                  AND m2.rank = 2
              LEFT OUTER JOIN match_player_ m3
                  ON m1.match_id = m3.match_id
                  AND m1.team_id = m3.team_id
                  AND m3.rank = 3
          WHERE m1.rank = 1
      ), inserted_teams_ AS (
          INSERT INTO team(t_p1_id, t_p2_id, t_p3_id)
          SELECT player_1, player_2, player_3
          FROM match_teams_
          ON CONFLICT DO NOTHING
          RETURNING t_id, t_p1_id, t_p2_id, t_p3_id
      ), team_ids_ AS (
          SELECT
              mt.match_id AS mid,
              mt.team_idx AS team_idx,
              COALESCE(it.t_id, st.t_id, 0) AS team_id
          FROM match_teams_ mt
              LEFT JOIN inserted_teams_ it
                  ON it.t_p1_id = mt.player_1
                  AND it.t_p2_id = mt.player_2
                  AND it.t_p3_id = mt.player_3
              LEFT JOIN team st
                  ON st.t_p1_id = mt.player_1
                  AND st.t_p2_id = mt.player_2
                  AND st.t_p3_id = mt.player_3
      )
      INSERT INTO team_encounter(
          te_t1_id,
          te_t2_id,
          te_match_id,
          te_start_date,
          te_duration,
          te_match_outcome,
          te_map_uuid,
          te_match_uuid,
          te_playlist_uuid,
          te_season_uuid
      )
      SELECT
          t1.team_id,
          t2.team_id,
          m.m_id,
          m.m_start_date,
          m.m_duration,
          mt.mt_match_outcome,
          m.m_map_uuid,
          m.m_match_uuid,
          m.m_playlist_uuid,
          m.m_season_uuid
      FROM match m, match_team mt, team_ids_ t1, team_ids_ t2
      WHERE m.m_id = mt.mt_match_id
      AND mt.mt_team_id = 1
      AND t1.mid = m.m_id
      AND t2.mid = m.m_id
      AND t1.team_idx = 1
      AND t2.team_idx = 2
      ON CONFLICT DO NOTHING;
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
-- REFRESH SPECIFIC MATERIALIZED VIEWS
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION didact_update_leaderboard()
RETURNS INT AS $$
    DECLARE
        t TIMESTAMP := clock_timestamp();
    BEGIN
        RAISE NOTICE 'Refreshing public.mv_leaderboard';
        EXECUTE 'REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_leaderboard';
        RAISE NOTICE 'Duration=%', clock_timestamp() - t;
        RETURN 1;
    END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION didact_data_maintenance()
RETURNS INT AS $$
    DECLARE
        t TIMESTAMP := clock_timestamp();
    BEGIN
        RAISE NOTICE 'Refreshing public.mv_player_to_match_player';
        EXECUTE 'REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_player_to_match_player';
        RAISE NOTICE 'Duration=%', clock_timestamp() - t;

        t := clock_timestamp();
        RAISE NOTICE 'Refreshing public.mv_player_encounters';
        EXECUTE 'REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_player_encounters';
        RAISE NOTICE 'Duration=%', clock_timestamp() - t;

        t := clock_timestamp();
        RAISE NOTICE 'Refreshing public.mv_team_gamertags';
        EXECUTE 'REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_team_gamertags';
        RAISE NOTICE 'Duration=%', clock_timestamp() - t;
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
-- COMMUNITIES
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION didact_community_join(c VARCHAR, gt VARCHAR)
RETURNS INTEGER AS $$
    DECLARE
        player_id INTEGER := 0;
        community_id INTEGER := 0;
    BEGIN
        SELECT c_id INTO community_id FROM community WHERE c_name = c;
        IF NOT FOUND THEN
            RAISE NOTICE 'community % not found', c;
            RETURN -1;
        END IF;

        SELECT p_id INTO player_id FROM player WHERE p_gamertag = gt;
        IF NOT FOUND THEN
            RAISE NOTICE 'player % not found', gt;
            RETURN -1;
        END IF;

        INSERT INTO community_member(cm_community_id, cm_player_id, cm_joined_at)
            VALUES (community_id, player_id, now())
            ON CONFLICT DO NOTHING;

        RAISE NOTICE 'added player % to community %', gt, c;
        RETURN 1;
    END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION didact_community_join_league_1v1(c VARCHAR, l VARCHAR, gt VARCHAR)
RETURNS INTEGER AS $$
    DECLARE
        community_id INTEGER := 0;
        league_id INTEGER := 0;
        league_size INTEGER := 0;
        player_id INTEGER := 0;
        team_id INTEGER := 0;
    BEGIN
        SELECT c_id INTO community_id FROM community WHERE c_name = c;
        IF NOT FOUND THEN
            RAISE NOTICE 'community % not found', c;
            RETURN -1;
        END IF;

        SELECT cl_id, cl_team_size INTO league_id, league_size FROM community_league WHERE cl_community_id = community_id AND cl_name = l;
        IF NOT FOUND THEN
            RAISE NOTICE 'league % not found', l;
            RETURN -1;
        END IF;
        IF league_size <> 1 THEN
            RAISE NOTICE 'league % is not a 1v1 league', l;
            RETURN -1;
        END IF;

        SELECT p_id INTO player_id FROM player WHERE p_gamertag = gt;
        IF NOT FOUND THEN
            RAISE NOTICE 'player % not found', gt;
            RETURN -1;
        END IF;

        SELECT t_id INTO team_id FROM team WHERE t_p1_id = player_id;
        IF NOT FOUND THEN
            RAISE NOTICE 'team for player % not found', gt;
            RETURN -1;
        END IF;

        INSERT INTO community_member(cm_community_id, cm_player_id, cm_joined_at)
            VALUES (community_id, player_id, now())
            ON CONFLICT DO NOTHING;

        INSERT INTO community_league_team(clp_community_id, clp_league_id, clp_team_id, clp_joined_at)
            VALUES (community_id, league_id, team_id, now())
            ON CONFLICT DO NOTHING;

        RAISE NOTICE 'added team % of player % to league % of community %', team_id, gt, l, c;
        RETURN 1;
    END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION didact_community_join_league_2v2(c VARCHAR, l VARCHAR, gt1 VARCHAR, gt2 VARCHAR)
RETURNS INTEGER AS $$
    DECLARE
        community_id INTEGER := 0;
        league_id INTEGER := 0;
        league_size INTEGER := 0;
        player1_id INTEGER := 0;
        player2_id INTEGER := 0;
        team_id INTEGER := 0;
    BEGIN
        SELECT c_id INTO community_id FROM community WHERE c_name = c;
        IF NOT FOUND THEN
            RAISE NOTICE 'community % not found', c;
            RETURN -1;
        END IF;

        SELECT cl_id, cl_team_size INTO league_id, league_size FROM community_league WHERE cl_community_id = community_id AND cl_name = l;
        IF NOT FOUND THEN
            RAISE NOTICE 'league % not found', l;
            RETURN -1;
        END IF;
        IF league_size <> 2 THEN
            RAISE NOTICE 'league % is not a 2v2 league', l;
            RETURN -1;
        END IF;

        SELECT p_id INTO player1_id FROM player WHERE p_gamertag = gt1;
        IF NOT FOUND THEN
            RAISE NOTICE 'player1 % not found', gt1;
            RETURN -1;
        END IF;

        SELECT p_id INTO player2_id FROM player WHERE p_gamertag = gt2;
        IF NOT FOUND THEN
            RAISE NOTICE 'player2 % not found', gt2;
            RETURN -1;
        END IF;

        SELECT t_id INTO team_id FROM team WHERE t_p1_id = player1_id AND t_p2_id = player2_id;
        IF NOT FOUND THEN
            RAISE NOTICE 'team for player1 % and player2 % not found', gt1, gt2;
            RETURN -1;
        END IF;

        INSERT INTO community_member(cm_community_id, cm_player_id, cm_joined_at)
            VALUES (community_id, player1_id, now()), (community_id, player2_id, now())
            ON CONFLICT DO NOTHING;

        INSERT INTO community_league_team(clp_community_id, clp_league_id, clp_team_id, clp_joined_at)
            VALUES (community_id, league_id, team_id, now())
            ON CONFLICT DO NOTHING;

        RAISE NOTICE 'added team % to league % of community %', team_id, l, c;
        RETURN 1;
    END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION didact_community_join_league_3v3(c VARCHAR, l VARCHAR, gt1 VARCHAR, gt2 VARCHAR, gt3 VARCHAR)
RETURNS INTEGER AS $$
    DECLARE
        community_id INTEGER := 0;
        league_id INTEGER := 0;
        league_size INTEGER := 0;
        player1_id INTEGER := 0;
        player2_id INTEGER := 0;
        player3_id INTEGER := 0;
        team_id INTEGER := 0;
    BEGIN
        SELECT c_id INTO community_id FROM community WHERE c_name = c;
        IF NOT FOUND THEN
            RAISE NOTICE 'community % not found', c;
            RETURN -1;
        END IF;

        SELECT cl_id, cl_team_size INTO league_id, league_size FROM community_league WHERE cl_community_id = community_id AND cl_name = l;
        IF NOT FOUND THEN
            RAISE NOTICE 'league % not found', l;
            RETURN -1;
        END IF;
        IF league_size <> 3 THEN
            RAISE NOTICE 'league % is not a 3v3 league', l;
            RETURN -1;
        END IF;

        SELECT p_id INTO player1_id FROM player WHERE p_gamertag = gt1;
        IF NOT FOUND THEN
            RAISE NOTICE 'player1 % not found', gt1;
            RETURN -1;
        END IF;

        SELECT p_id INTO player2_id FROM player WHERE p_gamertag = gt2;
        IF NOT FOUND THEN
            RAISE NOTICE 'player2 % not found', gt2;
            RETURN -1;
        END IF;

        SELECT p_id INTO player3_id FROM player WHERE p_gamertag = gt3;
        IF NOT FOUND THEN
            RAISE NOTICE 'player3 % not found', gt3;
            RETURN -1;
        END IF;

        SELECT t_id INTO team_id FROM team WHERE t_p1_id = player1_id AND t_p2_id = player2_id AND t_p3_id = player3_id;
        IF NOT FOUND THEN
            RAISE NOTICE 'team for player1 %, player2 % and player3 % not found', gt1, gt2, gt3;
            RETURN -1;
        END IF;

        INSERT INTO community_member(cm_community_id, cm_player_id, cm_joined_at)
            VALUES (community_id, player1_id, now()), (community_id, player2_id, now()), (community_id, player3_id, now())
            ON CONFLICT DO NOTHING;

        INSERT INTO community_league_team(clp_community_id, clp_league_id, clp_team_id, clp_joined_at)
            VALUES (community_id, league_id, team_id, now())
            ON CONFLICT DO NOTHING;

        RAISE NOTICE 'added team % to league % of community %', team_id, l, c;
        RETURN 1;
    END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION didact_community_league_matches(IN league_id INTEGER, IN interval_ INTERVAL)
RETURNS TABLE(
    m_id INTEGER
) AS $$
    SELECT te_match_id
    FROM community_league_team t1, community_league_team t2, team_encounter te
    WHERE t1.clp_league_id = t2.clp_league_id
    AND t1.clp_team_id <> t2.clp_team_id
    AND te.te_t1_id = t1.clp_team_id
    AND te.te_t2_id = t2.clp_team_id
    AND te.te_start_date > now() - interval_
$$ LANGUAGE sql;
