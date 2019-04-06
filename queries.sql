-- FUNCTIONS

SELECT oid::regprocedure FROM pg_proc WHERE proname like 'didact%' AND pg_function_is_visible(oid);

-- MATCHES

SELECT
    date_trunc('week', m.m_start_date) AS week,
    mpl.mpl_game_mode AS game_mode,
    mpl.mpl_ranking AS ranking,
    mpl.mpl_team_size AS team_size,
    COUNT(*) as matches
FROM match m, meta_playlist mpl
WHERE m.m_playlist_uuid = mpl.mpl_uuid
AND m.m_start_date >= make_date(2018, 8, 20)
AND m.m_start_date <= make_date(2018, 10, 1) 
GROUP BY week, mpl.mpl_game_mode, mpl.mpl_ranking, mpl.mpl_team_size

-- BUILD ORDERS

SELECT
    tag,
    make_interval(secs := (time_since_start * 1.0) / 1000.0) as time_since_start,
    player_idx,
    instance,
    COALESCE(mo_name, target) AS target,
    supply,
    energy,
    mp.mp_team_id AS team_id
FROM
    didact_match_events_build_order({{filter_values("match_id", 17167115)[0]}})
    INNER JOIN match_player mp
        ON mp.mp_player_idx = player_idx
    LEFT OUTER JOIN meta_object mo
        ON mo.mo_id = lower(target)
WHERE mp.mp_match_id = {{filter_values("match_id", 17167115)[0]}}
AND mo_name != 'Scatter Bomb'
ORDER BY time_since_start ASC

-- RESOURCE RATES

WITH frame_data_ AS (
    SELECT
        make_interval(secs := (time_since_start * 1.0) / 1000.0) as time_since_start,
        player_idx,
        mp.mp_team_id AS team_id,
        first_value(time_since_start) OVER (PARTITION BY player_idx ORDER BY time_since_start ASC ROWS BETWEEN 2 PRECEDING and CURRENT ROW) AS frame_time_lb,
        first_value(total_supply) OVER (PARTITION BY player_idx ORDER BY time_since_start ASC ROWS BETWEEN 2 PRECEDING and CURRENT ROW) AS frame_supply_lb,
        first_value(total_energy) OVER (PARTITION BY player_idx ORDER BY time_since_start ASC ROWS BETWEEN 2 PRECEDING and CURRENT ROW) AS frame_energy_lb,
        last_value(time_since_start) OVER (PARTITION BY player_idx ORDER BY time_since_start ASC ROWS BETWEEN CURRENT ROW and 2 FOLLOWING) AS frame_time_ub,
        last_value(total_supply) OVER (PARTITION BY player_idx ORDER BY time_since_start ASC ROWS BETWEEN CURRENT ROW and 2 FOLLOWING) AS frame_supply_ub,
        last_value(total_energy) OVER (PARTITION BY player_idx ORDER BY time_since_start ASC ROWS BETWEEN CURRENT ROW and 2 FOLLOWING) AS frame_energy_ub
    FROM
        didact_match_events_resource_heartbeats({{filter_values("match_id", 17167115)[0]}}),
        match_player mp
    WHERE mp.mp_match_id = {{filter_values("match_id", 17167115)[0]}}
    AND mp.mp_player_idx = player_idx
    ORDER BY time_since_start ASC
)
SELECT
    time_since_start,
    player_idx,
    team_id,
    (frame_time_ub - frame_time_lb) AS frame_duration,
    CASE WHEN (frame_time_ub - frame_time_lb) > 0 THEN (frame_supply_ub - frame_supply_lb) / (frame_time_ub - frame_time_lb) * 1000.0 ELSE 0 END as supply_rate,
    CASE WHEN (frame_time_ub - frame_time_lb) > 0 THEN (frame_energy_ub - frame_energy_lb) / (frame_time_ub - frame_time_lb) * 1000.0 ELSE 0 END as energy_rate
FROM frame_data_


-- UNIT KD

WITH death_events AS (
    SELECT
        (d.death->>'VictimPlayerIndex')::INTEGER AS victim_player_idx,
        d.death->>'VictimInstanceId' AS victim_instance_id,
        d.death->>'VictimObjectTypeId' AS victim_object_type_id,
        p.key::INTEGER AS participant_player_idx,
        op.key AS participant_object_type_id
    FROM
        (
            SELECT jsonb_array_elements(me_death) death
            FROM match_events
            WHERE me_match_id = {{filter_values("match_id", 17167115)[0]}}
        ) d,
        jsonb_each(d.death->'Participants') p,
        jsonb_each(p.value->'ObjectParticipants') op,
        jsonb_each(op.value->'CombatStats') cs
    GROUP BY
        victim_player_idx,
        victim_instance_id,
        victim_object_type_id,
        participant_player_idx,
        participant_object_type_id
), unit_trained AS (
    SELECT
        t."TimeSinceStartMilliseconds" AS time_since_start,
        t."PlayerIndex" AS player_idx,
        t."InstanceId" AS instance_id,
        t."SquadId" AS squad_id,
        t."SupplyCost" AS supply_cost,
        t."EnergyCost" AS energy_cost
    FROM
        (
            SELECT me_unit_trained
            FROM match_events
            WHERE me_match_id = {{filter_values("match_id", 17167115)[0]}}
        ) d,
        jsonb_to_recordset(d.me_unit_trained) AS t(
            "TimeSinceStartMilliseconds" INTEGER,
            "PlayerIndex" INTEGER,
            "InstanceId" INTEGER,
            "SquadId" VARCHAR,
            "SupplyCost" INTEGER,
            "EnergyCost" INTEGER
        )
), death_type_participations AS (
    SELECT
        participant_player_idx AS player_idx,
        participant_object_type_id AS object_type_id,
        COUNT(*) AS participations
    FROM death_events
    GROUP BY participant_player_idx, participant_object_type_id
), unit_type_built AS (
    SELECT player_idx, squad_id AS object_type_id, COUNT(*) AS built
    FROM unit_trained
    GROUP BY player_idx, squad_id
)
SELECT
    mp.mp_team_id AS team_id,
    u.player_idx,
    COALESCE(mo.mo_name, u.object_type_id) AS object_type_id,
    u.built AS built,
    COALESCE(d.participations, 0) as participations,
    COALESCE(d.participations, 0) * 1.0 / built as efficiency
FROM unit_type_built u
LEFT OUTER JOIN death_type_participations d
    ON u.player_idx = d.player_idx
    AND u.object_type_id = d.object_type_id
INNER JOIN match_player mp
    ON mp.mp_match_id = {{filter_values("match_id", 17167115)[0]}}
    AND mp.mp_player_idx = u.player_idx
LEFT OUTER JOIN meta_object mo
    ON mo.mo_id = lower(u.object_type_id)

-- RESOURCE HEARTBEATS

SELECT
    make_interval(secs := (time_since_start * 1.0) / 1000.0) as time_since_start,
    player_idx,
    supply,
    energy,
    population,
    population_cap,
    tech_level,
    command_points,
    total_supply,
    total_energy,
    total_command_points,
    command_xp,
    mp.mp_team_id AS team_id
FROM
    didact_match_events_resource_heartbeats({{filter_values("match_id", 17167115)[0]}}),
    match_player mp
WHERE mp.mp_match_id = {{filter_values("match_id", 17167115)[0]}}
AND mp.mp_player_idx = player_idx
ORDER BY time_since_start ASC

-- MATCH PLAYERS

WITH match_players_ AS (
    SELECT
        mp_team_id AS team_id,
        mp_player_idx AS player_idx,
        mp_gamertag AS gamertag,
        ml_name AS leader,
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
        ) AS csr_prev_raw,
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
        ) AS csr_new_raw,
        mp_mmr_prev_rating AS mmr_prev_rating,
        mp_mmr_new_rating AS mmr_new_rating
    FROM
        match,
        match_player,
        meta_leader
    WHERE m_id = {{filter_values("match_id", 17167115)[0]}}
    AND mp_match_id = m_id
    AND mp_leader_id = ml_id
), annotated_match_players_ AS (
    SELECT
        *,
        (csr_new_raw - csr_prev_raw) AS csr_delta,
        (mmr_new_rating - mmr_prev_rating) AS mmr_delta,
        rank() OVER (PARTITION BY team_id ORDER BY mmr_prev_rating DESC) mmr_team_rank,
        avg(mmr_prev_rating) OVER (PARTITION BY team_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) mmr_team_avg
    FROM match_players_
)
SELECT * FROM annotated_match_players_
ORDER BY player_idx

-- DEATHS

WITH deaths AS (
    SELECT jsonb_array_elements(me_death) death
    FROM match_events
    WHERE me_match_id = {{filter_values("match_id", 17167115)[0]}}
)
SELECT
    d.death->'VictimLocation'->>'x' as location_x,
    d.death->'VictimLocation'->>'y' as location_y,
    d.death->'VictimLocation'->>'z' as location_z,
    (d.death->>'VictimPlayerIndex')::INTEGER as victim_player_idx,
    d.death->>'VictimInstanceId' as victim_instance_id,
    d.death->>'VictimObjectTypeId' as victim_object_type_id,
    d.death->>'IsSuicide' as is_suicide,
    p.key AS participant_player_idx,
    op.key AS participant_object_type_id,
    cs.key AS participant_damage_type,
    (cs.value->'AttacksLanded') AS participant_attacks_landed
FROM
    deaths d,
    jsonb_each(d.death->'Participants') p,
    jsonb_each(p.value->'ObjectParticipants') op,
    jsonb_each(op.value->'CombatStats') cs

-- PLAYER TEAMS

WITH history_ AS (
    SELECT mh.*
    FROM match_history mh
    WHERE mh.mh_player_id = '{{filter_values("player_id", "2524")[0]}}'
    AND mh.mh_match_start_date > NOW() - INTERVAL '90 days'
), matches_ AS (
    SELECT *
    FROM history_ h, match m
    WHERE h.mh_match_uuid = m.m_match_uuid
), team_candidates_ AS (
    SELECT te_t1_id AS tc_id, (CASE WHEN te_match_outcome = 1 THEN 1 ELSE 0 END) as tc_win, te_duration as tc_duration
    FROM matches_ m, team_encounter te
    WHERE m.m_id = te.te_match_id

    UNION ALL

    SELECT te_t2_id AS tc_id, (CASE WHEN te_match_outcome != 1 THEN 1 ELSE 0 END) as tc_win, te_duration as tc_duration
    FROM matches_ m, team_encounter te
    WHERE m.m_id = te.te_match_id
), team_stats_ AS (
    SELECT
        t.t_id,
        t.t_p1_id,
        t.t_p2_id,
        t.t_p3_id,
        count(*) AS matches,
        sum(tc_win) AS wins,
        round(sum(tc_win) * 1.0 / count(*), 3) as win_ratio,
        sum(tc_duration) AS duration
    FROM team_candidates_ tc, team t
    WHERE t.t_id = tc.tc_id
    AND (
        t.t_p1_id = '{{filter_values("player_id", "2524")[0]}}'
        OR t.t_p2_id = '{{filter_values("player_id", "2524")[0]}}'
        OR t.t_p3_id = '{{filter_values("player_id", "2524")[0]}}'
    )
    GROUP BY t.t_id, t.t_p1_id, t.t_p2_id, t.t_p3_id
    ORDER BY matches DESC
    LIMIT 100
)
SELECT
    ts.t_id AS team_id,
    p1.p_gamertag AS player1,
    p2.p_gamertag AS player2,
    p3.p_gamertag AS player3,
    ts.matches,
    ts.wins,
    ts.win_ratio,
    ts.duration,
    CONCAT('<a href="https://www.didact.io/superset/dashboard/hw2-team-profile/?preselect_filters=%7B%22%2A%22%3A%7B%22team_id%22%3A%5B%22',
           ts.t_id,
           '%22%5D%7D%7D&standalone=true">Team Profile</a>'
    ) AS profile_link
FROM team_stats_ ts, player p1, player p2, player p3
WHERE ts.t_p1_id = p1.p_id
AND ts.t_p2_id = p2.p_id
AND ts.t_p3_id = p3.p_id

-- PLAYER MATCHES

WITH history_ AS (
    SELECT mh.*
    FROM match_history mh
    WHERE mh.mh_player_id = '{{filter_values("player_id", "2524")[0]}}'
    AND mh.mh_match_start_date > NOW() - INTERVAL '90 days'
), matches_ AS (
    SELECT *
    FROM history_ h, match m, team_encounter te
    WHERE h.mh_match_uuid = m.m_match_uuid
    AND m.m_id = te.te_match_id
), data_ AS (
    SELECT 
        m.mh_player_match_outcome AS match_outcome,
        m.m_id AS match_id,
        m.m_match_uuid AS match_uuid,
        m.m_playlist_uuid AS playlist_uuid,
        m.mh_match_start_date AS match_start_date,
        m.m_duration AS match_duration,
        m.te_t1_id AS team1,
        m.te_t2_id AS team2,
        p1.p_gamertag AS player1,
        p2.p_gamertag AS player2,
        p3.p_gamertag AS player3,
        p4.p_gamertag AS player4,
        p5.p_gamertag AS player5,
        p6.p_gamertag AS player6,
        mm.mm_name AS map,
        ml.ml_name AS leader
    FROM matches_ m,
        team t1,
        team t2,
        player p1, player p2, player p3,
        player p4, player p5, player p6,
        meta_map mm,
        meta_leader ml
    WHERE m.te_t1_id = t1.t_id
    AND m.te_t2_id = t2.t_id
    AND t1.t_p1_id = p1.p_id
    AND t1.t_p2_id = p2.p_id
    AND t1.t_p3_id = p3.p_id
    AND t2.t_p1_id = p4.p_id
    AND t2.t_p2_id = p5.p_id
    AND t2.t_p3_id = p6.p_id
    AND m.m_map_uuid = mm.mm_uuid
    AND m.mh_leader_id = ml.ml_id
)
SELECT
    d.*,
    COALESCE(mpl.mpl_game_mode, '-') AS game_mode,
    COALESCE(mpl.mpl_team_size, '-') AS team_size,
    COALESCE(mpl.mpl_ranking, '-') AS ranking,
    COALESCE(mpl.mpl_platform, '-') AS platform
FROM data_ d
    LEFT OUTER JOIN meta_playlist mpl
    ON d.playlist_uuid = mpl.mpl_uuid

-- KD

WITH player_ AS (
    SELECT
        mp_player_idx as player_idx,
        concat('T', mp_team_id, 'P', rank() over (partition by mp_team_id order by mp_player_idx)) as player_tag
    FROM match_player
    WHERE mp_match_id = {{match_id}}
    AND mp_team_id = {{team_id}}
), death_events AS (
    SELECT
        (d.death->>'VictimPlayerIndex')::INTEGER AS victim_player_idx,
        d.death->>'VictimInstanceId' AS victim_instance_id,
        d.death->>'VictimObjectTypeId' AS victim_object_type_id,
        p.key::INTEGER AS participant_player_idx,
        op.key AS participant_object_type_id
    FROM
        (
            SELECT jsonb_array_elements(me_death) death
            FROM match_events
            WHERE me_match_id = {{match_id}}
        ) d,
        jsonb_each(d.death->'Participants') p,
        jsonb_each(p.value->'ObjectParticipants') op,
        jsonb_each(op.value->'CombatStats') cs
    GROUP BY
        victim_player_idx,
        victim_instance_id,
        victim_object_type_id,
        participant_player_idx,
        participant_object_type_id
), unit_trained AS (
    SELECT
        t."TimeSinceStartMilliseconds" AS time_since_start,
        t."PlayerIndex" AS player_idx,
        t."InstanceId" AS instance_id,
        t."SquadId" AS squad_id,
        t."SupplyCost" AS supply_cost,
        t."EnergyCost" AS energy_cost
    FROM
        (
            SELECT me_unit_trained
            FROM match_events
            WHERE me_match_id = {{match_id}}
        ) d,
        jsonb_to_recordset(d.me_unit_trained) AS t(
            "TimeSinceStartMilliseconds" INTEGER,
            "PlayerIndex" INTEGER,
            "InstanceId" INTEGER,
            "SquadId" VARCHAR,
            "SupplyCost" INTEGER,
            "EnergyCost" INTEGER
        )
), death_type_participations AS (
    SELECT
        participant_player_idx AS player_idx,
        participant_object_type_id AS object_type_id,
        COUNT(*) AS participations
    FROM death_events
    GROUP BY participant_player_idx, participant_object_type_id
), unit_type_built AS (
    SELECT
        player_idx,
        squad_id AS object_type_id,
        COUNT(*) AS built,
        SUM(supply_cost) AS supply,
        SUM(energy_cost) AS energy
    FROM unit_trained
    GROUP BY player_idx, squad_id
), unit_kd AS (
    SELECT
        p.player_tag,
        COALESCE(mo.mo_name, u.object_type_id) AS object_type_id,
        u.built AS built,
        u.supply AS supply,
        u.energy AS energy,
        COALESCE(d.participations, 0) as attacks
    FROM unit_type_built u
    LEFT OUTER JOIN death_type_participations d
        ON u.player_idx = d.player_idx
        AND u.object_type_id = d.object_type_id
    INNER JOIN player_ p
        ON p.player_idx = u.player_idx
    LEFT OUTER JOIN meta_object mo
        ON mo.mo_id = lower(u.object_type_id)
    WHERE mo.mo_name != 'Scatter Bomb'
)
SELECT * FROM unit_kd
ORDER BY built DESC, supply DESC, energy DESC;

