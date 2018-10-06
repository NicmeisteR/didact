package core

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/bwmarrin/discordgo"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Bot struct {
	config    *Config
	dataStore *DataStore
	crawler   *Crawler
	session   *discordgo.Session
	channels  map[string]bool
}

func NewBot(config *Config, dataStore *DataStore, crawler *Crawler) *Bot {
	bot := new(Bot)
	bot.config = config
	bot.crawler = crawler
	bot.channels = make(map[string]bool)
	bot.dataStore = dataStore
	return bot
}

// -------------------------------------------------------------------------------------------------------------
// Control
// -------------------------------------------------------------------------------------------------------------

func (bot *Bot) Start() error {
	var err error

	// Create a new Discord session using the provided bot token.
	bot.session, err = discordgo.New("Bot " + bot.config.Bot.Key)
	if err != nil {
		return err
	}

	// Register the messageCreate func as a callback for MessageCreate events.
	bot.session.AddHandler(func(s *discordgo.Session, m *discordgo.MessageCreate) {
		bot.onMessageCreate(s, m)
	})

	bot.session.State.TrackChannels = true

	// Open a websocket connection to Discord and begin listening.
	err = bot.session.Open()
	if err != nil {
		return err
	}
	return nil
}

func (bot *Bot) Stop() {
	if bot.session != nil {
		bot.session.Close()
	}
	bot.session = nil
}

// -------------------------------------------------------------------------------------------------------------
// Messages
// -------------------------------------------------------------------------------------------------------------

// Broadcast a message
func (bot *Bot) broadcast(m string) {
	for cid := range bot.channels {
		bot.session.ChannelMessageSend(cid, m)
	}
}

// Broadcast an embed
func (bot *Bot) broadcastEmbed(m *discordgo.MessageEmbed) {
	for cid := range bot.channels {
		bot.session.ChannelMessageSendEmbed(cid, m)
	}
}

// Broadcast a complex message
func (bot *Bot) broadcastComplex(m *discordgo.MessageSend) {
	for cid := range bot.channels {
		bot.session.ChannelMessageSendComplex(cid, m)
	}
}

// Send a response
func (bot *Bot) sendResponse(m *discordgo.MessageCreate, r string) {
	// withMention := fmt.Sprintf("%s %s", m.Author.Mention(), r)
	bot.session.ChannelMessageSend(m.ChannelID, r)
}

// Mark as typing
func (bot *Bot) markAsTyping(channel string) {
	bot.session.ChannelTyping(channel)

}

// -------------------------------------------------------------------------------------------------------------
// URLs
// -------------------------------------------------------------------------------------------------------------

func (bot *Bot) dashboardURL(dashboardName string, params map[string]string) (*url.URL, error) {
	// Superset filter_values expects filter values wrapped in arrays
	wrappedParams := make(map[string][]string)
	for k, v := range params {
		var p []string
		p = append(p, v)
		wrappedParams[k] = p
	}
	filters := map[string]map[string][]string{
		"*": wrappedParams,
	}

	// Marshall params to string
	jsonBuffer, err := json.Marshal(filters)
	if err != nil {
		return nil, err
	}

	// Build URL
	u, err := url.Parse("https://www.didact.io/superset/dashboard/")
	if err != nil {
		return nil, err
	}
	u.Path += dashboardName
	q := u.Query()
	q.Set("preselect_filters", string(jsonBuffer))
	q.Set("standalone", "true")
	u.RawQuery = q.Encode()
	return u, nil
}

func (bot *Bot) waypointProfileURL(gamertag string) (*url.URL, error) {
	u, err := url.Parse("https://www.halowaypoint.com/en-us/games/halo-wars-2/service-record/players/")
	if err != nil {
		return nil, err
	}
	u.Path += gamertag
	return u, nil
}

// -------------------------------------------------------------------------------------------------------------
// Handlers
// -------------------------------------------------------------------------------------------------------------

// This function will be called (due to AddHandler above) every time a new
// message is created on any channel that the autenticated bot has access to.
func (bot *Bot) onMessageCreate(s *discordgo.Session, m *discordgo.MessageCreate) {

	// Ignore all messages created by the bot itself
	if m.Author.ID == s.State.User.ID {
		return
	}

	// Not addressing the bot?
	msgFields := strings.Fields(m.Content)
	if len(msgFields) < 2 || msgFields[0] != "!didact" {
		return
	}
	cmd := msgFields[1]

	// Extract the argument string
	args := strings.TrimSpace(m.Content)
	args = strings.TrimPrefix(args, "!didact")
	args = strings.TrimLeft(args, " ")
	args = strings.TrimPrefix(args, cmd)
	args = strings.TrimLeft(args, " ")

	// Known didact channel?
	_, ok := bot.channels[m.ChannelID]
	if !ok {
		// Get channel of message
		channel, err := s.State.Channel(m.ChannelID)
		if err != nil {
			bot.sendResponse(m, fmt.Sprintf("I don't know of channel **%s**.", m.ChannelID))
			return
		}

		// Didact channel?
		if channel.Name != "didact" {
			return
		}

		// Remember channel
		bot.channels[m.ChannelID] = true
	}

	// Mark as typing
	bot.markAsTyping(m.ChannelID)

	// Process the command
	switch cmd {
	case "help":
		break
	case "status":
		bot.getStatus(m)
		break
	case "profile":
		bot.getProfile(m, args)
		break

	case "s":
		fallthrough
	case "sp":
		fallthrough
	case "scan-player":
		bot.scanPlayer(m, args)
		break

	case "f":
		fallthrough
	case "fp":
		fallthrough
	case "find-player":
		bot.findPlayer(m, args)
		break

	case "t":
		fallthrough
	case "pt":
		fallthrough
	case "teams":
		fallthrough
	case "player-teams":
		bot.playerTeams(m, args)
		break

	case "history":
		break
	case "match":
		break
	default:
		bot.sendResponse(m, fmt.Sprintf("'%s' looks like nothing to me.", cmd))
	}
}

// -------------------------------------------------------------------------------------------------------------
// Status
// -------------------------------------------------------------------------------------------------------------

func (bot *Bot) getStatus(m *discordgo.MessageCreate) {
	r := &discordgo.MessageEmbed{
		Author:      &discordgo.MessageEmbedAuthor{},
		Color:       0x00ff00,
		Description: "",
		Fields:      []*discordgo.MessageEmbedField{},
		Timestamp:   time.Now().Format(time.RFC3339),
		Title:       "Server Status",
	}
	bot.session.ChannelMessageSendEmbed(m.ChannelID, r)
}

func (bot *Bot) getProfile(m *discordgo.MessageCreate, args string) {
	// desc := fmt.Sprintf("%s", m.Author.Mention())
	// base := "https://www.didact.io/superset/dashboard/hw2-profile/"
	// filter := url.QueryEscape()
	// url := fmt.Sprintf("%s", base)

	// r := &discordgo.MessageEmbed{
	// 	Title:       "Profile",
	// 	Author:      &discordgo.MessageEmbedAuthor{},
	// 	Color:       0x00ff00,
	// 	Description: desc,
	// 	Fields:      []*discordgo.MessageEmbedField{},
	// 	Timestamp:   time.Now().Format(time.RFC3339),
	// }

	// bot.session.ChannelMessageSendEmbed(m.ChannelID, r)
}

// -------------------------------------------------------------------------------------------------------------
// Scan player
// -------------------------------------------------------------------------------------------------------------

func (bot *Bot) scanPlayer(m *discordgo.MessageCreate, args string) {
	// Get the player ids
	argList := strings.Split(args, ",")
	playerIDs := make([]int, 0)
	for _, a := range argList {
		pidStr := strings.Trim(a, " ,\"'")
		if len(pidStr) > 0 {
			pid, err := strconv.Atoi(pidStr)
			if err != nil {
				bot.sendResponse(m, fmt.Sprintf("**%s** does not seem to be a valid player id.", pidStr))
				continue
			}
			playerIDs = append(playerIDs, pid)
		}
	}

	// Find player ids
	gamertags := make([]string, len(playerIDs))
	for i, pid := range playerIDs {
		bot.markAsTyping(m.ChannelID)

		gamertag, err := bot.dataStore.getPlayerGamertag(pid)
		if err != nil {
			bot.sendResponse(m, fmt.Sprintf("I don't know of any player with id **%d**.", pid))
			return
		}
		gamertags[i] = gamertag
		bot.sendResponse(m, fmt.Sprintf("I found player **%d** with gamertag **%s**.", pid, gamertag))
	}

	// Scan the match histories
	for i := range playerIDs {
		playerID := playerIDs[i]
		gamertag := gamertags[i]

		// Figure out where to start
		count := 25
		offset := 0
		newMatches := make([]string, 0)

		bot.sendResponse(m, fmt.Sprintf("I started the match history scan for player **%d**.", playerID))
		for {
			bot.markAsTyping(m.ChannelID)

			// Get the match history
			history, err := bot.crawler.loadMatchHistory(gamertag, offset, count)

			// No such user? (404)
			if err == ErrNotFound {
				bot.sendResponse(m, fmt.Sprintf("The Halo API does not return any data for the player **%d**.", playerID))
				return
			}

			// Hit the rate limit? (429)
			if err == ErrRateLimit {
				bot.sendResponse(m, fmt.Sprintf("I just hit the API rate limit, please repeat the scan of **%d**.", playerID))
				return
			}

			// Try again if there was an unexpected error
			if err != nil {
				bot.sendResponse(m, fmt.Sprintf("Ouch! Something went wrong: %v.", err))
				return
			}

			// Insert all matches into the database
			seenMatches := 0
			foundMatches := 0
			for _, result := range history.Results {
				seenMatches += 1
				row := bot.dataStore.storePlayerMatch(playerID, &result)
				var matchId string
				switch err := row.Scan(&matchId); err {
				case sql.ErrNoRows:
					break
				case nil:
					foundMatches += 1
					newMatches = append(newMatches, result.MatchId)
					break
				default:
					bot.sendResponse(m, fmt.Sprintf("Ouch! Something went wrong: %v.", err))
					break
				}
			}

			// Continue?
			if foundMatches > 0 {
				offset += count
			} else {
				break
			}
		}

		if len(newMatches) == 0 {
			bot.sendResponse(m, fmt.Sprintf("I found no new matches for player **%d**.", playerID))
		} else if len(newMatches) > 0 {
			bot.sendResponse(m, fmt.Sprintf("I found **%d** new match(es) for player **%d** and I will now load the match result(s).", len(newMatches), playerID))
			for _, newMatch := range newMatches {
				bot.updateMatchResult(m, newMatch)
			}
		}
		bot.sendResponse(m, fmt.Sprintf("I finished the match history scan for player **%d**.", playerID))
	}
}

// -------------------------------------------------------------------------------------------------------------
// Find player
// -------------------------------------------------------------------------------------------------------------

// Find a player
func (bot *Bot) findPlayer(m *discordgo.MessageCreate, args string) {
	prefix := strings.Trim(args, " ,\"'")

	bot.markAsTyping(m.ChannelID)

	results, err := bot.dataStore.db.Query(`
		SELECT p_id, p_gamertag
		FROM player
		WHERE p_gamertag ILIKE $1
		LIMIT 10
	`, "%"+prefix+"%")

	if err != nil {
		bot.sendResponse(m, fmt.Sprintf("Ouch! Something went wrong: %v.", err))
		return
	}

	pids := make([]int, 0)
	gamertags := make([]string, 0)

	// Iterate over results
	for results.Next() {
		var pid int
		var gamertag string
		err := results.Scan(&pid, &gamertag)
		if err != nil {
			bot.sendResponse(m, fmt.Sprintf("Ouch! Something went wrong: %v.", err))
			return
		}
		pids = append(pids, pid)
		gamertags = append(gamertags, gamertag)
	}

	fields := []*discordgo.MessageEmbedField{}
	for i, gt := range gamertags {
		pid := pids[i]
		name := fmt.Sprintf("%s", gt)

		pidParam := map[string]string{
			"player_id": strconv.Itoa(pid),
		}

		urlDashboard, _ := bot.dashboardURL("hw2-player-profile", pidParam)
		urlWaypoint, _ := bot.waypointProfileURL(gt)

		value := fmt.Sprintf("Didact ID: **%d**\n[Profile](%s), [Waypoint](%s)", pid, urlDashboard, urlWaypoint)
		fields = append(fields, &discordgo.MessageEmbedField{
			Name:   name,
			Value:  value,
			Inline: true,
		})
	}

	r := &discordgo.MessageEmbed{
		Title:       "Player Search",
		Author:      &discordgo.MessageEmbedAuthor{},
		Color:       0x010101,
		Description: fmt.Sprintf("String: %s, Limit: 10", prefix),
		Fields:      fields,
		Timestamp:   time.Now().Format(time.RFC3339),
	}

	bot.session.ChannelMessageSendEmbed(m.ChannelID, r)
}

// -------------------------------------------------------------------------------------------------------------
// Match events
// -------------------------------------------------------------------------------------------------------------

func (bot *Bot) loadMatchEvents(m *discordgo.MessageCreate, args string) {
	// Get message id
	mid, err := strconv.Atoi(args)
	if err != nil {
		bot.sendResponse(m, fmt.Sprintf("The match id **%d** is invalid.", args))
		return
	}

	// Match events already exist?
	mUUID, err := bot.dataStore.getMatchUUID(mid)
	if err != nil {
		bot.sendResponse(m, fmt.Sprintf("I don't know of any match with id **%d**.", args))
		return
	}
	bot.sendResponse(m, fmt.Sprintf("I found match **%d** with uuid **%s**.", mid, mUUID))

	// Load match events
	mEvents, err := bot.crawler.loadMatchEvents(mUUID)

	// No such match? (404)
	if err == ErrNotFound {
		bot.sendResponse(m, fmt.Sprintf("The Halo API does not return any events for the match **%s**.", mUUID))
		return
	}

	// Hit the rate limit? (429)
	if err == ErrRateLimit {
		bot.sendResponse(m, fmt.Sprintf("I just hit the API rate limit, please request the events again."))
		return
	}

	// Try again if there was an unexpected error
	if err != nil {
		bot.sendResponse(m, fmt.Sprintf("Ouch! Something went wrong: %v.", err))
		return
	}

	// Store match events
	err = bot.dataStore.storeMatchEvents(mid, mEvents)
	if err != nil {
		bot.sendResponse(m, fmt.Sprintf("Ouch! Something went wrong: %v.", err))
	}

	bot.sendResponse(m, fmt.Sprintf("I stored the events for match **%d**.", mid))
	return
}

// -------------------------------------------------------------------------------------------------------------
// Player teams
// -------------------------------------------------------------------------------------------------------------

func (bot *Bot) playerTeams(m *discordgo.MessageCreate, args string) {
	bot.markAsTyping(m.ChannelID)

	interval := 60
	limit := 5

	// Get the player id
	pidStr := strings.Trim(args, " \"'")
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		bot.sendResponse(m, fmt.Sprintf("**%s** does not seem to be a valid player id.", pidStr))
		return
	}

	// Check if the gamertag exists
	gamertag, err := bot.dataStore.getPlayerGamertag(pid)
	if err != nil {
		bot.sendResponse(m, fmt.Sprintf("I don't know of any player with id **%d**.", pid))
		return
	}
	bot.sendResponse(m, fmt.Sprintf("I found player **%d** with gamertag **%s** and will now analyze the team matches of the last **%d** days.\nThis can take a while (> 10s) depending on the length of the match history.", pid, gamertag, interval))
	bot.markAsTyping(m.ChannelID)

	// Get the player teams
	results, err := bot.dataStore.db.Query(`
		WITH history_ AS (
			SELECT mh.*
			FROM match_history mh
			WHERE mh.mh_player_id = $1
			AND mh.mh_match_start_date > NOW() - ($2 || ' days')::interval
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
				sum(tc_duration) AS duration
			FROM team_candidates_ tc, team t
			WHERE t.t_id = tc.tc_id
			AND (
				t.t_p1_id = $1
				OR t.t_p2_id = $1
				OR t.t_p3_id = $1
			)
			GROUP BY t.t_id, t.t_p1_id, t.t_p2_id, t.t_p3_id
			ORDER BY matches DESC
			LIMIT $3
		)
		SELECT
			ts.t_id AS team_id,
			p1.p_gamertag AS player1,
			p2.p_gamertag AS player2,
			p3.p_gamertag AS player3,
			ts.matches,
			ts.wins,
			EXTRACT (EPOCH FROM ts.duration)
		FROM team_stats_ ts, player p1, player p2, player p3
		WHERE ts.t_p1_id = p1.p_id
		AND ts.t_p2_id = p2.p_id
		AND ts.t_p3_id = p3.p_id
		ORDER BY matches DESC
		LIMIT $3
	`, pid, interval, limit)
	if err != nil {
		bot.sendResponse(m, fmt.Sprintf("Ouch! Something went wrong: %v.", err))
		return
	}

	fields := []*discordgo.MessageEmbedField{}

	// Iterate over results
	for results.Next() {
		var teamID int
		var player1 string
		var player2 string
		var player3 string
		var matches int
		var wins int
		var duration float64
		err := results.Scan(&teamID, &player1, &player2, &player3, &matches, &wins, &duration)
		if err != nil {
			bot.sendResponse(m, fmt.Sprintf("Ouch! Something went wrong: %v.", err))
			return
		}

		var name bytes.Buffer
		name.WriteString(player1)
		if player2 != "-" {
			name.WriteString(", ")
			name.WriteString(player2)
		}
		if player3 != "-" {
			name.WriteString(", ")
			name.WriteString(player3)
		}

		value := fmt.Sprintf("Matches: **%d**\nWin Percentage: **%.2f%%**\nDuration: **%s**", matches, (float64(wins) / float64(max(matches, 1)) * 100.0), fmtSeconds(duration))

		fields = append(fields, &discordgo.MessageEmbedField{
			Name:   name.String(),
			Value:  value,
			Inline: false,
		})
	}

	r := &discordgo.MessageEmbed{
		Title:       "Player Teams",
		Author:      &discordgo.MessageEmbedAuthor{},
		Color:       0x010101,
		Description: fmt.Sprintf("PlayerID: **%d**, Gamertag: **%s**, Interval: **%d days**, Limit: **%d**", pid, gamertag, interval, limit),
		Fields:      fields,
		Timestamp:   time.Now().Format(time.RFC3339),
	}

	bot.session.ChannelMessageSendEmbed(m.ChannelID, r)
}
