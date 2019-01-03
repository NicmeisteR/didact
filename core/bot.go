package core

import (
	"bytes"
	"database/sql"
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

func (bot *Bot) dashboardURL(dashboardUUID string, params map[string]string) (*url.URL, error) {
	// Build URL
	u, err := url.Parse("https://www.didact.io/public/dashboard/")
	if err != nil {
		return nil, err
	}
	u.Path += dashboardUUID
	q := u.Query()
	for key, value := range params {
		q.Set(key, value)
	}
	u.RawQuery = q.Encode()
	return u, nil
}

func (bot *Bot) waypointMatchURL(matchUUID string, gamertag string) (*url.URL, error) {
	u, err := url.Parse("https://www.halowaypoint.com/en-us/games/halo-wars-2/matches/")
	if err != nil {
		return nil, err
	}
	u.Path += matchUUID
	u.Path += "/players/"
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
	if len(msgFields) < 1 || msgFields[0] != "!didact" {
		return
	}

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
		if channel.Name != "didact" && channel.Name != "hw2-stat-bot" {
			return
		}

		// Remember channel
		bot.channels[m.ChannelID] = true
	}

	// Mark as typing
	bot.markAsTyping(m.ChannelID)

	// Print help
	if len(msgFields) == 1 {
		bot.help(m)
		return
	}

	// Extract the argument string
	cmd := msgFields[1]
	args := strings.TrimSpace(m.Content)
	args = strings.TrimPrefix(args, "!didact")
	args = strings.TrimLeft(args, " ")
	args = strings.TrimPrefix(args, cmd)
	args = strings.TrimLeft(args, " ")

	// Process the command
	switch cmd {
	case "status":
		bot.getStatus(m)
		break

	case "find":
		bot.findPlayer(m, args)
		return

	case "scan":
		pid, gt, ok := bot.getPlayerID(m, args)
		if !ok {
			return
		}
		bot.scanPlayer(m, pid, gt, false)
		break

	case "fullscan":
		pid, gt, ok := bot.getPlayerID(m, args)
		if !ok {
			return
		}
		bot.scanPlayer(m, pid, gt, true)
		break

	case "stats":
		bot.getStats(m, args)
		break

	case "analyse":
		fallthrough
	case "analyze":
		mid, err := strconv.Atoi(args)
		if err != nil {
			bot.sendResponse(m, fmt.Sprintf("The match id **%d** is invalid.", args))
			return
		}
		bot.analyzeMatch(m, mid)

	case "last":
		fallthrough
	case "latest":
		pid, gt, ok := bot.getPlayerID(m, args)
		if !ok {
			return
		}
		bot.scanPlayer(m, pid, gt, false)
		bot.getLatest(m, pid, gt)
		break

	case "help":
		fallthrough
	default:
		bot.help(m)
	}
}

// -------------------------------------------------------------------------------------------------------------
// Get latest
// -------------------------------------------------------------------------------------------------------------

func (bot *Bot) help(m *discordgo.MessageCreate) {
	teamStats := "https://www.didact.io/public/dashboard/79912130-2ef2-49ca-a14a-f2dac4c887f9"
	matchStats := "https://www.didact.io/public/dashboard/4222ecd2-6698-4149-8a45-0c02057d4efc"
	// search := "https://www.didact.io/public/dashboard/d8a4b068-0a33-4215-b949-6e2f45df2e17"

	fields := []*discordgo.MessageEmbedField{}
	fields = append(fields, &discordgo.MessageEmbedField{
		Name:   "Command - Player Search",
		Value:  "!didact find <query>",
		Inline: false,
	})
	fields = append(fields, &discordgo.MessageEmbedField{
		Name:   "Command - Statisitics",
		Value:  "!didact stats <p-1-1> [, <gamertag p-1-2>, <p-1-3>] [/ <p-2-1>, <p-2-2>, <p-2-3>]",
		Inline: false,
	})
	fields = append(fields, &discordgo.MessageEmbedField{
		Name:   "Command - Last Match",
		Value:  "!didact last <gamertag>",
		Inline: false,
	})
	fields = append(fields, &discordgo.MessageEmbedField{
		Name:   "Command - History Scan",
		Value:  "!didact scan <gamertag>",
		Inline: false,
	})
	fields = append(fields, &discordgo.MessageEmbedField{
		Name:   "Command - Full History Scan",
		Value:  "!didact fullscan <gamertag>",
		Inline: false,
	})
	fields = append(fields, &discordgo.MessageEmbedField{
		Name:   "Command - Match Analysis",
		Value:  "!didact analyse <match id>",
		Inline: false,
	})
	fields = append(fields, &discordgo.MessageEmbedField{
		Name:   "URL - Team Statistics",
		Value:  teamStats,
		Inline: false,
	})
	fields = append(fields, &discordgo.MessageEmbedField{
		Name:   "URL - Match Statistics",
		Value:  matchStats,
		Inline: false,
	})

	r := &discordgo.MessageEmbed{
		Title:       "Didact Discord Bot",
		Author:      &discordgo.MessageEmbedAuthor{},
		Color:       0x010101,
		Description: "If you find bugs, please tell @x6767.",
		Fields:      fields,
		Timestamp:   time.Now().Format(time.RFC3339),
	}

	bot.session.ChannelMessageSendEmbed(m.ChannelID, r)
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

// -------------------------------------------------------------------------------------------------------------
// Get player id
// -------------------------------------------------------------------------------------------------------------

func (bot *Bot) getPlayerID(m *discordgo.MessageCreate, args string) (int, string, bool) {
	gamertag := strings.Trim(args, " \"'")
	playerID, err := bot.dataStore.getPlayerID(gamertag)
	if err != nil {
		bot.sendResponse(m, fmt.Sprintf("Could not find player: **%v**.", gamertag))
		return 0, "", false
	}
	bot.sendResponse(m, fmt.Sprintf("I found player **%s** with id **%d**.", gamertag, playerID))
	return playerID, gamertag, true
}

// -------------------------------------------------------------------------------------------------------------
// Find player
// -------------------------------------------------------------------------------------------------------------

func (bot *Bot) findPlayer(m *discordgo.MessageCreate, search string) {
	gamertags, err := bot.dataStore.findPlayer(search)
	if err != nil {
		bot.sendResponse(m, fmt.Sprintf("Ouch! Something went wrong: %v.", err))
		return
	}
	if len(gamertags) == 0 {
		bot.sendResponse(m, fmt.Sprintf("Couldn't find any players for query **%s**.", search))
		return
	}

	var buffer bytes.Buffer
	for _, gt := range gamertags {
		buffer.WriteString(gt)
		buffer.WriteString("\n")
		fmt.Sprintf("%s\n", gt)
	}

	fields := []*discordgo.MessageEmbedField{}
	fields = append(fields, &discordgo.MessageEmbedField{
		Name:   "Result",
		Value:  buffer.String(),
		Inline: false,
	})

	r := &discordgo.MessageEmbed{
		Title:       "Player search",
		Author:      &discordgo.MessageEmbedAuthor{},
		Color:       0x010101,
		Description: fmt.Sprintf("Query: **%s** Limit: **12**", search),
		Fields:      fields,
		Timestamp:   time.Now().Format(time.RFC3339),
	}

	bot.session.ChannelMessageSendEmbed(m.ChannelID, r)
}

// -------------------------------------------------------------------------------------------------------------
// Get stats
// -------------------------------------------------------------------------------------------------------------

func (bot *Bot) getStats(m *discordgo.MessageCreate, args string) {
	if strings.Contains(args, "/") {
		// Get teams
		raw_teams := strings.Split(args, "/")
		if len(raw_teams) != 2 {
			bot.sendResponse(m, "Invalid teams.")
			return
		}

		// Get gamertags team 1
		raw_gamertags_t1 := strings.Split(raw_teams[0], ",")
		var gamertags_t1 []string = nil
		for _, gt := range raw_gamertags_t1 {
			gamertags_t1 = append(gamertags_t1, strings.Trim(gt, " \"'"))
		}

		// Get gamertags team 2
		raw_gamertags_t2 := strings.Split(raw_teams[1], ",")
		var gamertags_t2 []string = nil
		for _, gt := range raw_gamertags_t2 {
			gamertags_t2 = append(gamertags_t2, strings.Trim(gt, " \"'"))
		}

		// Build description
		var description bytes.Buffer
		for i, gt := range gamertags_t1 {
			if i > 0 {
				description.WriteString(", ")
			}
			description.WriteString("**")
			description.WriteString(gt)
			description.WriteString("**")
		}
		description.WriteString(" vs. ")
		for i, gt := range gamertags_t2 {
			if i > 0 {
				description.WriteString(", ")
			}
			description.WriteString("**")
			description.WriteString(gt)
			description.WriteString("**")
		}

		// Build link
		gamertags_t1 = append(gamertags_t1, "-")
		gamertags_t1 = append(gamertags_t1, "-")
		gamertags_t1 = append(gamertags_t1, "-")
		gamertags_t2 = append(gamertags_t2, "-")
		gamertags_t2 = append(gamertags_t2, "-")
		gamertags_t2 = append(gamertags_t2, "-")
		didactURL, _ := bot.dashboardURL("64d4d274-c18a-4f1e-b2c2-cf9672685ff2", map[string]string{
			"t1_p1": gamertags_t1[0],
			"t1_p2": gamertags_t1[1],
			"t1_p3": gamertags_t1[2],
			"t2_p1": gamertags_t2[0],
			"t2_p2": gamertags_t2[1],
			"t2_p3": gamertags_t2[2],
			"days":  "90",
		})

		// Build embed
		r := &discordgo.MessageEmbed{
			Title:       fmt.Sprintf("Statistics"),
			URL:         didactURL.String(),
			Author:      &discordgo.MessageEmbedAuthor{},
			Color:       0x010101,
			Description: description.String(),
			Timestamp:   time.Now().Format(time.RFC3339),
		}

		// Send embed
		bot.session.ChannelMessageSendEmbed(m.ChannelID, r)
	} else {
		// Get gamertags
		raw_gamertags := strings.Split(args, ",")
		var gamertags []string = nil
		for _, gt := range raw_gamertags {
			gamertags = append(gamertags, strings.Trim(gt, " \"'"))
		}

		// Build description
		var description bytes.Buffer
		for i, gt := range gamertags {
			if i > 0 {
				description.WriteString(", ")
			}
			description.WriteString("**")
			description.WriteString(gt)
			description.WriteString("**")
		}

		// Build link
		gamertags = append(gamertags, "-")
		gamertags = append(gamertags, "-")
		gamertags = append(gamertags, "-")
		didactURL, _ := bot.dashboardURL("79912130-2ef2-49ca-a14a-f2dac4c887f9", map[string]string{
			"t1_p1": gamertags[0],
			"t1_p2": gamertags[1],
			"t1_p3": gamertags[2],
			"days":  "90",
		})

		// Build embed
		r := &discordgo.MessageEmbed{
			Title:       fmt.Sprintf("Statistics"),
			URL:         didactURL.String(),
			Author:      &discordgo.MessageEmbedAuthor{},
			Color:       0x010101,
			Description: description.String(),
			Timestamp:   time.Now().Format(time.RFC3339),
		}

		// Send embed
		bot.session.ChannelMessageSendEmbed(m.ChannelID, r)
	}
}

// -------------------------------------------------------------------------------------------------------------
// Get latest
// -------------------------------------------------------------------------------------------------------------

func (bot *Bot) getLatest(m *discordgo.MessageCreate, playerID int, gamertag string) {
	// Get the latest match
	matchID, matchUUID, startDate, err := bot.dataStore.getLatestMatch(playerID)
	if err != nil {
		bot.sendResponse(m, fmt.Sprintf("Ouch! Something went wrong: %v.", err))
		return
	}

	// Analyze the match
	if !bot.analyzeMatch(m, matchID) {
		return
	}

	// Build URLs
	didactURL, _ := bot.dashboardURL("4222ecd2-6698-4149-8a45-0c02057d4efc", map[string]string{
		"match_id":   strconv.Itoa(matchID),
		"left_team":  "1",
		"right_team": "2",
	})
	waypointURL, _ := bot.waypointMatchURL(matchUUID, gamertag)

	fields := []*discordgo.MessageEmbedField{}
	fields = append(fields, &discordgo.MessageEmbedField{
		Name:   "Didact Statistics",
		Value:  fmt.Sprintf("%v", didactURL),
		Inline: false,
	})
	fields = append(fields, &discordgo.MessageEmbedField{
		Name:   "Waypoint Statistics",
		Value:  fmt.Sprintf("%v", waypointURL),
		Inline: false,
	})

	r := &discordgo.MessageEmbed{
		Title:       "Latest known match",
		Author:      &discordgo.MessageEmbedAuthor{},
		Color:       0x010101,
		Description: fmt.Sprintf("Player: **%d** Gamertag: **%s** Start: **%v**", playerID, gamertag, startDate),
		Fields:      fields,
		Timestamp:   time.Now().Format(time.RFC3339),
	}

	bot.session.ChannelMessageSendEmbed(m.ChannelID, r)
}

// -------------------------------------------------------------------------------------------------------------
// Scan player
// -------------------------------------------------------------------------------------------------------------

func (bot *Bot) scanPlayer(m *discordgo.MessageCreate, playerID int, gamertag string, full bool) {
	// Figure out where to start
	count := 25
	offset := 0
	newMatches := make([]string, 0)

	totalSeenMatches := 0
	lastNotification := 0
	notificationThreshold := 25

	// Update the playlist stats
	stats, err := bot.crawler.loadPlayerStats(gamertag)
	if err != nil {
		bot.sendResponse(m, fmt.Sprintf("Ouch! Something went wrong: %v.", err))
		return
	}
	err = bot.crawler.dataStore.storePlayerStats(playerID, stats)
	if err != nil {
		bot.sendResponse(m, fmt.Sprintf("Ouch! Something went wrong: %v.", err))
		return
	}
	bot.sendResponse(m, fmt.Sprintf("I updated the playlist stats from player **%d**.", playerID))

	bot.sendResponse(m, fmt.Sprintf("I started the match history scan for player **%d** with gamertag **%s**.", playerID, gamertag))
	bot.markAsTyping(m.ChannelID)

	for {
		// Notify user about progress?
		if (totalSeenMatches - lastNotification) >= notificationThreshold {
			bot.sendResponse(m, fmt.Sprintf("I scanned **%d** matches from player **%d**. (**%d** total, **%d** new)", totalSeenMatches-lastNotification, playerID, totalSeenMatches, len(newMatches)))
			bot.markAsTyping(m.ChannelID)
			lastNotification = totalSeenMatches

			// Exponential backoff for notifications
			notificationThreshold *= 2
			if notificationThreshold >= 200 {
				notificationThreshold = 200
			}
		}

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

		// Remember total seen matches
		totalSeenMatches += seenMatches

		// Continue?
		if (foundMatches > 0) || (full && (len(history.Results) > 0)) {
			offset += count
		} else {
			break
		}
	}

	if len(newMatches) == 0 {
		bot.sendResponse(m, fmt.Sprintf("I found no new matches for player **%d**.", playerID))
	} else if len(newMatches) > 0 {
		bot.sendResponse(m, fmt.Sprintf("I found **%d** new match(es) from player **%d** and will now load the match results.", len(newMatches), playerID))

		notificationThreshold = 5
		lastNotification = 0

		for i, newMatch := range newMatches {
			// Notify user about progress?
			if (i - lastNotification) >= notificationThreshold {
				bot.sendResponse(m, fmt.Sprintf("I loaded **%d** match results from player **%d**. (**%d** total)", (i-lastNotification), playerID, i))
				bot.markAsTyping(m.ChannelID)
				lastNotification = i

				// Exponential backoff for notifications
				notificationThreshold *= 2
				if notificationThreshold >= 20 {
					notificationThreshold = 20
				}
			}

			// Update the match result
			bot.updateMatchResult(m, newMatch)
		}
	}
	bot.sendResponse(m, fmt.Sprintf("I finished the match history scan for player **%d**.", playerID))
}

// -------------------------------------------------------------------------------------------------------------
// Match events
// -------------------------------------------------------------------------------------------------------------

func (bot *Bot) analyzeMatch(m *discordgo.MessageCreate, mid int) bool {
	bot.markAsTyping(m.ChannelID)

	// Match events already exist?
	mUUID, err := bot.dataStore.getMatchUUID(mid)
	if err != nil {
		bot.sendResponse(m, fmt.Sprintf("I don't know of any match with id **%d**.", mid))
		return false
	}
	bot.sendResponse(m, fmt.Sprintf("I found match **%d** with uuid **%s**.", mid, mUUID))

	bot.markAsTyping(m.ChannelID)

	// Load match events
	mEvents, err := bot.crawler.loadMatchEvents(mUUID)

	// No such match? (404)
	if err == ErrNotFound {
		bot.sendResponse(m, fmt.Sprintf("The Halo API does not return any events for the match **%s**.", mUUID))
		return false
	}

	// Hit the rate limit? (429)
	if err == ErrRateLimit {
		bot.sendResponse(m, fmt.Sprintf("I just hit the API rate limit, please request the events again."))
		return false
	}

	// Try again if there was an unexpected error
	if err != nil {
		bot.sendResponse(m, fmt.Sprintf("Ouch! Something went wrong: %v.", err))
		return false
	}

	bot.markAsTyping(m.ChannelID)

	// Store match events
	err = bot.dataStore.storeMatchEvents(mid, mEvents)
	if err != nil {
		bot.sendResponse(m, fmt.Sprintf("Ouch! Something went wrong: %v.", err))
	}

	bot.sendResponse(m, fmt.Sprintf("I stored the events for match **%d**.", mid))
	return true
}
