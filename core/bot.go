package core

import (
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

func (bot *Bot) waypointMatchURL(matchUUID string, gamertag string) (*url.URL, error) {
	u, err := url.Parse("https://www.halowaypoint.com/de-de/games/halo-wars-2/matches/")
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

	case "s":
		fallthrough
	case "scan":
		bot.scanPlayer(m, args)
		break

	case "analyze":
		bot.analyzeMatch(m, args)
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

// -------------------------------------------------------------------------------------------------------------
// Scan player
// -------------------------------------------------------------------------------------------------------------

func (bot *Bot) scanPlayer(m *discordgo.MessageCreate, args string) {
	// Get player id
	gamertag := strings.Trim(args, " \"'")
	playerID, err := bot.dataStore.getPlayerID(gamertag)
	if err != nil {
		bot.sendResponse(m, fmt.Sprintf("Could not find player: **%v**.", gamertag))
		return
	}
	bot.sendResponse(m, fmt.Sprintf("I found player **%s** with id **%d**.", gamertag, playerID))

	// Figure out where to start
	count := 25
	offset := 0
	newMatches := make([]string, 0)

	bot.sendResponse(m, fmt.Sprintf("I started the match history scan for player **%d** with gamertag **%s**.", playerID, gamertag))
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
			bot.markAsTyping(m.ChannelID)
			bot.updateMatchResult(m, newMatch)
		}
	}
	bot.sendResponse(m, fmt.Sprintf("I finished the match history scan for player **%d**.", playerID))
}

// -------------------------------------------------------------------------------------------------------------
// Match events
// -------------------------------------------------------------------------------------------------------------

func (bot *Bot) analyzeMatch(m *discordgo.MessageCreate, args string) {
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
