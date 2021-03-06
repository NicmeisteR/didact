package core

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/bwmarrin/discordgo"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type DiscordBot struct {
	config    *Config
	dataStore *DataStore
	crawler   *Crawler
	session   *discordgo.Session
	channels  map[string]bool
}

func NewDiscordBot(config *Config, dataStore *DataStore, crawler *Crawler) *DiscordBot {
	bot := new(DiscordBot)
	bot.config = config
	bot.crawler = crawler
	bot.channels = make(map[string]bool)
	bot.dataStore = dataStore
	return bot
}

// -------------------------------------------------------------------------------------------------------------
// Control
// -------------------------------------------------------------------------------------------------------------

func (bot *DiscordBot) Start() error {
	var err error

	// Create a new Discord session using the provided bot token.
	bot.session, err = discordgo.New("Bot " + bot.config.DiscordBot.Key)
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

func (bot *DiscordBot) Stop() {
	if bot.session != nil {
		bot.session.Close()
	}
	bot.session = nil
}

// -------------------------------------------------------------------------------------------------------------
// Messages
// -------------------------------------------------------------------------------------------------------------

// Broadcast a message
func (bot *DiscordBot) broadcast(m string) {
	for cid := range bot.channels {
		bot.session.ChannelMessageSend(cid, m)
	}
}

// Broadcast an embed
func (bot *DiscordBot) broadcastEmbed(m *discordgo.MessageEmbed) {
	for cid := range bot.channels {
		bot.session.ChannelMessageSendEmbed(cid, m)
	}
}

// Broadcast a complex message
func (bot *DiscordBot) broadcastComplex(m *discordgo.MessageSend) {
	for cid := range bot.channels {
		bot.session.ChannelMessageSendComplex(cid, m)
	}
}

// Send a response
func (bot *DiscordBot) sendResponse(m *discordgo.MessageCreate, r string) {
	// withMention := fmt.Sprintf("%s %s", m.Author.Mention(), r)
	bot.session.ChannelMessageSend(m.ChannelID, r)
}

// Mark as typing
func (bot *DiscordBot) markAsTyping(channel string) {
	bot.session.ChannelTyping(channel)

}

// -------------------------------------------------------------------------------------------------------------
// URLs
// -------------------------------------------------------------------------------------------------------------

func (bot *DiscordBot) dashboardURL(dashboardUUID string, params map[string]string) (*url.URL, error) {
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

func (bot *DiscordBot) waypointMatchURL(matchUUID string, gamertag string) (*url.URL, error) {
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

var authorizedGuildIDs = map[string]bool{
	"493106248472985602": true, // Test
	"451897042436882456": true, // HWTC
}

var authorizedChannelIDs = map[string]bool{
	"493300632913182720": true, // Test - didact
	"513788534486728705": true, // HWTC - didact
}

var authorizedUserIDs = map[string]bool{
	"426126402279178261": true, // x6767
	"165889087905988609": true, // Mike
	"156904361350529024": true, // Admiration
	"409845863934853121": true, // L1am
	"410479385720651780": true, // Roger
}

// Is authorized?
func isAuthorized(m *discordgo.MessageCreate) bool {
	_, guildOK := authorizedGuildIDs[m.GuildID]
	_, channelOK := authorizedChannelIDs[m.ChannelID]
	_, userOK := authorizedUserIDs[m.Author.ID]
	return guildOK && channelOK && userOK
}

// This function will be called (due to AddHandler above) every time a new
// message is created on any channel that the autenticated bot has access to.
func (bot *DiscordBot) onMessageCreate(s *discordgo.Session, m *discordgo.MessageCreate) {

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

	notAuthorized := func() {
		bot.sendResponse(m, "You are not authorized to run this command.")
	}

	// Process the command
	switch cmd {
	case "whoami":
		bot.sendResponse(m, fmt.Sprintf("Guild: **%s**\nChannel: **%s**\nUser: **%s**\nAuthorized: **%t**", m.GuildID, m.ChannelID, m.Author.ID, isAuthorized(m)))
		break

	case "status":
		bot.getStatus(m)
		break

	case "find":
		bot.findPlayer(m, args)
		return

	case "annotate":
		if !isAuthorized(m) {
			notAuthorized()
			return
		}
		bot.annotateTeam(m, args)
		break

	case "communityscan":
		if !isAuthorized(m) {
			notAuthorized()
			return
		}
		bot.scanCommunity(m, args)
		break

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

func (bot *DiscordBot) help(m *discordgo.MessageCreate) {
	fields := []*discordgo.MessageEmbedField{}
	fields = append(fields, &discordgo.MessageEmbedField{
		Name:   "Command - Status",
		Value:  "!didact status",
		Inline: false,
	})
	fields = append(fields, &discordgo.MessageEmbedField{
		Name:   "Command - Player Search",
		Value:  "!didact find <query>",
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

	r := &discordgo.MessageEmbed{
		Title:       "Didact Discord DiscordBot",
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

func (bot *DiscordBot) getStatus(m *discordgo.MessageCreate) {
	p := message.NewPrinter(language.English)

	stats, err := bot.dataStore.getSystemStats()
	if err != nil {
		bot.sendResponse(m, fmt.Sprintf("Ouch! Something went wrong: %v.", err))
		return
	}

	fields := []*discordgo.MessageEmbedField{}

	// Relation Stats
	fields = append(fields, &discordgo.MessageEmbedField{
		Name:   "Size",
		Value:  stats.DBSize,
		Inline: false,
	})

	// Relation Stats
	var relStats bytes.Buffer
	for _, rel := range stats.Relations {
		relStats.WriteString(p.Sprintf("%s: **%d**\n", rel.Name, rel.Tuples))
	}
	fields = append(fields, &discordgo.MessageEmbedField{
		Name:   "Relations",
		Value:  relStats.String(),
		Inline: false,
	})

	// Task Stats
	var taskStats bytes.Buffer
	for _, g := range stats.Tasks {
		statusName := ""
		switch g.Status {
		case TaskQueued:
			statusName = "queued"
			break
		case TaskDeferred:
			statusName = "deferred"
			break
		case TaskActive:
			statusName = "active"
			break
		case TaskBlocked:
			statusName = "blocked"
			break
		}
		taskStats.WriteString(p.Sprintf("%s: **%d**\n", statusName, g.Tuples))
	}
	fields = append(fields, &discordgo.MessageEmbedField{
		Name:   "Crawler Tasks",
		Value:  taskStats.String(),
		Inline: false,
	})

	r := &discordgo.MessageEmbed{
		Title:       "Didact Status",
		Author:      &discordgo.MessageEmbedAuthor{},
		Color:       0x010101,
		Description: "",
		Fields:      fields,
		Timestamp:   time.Now().Format(time.RFC3339),
	}

	bot.session.ChannelMessageSendEmbed(m.ChannelID, r)
}

// -------------------------------------------------------------------------------------------------------------
// Scan init
// -------------------------------------------------------------------------------------------------------------

func (bot *DiscordBot) scanCommunity(m *discordgo.MessageCreate, arg string) {
	p := message.NewPrinter(language.English)

	var err error
	var initReturnCode *int

	// Requested full scan?
	if len(arg) == 0 {
		bot.sendResponse(m, "I will try to initialize a scan of ALL players.")
		initReturnCode, err = bot.dataStore.initFullPlayerScan()
	} else {
		// Parse the days
		days, err := strconv.Atoi(arg)
		if err != nil {
			bot.sendResponse(m, fmt.Sprintf("The interval **%v** is invalid.", arg))
			return
		}

		// Init active player scan
		bot.sendResponse(m, fmt.Sprintf("I will try to initialize a scan of players that played at least a single match in the last %d days.", days))
		initReturnCode, err = bot.dataStore.initActivePlayerScan(days)
	}

	// Initialization failed?
	if err != nil {
		bot.sendResponse(m, fmt.Sprintf("Ouch! Something went wrong: %v.", err))
		return
	}

	// Return code != 0
	if initReturnCode == nil || *initReturnCode != 0 {
		bot.sendResponse(m, "I failed to initialize the scan. **The crawler must be idle!**")
		return
	}

	stats, err := bot.dataStore.getTaskStats()
	if err != nil {
		bot.sendResponse(m, fmt.Sprintf("Ouch! Something went wrong: %v.", err))
		return
	}

	fields := []*discordgo.MessageEmbedField{}

	// Task Stats
	var taskStats bytes.Buffer
	for _, g := range stats {
		statusName := ""
		switch g.Status {
		case TaskQueued:
			statusName = "queued"
			break
		case TaskDeferred:
			statusName = "deferred"
			break
		case TaskActive:
			statusName = "active"
			break
		case TaskBlocked:
			statusName = "blocked"
			break
		}
		taskStats.WriteString(p.Sprintf("%s: **%d**\n", statusName, g.Tuples))
	}

	r := &discordgo.MessageEmbed{
		Title:       "Crawler Tasks",
		Author:      &discordgo.MessageEmbedAuthor{},
		Color:       0x010101,
		Description: taskStats.String(),
		Fields:      fields,
		Timestamp:   time.Now().Format(time.RFC3339),
	}

	bot.session.ChannelMessageSendEmbed(m.ChannelID, r)
}

// -------------------------------------------------------------------------------------------------------------
// Get player id
// -------------------------------------------------------------------------------------------------------------

func (bot *DiscordBot) getPlayerID(m *discordgo.MessageCreate, args string) (int, string, bool) {
	gamertag := strings.Trim(args, " \"'")
	playerID, gamertag, err := bot.dataStore.getPlayerID(gamertag)
	if err != nil {
		bot.sendResponse(m, fmt.Sprintf("Could not find player: **%v**.", gamertag))
		return 0, "", false
	}
	bot.sendResponse(m, fmt.Sprintf("I found player **%s** with id **%d**.", gamertag, playerID))
	return playerID, gamertag, true
}

// -------------------------------------------------------------------------------------------------------------
// Annotate a team
// -------------------------------------------------------------------------------------------------------------

func (bot *DiscordBot) annotateTeam(m *discordgo.MessageCreate, argString string) {
	printUsage := func() {
		bot.sendResponse(m, "Invalid usage. !didact annotate <p1>, (<p2>, <p3>): <label>")
		return
	}

	// Read sections
	sections := strings.Split(argString, ":")
	if len(sections) != 2 {
		printUsage()
		return
	}

	// Get labels and gamertags
	rawLabel := strings.Trim(sections[1], " \"'")
	gts := []string{}
	for _, rawGT := range strings.Split(sections[0], ",") {
		gts = append(gts, strings.Trim(rawGT, " \"'"))
	}
	if len(gts) == 0 {
		printUsage()
		return
	}

	// Mark as busy
	bot.markAsTyping(m.ChannelID)

	// Get player ids
	pids := []int{}
	for _, gt := range gts {
		playerID, gamertag, err := bot.dataStore.getPlayerID(gt)
		if err != nil {
			bot.sendResponse(m, fmt.Sprintf("Could not find player: **%v**.", gt))
			return
		}
		bot.sendResponse(m, fmt.Sprintf("I found player **%s** with id **%d**.", gamertag, playerID))
		pids = append(pids, playerID)
	}

	// Get label id
	labelID, label, err := bot.dataStore.getLabelID(rawLabel)
	if err != nil {
		bot.sendResponse(m, fmt.Sprintf("Could not find label: **%v**.", rawLabel))
		return
	}
	bot.sendResponse(m, fmt.Sprintf("I found label **%s** with id **%d**.", label, labelID))

	// Mark as busy
	bot.markAsTyping(m.ChannelID)

	// Annotate the team
	err = bot.dataStore.annotateTeam(pids, labelID)
	if err != nil {
		bot.sendResponse(m, fmt.Sprintf("Ouch! Something went wrong: %v.", err))
		return
	}
	bot.sendResponse(m, fmt.Sprintf("Annotated player(s) **%v** with label **%d**", pids, labelID))
}

// -------------------------------------------------------------------------------------------------------------
// Find player
// -------------------------------------------------------------------------------------------------------------

func (bot *DiscordBot) findPlayer(m *discordgo.MessageCreate, search string) {
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

func (bot *DiscordBot) getStats(m *discordgo.MessageCreate, raw string) {
	args := make([]string, 0)
	last := 0

	// Get arguments
	pos := 0
	for ; pos < len(raw); pos++ {
		if raw[pos] == ' ' {
			trimmed := strings.TrimSpace(raw[last:pos])
			if len(trimmed) == 0 {
				continue
			}
			args = append(args, trimmed)
			last = pos
		}
		if len(args) == 2 {
			break
		}
	}
	if last < len(raw) {
		args = append(args, strings.TrimSpace(raw[last:len(raw)]))
	}

	// Too few?
	if len(args) < 3 {
		bot.sendResponse(m, "Usage: !didact stats 1r/2r/3r <days> <gamertag>")
		return
	}

	// Parse the days
	days, err := strconv.Atoi(args[1])
	if err != nil {
		bot.sendResponse(m, fmt.Sprintf("The interval **%v** is invalid.", args[1]))
		return
	}

	// Get the player
	pid, gt, ok := bot.getPlayerID(m, args[2])
	if !ok {
		return
	}

	// Scan the player
	ok = bot.scanPlayer(m, pid, gt, false)
	if !ok {
		return
	}
	bot.markAsTyping(m.ChannelID)

	// Get team stats
	var allStats []*PlayerMatchAggregates
	switch args[0] {
	case "1r":
		allStats, err = bot.dataStore.getPlayerMatchAggregates(pid, gt, days, 1)
		break
	case "2r":
		allStats, err = bot.dataStore.getPlayerMatchAggregates(pid, gt, days, 2)
		break
	case "3r":
		allStats, err = bot.dataStore.getPlayerMatchAggregates(pid, gt, days, 3)
		break
	}

	// Failed to get team stats?
	if err != nil {
		bot.sendResponse(m, fmt.Sprintf("Ouch! Something went wrong: %v.", err))
		return
	}

	// Group results
	playerStats := &PlayerMatchAggregates{}
	mapStats := make([]*PlayerMatchAggregates, 0)
	leaderStats := make([]*PlayerMatchAggregates, 0)

	for _, stats := range allStats {
		if stats.Map == nil && stats.Leader == nil {
			playerStats = stats
		} else if stats.Map != nil && stats.Leader == nil {
			mapStats = append(mapStats, stats)
		} else if stats.Map == nil && stats.Leader != nil {
			leaderStats = append(leaderStats, stats)
		}
	}

	// Build title
	title := "Statistics 1v1 Ranked"
	switch args[0] {
	case "1r":
		break
	case "2r":
		title = "Statistics 2v2 Ranked"
		break
	case "3r":
		title = "Statistics 3v3 Ranked"
		break
	}

	// Build description
	var desc bytes.Buffer
	desc.WriteString("Gamertag: **")
	desc.WriteString(gt)
	desc.WriteString("**\n")

	matches := playerStats.Matches
	wl := 0.0
	minutes := 0.0
	mmr := 0.0
	csr := 0.0
	if matches > 0 {
		wl = float64(playerStats.Wins) * 100 / float64(playerStats.Matches)
		minutes = playerStats.Duration / float64(playerStats.Matches) / 60.0
		mmr = playerStats.MMR / float64(playerStats.Matches)
		csr = float64(playerStats.CSR) / float64(playerStats.Matches)
	}
	var general bytes.Buffer
	general.WriteString(fmt.Sprintf("Days: **%d**\n", days))
	general.WriteString(fmt.Sprintf("Matches: **%d**\n", playerStats.Matches))
	general.WriteString(fmt.Sprintf("Wins: **%d** (**%.2f**%%)\n", playerStats.Wins, wl))
	general.WriteString(fmt.Sprintf("MMR: **%+.2f** (Ø **%+.2f**)\n", playerStats.MMR, mmr))
	general.WriteString(fmt.Sprintf("CSR: **%+d** (Ø **%+.2f**)\n", playerStats.CSR, csr))
	general.WriteString(fmt.Sprintf("Duration: **%.2f**h (Ø **%.2f**min)\n", float64(playerStats.Duration)/60.0/60.0, minutes))

	fields := []*discordgo.MessageEmbedField{}
	fields = append(fields, &discordgo.MessageEmbedField{
		Name:   "Total",
		Value:  general.String(),
		Inline: true,
	})

	var printStatsRow = func(b *bytes.Buffer, s *PlayerMatchAggregates, row int) {
		matches := s.Matches
		indicator := 0
		minutes := 0.0
		mmr := 0.0
		if matches > 0 {
			indicator = int(s.Wins * 6 / s.Matches)
			minutes = s.Duration / float64(s.Matches) / 60.0
			mmr = s.MMR / float64(s.Matches)
		}
		b.WriteString(fmt.Sprintf("%-2d", row))
		b.WriteString(" [")
		b.WriteString(strings.Repeat("#", indicator))
		b.WriteString(strings.Repeat(" ", 6-indicator))
		b.WriteString("] ")
		b.WriteString(fmt.Sprintf("%d %d %.2f %.1f", s.Matches, s.Wins, mmr, minutes))
		b.WriteString("\n")
	}

	// Maps
	desc = bytes.Buffer{}
	desc.WriteString("```m  w/l      * wins Ømmr Øt\n")
	mapNames := make([]string, 0)
	for i, s := range mapStats {
		printStatsRow(&desc, s, i)
		mapNames = append(mapNames, *s.Map)
	}
	desc.WriteString("\n")
	for i, n := range mapNames {
		if i > 0 {
			desc.WriteString(", ")
		}
		desc.WriteString(strconv.Itoa(i))
		desc.WriteString("-")
		desc.WriteString(n)
	}
	desc.WriteString("```")
	fields = append(fields, &discordgo.MessageEmbedField{
		Name:   "Maps",
		Value:  desc.String(),
		Inline: false,
	})

	// Leaders
	desc = bytes.Buffer{}
	desc.WriteString("```l  w/l      * wins Ømmr Øt\n")
	leaderNames := make([]string, 0)
	for i, s := range leaderStats {
		printStatsRow(&desc, s, i)
		leaderNames = append(leaderNames, *s.Leader)
	}
	desc.WriteString("\n")
	for i, n := range leaderNames {
		if i > 0 {
			desc.WriteString(", ")
		}
		desc.WriteString(strconv.Itoa(i))
		desc.WriteString("-")
		desc.WriteString(n)
	}
	desc.WriteString("```")
	fields = append(fields, &discordgo.MessageEmbedField{
		Name:   "Leaders",
		Value:  desc.String(),
		Inline: false,
	})

	// Build embed
	r := &discordgo.MessageEmbed{
		Title:     title,
		Author:    &discordgo.MessageEmbedAuthor{},
		Color:     0x010101,
		Fields:    fields,
		Timestamp: time.Now().Format(time.RFC3339),
	}

	// Send message
	bot.session.ChannelMessageSendEmbed(m.ChannelID, r)
}

// -------------------------------------------------------------------------------------------------------------
// Get latest
// -------------------------------------------------------------------------------------------------------------

func (bot *DiscordBot) getLatest(m *discordgo.MessageCreate, playerID int, gamertag string) {
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

func (bot *DiscordBot) scanPlayer(m *discordgo.MessageCreate, playerID int, gamertag string, full bool) bool {
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
		return false
	}
	err = bot.crawler.dataStore.storePlayerStats(playerID, stats)
	if err != nil {
		bot.sendResponse(m, fmt.Sprintf("Ouch! Something went wrong: %v.", err))
		return false
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
			return false
		}

		// Hit the rate limit? (429)
		if err == ErrRateLimit {
			bot.sendResponse(m, fmt.Sprintf("I just hit the API rate limit, please repeat the scan of **%d**.", playerID))
			return false
		}

		// Try again if there was an unexpected error
		if err != nil {
			bot.sendResponse(m, fmt.Sprintf("Ouch! Something went wrong: %v.", err))
			return false
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
	return true
}

// -------------------------------------------------------------------------------------------------------------
// Match events
// -------------------------------------------------------------------------------------------------------------

func (bot *DiscordBot) analyzeMatch(m *discordgo.MessageCreate, mid int) bool {
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
