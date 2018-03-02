package core

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/bwmarrin/discordgo"
	_ "github.com/lib/pq"
	"github.com/tidwall/gjson"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"

	"log"
	"sync"
	"sync/atomic"
	"time"
)

type Crawler struct {
	config            *Config
	taskWorkers       sync.WaitGroup
	taskWorkerCount   int
	apiKeys           *apiKeyPool
	stopSignal        uint32
	interruptChannels []chan bool
	dataStore         *DataStore
	httpClient        *http.Client
}

func NewCrawler(config *Config, dataStore *DataStore) *Crawler {
	crawler := new(Crawler)
	crawler.config = config
	crawler.taskWorkerCount = 0
	crawler.apiKeys = newAPIKeyPool(config.Crawler.Keys, config.Crawler.BlockDuration)
	crawler.stopSignal = 0
	crawler.interruptChannels = nil
	crawler.dataStore = dataStore
	crawler.httpClient = new(http.Client)
	return crawler
}

func (crawler *Crawler) interruptTaskWorkers() {
	for i := 0; i < crawler.taskWorkerCount; i++ {
		crawler.interruptChannels[i] <- true
	}
}

// -------------------------------------------------------------------------------------------------------------
// API calls
// -------------------------------------------------------------------------------------------------------------

func (crawler *Crawler) apiBaseURL() *url.URL {
	r, _ := url.Parse("https://www.haloapi.com")
	return r
}

func (crawler *Crawler) apiCall(req *http.Request) (*http.Response, error) {
	key := crawler.apiKeys.pop()
	defer crawler.apiKeys.push(key)
	req.Header.Set("Ocp-Apim-Subscription-Key", key.value)
	return crawler.httpClient.Do(req)
}

// -------------------------------------------------------------------------------------------------------------
// Control
// -------------------------------------------------------------------------------------------------------------

// Stop the task workers
func (crawler *Crawler) Stop() {
	atomic.StoreUint32(&crawler.stopSignal, 1)
	crawler.interruptTaskWorkers()
	crawler.taskWorkers.Wait()
	crawler.taskWorkerCount = 0
	crawler.stopSignal = 0
	crawler.interruptChannels = nil
}

// Start the task workers
func (crawler *Crawler) Start() {
	// Check worker count
	if crawler.taskWorkerCount > 0 {
		log.Println("task workers already active")
		return
	}

	// Run workers
	for i := 0; i < crawler.config.Crawler.TaskWorkers; i++ {
		crawler.taskWorkers.Add(1)
		crawler.taskWorkerCount++
		crawler.interruptChannels = append(crawler.interruptChannels, make(chan bool, 1))

		// Run worker
		go func(workerID int) {
			// Log that the worker is active
			log.Printf("task worker %d active\n", workerID)

			// Loop until workers are dying
			for atomic.LoadUint32(&crawler.stopSignal) == 0 {
				// Get a new key
				// This may block if no keys are available
				key := crawler.apiKeys.pop()

				// Check if we have to shutdown
				if atomic.LoadUint32(&crawler.stopSignal) != 0 {
					break
				}

				// Fetch task
				task, err := crawler.dataStore.startTask()
				if err == sql.ErrNoRows {
					// Release the key
					crawler.apiKeys.pushUnused(key)

					// We could be interrupted
					select {
					case <-crawler.interruptChannels[workerID]:
					case <-time.After(1 * time.Minute):
					}

					// Check if we have to shutdown
					if atomic.LoadUint32(&crawler.stopSignal) != 0 {
						break
					}
					continue
				}
				if err != nil {
					log.Println(err)
					crawler.apiKeys.pushUnused(key)
					continue
				}

				switch task.Type {
				case TaskMatchHistoryScan:
					err = crawler.scanMatchHistory(task)
				case TaskPlayerStatsUpdate:
					err = crawler.updatePlayerStats(task)
				case TaskMatchResultUpdate:
					err = crawler.updateMatchResult(task)
				case TaskMatchEventsUpdate:
					err = crawler.updateMatchEvents(task)
				default:
					log.Printf("unknown task type: %d\n", task.ID)
				}

				if err != nil {
					log.Println(err)
					crawler.apiKeys.push(key)
					continue
				}

				crawler.apiKeys.push(key)
			}

			// Mark worker as stopped
			crawler.taskWorkers.Done()
		}(i)
	}
}

// -------------------------------------------------------------------------------------------------------------
// Match
// -------------------------------------------------------------------------------------------------------------

// Get a match
func (crawler *Crawler) loadMatch(matchId string) (*Match, error) {
	// Build URL
	u := crawler.apiBaseURL()
	u.Path += "/stats/hw2/matches/"
	u.Path += matchId

	// Send request
	req, err := http.NewRequest("GET", u.String(), nil)
	resp, err := crawler.apiCall(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == 403 {
		return nil, ErrForbidden
	}
	if resp.StatusCode == 404 {
		return nil, ErrNotFound
	}
	if resp.StatusCode == 429 {
		return nil, ErrRateLimit
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, ErrNoSuccess
	}
	data, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		log.Println(err)
		return nil, err
	}

	// Parse response
	match := new(Match)
	err = json.Unmarshal(data, match)
	if err != nil {
		return nil, err
	}
	return match, nil
}

func (crawler *Crawler) updateMatchResult(task *Task) error {
	// The match exists already?
	if crawler.dataStore.matchExists(task.Data.MatchUUID) {
		log.Printf("[match %s] already exists", task.Data.MatchUUID)
		crawler.dataStore.finishTask(task)
		return nil
	}

	// Get the match
	match, err := crawler.loadMatch(task.Data.MatchUUID)

	// No such user? (404)
	if err == ErrNotFound {
		log.Printf("[match %s] match not found", task.Data.MatchUUID)
		return crawler.dataStore.finishTask(task)
	}

	// Hit the rate limit? (429)
	if err == ErrRateLimit {
		log.Printf("[match %s] reschedule due to rate limit", task.Data.MatchUUID)
		return crawler.dataStore.deferTask(task)
	}

	// Try again if there was an unexpected error
	if err != nil {
		log.Printf("[match %s] failed to query match: %v", task.Data.MatchUUID, err)
		return crawler.dataStore.blockTask(task)
	}

	// Insert match
	err = crawler.dataStore.storeMatch(match)
	if err == ErrMetadataIncomplete {
		log.Printf("[match %s] incomplete metadata", task.Data.MatchUUID)
		return crawler.dataStore.blockTask(task)
	}
	if err != nil {
		log.Printf("[match %s] failed to store match: %v", task.Data.MatchUUID, err)
		return crawler.dataStore.blockTask(task)
	}

	log.Printf("[match %s] updated match", task.Data.MatchUUID)
	crawler.dataStore.finishTask(task)
	return nil
}

func (bot *Bot) updateMatchResult(msg *discordgo.MessageCreate, matchUUID string) {

	// The match exists already?
	if bot.dataStore.matchExists(matchUUID) {
		return
	}

	// Get the match
	match, err := bot.crawler.loadMatch(matchUUID)

	// No such user? (404)
	if err == ErrNotFound {
		bot.sendResponse(msg, fmt.Sprintf("The Halo API does not return any data for the match **%s**.", matchUUID))
		return
	}

	// Hit the rate limit? (429)
	if err == ErrRateLimit {
		bot.sendResponse(msg, fmt.Sprintf("I just hit the API rate limit, please ask x6767 to update the match manually **%s**.", matchUUID))
		return
	}

	// Try again if there was an unexpected error
	if err != nil {
		bot.sendResponse(msg, fmt.Sprintf("Ouch! Something went wrong loading the match results: %v.", err))
		return
	}

	// Insert match
	err = bot.dataStore.storeMatch(match)
	if err == ErrMetadataIncomplete {
		bot.sendResponse(msg, fmt.Sprintf("It seems like this match has metadata that I don't understand: **%s**.", matchUUID))
		return
	}
	if err != nil {
		bot.sendResponse(msg, fmt.Sprintf("Ouch! Something went wrong storing the match results: %v.", err))
		return
	}
	return
}

// -------------------------------------------------------------------------------------------------------------
// Match Events
// -------------------------------------------------------------------------------------------------------------

// Get match events
func (crawler *Crawler) loadMatchEvents(matchUUID string) (*MatchEvents, error) {
	// Build URL
	u := crawler.apiBaseURL()
	u.Path += "/stats/hw2/matches/"
	u.Path += matchUUID
	u.Path += "/events"

	// Send request
	req, err := http.NewRequest("GET", u.String(), nil)
	resp, err := crawler.apiCall(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == 403 {
		return nil, ErrForbidden
	}
	if resp.StatusCode == 404 {
		return nil, ErrNotFound
	}
	if resp.StatusCode == 429 {
		return nil, ErrRateLimit
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, ErrNoSuccess
	}

	// Read into buffer
	buffer, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		log.Println(err)
		return nil, err
	}

	// Build match events
	events := new(MatchEvents)

	// Check if complete
	data := gjson.Parse(string(buffer))
	events.isCompleteSet = data.Get("IsCompleteSetOfEvents").Bool()

	// Unmarshal events
	for _, gameEvent := range data.Get("GameEvents").Array() {
		raw := []byte(gameEvent.Raw)
		switch gameEvent.Get("EventName").String() {
		case "BuildingConstructionQueued":
			event := new(MatchEventBuildingConstructionQueued)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.BuildingConstructionQueued = append(events.BuildingConstructionQueued, event)
			}
			break
		case "BuildingConstructionCompleted":
			event := new(MatchEventBuildingConstructionCompleted)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.BuildingConstructionCompleted = append(events.BuildingConstructionCompleted, event)
			}
			break
		case "BuildingRecycled":
			event := new(MatchEventBuildingRecycled)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.BuildingRecycled = append(events.BuildingRecycled, event)
			}
			break
		case "BuildingUpgraded":
			event := new(MatchEventBuildingUpgraded)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.BuildingUpgraded = append(events.BuildingUpgraded, event)
			}
			break
		case "CardCycled":
			event := new(MatchEventCardCycled)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.CardCycled = append(events.CardCycled, event)
			}
			break
		case "CardPlayed":
			event := new(MatchEventCardPlayed)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.CardPlayed = append(events.CardPlayed, event)
			}
			break
		case "Death":
			event := new(MatchEventDeath)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.Death = append(events.Death, event)
			}
			break
		case "FirefightWaveCompleted":
			event := new(MatchEventFirefightWaveCompleted)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.FirefightWaveCompleted = append(events.FirefightWaveCompleted, event)
			}
			break
		case "FirefightWaveSpawned":
			event := new(MatchEventFirefightWaveSpawned)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.FirefightWaveSpawned = append(events.FirefightWaveSpawned, event)
			}
			break
		case "FirefightWaveStarted":
			event := new(MatchEventFirefightWaveStarted)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.FirefightWaveStarted = append(events.FirefightWaveStarted, event)
			}
			break
		case "LeaderPowerCast":
			event := new(MatchEventLeaderPowerCast)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.LeaderPowerCast = append(events.LeaderPowerCast, event)
			}
			break
		case "LeaderPowerUnlocked":
			event := new(MatchEventLeaderPowerUnlocked)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.LeaderPowerUnlocked = append(events.LeaderPowerUnlocked, event)
			}
			break
		case "ManaOrbCollected":
			event := new(MatchEventManaOrbCollected)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.ManaOrbCollected = append(events.ManaOrbCollected, event)
			}
			break
		case "MatchEnd":
			event := new(MatchEventMatchEnd)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.MatchEnd = append(events.MatchEnd, event)
			}
			break
		case "MatchStart":
			event := new(MatchEventMatchStart)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.MatchStart = append(events.MatchStart, event)
			}
			break
		case "PlayerEliminated":
			event := new(MatchEventPlayerEliminated)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.PlayerEliminated = append(events.PlayerEliminated, event)
			}
			break
		case "PlayerJoinedMatch":
			event := new(MatchEventPlayerJoinedMatch)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.PlayerJoined = append(events.PlayerJoined, event)
			}
			break
		case "PlayerLeftMatch":
			event := new(MatchEventPlayerLeftMatch)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.PlayerLeft = append(events.PlayerLeft, event)
			}
			break
		case "PointCaptured":
			event := new(MatchEventPointCaptured)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.PointCaptured = append(events.PointCaptured, event)
			}
			break
		case "PointCreated":
			event := new(MatchEventPointCreated)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.PointCreated = append(events.PointCreated, event)
			}
			break
		case "PointStatusChange":
			event := new(MatchEventPointStatusChange)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.PointStatusChange = append(events.PointStatusChange, event)
			}
			break
		case "ResourceHeartbeat":
			event := new(MatchEventResourceHeartbeat)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.ResourceHeartbeat = append(events.ResourceHeartbeat, event)
			}
			break
		case "ResourceTransferred":
			event := new(MatchEventResourceTransferred)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.ResourceTransferred = append(events.ResourceTransferred, event)
			}
			break
		case "TechResearched":
			event := new(MatchEventTechResearched)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.TechResearched = append(events.TechResearched, event)
			}
			break
		case "UnitControlTransferred":
			event := new(MatchEventUnitControlTransferred)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.UnitControlTransferred = append(events.UnitControlTransferred, event)
			}
			break
		case "UnitPromoted":
			event := new(MatchEventUnitPromoted)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.UnitPromoted = append(events.UnitPromoted, event)
			}
			break
		case "UnitTrained":
			event := new(MatchEventUnitTrained)
			err = json.Unmarshal(raw, &event)
			if err == nil {
				events.UnitTrained = append(events.UnitTrained, event)
			}
			break
		default:
			break
		}
	}

	// Return match events
	return events, nil
}

func (crawler *Crawler) updateMatchEvents(task *Task) error {
	// Get the match events
	matchEvents, err := crawler.loadMatchEvents(task.Data.MatchUUID)

	// No such match? (404)
	if err == ErrNotFound {
		log.Printf("[match_event %s] match not found", task.Data.MatchUUID)
		return crawler.dataStore.finishTask(task)
	}

	// Hit the rate limit? (429)
	if err == ErrRateLimit {
		log.Printf("[match_event %s] reschedule due to rate limit", task.Data.MatchUUID)
		return crawler.dataStore.deferTask(task)
	}

	// Try again if there was an unexpected error
	if err != nil {
		log.Printf("[match_event %s] failed to get events: %v", task.Data.MatchUUID, err)
		return crawler.dataStore.blockTask(task)
	}

	// Insert player stats
	err = crawler.dataStore.storeMatchEvents(task.Data.MatchID, matchEvents)
	if err != nil {
		log.Printf("[match_event %s] failed to store events: %v", task.Data.MatchUUID, err)
		return crawler.dataStore.blockTask(task)
	}

	log.Printf("[match_event %s] stored events", task.Data.MatchUUID)
	crawler.dataStore.finishTask(task)
	return nil
}

// -------------------------------------------------------------------------------------------------------------
// Match History
// -------------------------------------------------------------------------------------------------------------

// Get the match history for a gamer tag
func (crawler *Crawler) loadMatchHistory(gamertag string, start int, count int) (*PlayerMatchHistory, error) {
	// Build URL
	u := crawler.apiBaseURL()
	u.Path += "/stats/hw2/players/"
	u.Path += gamertag
	u.Path += "/matches"

	// Build query parameters
	q := &url.Values{}
	q.Add("start", strconv.Itoa(start))
	q.Add("count", strconv.Itoa(count))
	u.RawQuery = q.Encode()

	// Send request
	req, err := http.NewRequest("GET", u.String(), nil)
	resp, err := crawler.apiCall(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == 403 {
		return nil, ErrForbidden
	}
	if resp.StatusCode == 404 {
		return nil, ErrNotFound
	}
	if resp.StatusCode == 429 {
		return nil, ErrRateLimit
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, ErrNoSuccess
	}
	data, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		log.Println(err)
		return nil, err
	}

	// Parse response
	history := new(PlayerMatchHistory)
	err = json.Unmarshal(data, history)
	if err != nil {
		return nil, err
	}
	return history, nil
}

// Scan a match history
func (crawler *Crawler) scanMatchHistory(task *Task) error {
	// Figure out where to start
	count := 25
	start := 0
	if task.Data.ScanDirection == ScanFromLowerBound {
		start = task.Data.LowerBound
	} else {
		start = max(task.Data.UpperBound-count, task.Data.LowerBound)
	}

	// Get the match history
	history, err := crawler.loadMatchHistory(task.Data.Gamertag, start, count)

	// No such user? (404)
	if err == ErrNotFound {
		log.Printf("[match_history %s] player not found", task.Data.Gamertag)
		return crawler.dataStore.finishTask(task)
	}

	// Hit the rate limit? (429)
	if err == ErrRateLimit {
		log.Printf("[match_history %s] reschedule due to rate limit", task.Data.Gamertag)
		return crawler.dataStore.deferTask(task)
	}

	// Try again if there was an unexpected error
	if err != nil {
		log.Printf("[match_history %s] failed to query history: %v", task.Data.Gamertag, err)
		return crawler.dataStore.blockTask(task)
	}

	// Insert all matches into the database
	totalMatches := 0
	var newMatches []string
	for _, result := range history.Results {
		totalMatches += 1
		row := crawler.dataStore.storePlayerMatch(task.Data.PlayerID, &result)
		var matchId string
		switch err := row.Scan(&matchId); err {
		case sql.ErrNoRows:
			log.Printf("[match_history %s] duplicate match (+%d): %s", task.Data.Gamertag, start, result.MatchId)
			break
		case nil:
			newMatches = append(newMatches, result.MatchId)
			log.Printf("[match_history %s] new match (+%d): %s", task.Data.Gamertag, start, result.MatchId)
			break
		default:
			log.Printf("[match_history %s] failed to insert match (+%d): %s", task.Data.Gamertag, start, result.MatchId)
			log.Println(err)
			break
		}
	}

	// Create match update tasks for every new match
	for i := range newMatches {
		var task Task
		task.Data.MatchUUID = newMatches[i]
		task.Type = TaskMatchResultUpdate
		task.Updated = time.Now()
		task.Status = TaskQueued
		task.Priority = 20
		err = crawler.dataStore.postTask(&task)
		if err != nil {
			log.Printf("[match_history %s] failed to initialize match update: %v", task.Data.Gamertag, err)
		}
	}

	// Did we work on the lower bound?
	if task.Data.ScanDirection == ScanFromLowerBound {
		// Full table scan?
		if task.Data.FullScan && totalMatches > 0 {
			task.Data.LowerBound += count
			return crawler.dataStore.deferTask(task)
		}

		// Continue with the lower bound?
		if len(newMatches) > 0 {
			task.Data.LowerBound += count
			return crawler.dataStore.deferTask(task)
		}

		// Should we switch to the upper bound?
		if task.Data.UpperBound > task.Data.LowerBound {
			task.Data.ScanDirection = ScanFromUpperBound
			return crawler.dataStore.deferTask(task)
		}

	} else {
		// Update upper bound
		if len(newMatches) > 0 {
			task.Data.UpperBound = max(task.Data.UpperBound-count, 0)
			return crawler.dataStore.deferTask(task)

		}
	}

	// Finish the task otherwise
	log.Printf("[match_history %s] finished history scan", task.Data.Gamertag)
	crawler.dataStore.finishTask(task)
	return nil
}

// -------------------------------------------------------------------------------------------------------------
// Player Stats
// -------------------------------------------------------------------------------------------------------------

// Get the match history for a gamer tag
func (crawler *Crawler) loadPlayerStats(gamertag string) (*PlayerStats, error) {
	// Build URL
	u := crawler.apiBaseURL()
	u.Path += "/stats/hw2/players/"
	u.Path += gamertag
	u.Path += "/stats"

	// Send request
	req, err := http.NewRequest("GET", u.String(), nil)
	resp, err := crawler.apiCall(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == 403 {
		return nil, ErrForbidden
	}
	if resp.StatusCode == 404 {
		return nil, ErrNotFound
	}
	if resp.StatusCode == 429 {
		return nil, ErrRateLimit
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, ErrNoSuccess
	}
	data, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		log.Println(err)
		return nil, err
	}

	// Parse response
	stats := new(PlayerStats)
	err = json.Unmarshal(data, stats)
	if err != nil {
		return nil, err
	}
	return stats, nil
}

func (crawler *Crawler) updatePlayerStats(task *Task) error {
	// Get the player stats
	stats, err := crawler.loadPlayerStats(task.Data.Gamertag)

	// No such user? (404)
	if err == ErrNotFound {
		log.Printf("[player_stats %s] player not found", task.Data.Gamertag)
		return crawler.dataStore.finishTask(task)
	}

	// Hit the rate limit? (429)
	if err == ErrRateLimit {
		log.Printf("[player_stats %s] reschedule due to rate limit", task.Data.Gamertag)
		return crawler.dataStore.deferTask(task)
	}

	// Try again if there was an unexpected error
	if err != nil {
		log.Printf("[player_stats %s] failed to query player stats: %v", task.Data.Gamertag, err)
		return crawler.dataStore.blockTask(task)
	}

	// Needs history scan?
	needsScan, _, err := crawler.dataStore.playerNeedsHistoryScan(task.Data.PlayerID, stats)
	if err != nil {
		log.Printf("[player_stats %s] failed to check player match count: %v", task.Data.Gamertag, err)
	}
	if needsScan {
		var scan Task
		scan.Data.Gamertag = task.Data.Gamertag
		scan.Data.PlayerID = task.Data.PlayerID
		scan.Data.LowerBound = 0
		scan.Data.UpperBound = 0
		scan.Data.ScanDirection = ScanFromLowerBound
		scan.Type = TaskMatchHistoryScan
		scan.Updated = time.Now()
		scan.Status = TaskQueued
		scan.Priority = 10
		err = crawler.dataStore.postTask(&scan)
		if err != nil {
			log.Printf("[player_stats %s] failed to initialize history scan: %v", task.Data.Gamertag, err)
		}
	}

	// Insert player stats
	err = crawler.dataStore.storePlayerStats(task.Data.PlayerID, stats)
	if err != nil {
		log.Printf("[player_stats %s] failed to insert player stats: %v", task.Data.Gamertag, err)
		return crawler.dataStore.blockTask(task)
	}

	if needsScan {
		log.Printf("[player_stats %s] scheduled history scan", task.Data.Gamertag)
	} else {
		log.Printf("[player_stats %s] no new matches", task.Data.Gamertag)
	}
	crawler.dataStore.finishTask(task)
	return nil
}
