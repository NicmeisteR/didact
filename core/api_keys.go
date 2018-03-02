package core

import (
	"time"
)

type apiKeyPool struct {
	queue         chan *apiKeyToken
	blockDuration time.Duration
}

type apiKeyToken struct {
	pool         *apiKeyPool
	value        string
	blockedUntil time.Time
}

// Push a key to the pool
func (p *apiKeyPool) push(key *apiKeyToken) {
	// Wait the remaining time
	now := time.Now()
	delta := key.blockedUntil.Sub(now)
	if delta > 10*time.Millisecond {
		time.Sleep(delta)
	}
	key.blockedUntil = now.Add(delta + p.blockDuration)

	// Release to pool
	p.queue <- key
}

// Push an unused key to the pool
func (p *apiKeyPool) pushUnused(key *apiKeyToken) {
	p.queue <- key
}

// Pop a key from the pool
func (p *apiKeyPool) pop() *apiKeyToken {
	return <-p.queue
}

// Create a new key pool
func newAPIKeyPool(keys []APIKeyConfig, blockDuration int) *apiKeyPool {
	p := new(apiKeyPool)
	p.blockDuration = time.Duration(blockDuration) * time.Millisecond

	// Create queue
	tokenCount := 0
	for _, key := range keys {
		tokenCount += key.Rate
	}
	p.queue = make(chan *apiKeyToken, tokenCount)

	// Create keys
	for _, key := range keys {
		for i := 0; i < key.Rate; i++ {
			k := new(apiKeyToken)
			k.pool = p
			k.value = key.Key
			k.blockedUntil = time.Now()
			p.queue <- k
		}
	}
	return p
}
