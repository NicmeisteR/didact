package main

import (
	"gitlab.kohn.io/ankoh/didact/core"
	"log"
	"os"
	"os/signal"
)

func main() {
	// Set log flags
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Check arguments
	args := os.Args[1:]
	if len(args) != 1 {
		log.Println("usage: didact <config file>")
		os.Exit(1)
	}

	// Parse configuration
	config, err := core.ParseConfigFile(args[0])
	if err != nil {
		log.Printf("failed to parse config: %v\n", err)
		os.Exit(1)
	}
	log.Println("parsed config")

	// Create data store
	dataStore, err := core.NewDataStore(config)
	if err != nil {
		log.Printf("failed to create data store: %v\n", err)
		os.Exit(1)
	}
	log.Println("connected data store")

	// Create crawler
	crawler := core.NewCrawler(config, dataStore)
	crawler.Start()
	log.Println("started crawler")

	// Create bot
	bot := core.NewBot(config, dataStore, crawler)
	err = bot.Start()
	if err != nil {
		log.Printf("failed to create bot: %v\n", err)
		os.Exit(1)
	}
	log.Println("started bot")

	// handle sigint
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	for _ = range sigChan {
		log.Println("received an interrupt, stopping workers...")
		bot.Stop()
		crawler.Stop()
		return
	}
}
