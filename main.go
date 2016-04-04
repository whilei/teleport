package main

import (
	"fmt"
	"github.com/pagarme/teleport/config"
	"github.com/pagarme/teleport/server"
)

func main() {
	config := config.New()

	// Load config file
	err := config.ReadFromFile("source_config.yml")

	// Start db
	if err = config.Database.Start(); err != nil {
		fmt.Printf("ERROR STARTING DATABASE: %v\n", err)
	}

	// Install triggers for each target
	for _, target := range config.Targets {
		config.Database.InstallTriggers(target.SourceTables)
	}

	server := server.New(&config.Database, config.ServerHTTP)

	// Start HTTP server
	if err = server.Start(); err != nil {
		fmt.Printf("ERROR STARTING SERVER: %v\n", err)
	}

	config.Database.WatchEvents(5)
}
