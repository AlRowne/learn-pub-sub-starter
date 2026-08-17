package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func run() error {
	const URL = "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(URL)
	if err != nil {
		return err
	}
	defer conn.Close()
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		return err
	}
	queueName := fmt.Sprintf("%s.%s", routing.PauseKey, username)
	ch, _, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.Transient)
	if err != nil {
		return err
	}
	defer ch.Close()

	fmt.Println("Starting Peril client...")

	gamelogic.PrintClientHelp()
	gs := gamelogic.NewGameState(username)

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch input[0] {
		case "spawn":
			if err := gs.CommandSpawn(input); err != nil {
				fmt.Println(err)
				continue
			}
		case "move":
			_, err := gs.CommandMove(input)
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Println("move successful")
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return nil
		default:
			fmt.Println("command not recognized...")
			continue
		}
	}
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
