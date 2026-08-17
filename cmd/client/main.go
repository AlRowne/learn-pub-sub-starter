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

	fmt.Println("Starting Peril client...")

	gamelogic.PrintClientHelp()
	gs := gamelogic.NewGameState(username)

	if err := pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		queueName,
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gs)); err != nil {
		return err
	}
	mvRoutingKey := fmt.Sprintf("army_moves.%s", username)
	if err := pubsub.SubscribeJSON(conn,
		routing.ExchangePerilTopic,
		mvRoutingKey,
		fmt.Sprintf("%s.*", routing.ArmyMovesPrefix),
		pubsub.Transient,
		handlerMove(gs)); err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

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
			move, err := gs.CommandMove(input)
			if err != nil {
				fmt.Println(err)
				continue
			}
			if err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				mvRoutingKey,
				move); err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Println("move published successfully")
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
func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
	return func(move gamelogic.ArmyMove) {
		defer fmt.Print("> ")
		gs.HandleMove(move)
	}
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
