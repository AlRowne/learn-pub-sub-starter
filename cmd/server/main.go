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
	fmt.Println("Connection successful!")
	amqpChan, err := conn.Channel()
	if err != nil {
		return err
	}
	defer amqpChan.Close()

	ch, _, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, routing.GameLogSlug, "game_logs.*", pubsub.Durable)
	if err != nil {
		return err
	}
	defer ch.Close()

	gamelogic.PrintServerHelp()

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch input[0] {
		case "pause":
			err := pubsub.PublishJSON(amqpChan,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true,
				})
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Println("pausing the game ...")
		case "resume":
			err := pubsub.PublishJSON(amqpChan,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				})
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Println("resuming the game...")
		case "quit":
			fmt.Println("exiting...")
			return nil
		default:
			fmt.Println("unknown command...")
		}
	}
}
func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
