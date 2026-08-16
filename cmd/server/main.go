package main

import (
	"fmt"
	"os"
	"os/signal"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const URL = "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(URL)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	fmt.Println("Connection successful!")
	amqpChan, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Closing the server ...")
}
