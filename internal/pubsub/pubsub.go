package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func DeclareAndBind(conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, err
	}
	var isDurable bool
	var autodelete bool
	var exclusive bool
	switch queueType {
	case Durable:
		isDurable = true
		autodelete = false
		exclusive = false
	case Transient:
		isDurable = false
		autodelete = true
		exclusive = true
	default:
		return &amqp.Channel{}, amqp.Queue{}, errors.New("no valid queueType")
	}
	q, err := ch.QueueDeclare(queueName, isDurable, autodelete, exclusive, false, nil)
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, err
	}
	if err := ch.QueueBind(queueName, key, exchange, false, nil); err != nil {
		return &amqp.Channel{}, amqp.Queue{}, err
	}
	return ch, q, nil
}

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonBody, err := json.Marshal(&val)
	if err != nil {
		return err
	}
	err = ch.PublishWithContext(context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        jsonBody,
		})
	if err != nil {
		return err
	}
	return nil
}

func SubscribeJSON[T any](conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	ch, q, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	deliveryCh, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for delivery := range deliveryCh {
			var target T
			if err := json.Unmarshal(delivery.Body, &target); err != nil {
				if err := delivery.Reject(false); err != nil {
					fmt.Println(err)
					continue
				}
				fmt.Println(err)
				continue
			}
			switch handler(target) {
			case Ack:
				delivery.Ack(false)
				fmt.Println("msg processed successfully")
			case NackRequeue:
				delivery.Nack(false, true)
				fmt.Println("msg requeued")
			case NackDiscard:
				delivery.Nack(false, false)
				fmt.Println("msg discarded")
			}
		}
	}()
	return nil
}
