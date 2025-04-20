package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int
type Acktype int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

const (
	Ack Acktype = iota
	NackRequeue
	NackDiscard
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	dat, err := json.Marshal(val)
	if err != nil {
		return err
	}
	return ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        dat,
	})
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return err
	}
	dels, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() error {
		for del := range dels {
			var msg T
			if err := json.Unmarshal(del.Body, &msg); err != nil {
				fmt.Printf("Error unmarshaling message: %v\n", err)
				continue
			}
			ack := handler(msg)
			switch ack {
			case Ack:
				if err := del.Ack(false); err != nil {
					fmt.Printf("error acknowleding message: %v\n", err)
				}
				fmt.Println("Ack")
			case NackRequeue:
				if err := del.Nack(false, true); err != nil {
					fmt.Printf("error acknowleding message: %v\n", err)
				}
				fmt.Println("NackRequeue")
			case NackDiscard:
				if err := del.Nack(false, false); err != nil {
					fmt.Printf("error acknowleding message: %v\n", err)
				}
				fmt.Println("NackDiscard")
			}

		}
		return nil
	}()
	return nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	var durable, autoDelete, exclusive bool
	switch simpleQueueType {
	case 0:
		durable, autoDelete, exclusive = true, false, false
	case 1:
		durable, autoDelete, exclusive = false, true, true
	}
	q, err := ch.QueueDeclare(queueName, durable, autoDelete, exclusive, false, amqp.Table{"x-dead-letter-exchange": "peril_dlx"})
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	err = ch.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	return ch, q, nil
}
