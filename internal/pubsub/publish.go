package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonData, err := json.Marshal(val)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        jsonData,
	})
	if err != nil {
		return err
	}

	return nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	// make sure the given queue exists and is bound to the exchange
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	// get a new chan
	newChan, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	// start a goroutine that ranges over the channel of deliveries and act on each message
	go func() {
		for message := range newChan {
			// unmarshal the bode of each message delivery
			var v T
			innerErr := json.Unmarshal(message.Body, &v)
			if innerErr != nil {
				continue
			}
			// call the given handler func
			acktype := handler(v)
			// acknowledge the message
			switch acktype {
			case Ack:
				innerErr = message.Ack(false)
				fmt.Println("Ack")
			case NackRequeue:
				innerErr = message.Nack(false, true)
				fmt.Println("NackRequeue")
			case NackDiscard:
				innerErr = message.Nack(false, false)
				fmt.Println("NackDiscard")
			}
			if innerErr != nil {
				continue
			}

		}
	}()

	return nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // is an enum type to represent durable or transient
) (*amqp.Channel, amqp.Queue, error) {
	var q amqp.Queue
	// create a new channel on the connection
	ch, err := conn.Channel()
	if err != nil {
		return ch, q, err
	}

	// declare a new queue
	switch queueType {
	case SimpleQueueDurable:
		q, err = ch.QueueDeclare(queueName, true, false, false, false, amqp.Table{"x-dead-letter-exchange": routing.ExchangePerilDLX})
	case SimpleQueueTransient:
		q, err = ch.QueueDeclare(queueName, false, true, true, false, amqp.Table{"x-dead-letter-exchange": routing.ExchangePerilDLX})
	}
	if err != nil {
		return ch, q, err
	}

	// bind the queue to the exchange
	err = ch.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return ch, q, err
	}

	return ch, q, nil
}
