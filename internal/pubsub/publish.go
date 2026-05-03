package pubsub

import (
	"context"
	"encoding/json"

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
	if queueType == SimpleQueueDurable {
		q, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
	} else if queueType == SimpleQueueTransient {
		q, err = ch.QueueDeclare(queueName, false, true, true, false, nil)
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
