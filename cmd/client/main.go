package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	// create a new connection to RabbitMQ
	url := "amqp://guest:guest@localhost:5672/"
	c, err := amqp.Dial(url)
	if err != nil {
		log.Println("Error:", err)
		return
	}
	defer c.Close()
	fmt.Println("Conenction successful")

	// prompt the user for a user name
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Println("Error:", err)
		return
	}

	// declare and bind the queue
	_, _, err = pubsub.DeclareAndBind(
		c,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, username),
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
	)
	if err != nil {
		log.Println("Error:", err)
		return
	}

	// listen for a signal
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	// wait until signal received
	<-signalChan
}
