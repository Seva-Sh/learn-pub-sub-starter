package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	// create a new connection to RabbitMQ
	url := "amqp://guest:guest@localhost:5672/"
	c, err := amqp.Dial(url)
	if err != nil {
		log.Println("Error:", err)
		return
	}
	defer c.Close()

	fmt.Println("Connection successful")

	// create a channel
	ch, err := c.Channel()
	if err != nil {
		log.Println("Error:", err)
		return
	}

	gamelogic.PrintServerHelp()
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "pause":
			fmt.Println("Sending a pause message")
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			if err != nil {
				log.Println("Error:", err)
				return
			}
		case "resume":
			fmt.Println("Sending a resume message:")
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			if err != nil {
				log.Println("Error:", err)
				return
			}
		case "quit":
			fmt.Println("Exiting the game")
			return
		default:
			fmt.Println("Unknown command")
		}

	}

	// // listen for a signal
	// signalChan := make(chan os.Signal, 1)
	// signal.Notify(signalChan, os.Interrupt)
	// // wait until signal received
	// <-signalChan

	// fmt.Println("Program is Shutting Down")
}
