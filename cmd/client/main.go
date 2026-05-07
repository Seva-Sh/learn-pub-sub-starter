package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
	}
}

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
	fmt.Println("Connection successful")

	// prompt the user for a user name
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Println("Error:", err)
		return
	}

	// // declare and bind the pause queue
	// _, _, err = pubsub.DeclareAndBind(
	// 	c,
	// 	routing.ExchangePerilDirect,
	// 	fmt.Sprintf("%s.%s", routing.PauseKey, username),
	// 	routing.PauseKey,
	// 	pubsub.SimpleQueueTransient,
	// )
	// if err != nil {
	// 	log.Println("Error:", err)
	// 	return
	// }

	gameState := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(
		c,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, username),
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
		handlerPause(gameState),
	)
	if err != nil {
		log.Println("Error:", err)
		return
	}

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "spawn":
			err = gameState.CommandSpawn(words)
			if err != nil {
				log.Println("Error:", err)
				continue
			}
		case "move":
			_, err := gameState.CommandMove(words)
			if err != nil {
				log.Println("Error:", err)
				continue
			}
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
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
}
