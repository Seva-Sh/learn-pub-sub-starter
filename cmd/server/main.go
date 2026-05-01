package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	// create a new connection to RabbitMQ
	url := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(url)
	if err != nil {
		log.Println("Error:", err)
		return
	}
	defer conn.Close()

	fmt.Println("Connection successful")

	// listen for a signal
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	// wait until signal received
	<-signalChan

	fmt.Println("Program is Shutting Down")
}
