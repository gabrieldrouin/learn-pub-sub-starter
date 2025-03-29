package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	url := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(url)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}

	defer conn.Close()
	fmt.Println("Connection succesful")

	connChan, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not make channel: %v", err)
	}
	data := routing.PlayingState{
		IsPaused: true,
	}
	err = pubsub.PublishJSON(connChan, routing.ExchangePerilDirect, routing.PauseKey, data)
	if err != nil {
		log.Fatalf("could not publish JSON: %v", err)
	}
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	sig := <-signalChan
	fmt.Println("Received signal:", sig)
	fmt.Println("Shutting down Peril server...")
}
