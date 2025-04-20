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
	fmt.Println("Starting Peril client...")
	const rabbitConnString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("could not retrieve username: %v", err)
	}

	publishCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	if _, _, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+username,
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
	); err != nil {
		log.Fatalf("could not declare and bind: %v", err)
	}

	gamestate := gamelogic.NewGameState(username)

	if err := pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, routing.PauseKey+"."+username, routing.PauseKey, pubsub.SimpleQueueTransient, handlerPause(gamestate)); err != nil {
		fmt.Println(err)
	}

	if err := pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+username, routing.ArmyMovesPrefix+".*", pubsub.SimpleQueueTransient, handlerMove(gamestate, publishCh)); err != nil {
		fmt.Println(err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "move":
			mv, err := gamestate.CommandMove(words)
			if err != nil {
				fmt.Println(err)
				continue
			} else if err := pubsub.PublishJSON(publishCh, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+username, mv); err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Printf("Moved %v units to %s\n", len(mv.Units), mv.ToLocation)
		case "spawn":
			if err := gamestate.CommandSpawn(words); err != nil {
				fmt.Println(err)
				continue
			}
		case "status":
			gamestate.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("unknown command")
		}
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.Acktype {
	return func(ps routing.PlayingState) pubsub.Acktype {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.Acktype {
	return func(move gamelogic.ArmyMove) pubsub.Acktype {
		defer fmt.Print("> ")
		mov := gs.HandleMove(move)
		if mov == gamelogic.MoveOutComeSafe {
			return pubsub.Ack
		} else if mov == gamelogic.MoveOutcomeMakeWar {
			if err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, string(routing.WarRecognitionsPrefix)+"."+gs.GetUsername(), mov); err != nil {
				fmt.Println(err)
			}
			return pubsub.NackRequeue
		} else {
			return pubsub.NackDiscard
		}
	}
}
