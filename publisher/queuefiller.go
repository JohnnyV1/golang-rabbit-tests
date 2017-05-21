package main

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

func main() {
	connection, err := amqp.Dial("amqp://guest:guest@localhost:5672")

	defer connection.Close()

	failOnError(err, "Failed to connect")

	channel, err := connection.Channel()

	q, err := channel.QueueDeclare(
		"hello",
		false,
		false,
		false,
		false,
		nil,
	)

	body := "hello"

	for i := 0; i < 100; i++ {
		channel.Publish(
			"",
			q.Name,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			},
		)
	}
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
