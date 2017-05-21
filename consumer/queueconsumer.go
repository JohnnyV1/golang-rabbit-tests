package main

import (
	"fmt"
	"log"
	"runtime"

	"time"

	"os"

	"sync"

	"github.com/streadway/amqp"
)

var counter int
var starttime time.Time
var mu sync.Mutex

const (
	numberOfMessages        int = 1000
	numberOfGoRoutines          = 1000
	inputToLargestPrimeFunc     = 10000
	numberOfProcessors          = -1
)

func main() {

	runtime.GOMAXPROCS(numberOfProcessors)

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when usused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	body := "hello"

	for i := 0; i < numberOfMessages; i++ {
		ch.Publish(
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

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	starttime = time.Now()

	for i := 1; i <= numberOfGoRoutines; i++ {
		go func(i int) {
			for d := range msgs {
				log.Printf("Received a message %d: %s\n", i, d.Body)
				value, _ := largestPrimeFactor(inputToLargestPrimeFunc)
				log.Printf("Largest prime %d\n", value)
				exit()
			}
		}(i)
	}

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever

}

func exit() {
	mu.Lock()
	counter++
	if counter == numberOfMessages {
		i := time.Now().Unix() - starttime.Unix()
		log.Printf("Time Taken: %d", i)
		os.Exit(0)
	}
	mu.Unlock()
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func largestPrimeFactor(n int) (int, error) {
	if n == 2 || n == 3 {
		return n, nil
	}

	largestFactor := 0
	for factor := 2; factor < n; factor++ {
		isPrime := true
		for i := 2; i < factor; i++ {
			if 0 == (factor % i) {
				isPrime = false
				break
			}
		}
		if isPrime && 0 == (n%factor) {
			largestFactor = factor
		}
	}

	return largestFactor, nil
}
