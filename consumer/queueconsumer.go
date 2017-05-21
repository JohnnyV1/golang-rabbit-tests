package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/streadway/amqp"
	"github.com/vinujohn/qsvc"
)

var counter int         //keeps track of messages processed
var starttime time.Time //time before goroutines started
var mu sync.Mutex       //sync between shared goroutine state

const (
	numberOfMessages        int = 1000  //number of messages to publish to the queue
	inputToLargestPrimeFunc     = 10000 //input to the work function which each goroutine calls
	numberOfProcessors          = -1    //number of processors to use for this program. -1 signifies use default which should be cores on the machine.
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

	starttime = time.Now()

	svc := qsvc.NewQueueService("amqp://guest:guest@localhost:5672/")

	svc.Receive("hello", receiver)
}
func receiver(b []byte) {
	log.Printf("Received a message: %s\n", b)
	value, _ := largestPrimeFactor(inputToLargestPrimeFunc)
	log.Printf("Largest prime %d\n", value)
	exit()
}

func exit() {
	mu.Lock()
	counter++
	if counter == numberOfMessages {
		i := time.Since(starttime)
		log.Printf("Time Taken: %d", (i.Nanoseconds() / int64(1000000)))
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
