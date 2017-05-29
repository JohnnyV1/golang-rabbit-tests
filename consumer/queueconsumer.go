//1,000,000 hello message processed in 140697 mm prefetchCount:0
//1,000,000 hello message processed in 120268 mm prefetchCount:20
//1,000,000 hello message processed in 103197 mm prefetchCount:100

package main

import (
	"fmt"
	"log"
	"runtime"
	"strconv"
	"sync"
	"time"

	"strings"

	"github.com/vinujohn/qsvc"
)

var counter int   //keeps track of messages processed
var mu sync.Mutex //sync between shared goroutine state

const (
	numberOfMessages        int = 100  //number of messages to publish to the queue
	inputToLargestPrimeFunc     = 1000 //input to the work function which each goroutine calls
	numberOfProcessors          = -1   //number of processors to use for this program. -1 signifies use default which should be cores on the machine.
)

type processor struct {
	subscriberName string
}

func (p processor) Process(message []byte) {
	log.Printf("Processing message: %s\n", message)
	value, _ := largestPrimeFactor(inputToLargestPrimeFunc)
	log.Printf("Largest prime %d\n", value)
	zero := 0
	if string(message) == "what" {
		//panic("Not expecting this value.")
		log.Printf("%d", 5/zero)
	}
}

func main() {

	runtime.GOMAXPROCS(numberOfProcessors)

	svc := qsvc.New("amqp://guest:guest@localhost:5672/")

	starttime := time.Now()
	for i := 0; i < numberOfMessages; i++ {
		message := strings.Join([]string{"hello world ", strconv.FormatInt(int64(i), 10)}, "")
		svc.Publish("hello", []byte(message))
	}
	log.Printf("Time Taken to publish %d: %d", numberOfMessages, getElapsedTime(starttime))

	svc.Subscribe("hello", processor{})

	//go svc2.Subscribe("hello", processor{subscriberName: "sub3"})

}

func getElapsedTime(startTime time.Time) int64 {
	return time.Since(startTime).Nanoseconds() / int64(1000000)
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
