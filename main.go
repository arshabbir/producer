package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "net/http/pprof"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {

	fmt.Println("Starting the Producer ....")

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": os.Getenv("KAFKA_HOST")})

	if err != nil {
		panic(err)
	}

	defer p.Close()

	wait := make(chan int)
	//Start the matrics API
	go startMetrics(wait)

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	interval, err := strconv.Atoi(os.Getenv("TRAFFIC_INTERVAL"))

	if err != nil {
		interval = 30
	}
	topic := os.Getenv("TOPIC")
	for {

		msg := randGen()
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic},
			Value:          []byte(msg),
		}, nil)
		fmt.Println(msg)
		time.Sleep(time.Second * time.Duration(interval))
	}

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)

	<-wait
}

func randGen() string {
	//Generate random traffic
	r := rand.Int63n(50000000000)
	t := time.Now()
	return fmt.Sprintf("Name:name%d,Dept=OSS%d,EmplD:%d, Time=%s", r, r, r, t)
}

func startMetrics(wait chan int) {
	r := mux.NewRouter()

	r.Handle("/metrics", promhttp.Handler())

	r.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux)

	defer func() {
		wait <- 0
	}()

	srv := &http.Server{
		Handler: r,
		Addr:    os.Getenv("PORT"),
		// Good practice: enforce timeouts for servers you create!
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	log.Fatal(srv.ListenAndServe())

}
