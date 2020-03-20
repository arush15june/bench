package main

import (
	"fmt"
	"time"

	"github.com/arush15june/bench"
	"github.com/arush15june/bench/requester"
)

func main() {
	r := &requester.PulsarRequesterFactory{
		URL:         "pulsar://localhost:6650",
		PayloadSize: 500,
		Topic:       "benchmark",
	}

	benchmark := bench.NewBenchmark(r, 10000, 1, 30*time.Second, 0)
	summary, err := benchmark.Run()
	if err != nil {
		panic(err)
	}

	fmt.Println(summary)
	summary.GenerateLatencyDistribution(nil, "nats.txt")
}
