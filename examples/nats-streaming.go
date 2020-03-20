package main

import (
	"fmt"
	"time"
	"io/ioutil"

	"github.com/arush15june/bench"
	"github.com/arush15june/bench/requester"
)

func main() {
	payloads := []int{512, 1024, 100000, 500000, 1000000}
	var requestRate uint64 = 10000
	for i := range payloads {
		fmt.Println("Payload: ", payloads[i])
		r := &requester.NATSStreamingRequesterFactory{
			URL:         "nats://68.183.245.246:4222",
			PayloadSize: payloads[i],
			ClusterID: "stan",
			Subject:     "foo",
			ClientID:    "benchmark",
		}
	
		benchmark := bench.NewBenchmark(r, requestRate, 1, 30*time.Second, 0)
		summary, err := benchmark.Run()
		if err != nil {
			panic(err)
		}
	
		fmt.Println(summary)
		ioutil.WriteFile(fmt.Sprintf("nats-streaming-summary-%d-%d.txt", requestRate, payloads[i]), []byte(fmt.Sprintf("%v", summary)), 0644)
		summary.GenerateLatencyDistribution(nil, fmt.Sprintf("nats-streaming-%d-%d.txt", requestRate, payloads[i]))
	}
}
