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
		r := &requester.PulsarRequesterFactory{
			URL:         "pulsar://64.225.84.34:6650",
			PayloadSize: payloads[i],
			Topic:       "benchmark",
		}
		
		benchmark := bench.NewBenchmark(r, 1500, 1, 30*time.Second, 0)
		summary, err := benchmark.Run()
		if err != nil {
			panic(err)
		}
		
		fmt.Println(summary)
		ioutil.WriteFile(fmt.Sprintf("pulsar-summary-%d-%d.txt", requestRate, payloads[i]), []byte(fmt.Sprintf("%v", summary)), 0644)
		summary.GenerateLatencyDistribution(nil, fmt.Sprintf("pulsar-%d-%d.txt", requestRate, payloads[i]))
	}
}
