package requester

import (
	"context"
	"math/rand"
	"runtime"
	"strconv"

	"github.com/apache/pulsar/pulsar-client-go/pulsar"
	"github.com/arush15june/bench"
)

// PulsarRequesterFactory implements RequesterFactory by creating a Requester
// which publishes messages to Pulsar and waits to receive them.
type PulsarRequesterFactory struct {
	URL         string
	PayloadSize int
	Topic       string
}

// GetRequester returns a new Requester, called for each Benchmark connection.
func (n *PulsarRequesterFactory) GetRequester(num uint64) bench.Requester {
	return &PulsarRequester{
		url:         n.URL,
		payloadSize: n.PayloadSize,
		topic:       n.Topic + "-" + strconv.FormatUint(num, 10),
		messageChan: make(chan pulsar.ConsumerMessage),
	}
}

// PulsarRequester implements Requester by publishing a message to Pulsar and
// waiting to receive it.
type PulsarRequester struct {
	url         string
	payloadSize int
	topic       string
	client      pulsar.Client
	producer    pulsar.Producer
	consumer    pulsar.Consumer
	messageChan chan pulsar.ConsumerMessage
	msg         pulsar.ProducerMessage
	ctx         context.Context
}

// Setup prepares the Requester for benchmarking.
func (n *PulsarRequester) Setup() error {
	// Get client for producers and consumers.
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:                     n.url,
		OperationTimeoutSeconds: 5,
		MessageListenerThreads:  runtime.NumCPU(),
	})

	if err != nil {
		return err
	}
	n.client = client

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: n.topic,
	})

	if err != nil {
		return err
	}

	n.producer = producer

	consumerOpts := pulsar.ConsumerOptions{
		Topic:            n.topic,
		SubscriptionName: "my-subscription-1",
		MessageChannel:   n.messageChan,
	}

	consumer, err := client.Subscribe(consumerOpts)

	if err != nil {
		return err
	}

	n.consumer = consumer

	msgValue := make([]byte, n.payloadSize)
	for i := 0; i < n.payloadSize; i++ {
		msgValue[i] = 'A' + uint8(rand.Intn(26))
	}
	msg := pulsar.ProducerMessage{
		Payload: msgValue,
	}
	n.msg = msg

	n.ctx = context.Background()

	return nil
}

// Request performs a synchronous request to the system under test.
func (n *PulsarRequester) Request() error {
	if err := n.producer.Send(context.Background(), n.msg); err != nil {
		return err
	}

	msg := <-n.messageChan
	n.consumer.Ack(msg.Message)

	return nil
}

// Teardown is called upon benchmark completion.
func (n *PulsarRequester) Teardown() error {
	n.producer.Close()
	n.consumer.Close()
	n.producer = nil
	n.consumer = nil
	return nil
}
