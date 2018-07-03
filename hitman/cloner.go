package main

import (
	"fmt"
	"log"
	"time"

	"sync"

	"github.com/Shopify/sarama"
	"github.com/ahmetb/go-linq"
	errors "github.com/pkg/errors"
)

// return true to kill message
type KillContract func(partition int32, offset int64) bool

//TODO replace Clone() with kafka-topic-cloner
func Clone(client sarama.Client, topicSource string, topicSink string, contract KillContract) error {
	err := ensureTopics(client, topicSource, topicSink)
	if err != nil {
		return err
	}
	consumer, err := newConsumer(client)
	if err != nil {
		return err
	}
	defer consumer.Close()

	producer, err := newProducer(client)
	if err != nil {
		return err
	}
	defer producer.Close()

	sourcePartitions, err := consumer.Partitions(topicSource)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	for _, partition := range sourcePartitions {
		partitionConsumer, err := consumer.ConsumePartition(topicSource, partition, sarama.OffsetOldest)
		defer partitionConsumer.Close()
		if err != nil {
			return err
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			clonePartition(partitionConsumer, producer, topicSink, contract)
		}()
	}

	wg.Wait()
	fmt.Println("cloning done")

	return nil
}

func clonePartition(partitionConsumer sarama.PartitionConsumer, producer sarama.AsyncProducer, topicSink string, istTarget KillContract) {
loop:
	for {
		select {
		case msg, open := <-partitionConsumer.Messages():
			if !open {
				continue
			}
			if istTarget(msg.Partition, msg.Offset) {
				fmt.Printf("found tagrget, removing: %v\n", string(msg.Value))
				continue
			}

			msgP := &sarama.ProducerMessage{
				Topic: topicSink,
			}
			if msg.Value != nil {
				msgP.Value = sarama.ByteEncoder(msg.Value)
			}
			if msg.Key != nil {
				msgP.Key = sarama.ByteEncoder(msg.Key)
			}
			msgP.Timestamp = msg.Timestamp
			//TODO headers
			producer.Input() <- msgP
		case <-time.After(3 * time.Second):
			break loop
		}
	}
}

func newConsumer(client sarama.Client) (sarama.Consumer, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V1_1_0_0
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Fetch.Max = 1024 * 1024 * 2 //2 Mo
	cfg.Consumer.Fetch.Default = 1024 * 512
	cfg.Consumer.Fetch.Min = 1024 * 10
	cfg.Producer.Partitioner = sarama.NewCustomHashPartitioner(MurmurHasher)

	consumer, err := sarama.NewConsumer(getAddrFromClient(client), cfg)
	return consumer, err
}

func newProducer(client sarama.Client) (sarama.AsyncProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V1_1_0_0
	cfg.Producer.Return.Successes = false
	cfg.Producer.Return.Errors = true
	cfg.Producer.RequiredAcks = sarama.WaitForLocal
	cfg.Net.MaxOpenRequests = 1
	cfg.Producer.Flush.Frequency = 100 * time.Millisecond

	producer, err := sarama.NewAsyncProducer(getAddrFromClient(client), cfg)
	if err != nil {
		return nil, err
	}

	go func() {
		for err := range producer.Errors() {
			log.Fatalf("Failed to produce message: %+v\n", err)
		}
	}()

	go func() {
		for range producer.Successes() {
		}
	}()

	return producer, nil
}

func getAddrFromClient(client sarama.Client) []string {
	addrs := []string{}
	for _, broker := range client.Brokers() {
		addrs = append(addrs, broker.Addr())
	}
	return addrs
}

func ensureTopics(client sarama.Client, topicSource string, topicSink string) error {
	SourcePartitions, err := client.Partitions(topicSource)
	if err != nil {
		return err
	}
	if len(SourcePartitions) <= 0 {
		return errors.New("topicSource does not exist?")
	}

	// Need to list topics instead of just requesting partition because Partition request actually create the topic if not exist
	topics, err := client.Topics()
	if err != nil {
		return err
	}
	if linq.From(topics).Contains(topicSink) {
		SinkPartitions, err := client.Partitions(topicSink)
		if err != nil {
			return err
		}

		if len(SinkPartitions) != len(SourcePartitions) {
			return errors.New("topicSink already exist and is not copartitionned with topicSource")
		}

		err = cleanTopic(client, topicSink)
		if err != nil {
			return err
		}
		return nil
	}
	// Dest does not exist
	// Create it using same number of partition

	createTopicReq := sarama.CreateTopicsRequest{
		Version: 2,
		Timeout: 10 * time.Second,
		TopicDetails: map[string]*sarama.TopicDetail{
			topicSink: &sarama.TopicDetail{
				NumPartitions:     int32(len(SourcePartitions)),
				ReplicationFactor: 1,
			},
		},
	}

	controller, err := client.Controller()
	if err != nil {
		return err
	}
	createTopicResp, err := controller.CreateTopics(&createTopicReq)
	if err != nil {
		return err
	}

	tErr, ok := createTopicResp.TopicErrors[topicSink]
	if ok && tErr != nil && tErr.Err != sarama.ErrNoError {
		return errors.Wrap(tErr.Err, getStringOrDefault(tErr.ErrMsg))
	}

	err = cleanTopic(client, topicSink)
	if err != nil {
		return err
	}
	return nil
}

func getStringOrDefault(s *string) string {
	if s != nil {
		return *s
	}
	return ""
}
