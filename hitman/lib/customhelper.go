package lib

import (
	"github.com/Shopify/sarama"
	"log"
	"time"
)

func getConsumerListFromOffsetList(consumerGroupsOffsets map[string]map[int32]int64) []string {
	var consumerGroups []string
	for consumerGroup := range consumerGroupsOffsets {
		consumerGroups = append(consumerGroups, consumerGroup)
	}
	return consumerGroups
}

func getBrokersFromClient(client sarama.Client) []string {
	var brokers []string
	for _, b := range client.Brokers() {
		brokers = append(brokers, b.Addr())
	}
	return brokers
}

func getStringOrDefault(s *string) string {
	if s != nil {
		return *s
	}
	return ""
}

func newConsumer(brokers []string) (sarama.Consumer, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V1_1_0_0
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Fetch.Max = 1024 * 1024 * 2 //2 Mo
	cfg.Consumer.Fetch.Default = 1024 * 512
	cfg.Consumer.Fetch.Min = 1024 * 10

	consumer, err := sarama.NewConsumer(brokers, cfg)
	return consumer, err
}

func newManualProducer(brokers []string) (sarama.AsyncProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V1_1_0_0
	cfg.Producer.Return.Successes = false
	cfg.Producer.Return.Errors = true
	cfg.Producer.RequiredAcks = sarama.WaitForLocal
	cfg.Net.MaxOpenRequests = 1
	cfg.Producer.Flush.Frequency = 100 * time.Millisecond
	cfg.Producer.Partitioner = func(topic string) sarama.Partitioner { return sarama.NewManualPartitioner(topic) }

	producer, err := sarama.NewAsyncProducer(brokers, cfg)
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
			log.Printf("produce success")
		}
	}()

	return producer, nil
}

func newClient(brokers []string) (sarama.Client, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V1_1_0_0
	return sarama.NewClient(brokers, cfg)
}
