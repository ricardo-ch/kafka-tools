package main

import (
	"github.com/Shopify/sarama"
	"github.com/ahmetb/go-linq"
	"github.com/pkg/errors"
	"log"
	"time"
)

func GetConsumerListFromOffsetList(consumerGroupsOffsets map[string]map[int32]int64) []string {
	var consumerGroups []string
	for consumerGroup := range consumerGroupsOffsets {
		consumerGroups = append(consumerGroups, consumerGroup)
	}
	return consumerGroups
}

// ensure that:
// 	- source exist
//	- sink has enough partitions  (more than source), create it if does not exist
//	- remove all messages in sink
func EnsureTopics(client sarama.Client, topicSource string, intermediateTopic string) error {
	SourcePartitions, err := client.Partitions(topicSource)
	if err != nil {
		return err
	}
	if len(SourcePartitions) <= 0 {
		return errors.New("topicSource does not exist?")
	}

	// Need to list topics instead of just requesting partition because Partition request actually create the topicSource if not exist
	topics, err := client.Topics()
	if err != nil {
		return err
	}

	if linq.From(topics).Contains(intermediateTopic) {
		SinkPartitions, err := client.Partitions(intermediateTopic)
		if err != nil {
			return err
		}

		if len(SinkPartitions) < len(SourcePartitions) {
			return errors.New("intermediateTopic already exist and does not have enough partitions")
		}
	} else {
		// Sink does not exist
		// Create it using same number of partition
		createTopicReq := sarama.CreateTopicsRequest{
			Version: 2,
			Timeout: 10 * time.Second,
			TopicDetails: map[string]*sarama.TopicDetail{
				intermediateTopic: {
					NumPartitions:     int32(len(SourcePartitions)),
					ReplicationFactor: 1, //TODO
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

		tErr, ok := createTopicResp.TopicErrors[intermediateTopic]
		if ok && tErr != nil && tErr.Err != sarama.ErrNoError {
			return errors.Wrap(tErr.Err, GetStringOrDefault(tErr.ErrMsg))
		}
	}

	err = cleanTopic(client, intermediateTopic)
	if err != nil {
		return err
	}

	return nil
}

func GetStringOrDefault(s *string) string {
	if s != nil {
		return *s
	}
	return ""
}

func NewConsumer() (sarama.Consumer, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V1_1_0_0
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Fetch.Max = 1024 * 1024 * 2 //2 Mo
	cfg.Consumer.Fetch.Default = 1024 * 512
	cfg.Consumer.Fetch.Min = 1024 * 10

	consumer, err := sarama.NewConsumer(bootstrapserver, cfg)
	return consumer, err
}

func NewManualProducer() (sarama.AsyncProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V1_1_0_0
	cfg.Producer.Return.Successes = false
	cfg.Producer.Return.Errors = true
	cfg.Producer.RequiredAcks = sarama.WaitForLocal
	cfg.Net.MaxOpenRequests = 1
	cfg.Producer.Flush.Frequency = 100 * time.Millisecond
	cfg.Producer.Partitioner = func(topic string) sarama.Partitioner { return sarama.NewManualPartitioner(topic) }

	producer, err := sarama.NewAsyncProducer(bootstrapserver, cfg)
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

func NewClient(brokers []string) (sarama.Client, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V1_1_0_0
	return sarama.NewClient(brokers, cfg)
}
