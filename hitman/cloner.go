package main

import (
	"fmt"
	"log"
	"time"

	"sync"

	"github.com/Shopify/sarama"
	"github.com/ahmetb/go-linq"
	"github.com/pkg/errors"
)

// return true to kill message
type KillContract func(partition int32, offset int64) bool

//TODO replace CloneTopic() with kafka-topicSource-cloner

// CloneTopic copy msg from topicSource to TopicSink
// accept a KillContract parameter that define which message NOT to copy
// return newConsumerGroupsOffsets which can be use to commit offset of consumerGroup on topicSink to keep them at the same message
func CloneTopic(client sarama.Client, topicSource string, topicSink string, contract KillContract, oldGroupsOffsets map[string]map[int32]int64) (newGroupsOffsets map[string]map[int32]int64, err error) {
	consumer, err := newConsumer()
	if err != nil {
		return nil, err
	}
	defer consumer.Close()

	sourcePartitions, err := consumer.Partitions(topicSource)
	if err != nil {
		return nil, err
	}

	lock := sync.Mutex{}
	newGroupsOffsets, err = initGroupOffsetAtTopicEnd(client, topicSink, getConsumerListFromOffsetList(oldGroupsOffsets))
	if err != nil {
		return nil, err
	}

	wg := sync.WaitGroup{}
	for _, partition := range sourcePartitions {
		partitionConsumer, err := consumer.ConsumePartition(topicSource, partition, sarama.OffsetOldest)
		if err != nil {
			return nil, err
		}

		wg.Add(1)
		go func() {
			defer partitionConsumer.Close()
			defer wg.Done()

			offsetDeltas, err := clonePartition(client, partitionConsumer, topicSink, contract, oldGroupsOffsets)
			if err != nil {
				log.Fatal(err)
			}

			lock.Lock()
			defer lock.Unlock()
			for group := range offsetDeltas {
				for partition := range offsetDeltas[group] {
					newGroupsOffsets[group][partition] += offsetDeltas[group][partition]
				}
			}
		}()
	}

	//wg.Wait()
	fmt.Println("cloning done")

	return newGroupsOffsets, nil
}

func initGroupOffsetAtTopicEnd(client sarama.Client, topic string, groups []string) (map[string]map[int32]int64, error) {
	topicOffset, err := getCurrentTopicOffset(client, topic)
	if err != nil {
		return nil, err
	}

	groupsOffset := map[string]map[int32]int64{}
	for _, group := range groups {
		groupsOffset[group] = topicOffset
	}
	return groupsOffset, nil
}

func clonePartition(client sarama.Client, partitionConsumer sarama.PartitionConsumer, topicSink string, istTarget KillContract, oldGroupOffset map[string]map[int32]int64) (newGroupOffsetDelta map[string]map[int32]int64, err error) {
	newGroupOffsetDelta = map[string]map[int32]int64{}

	producer, err := newManualProducer()
	defer producer.Close()
	if err != nil {
		return nil, err
	}

	//TODO instead of timeout we can know for sure the end of a topicSource if we query topicSource offset
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

			// Increment offset delta of consumer groups
			for consumerGroup := range oldGroupOffset {
				if msg.Offset < oldGroupOffset[consumerGroup][msg.Partition] {
					if newGroupOffsetDelta[consumerGroup] == nil {
						newGroupOffsetDelta[consumerGroup] = map[int32]int64{}
					}
					newGroupOffsetDelta[consumerGroup][msg.Partition]++
				}
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
			msgP.Partition = msg.Partition
			msgP.Timestamp = msg.Timestamp
			//TODO headers
			producer.Input() <- msgP
		case <-time.After(3 * time.Second):
			break loop
		}
	}

	return newGroupOffsetDelta, nil
}

func newConsumer() (sarama.Consumer, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V1_1_0_0
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Fetch.Max = 1024 * 1024 * 2 //2 Mo
	cfg.Consumer.Fetch.Default = 1024 * 512
	cfg.Consumer.Fetch.Min = 1024 * 10

	consumer, err := sarama.NewConsumer(bootstrapserver, cfg)
	return consumer, err
}

func newManualProducer() (sarama.AsyncProducer, error) {
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
		}
	}()

	return producer, nil
}

// ensure that:
// 	- source exist
//	- sink has enough partitions  (more than source), create it if does not exist
//	- remove all messages in sink

func ensureTopics(client sarama.Client, topicSource string, topicSink string) error {
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

	if linq.From(topics).Contains(topicSink) {
		SinkPartitions, err := client.Partitions(topicSink)
		if err != nil {
			return err
		}

		if len(SinkPartitions) < len(SourcePartitions) {
			return errors.New("topicSink already exist and does not have enough partitions")
		}
	} else {
		// Sink does not exist
		// Create it using same number of partition
		createTopicReq := sarama.CreateTopicsRequest{
			Version: 2,
			Timeout: 10 * time.Second,
			TopicDetails: map[string]*sarama.TopicDetail{
				topicSink: {
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
