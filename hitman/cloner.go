package main

import (
	"fmt"
	"log"
	"time"

	"sync"

	"github.com/Shopify/sarama"
)

// return true to kill message
type KillContract func(partition int32, offset int64) bool

var NoKillContract = func(partition int32, offset int64) bool { return false }

//TODO replace CloneTopic() with kafka-topicSource-cloner

// CloneTopic copy msg from topicSource to TopicSink
// accept a KillContract parameter that define which message NOT to copy
// return newConsumerGroupsOffsets which can be use to commit offset of consumerGroup on intermediateTopic to keep them at the same message
func CloneTopic(client sarama.Client, topicSource string, topicSink string, contract KillContract, oldGroupsOffsets map[string]map[int32]int64) (newGroupsOffsets map[string]map[int32]int64, err error) {
	consumer, err := newConsumer(getBrokersFromClient(client))
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

	wg.Wait()
	fmt.Println("cloning done")

	return newGroupsOffsets, nil
}

func clonePartition(client sarama.Client, partitionConsumer sarama.PartitionConsumer, topicSink string, istTarget KillContract, oldGroupOffset map[string]map[int32]int64) (newGroupOffsetDelta map[string]map[int32]int64, err error) {
	newGroupOffsetDelta = map[string]map[int32]int64{}

	producer, err := newManualProducer(getBrokersFromClient(client))
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

func initGroupOffsetAtTopicEnd(client sarama.Client, topic string, groups []string) (map[string]map[int32]int64, error) {
	topicOffset, err := GetCurrentTopicOffset(client, topic)
	if err != nil {
		return nil, err
	}

	groupsOffset := map[string]map[int32]int64{}
	for _, group := range groups {
		groupsOffset[group] = topicOffset
	}
	return groupsOffset, nil
}
