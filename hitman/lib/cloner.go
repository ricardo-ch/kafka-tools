package lib

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"log"
)

// return true to kill message
type KillContract func(message *sarama.ConsumerMessage) bool

var NoKillContract = func(message *sarama.ConsumerMessage) bool { return false }

//TODO replace CloneTopic() with kafka-topicSource-cloner

// CloneTopic copy msg from topicSource to TopicSink
// accept a KillContract parameter that define which message NOT to copy
// return newConsumerGroupsOffsets which can be use to commit offset of consumerGroup on intermediateTopic to keep them at the same message
func CloneTopic(client sarama.Client, topicSource string, topicSink string, contract KillContract, oldGroupsOffsets map[string]map[int32]int64) (map[string]map[int32]int64, error) {
	sourcePartitions, err := client.Partitions(topicSource)
	if err != nil {
		return nil, err
	}

	newGroupsOffsets, err := initGroupOffsetAtTopicEnd(client, topicSink, getConsumerListFromOffsetList(oldGroupsOffsets))
	if err != nil {
		return nil, err
	}

	for _, partition := range sourcePartitions {
		offsetDeltas, err := clonePartition(client, topicSource, topicSink, partition, contract, oldGroupsOffsets)
		if err != nil {
			log.Fatal(err)
		}

		for group := range offsetDeltas {
			newGroupsOffsets[group][partition] += offsetDeltas[group][partition]
		}
	}

	fmt.Println("cloning done")
	return newGroupsOffsets, nil
}

// clonePartition consume a partition on topicSource and output all messages as is on topicSink
//	returns deltaOffset on topicSink for each consumerGroup
//
// technically, since it consume a partition and produce to the exact same on the other topic
// 	it will only return offset of one partition
//	but still return map[group]map[partition]offset because it is easier to manipulate the same type
func clonePartition(
	client sarama.Client, topicSource, topicSink string, partition int32, istTarget KillContract, oldGroupOffset map[string]map[int32]int64) (map[string]map[int32]int64, error) {

	newGroupOffsetDelta := map[string]map[int32]int64{}

	maxTopicOffset, err := GetCurrentMaxTopicOffset(client, topicSource)
	if err != nil {
		return nil, err
	}
	maxOffset := maxTopicOffset[partition] - 1 // -1 because topic offset actually mean the offset of next posted message

	minTopicOffset, err := GetCurrentMinTopicOffset(client, topicSource)
	if err != nil {
		return nil, err
	}
	if minTopicOffset[partition] > maxOffset {
		return nil, nil // there is nothing to consume
	}

	consumer, err := newConsumer(getBrokersFromClient(client))
	if err != nil {
		return nil, err
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topicSource, partition, sarama.OffsetOldest)
	if err != nil {
		return nil, err
	}
	defer partitionConsumer.Close()

	producer, err := newManualProducer(getBrokersFromClient(client))
	if err != nil {
		return nil, err
	}
	defer producer.Close()

	currentOffset := int64(0)
	for msg := range partitionConsumer.Messages() {
		currentOffset = msg.Offset
		if istTarget(msg) {
			fmt.Printf("found tagrget, removing: %v\n", string(msg.Value))
			if currentOffset >= maxOffset {
				break
			} else {
				continue
			}
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

		if currentOffset >= maxOffset {
			break
		}
	}

	if currentOffset < maxOffset {
		return nil, errors.New("did not processed the whole topic")
	}
	return newGroupOffsetDelta, nil
}

func initGroupOffsetAtTopicEnd(client sarama.Client, topic string, groups []string) (map[string]map[int32]int64, error) {
	topicOffset, err := GetCurrentMaxTopicOffset(client, topic)
	if err != nil {
		return nil, err
	}

	groupsOffset := map[string]map[int32]int64{}
	for _, group := range groups {
		groupsOffset[group] = topicOffset
	}
	return groupsOffset, nil
}
