package lib

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"gopkg.in/cheggaaa/pb.v1"
	"log"
	"time"
	ktbox "github.com/ricardo-ch/kafka-tools/ktbox/lib"
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

	maxTopicOffset, err := ktbox.GetCurrentMaxTopicOffset(client, topicSource)
	if err != nil {
		return nil, err
	}
	maxOffset := maxTopicOffset[partition] - 1 // -1 because topic offset actually mean the offset of next posted message

	minTopicOffset, err := ktbox.GetCurrentMinTopicOffset(client, topicSource)
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

	go func() {
		for err := range partitionConsumer.Errors() {
			log.Fatal(err)
		}
	}()

	producer, err := newManualProducer(getBrokersFromClient(client))
	if err != nil {
		return nil, err
	}
	defer producer.Close()

	currentOffset := int64(0)

	progressBar := pb.New64(maxOffset)
	progressBar.Start()

loop:
	for {
		select {
		case msg, open := <-partitionConsumer.Messages():
			if !open {
				break loop
			}

			currentOffset = msg.Offset
			if istTarget(msg) {
				fmt.Printf("found tagrget, removing: %v\n", string(msg.Value))
				if currentOffset >= maxOffset {
					break loop
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
				Topic:     topicSink,
				Partition: msg.Partition,
				Timestamp: msg.Timestamp,
			}
			if msg.Value != nil {
				msgP.Value = sarama.ByteEncoder(msg.Value)
			}
			if msg.Key != nil {
				msgP.Key = sarama.ByteEncoder(msg.Key)
			}

			for _, header := range msg.Headers {
				if header != nil {
					msgP.Headers = append(msgP.Headers, *header)
				}
			}

			progressBar.Set64(currentOffset)

			producer.Input() <- msgP
			if currentOffset >= maxOffset {
				break loop
			}
		case <-time.After(3 * time.Second):
			break loop
		}
	}

	if currentOffset < maxOffset-1 {
		return nil, errors.New("did not processed the whole topic")
	} else if currentOffset == maxOffset-1 {
		// partitionConsumer does not return control messages so timeout is used in order not to hang on last message
		// TODO go deeper in api and use something that does get access to control records
		fmt.Println("did not quite get the last offset, maybe this is an exactly once topic")
	}

	progressBar.FinishPrint(fmt.Sprintf("partition: %v", partition))
	return newGroupOffsetDelta, nil
}

func initGroupOffsetAtTopicEnd(client sarama.Client, topic string, groups []string) (map[string]map[int32]int64, error) {
	topicOffset, err := ktbox.GetCurrentMaxTopicOffset(client, topic)
	if err != nil {
		return nil, err
	}

	groupsOffset := map[string]map[int32]int64{}
	for _, group := range groups {
		groupsOffset[group] = topicOffset
	}
	return groupsOffset, nil
}
