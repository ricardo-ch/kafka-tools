package main

import (
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/davecgh/go-spew/spew"
)

var (
	topic           = "test-francois"
	topicSink       = "test-francois-2"
	bootstrapserver = []string{"rm-be-k8k73.beta.local:9092"}
	contract        = func(partition int32, offset int64) bool {
		return offset == 8
	}
)

func main() {
	var err error
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V1_1_0_0
	client, err := sarama.NewClient(bootstrapserver, cfg)
	if err != nil {
		log.Fatal(err)
	}

	consumerGroups, err := getConsumerGroup(client, topic)
	if err != nil {
		log.Fatal(err)
	}
	spew.Dump(consumerGroups)

	err = Clone(client, topic, topicSink, contract)
	if err != nil {
		log.Fatal(err)
	}

	err = cleanTopic(client, topic)
	if err != nil {
		log.Fatal(err)
	}

	err = Clone(client, topic, topicSink, func(partition int32, offset int64) bool { return false })
	if err != nil {
		log.Fatal(err)
	}

	//TODO reset consumerGroup offset with consumerGroups var
}

func cleanTopic(client sarama.Client, topic string) error {
	topicsOffset, err := getCurrentTopicOffset(client, topic)
	if err != nil {
		return err
	}

	// increment because deleteRecordRequest delete up to this offset, not the upper limit
	for i := range topicsOffset {
		topicsOffset[i]++
	}

	deleteReq := &sarama.DeleteRecordsRequest{
		Topics: map[string]*sarama.DeleteRecordsRequestTopic{
			topic: {
				PartitionOffsets: topicsOffset,
			},
		},
		Timeout: 10 * time.Second,
	}

	broker, err := client.Controller()
	if err != nil {
		return err
	}
	_, err = broker.DeleteRecords(deleteReq)
	if err != nil {
		log.Fatal(err)
	}
	return nil
}

func getCurrentTopicOffset(client sarama.Client, topic string) (map[int32]int64, error) {
	partitions, err := client.Partitions(topic)
	if err != nil {
		return nil, err
	}

	result := make(map[int32]int64, len(partitions))

	for _, partition := range partitions {
		result[partition], err = client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// map[ConsumerGroup]map[Partition]Offset
func getConsumerGroup(client sarama.Client, topic string) (map[string]map[int32]int64, error) {
	broker, err := client.Controller()
	if err != nil {
		return nil, err
	}
	topicPartitions, err := client.Partitions(topic)
	if err != nil {
		return nil, err
	}

	consumersOffset := map[string]map[int32]int64{}
	listResp, err := broker.ListGroups(&sarama.ListGroupsRequest{})
	if err != nil {
		return nil, err
	}
	// list through all consumer to get only those which have a commit on this topic
	// There is probably a way to have this list directly from kafka API but i can't find it
	for consumerGroup := range listResp.Groups {
		coordinator, err := client.Coordinator(consumerGroup)
		if err != nil {
			return nil, err
		}
		offsetReq := &sarama.OffsetFetchRequest{ConsumerGroup: consumerGroup, Version: 1}
		for _, partition := range topicPartitions {
			offsetReq.AddPartition(topic, partition)
		}

		offsetResp, err := coordinator.FetchOffset(offsetReq)
		if err != nil {
			return nil, err
		}

		consumerPartitionsOffsets := map[int32]int64{}
		for partition, fetchBlock := range offsetResp.Blocks[topic] {
			if fetchBlock.Offset >= 0 {
				// filter out group that has no commited offset for this topic
				consumerPartitionsOffsets[partition] = fetchBlock.Offset
			}
		}
		if len(consumerPartitionsOffsets) > 0 {
			consumersOffset[consumerGroup] = consumerPartitionsOffsets
		}
	}
	return consumersOffset, nil
}
