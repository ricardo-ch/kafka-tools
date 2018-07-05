package main

import (
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

var (
	topicSource     = "test-francois"
	topicSink       = "test-francois-2" //TODO use specific topicSource with loads of partition in there
	bootstrapserver = []string{"kafka:9092"}

	contract = func(partition int32, offset int64) bool {
		return offset == 3 && partition == 0
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

	consumerGroupsOffsets, err := getConsumerGroup(client, topicSource)
	if err != nil {
		log.Fatal(err)
	}

	// Do that at the beginning for fast-fail.
	// Do that again at the last moment for best effort check
	err = ensureConsumerGroupsInactive(client, getConsumerListFromOffsetList(consumerGroupsOffsets))
	if err != nil {
		log.Fatal(err)
	}
	//TODO use acl to block write on topicSource

	err = ensureTopics(client, topicSource, topicSink)
	if err != nil {
		log.Fatal(err)
	}

	newConsumerGroupOffset, err := CloneTopic(client, topicSource, topicSink, contract, consumerGroupsOffsets)
	if err != nil {
		log.Fatal(err)
	}

	err = cleanTopic(client, topicSource)
	if err != nil {
		log.Fatal(err)
	}

	newConsumerGroupOffset, err = CloneTopic(client, topicSink, topicSource, func(partition int32, offset int64) bool { return false }, newConsumerGroupOffset)
	if err != nil {
		log.Fatal(err)
	}

	// TODO should I delete topicSource instead of cleaning it?
	err = cleanTopic(client, topicSink)
	if err != nil {
		log.Fatal(err)
	}

	err = updateConsumerGroupOffset(client, topicSource, newConsumerGroupOffset)
	if err != nil {
		log.Fatal(err)
	}
}

func ensureConsumerGroupsInactive(client sarama.Client, consumerGroups []string) error {
	activeMember := 0

	for _, consumerGroup := range consumerGroups {
		broker, err := client.Coordinator(consumerGroup)
		if err != nil {
			return err
		}

		req := sarama.DescribeGroupsRequest{
			Groups: consumerGroups,
		}
		description, err := broker.DescribeGroups(&req)
		if err != nil {
			return err
		}
		if len(description.Groups) <= 0 {
			return errors.New("no response for describe group")
		}

		if len(description.Groups[0].Members) > 0 {
			log.Printf("membe %v is active: ", description.Groups[0].State)
			activeMember++
		}
	}
	if activeMember > 0 {
		return errors.New("witnesses are watching")
	}
	return nil
}

func updateConsumerGroupOffset(client sarama.Client, topic string, newConsumerGroupOffsets map[string]map[int32]int64) error {
	log.Printf("beginning reset offset on topicSource %s to these values: %+v", topic, newConsumerGroupOffsets)

	err := ensureConsumerGroupsInactive(client, getConsumerListFromOffsetList(newConsumerGroupOffsets))
	if err != nil {
		return err
	}

	for consumerGroup, partitions := range newConsumerGroupOffsets {
		offsetManager, err := sarama.NewOffsetManagerFromClient(consumerGroup, client)
		if err != nil {
			return err
		}
		for partition, offset := range partitions {
			partitionManager, err := offsetManager.ManagePartition(topic, partition)
			if err != nil {
				return err
			}

			partitionManager.MarkOffset(offset, "")
			err = partitionManager.Close()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func getConsumerListFromOffsetList(consumerGroupsOffsets map[string]map[int32]int64) []string {
	var consumerGroups []string
	for consumerGroup := range consumerGroupsOffsets {
		consumerGroups = append(consumerGroups, consumerGroup)
	}
	return consumerGroups
}

func cleanTopic(client sarama.Client, topic string) error {
	topicsOffset, err := getCurrentTopicOffset(client, topic)
	if err != nil {
		return err
	}

	//Delete by partition because need to query the leader
	for partition, offset := range topicsOffset {
		broker, err := client.Leader(topic, partition)
		if err != nil {
			return err
		}

		deleteReq := &sarama.DeleteRecordsRequest{
			Topics: map[string]*sarama.DeleteRecordsRequestTopic{
				topic: {
					PartitionOffsets: map[int32]int64{partition: offset},
				},
			},
			Timeout: 10 * time.Second,
		}
		deleteResp, err := broker.DeleteRecords(deleteReq)
		if err != nil {
			log.Fatal(err)
		}
		if deleteResp.Topics[topic].Partitions[partition].Err != sarama.ErrNoError {
			return deleteResp.Topics[topic].Partitions[partition].Err
		}
	}
	return nil
}

// returns map[partition]offset
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

// returns map[ConsumerGroup]map[Partition]Offset
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
	// list through all consumer to get only those which have a commit on this topicSource
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
				// filter out group that has no commited offset for this topicSource
				consumerPartitionsOffsets[partition] = fetchBlock.Offset
			}
		}
		if len(consumerPartitionsOffsets) > 0 {
			consumersOffset[consumerGroup] = consumerPartitionsOffsets
		}
	}
	return consumersOffset, nil
}
