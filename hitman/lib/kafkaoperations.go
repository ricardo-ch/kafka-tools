package lib

import (
	"errors"
	"github.com/Shopify/sarama"
	"log"
)

// returns map[ConsumerGroup]map[Partition]Offset
func GetConsumerGroup(client sarama.Client, topic string) (map[string]map[int32]int64, error) {
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
	if listResp.Err != sarama.ErrNoError {
		return nil, listResp.Err
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
			if fetchBlock.Err != sarama.ErrNoError {
				return nil, fetchBlock.Err
			}

			if fetchBlock.Offset >= 0 {
				// filter out partition with no committed offset
				consumerPartitionsOffsets[partition] = fetchBlock.Offset
			}
		}
		if len(consumerPartitionsOffsets) > 0 {
			// filter out group that has no committed offset for this topic
			consumersOffset[consumerGroup] = consumerPartitionsOffsets
		}
	}
	return consumersOffset, nil
}

func EnsureConsumerGroupsInactive(client sarama.Client, consumerGroups []string) error {
	activeMember := 0

	for _, consumerGroup := range consumerGroups {
		broker, err := client.Coordinator(consumerGroup)
		if err != nil {
			return err
		}

		//TODO not sure I actually need to call the coordinator, if not I could make only one query
		req := sarama.DescribeGroupsRequest{
			Groups: []string{consumerGroup},
		}
		description, err := broker.DescribeGroups(&req)
		if err != nil {
			return err
		}

		if len(description.Groups) <= 0 {
			return errors.New("no response for describe group")
		}
		if description.Groups[0].Err != sarama.ErrNoError {
			return description.Groups[0].Err
		}
		if len(description.Groups[0].Members) > 0 {
			log.Printf("membre %v is active (%v) with %v members", consumerGroup, description.Groups[0].State, len(description.Groups[0].Members))
			activeMember++
		}
	}
	if activeMember > 0 {
		return errors.New("witnesses are watching")
	}
	return nil
}

func UpdateConsumerGroupOffset(client sarama.Client, topic string, newConsumerGroupOffsets map[string]map[int32]int64) error {
	log.Printf("beginning reset offset on topicSource %s to these values: %+v", topic, newConsumerGroupOffsets)

	err := EnsureConsumerGroupsInactive(client, getConsumerListFromOffsetList(newConsumerGroupOffsets))
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
