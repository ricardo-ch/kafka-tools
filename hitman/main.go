package main

import (
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/davecgh/go-spew/spew"
	"github.com/pkg/errors"
)

var (
	topic           = "test-francois"
	topicSink       = "test-francois-2" //TODO use specific topic with loads of partition in there
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

	consumerGroupsOffsets, err := getConsumerGroup(client, topic)
	if err != nil {
		log.Fatal(err)
	}

	// Do that at the beginning for fast-fail.
	// Do that again at the last moment for best effort check
	err = ensureConsumerGroupsInactive(client, getConsumerListFromOffsetList(consumerGroupsOffsets))
	if err != nil {
		log.Fatal(err)
	}
	//TODO use acl to block write on topic

	// This middleware on contract predict new offset of consumer group once topic source is clean again
	contract = makeUpdateGroupOffsetOnKill(contract, &consumerGroupsOffsets)

	err = Clone(client, topic, topicSink, contract)
	if err != nil {
		log.Fatal(err)
	}

	err = cleanTopic(client, topic)
	if err != nil {
		log.Fatal(err)
	}

	err = Clone(client, topicSink, topic, func(partition int32, offset int64) bool { return false })
	if err != nil {
		log.Fatal(err)
	}

	// TODO should I delete topic instead of cleaning it?
	err = cleanTopic(client, topicSink)
	if err != nil {
		log.Fatal(err)
	}

	err = updateConsumerGroupOffset(client, topic, consumerGroupsOffsets)
	if err != nil {
		log.Fatal(err)
	}
}

func makeUpdateGroupOffsetOnKill(contract KillContract, consumerGroupsOffsets *map[string]map[int32]int64) KillContract {
	if consumerGroupsOffsets == nil || len(*consumerGroupsOffsets) <= 0 {
		log.Println("no consumergroup to update")
		return contract
	}

	return func(partition int32, offset int64) bool {
		target := contract(partition, offset)
		for consumerGroup := range *consumerGroupsOffsets {
			consumerGroupOffset := (*consumerGroupsOffsets)[consumerGroup][partition]
			if !target && offset <= consumerGroupOffset {
				(*consumerGroupsOffsets)[consumerGroup][partition]++
			}
		}
		return target
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
	log.Printf("beginning reset offset on topic %s to these values: %+v", topic, newConsumerGroupOffsets)

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
		spew.Dump(deleteResp)
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
