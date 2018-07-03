package main

import (
	"log"

	"github.com/Shopify/sarama"
	"github.com/davecgh/go-spew/spew"
)

var (
	offset    = int64(1)
	partition = int32(1)
	topic     = "test-francois"

	bootstrapserver = []string{"rm-be-k8k73.beta.local:9092"}
)

func main() {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V1_1_0_0
	client, err := sarama.NewClient(bootstrapserver, cfg)
	if err != nil {
		log.Fatal(err)
	}

	topicsOffset := getCurrentTopicOffset(client)
	consumerGroups := getConsumerGroup(client, topic, topicsOffset)
	spew.Dump(consumerGroups)

	//
	//deleteReq := &sarama.DeleteRecordsRequest{
	//	Topics: map[string]*sarama.DeleteRecordsRequestTopic{
	//		topic: {
	//			PartitionOffsets:map[int32]int64{
	//				partition:4,
	//			},
	//		},
	//	},
	//	Timeout:10 * time.Second,
	//}
	//deleteResp, err := broker.DeleteRecords(deleteReq)
	//if err != nil {
	//	log.Fatal(err)
	//}

}

func getCurrentTopicOffset(client sarama.Client) map[int32]int64 {
	partitions, err := client.Partitions(topic)
	if err != nil {
		log.Fatal(err)
	}

	result := make(map[int32]int64, len(partitions))

	for _, partition := range partitions {
		result[partition], err = client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Fatal(err)
		}
	}
	return result
}

// map[ConsumerGroup]map[Partition]Offset
func getConsumerGroup(client sarama.Client, topic string, topicOffsets map[int32]int64) map[string]map[int32]int64 {
	brokers := client.Brokers()

	consumersOffset := map[string]map[int32]int64{}
	broker := brokers[0]
	listResp, err := broker.ListGroups(&sarama.ListGroupsRequest{})
	if err != nil {
		log.Fatal(err)
	}
	// list through all consumer to get only those which have a commit on this topic
	// There is probably a way to have this list directly from kafka API but i can't find it
	for consumerGroup := range listResp.Groups {
		coordinator, err := client.Coordinator(consumerGroup)
		if err != nil {
			log.Fatal(err)
		}
		offsetReq := &sarama.OffsetFetchRequest{ConsumerGroup: consumerGroup, Version: 1}
		for partition := range topicOffsets {
			offsetReq.AddPartition(topic, partition)
		}

		offsetResp, err := coordinator.FetchOffset(offsetReq)
		if err != nil {
			log.Fatal(err)
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
	return consumersOffset
}

func getRecord(client sarama.Client, broker *sarama.Broker, cfg *sarama.Config) *sarama.Record {
	fetchReq := sarama.FetchRequest{
		Version:   4,
		Isolation: sarama.ReadCommitted,
	}
	fetchReq.AddBlock(topic, partition, offset, cfg.Consumer.Fetch.Max)

	fetchResp, err := broker.Fetch(&fetchReq)
	if err != nil {
		log.Fatal(err)
	}

	topicBlock, ok := fetchResp.Blocks[topic]
	if !ok {
		log.Fatal("response does not contains expected topic")
	}
	partitionBlock, ok := topicBlock[partition]
	if !ok {
		log.Fatal("response does not contains expected partition")
	}

	for _, recordSet := range partitionBlock.RecordsSet {
		for _, record := range recordSet.RecordBatch.Records {
			currentOffset := recordSet.RecordBatch.FirstOffset + record.OffsetDelta
			if currentOffset < offset {
				continue
			}
			if currentOffset > offset {
				log.Fatal("couldn't find expected offset")
			}
			return record
		}
	}
	return nil
}
