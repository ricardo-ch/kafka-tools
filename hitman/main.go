package main

import (
	"github.com/Shopify/sarama"
	"log"
	"github.com/davecgh/go-spew/spew"
)
var (
	offset = int64(1)
	partition = int32(1)
	topic ="test-francois"

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
	spew.Dump(topicsOffset)
	getConsumer(client)
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
func getCurrentTopicOffset(client sarama.Client) map[int32]int64{
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

type ConsumerOffset struct {
	offset int64
	partition int32
}

func getConsumerGroup(client sarama.Client) {
	brokers := client.Brokers()
	req := sarama.ListGroupsRequest{}

	consumers := map[string]sarama.{}

	for _, broker := range brokers {
		listResp, err := broker.ListGroups(&req)
		if err != nil {
			log.Fatal(err)
		}
		for consumer := range listResp.Groups {


			consumers = append(consumers, consumer)
		}

		spew.Dump(listResp)
		break
	}
}

func getRecord(client sarama.Client, broker *sarama.Broker, cfg *sarama.Config) *sarama.Record{
	fetchReq := sarama.FetchRequest{
		Version:4,
		Isolation:sarama.ReadCommitted,
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