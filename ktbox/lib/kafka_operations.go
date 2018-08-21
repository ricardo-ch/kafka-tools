package lib

import (
	"github.com/Shopify/sarama"
	"github.com/ahmetb/go-linq"
	"time"
)

func NewClient(brokers []string) (sarama.Client, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V1_1_0_0
	return sarama.NewClient(brokers, cfg)
}

// Remove messages from a kafka topic
// Clean only specific partition using partitions (nil or empty for all)
func CleanTopic(client sarama.Client, topic string, partitions []int32) error {
	topicsOffset, err := GetCurrentMaxTopicOffset(client, topic)
	if err != nil {
		return err
	}

	//Delete by partition because need to query the leader
	for partition, offset := range topicsOffset {
		if len(partitions) > 0 && !linq.From(partitions).Contains(partition) {
			continue
		}

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
			Timeout: 2 * time.Minute,
		}
		deleteResp, err := broker.DeleteRecords(deleteReq)
		if err != nil {
			return err
		}
		if deleteResp.Topics[topic].Partitions[partition].Err != sarama.ErrNoError {
			return deleteResp.Topics[topic].Partitions[partition].Err
		}
	}
	return nil
}

// returns map[partition]offset
func GetCurrentMaxTopicOffset(client sarama.Client, topic string) (map[int32]int64, error) {
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

// returns map[partition]offset
func GetCurrentMinTopicOffset(client sarama.Client, topic string) (map[int32]int64, error) {
	partitions, err := client.Partitions(topic)
	if err != nil {
		return nil, err
	}

	result := make(map[int32]int64, len(partitions))

	for _, partition := range partitions {
		result[partition], err = client.GetOffset(topic, partition, sarama.OffsetOldest)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func DeleteConsumerGroup(client sarama.Client, group string) error {
	broker, err := client.Coordinator(group)
	if err != nil {
		return err
	}

	req := &sarama.DeleteGroupsRequest{}
	req.AddGroup(group)
	resp, err := broker.DeleteGroups(req)
	if err != nil {
		return err
	}
	for _, kErr := range resp.GroupErrorCodes {
		if kErr != sarama.ErrNoError {
			return kErr
		}
	}

	return nil
}
