package lib

import (
	"github.com/Shopify/sarama"
	"github.com/bouk/monkey"
	"github.com/ricardo-ch/kafka-tools/hitman/lib/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

func Test_CloneTopic(t *testing.T) {
	patch := monkey.Patch(getBrokersFromClient, func(client sarama.Client) []string {
		return []string{"broker1"}
	})
	defer patch.Unpatch()

	patch = monkey.Patch(clonePartition,
		func(client sarama.Client, topicSource, topicSink string, partition int32, istTarget KillContract, oldGroupOffset map[string]map[int32]int64) (newGroupOffsetDelta map[string]map[int32]int64, err error) {
			switch partition {
			case 0:
				return map[string]map[int32]int64{
					"group1": {
						0: 4,
						1: 0,
					},
				}, nil
			case 1:
				return map[string]map[int32]int64{
					"group1": {
						0: 0,
						1: 9,
					},
				}, nil
			}
			panic("how did I get there?")
		})
	defer patch.Unpatch()

	client := new(mocks.Client)
	client.On("Partitions", "topic1").Return([]int32{0, 1}, nil)
	client.On("Partitions", "topic2").Return([]int32{0, 1}, nil)
	client.On("GetOffset", "topic2", int32(0), sarama.OffsetNewest).Return(int64(42), nil)
	client.On("GetOffset", "topic2", int32(1), sarama.OffsetNewest).Return(int64(6), nil)

	newOffset, err := CloneTopic(client, "topic1", "topic2", NoKillContract,
		map[string]map[int32]int64{
			"group1": {
				0: 3,
				1: 8,
			},
		},
	)

	assert.NoError(t, err)
	assert.Equal(t, map[string]map[int32]int64{
		"group1": {
			0: 46,
			1: 15,
		},
	}, newOffset)
}

func Test_clonePartition(t *testing.T) {
	client := new(mocks.Client)

	msgCh := make(chan *sarama.ConsumerMessage)

	partitionConsumer := new(mocks.PartitionConsumer)
	partitionConsumer.On("Close").Return(nil)
	partitionConsumer.On("Messages").Return(msgCh)

	consumer := new(mocks.Consumer)
	consumer.On("ConsumePartition", "topic1", mock.Anything, sarama.OffsetOldest).Return(partitionConsumer, nil)
	consumer.On("Close").Return(nil)

	producer := new(mocks.AsyncProducer)
	producer.On("Close").Return(nil)

	patch := monkey.Patch(newConsumer, func(brokers []string) (sarama.Consumer, error) {
		return consumer, nil
	})
	defer patch.Unpatch()

	patch = monkey.Patch(getBrokersFromClient, func(client sarama.Client) []string {
		return []string{}
	})
	defer patch.Unpatch()

	patch = monkey.Patch(newManualProducer, func(brokers []string) (sarama.AsyncProducer, error) {
		return producer, nil
	})
	defer patch.Unpatch()

	newManualProducer

	target := func(partition int32, offset int64) bool { return true }
	oldGroupOffset := map[string]map[int32]int64{}

	offsetDelta, err := clonePartition(client, "topic1", "topic2", 1, target, oldGroupOffset)
	//TODO clonePartition not unit testable as is. implement getTopicOffset method

}
