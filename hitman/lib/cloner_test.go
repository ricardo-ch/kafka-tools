package lib

import (
	"github.com/Shopify/sarama"
	"github.com/bouk/monkey"
	"github.com/ricardo-ch/kafka-tools/hitman/lib/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
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

func Test_clonePartition_NoKill(t *testing.T) {
	// ---- Mocks ----
	now := time.Now()
	client := new(mocks.Client)

	inMsgCh := make(chan *sarama.ConsumerMessage, 50)
	outMsgCh := make(chan *sarama.ProducerMessage, 50)

	inMsgCh <- &sarama.ConsumerMessage{
		Offset:    4,
		Partition: 1,
		Key:       []byte("key1"),
		Value:     []byte("value1"),
		Topic:     "topic1",
		Timestamp: now,
	}
	inMsgCh <- &sarama.ConsumerMessage{
		Offset:    5,
		Partition: 1,
		Key:       []byte("key2"),
		Value:     []byte("value2"),
		Topic:     "topic1",
		Timestamp: now,
	}
	inMsgCh <- &sarama.ConsumerMessage{
		Offset:    6,
		Partition: 1,
		Key:       []byte("key3"),
		Value:     []byte("value3"),
		Topic:     "topic1",
		Timestamp: now,
	}

	partitionConsumer := new(mocks.PartitionConsumer)
	partitionConsumer.On("Close").Return(nil)
	partitionConsumer.On("Messages").Return((<-chan *sarama.ConsumerMessage)(inMsgCh))

	consumer := new(mocks.Consumer)
	consumer.On("ConsumePartition", "topic1", mock.Anything, sarama.OffsetOldest).Return(partitionConsumer, nil)
	consumer.On("Close").Return(nil)

	producer := new(mocks.AsyncProducer)
	producer.On("Close").Return(nil)
	producer.On("Input").Return((chan<- *sarama.ProducerMessage)(outMsgCh))

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

	patch = monkey.Patch(GetCurrentMaxTopicOffset, func(client sarama.Client, topic string) (map[int32]int64, error) {
		return map[int32]int64{0: 42, 1: 7}, nil
	})
	defer patch.Unpatch()

	patch = monkey.Patch(GetCurrentMinTopicOffset, func(client sarama.Client, topic string) (map[int32]int64, error) {
		return map[int32]int64{0: 42, 1: 4}, nil
	})
	defer patch.Unpatch()

	target := func(partition int32, offset int64) bool { return partition == 1 && offset == 5 }
	oldGroupOffset := map[string]map[int32]int64{
		"group1": {
			0: 40,
			1: 5,
		},
	}

	// ---- Test ----
	offsetDelta, err := clonePartition(client, "topic1", "topic2", 1, target, oldGroupOffset)
	assert.NoError(t, err)

	// ---- Asserts ----

	// 3 messages on topic:
	//	- 1 is before consumerGroup offset
	//	- 1 is deleted
	//	- 1 is after
	// -> delta of 1
	assert.Equal(t, map[string]map[int32]int64{"group1": {1: 1}}, offsetDelta)

	close(outMsgCh)
	var msgs []*sarama.ProducerMessage
	for msg := range outMsgCh {
		msgs = append(msgs, msg)
	}
	assert.Equal(t, []*sarama.ProducerMessage{
		{
			Partition: 1,
			Key:       sarama.ByteEncoder("key1"),
			Value:     sarama.ByteEncoder("value1"),
			Topic:     "topic2",
			Timestamp: now,
		}, {
			Partition: 1,
			Key:       sarama.ByteEncoder("key3"),
			Value:     sarama.ByteEncoder("value3"),
			Topic:     "topic2",
			Timestamp: now,
		},
	}, msgs)
}
