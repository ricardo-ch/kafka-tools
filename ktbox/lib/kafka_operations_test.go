package lib

import (
	"github.com/Shopify/sarama"
	"github.com/bouk/monkey"
	"github.com/ricardo-ch/kafka-tools/ktbox/lib/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"reflect"
	"testing"
)

func Test_CleanTopic(t *testing.T) {
	broker := &sarama.Broker{}
	called := false
	patch := monkey.PatchInstanceMethod(reflect.TypeOf(broker), "DeleteRecords", func(b *sarama.Broker, request *sarama.DeleteRecordsRequest) (*sarama.DeleteRecordsResponse, error) {
		called = true
		resp := &sarama.DeleteRecordsResponse{
			Topics: map[string]*sarama.DeleteRecordsResponseTopic{},
		}
		for topic := range request.Topics {
			resp.Topics[topic] = &sarama.DeleteRecordsResponseTopic{
				Partitions: map[int32]*sarama.DeleteRecordsResponsePartition{},
			}
			for partition := range (*request.Topics[topic]).PartitionOffsets {
				resp.Topics[topic].Partitions[partition] = &sarama.DeleteRecordsResponsePartition{
					Err: sarama.ErrNoError,
				}
			}
		}

		return resp, nil
	})
	defer patch.Unpatch()

	client := new(mocks.Client)
	client.On("Partitions", "topic1").Return([]int32{0, 1}, nil)
	client.On("Leader", "topic1", mock.Anything).Return(broker, nil)
	client.On("GetOffset", "topic1", mock.Anything, sarama.OffsetNewest).Return(int64(42), nil)

	err := CleanTopic(client, "topic1", nil)
	assert.NoError(t, err)
	assert.True(t, called)
}

func Test_GetCurrentTopicOffset(t *testing.T) {
	client := new(mocks.Client)
	client.On("Partitions", "topic1").Return([]int32{0, 1}, nil)
	client.On("GetOffset", "topic1", int32(0), sarama.OffsetNewest).Return(int64(42), nil)
	client.On("GetOffset", "topic1", int32(1), sarama.OffsetNewest).Return(int64(6), nil)

	offsets, err := GetCurrentMaxTopicOffset(client, "topic1")
	assert.NoError(t, err)
	assert.Equal(t, map[int32]int64{0: int64(42), 1: int64(6)}, offsets)
}
