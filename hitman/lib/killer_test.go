package lib

import (
	"github.com/Shopify/sarama"
	"github.com/bouk/monkey"
	"github.com/ricardo-ch/kafka-tools/hitman/lib/mocks"
	ktbox "github.com/ricardo-ch/kafka-tools/ktbox/lib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"reflect"
	"testing"
)

func Test_ensureTopic_ok(t *testing.T) {
	// ok Broker are not an interface yet and the usual way is worse than usual because there is no NewMockCreateTopicResponse implemented
	// TODO when interface is available change that patch to a mockery

	broker := &sarama.Broker{}
	patch := monkey.PatchInstanceMethod(reflect.TypeOf(broker), "DeleteRecords", func(b *sarama.Broker, request *sarama.DeleteRecordsRequest) (*sarama.DeleteRecordsResponse, error) {
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
	client.On("Partitions", "topic2").Return([]int32{0, 1, 2}, nil)
	client.On("Topics").Return([]string{"topic1", "topic2"}, nil)
	client.On("Leader", "topic2", mock.Anything).Return(broker, nil)
	client.On("GetOffset", "topic2", mock.Anything, sarama.OffsetNewest).Return(int64(42), nil)

	err := ensureTopics(client, "topic1", "topic2")
	assert.NoError(t, err)
	client.AssertExpectations(t)

}

func Test_ensureTopics_sourceDoesNotExist(t *testing.T) {
	patch := monkey.Patch(ktbox.CleanTopic, func(client sarama.Client, topic string, partitions []int32) error { return nil })
	defer patch.Unpatch()

	client := new(mocks.Client)
	client.On("Partitions", "topic1").Return([]int32{}, nil)
	client.On("Partitions", "topic2").Return([]int32{0, 1, 2}, nil)
	client.On("Topics").Return([]string{"topic2"}, nil)

	err := ensureTopics(client, "topic1", "topic2")
	assert.Error(t, err)
}

func Test_KillMessage_ok(t *testing.T) {
	SetIntermediateTopic("topic2")
	client := new(mocks.Client)

	patch := monkey.Patch(ktbox.NewClient, func(brokers []string) (sarama.Client, error) { return client, nil })
	defer patch.Unpatch()

	patch = monkey.Patch(GetConsumerGroup, func(client sarama.Client, topic string) (map[string]map[int32]int64, error) {
		assert.Equal(t, "topic1", topic)
		return map[string]map[int32]int64{
			"group1": {
				0: 7,
				1: 5,
			},
		}, nil
	})
	defer patch.Unpatch()

	patch = monkey.Patch(EnsureConsumerGroupsInactive, func(client sarama.Client, consumerGroups []string) error {
		assert.Equal(t, []string{"group1"}, consumerGroups)
		return nil
	})
	defer patch.Unpatch()

	patch = monkey.Patch(ensureTopics, func(client sarama.Client, topicSource string, intermediateTopic string) error {
		assert.Equal(t, "topic1", topicSource)
		return nil
	})
	defer patch.Unpatch()

	{
		nbCalled := 0
		patch = monkey.Patch(CloneTopic, func(client sarama.Client, topicSource string, topicSink string, contract KillContract, oldGroupsOffsets map[string]map[int32]int64) (map[string]map[int32]int64, error) {
			nbCalled++
			switch nbCalled {
			case 1:
				assert.Equal(t, "topic1", topicSource)
			case 2:
				assert.Equal(t, "topic2", topicSource)
			}
			return oldGroupsOffsets, nil
		})
		defer patch.Unpatch()
		defer func() { assert.Equal(t, 2, nbCalled, "CloneTopic") }()
	}
	{
		nbCalled := 0
		patch = monkey.Patch(ktbox.CleanTopic, func(client sarama.Client, topic string, partitions []int32) error {
			nbCalled++
			switch nbCalled {
			case 2:
				assert.Equal(t, "topic2", topic)
			case 1:
				assert.Equal(t, "topic1", topic)
			}
			return nil
		})
		defer patch.Unpatch()
		defer func() { assert.Equal(t, 2, nbCalled, "CleanTopic") }() //since ensure is mocked, clean is called only twice
	}
	{
		nbCalled := 0
		patch = monkey.Patch(UpdateConsumerGroupOffset, func(client sarama.Client, topic string, newConsumerGroupOffsets map[string]map[int32]int64) error {
			nbCalled++
			assert.Equal(t, "topic1", topic)
			return nil
		})
		defer patch.Unpatch()
		defer func() { assert.Equal(t, 1, nbCalled, "UpdateConsumerGroupOffset") }()
	}
	err := KillMessage([]string{}, "topic1", func(msg *sarama.ConsumerMessage) bool { return false })
	assert.NoError(t, err)
}
