package lib

import (
	"github.com/Shopify/sarama"
	"github.com/bouk/monkey"
	"github.com/ricardo-ch/kafka-tools/hitman/lib/mocks"
	"github.com/stretchr/testify/assert"
	tmocks "github.com/stretchr/testify/mock"
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
	client.On("Leader", "topic2", tmocks.Anything).Return(broker, nil)
	client.On("GetOffset", "topic2", tmocks.Anything, sarama.OffsetNewest).Return(int64(42), nil)

	err := ensureTopics(client, "topic1", "topic2")
	assert.NoError(t, err)

}

func Test_ensureTopics_sourceDoesNotExist(t *testing.T) {
	patch := monkey.Patch(CleanTopic, func(client sarama.Client, topic string) error { return nil })
	defer patch.Unpatch()

	client := new(mocks.Client)
	client.On("Partitions", "topic1").Return([]int32{}, nil)
	client.On("Partitions", "topic2").Return([]int32{0, 1, 2}, nil)
	client.On("Topics").Return([]string{"topic2"}, nil)

	err := ensureTopics(client, "topic1", "topic2")
	assert.Error(t, err)
}

func Test_KillMessage_ok(t *testing.T) {
	client := new(mocks.Client)

	patch := monkey.Patch(newClient, func(brokers []string) (sarama.Client, error) { return client, nil })
	defer patch.Unpatch()

}
