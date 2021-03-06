package lib

import (
	"github.com/Shopify/sarama"
	"github.com/bouk/monkey"
	"github.com/ricardo-ch/kafka-tools/hitman/lib/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"reflect"
	"testing"
)

func Test_GetConsumerGroup(t *testing.T) {
	broker := &sarama.Broker{}
	patch := monkey.PatchInstanceMethod(reflect.TypeOf(broker), "ListGroups", func(b *sarama.Broker, request *sarama.ListGroupsRequest) (*sarama.ListGroupsResponse, error) {
		resp := &sarama.ListGroupsResponse{
			Groups: map[string]string{"group1": "client"},
		}

		return resp, nil
	})
	defer patch.Unpatch()
	patch = monkey.PatchInstanceMethod(reflect.TypeOf(broker), "FetchOffset", func(b *sarama.Broker, request *sarama.OffsetFetchRequest) (*sarama.OffsetFetchResponse, error) {
		resp := &sarama.OffsetFetchResponse{
			Blocks: map[string]map[int32]*sarama.OffsetFetchResponseBlock{
				"topic1": {
					0: {Offset: 4, Err: sarama.ErrNoError},
					1: {Offset: 7, Err: sarama.ErrNoError},
				},
			},
		}

		return resp, nil
	})
	defer patch.Unpatch()

	client := new(mocks.Client)
	client.On("Controller").Return(broker, nil)
	client.On("Partitions", "topic1").Return([]int32{0, 1}, nil)
	client.On("Coordinator", "group1").Return(broker, nil)

	group, err := GetConsumerGroup(client, "topic1")
	assert.NoError(t, err)
	assert.Equal(t, map[string]map[int32]int64{"group1": {0: 4, 1: 7}}, group)
}

func Test_EnsureConsumerGroupsInactive(t *testing.T) {
	broker := &sarama.Broker{}
	patch := monkey.PatchInstanceMethod(reflect.TypeOf(broker), "DescribeGroups", func(b *sarama.Broker, request *sarama.DescribeGroupsRequest) (*sarama.DescribeGroupsResponse, error) {
		resp := &sarama.DescribeGroupsResponse{
			Groups: []*sarama.GroupDescription{
				{
					Err:     sarama.ErrNoError,
					State:   "empty",
					Members: map[string]*sarama.GroupMemberDescription{},
				},
			},
		}

		return resp, nil
	})
	defer patch.Unpatch()

	client := new(mocks.Client)
	client.On("Coordinator", "group1").Return(broker, nil)

	err := EnsureConsumerGroupsInactive(client, []string{"group1"})
	assert.NoError(t, err)
}

func Test_EnsureConsumerGroupsInactive_Active(t *testing.T) {
	broker := &sarama.Broker{}
	patch := monkey.PatchInstanceMethod(reflect.TypeOf(broker), "DescribeGroups", func(b *sarama.Broker, request *sarama.DescribeGroupsRequest) (*sarama.DescribeGroupsResponse, error) {
		resp := &sarama.DescribeGroupsResponse{
			Groups: []*sarama.GroupDescription{
				{
					Err:   sarama.ErrNoError,
					State: "empty",
					Members: map[string]*sarama.GroupMemberDescription{
						"member1": {},
					},
				},
			},
		}

		return resp, nil
	})
	defer patch.Unpatch()

	client := new(mocks.Client)
	client.On("Coordinator", "group1").Return(broker, nil)

	err := EnsureConsumerGroupsInactive(client, []string{"group1"})
	assert.Error(t, err)
}

func Test_UpdateConsumerGroupOffset(t *testing.T) {
	update := map[string]map[int32]int64{
		"group1": {
			0: 5,
			1: 6,
		},
	}

	patch := monkey.Patch(EnsureConsumerGroupsInactive, func(client sarama.Client, consumerGroups []string) error {
		assert.Equal(t, []string{"group1"}, consumerGroups)
		return nil
	})
	defer patch.Unpatch()

	partitionManager := new(mocks.PartitionOffsetManager)
	partitionManager.On("MarkOffset", mock.Anything, mock.Anything).Return()
	partitionManager.On("Close").Return(nil)

	offsetManager := &mocks.OffsetManager{}
	offsetManager.On("ManagePartition", "topic1", mock.Anything).Return(partitionManager, nil)

	patch = monkey.Patch(sarama.NewOffsetManagerFromClient, func(group string, client sarama.Client) (sarama.OffsetManager, error) {
		return offsetManager, nil
	})
	defer patch.Unpatch()

	client := new(mocks.Client)

	err := UpdateConsumerGroupOffset(client, "topic1", update)
	assert.NoError(t, err)
}
