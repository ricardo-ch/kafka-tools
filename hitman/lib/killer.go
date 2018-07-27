package lib

import (
	"github.com/Shopify/sarama"
	"github.com/ahmetb/go-linq"
	"github.com/pkg/errors"
	"time"
	ktbox "github.com/ricardo-ch/kafka-tools/ktbox/lib"
)

var intermediateTopic = "kafka-hitman-work"

func SetIntermediateTopic(topic string) {
	intermediateTopic = topic
}

// CreateWorkTopic is the first step of process
// create a temporary topic in the expected state so that you can assert the operation is doing what you expect
// return offset of groups on this temporary topic matching message of original topic
func CreateWorkTopic(brokers []string, topic string, contract KillContract) (map[string]map[int32]int64, error) {
	client, err := ktbox.NewClient(brokers)
	if err != nil {
		return nil, err
	}

	consumerGroupsOffsets, err := GetConsumerGroup(client, topic)
	if err != nil {
		return nil, err
	}

	// Do that at the beginning for fast-fail.
	// Do that again at the last moment for best effort check
	err = EnsureConsumerGroupsInactive(client, getConsumerListFromOffsetList(consumerGroupsOffsets))
	if err != nil {
		return nil, err
	}
	//TODO use acl to block write on topicSource

	err = ensureTopics(client, topic, intermediateTopic)
	if err != nil {
		return nil, err
	}

	tmpGroupOffset, err := CloneTopic(client, topic, intermediateTopic, contract, consumerGroupsOffsets)
	if err != nil {
		return nil, err
	}
	return tmpGroupOffset, nil
}

// Commit delete data from `topic` and put new one from the workTopic
// This is the risky step that definitely alter your data, you should check that every thing is ok previously
// 	tmpGroupOffset: offset of consumer group on workTopic. Used to match new offset of consumer groups after transformation
func Commit(brokers []string, topic string, tmpGroupOffset map[string]map[int32]int64) (err error) {
	client, err := ktbox.NewClient(brokers)
	if err != nil {
		return err
	}

	// Do that at the beginning for fast-fail.
	// Do that again at the last moment for best effort check
	err = EnsureConsumerGroupsInactive(client, getConsumerListFromOffsetList(tmpGroupOffset))
	if err != nil {
		return err
	}
	//TODO use acl to block write on topicSource

	err = ktbox.CleanTopic(client, topic)
	if err != nil {
		return err
	}

	newGroupOffset, err := CloneTopic(client, intermediateTopic, topic, NoKillContract, tmpGroupOffset)
	if err != nil {
		return err
	}

	err = UpdateConsumerGroupOffset(client, topic, newGroupOffset)
	if err != nil {
		return err
	}
	return nil
}

// CleanUp remove data from work topic.
// Once `Commit` and `Cleanup` are called, data is irremediably altered
func CleanUp(brokers []string) error {
	client, err := ktbox.NewClient(brokers)
	if err != nil {
		return err
	}
	err = ktbox.CleanTopic(client, intermediateTopic)
	if err != nil {
		return err
	}
	return nil
}

// Kill a message in one step.
// You should may call all 3 steps independently if you want more control over the process. In order these are:
//	- CreateWorkTopic
//	- Commit
//	- Cleanup
func KillMessage(brokers []string, topic string, contract KillContract) error {
	tmpGroupOffset, err := CreateWorkTopic(brokers, topic, contract)
	if err != nil {
		return err
	}
	err = Commit(brokers, topic, tmpGroupOffset)
	if err != nil {
		return err
	}
	err = CleanUp(brokers)
	if err != nil {
		return err
	}
	return nil
}

// ensure that:
// 	- source exist
//	- sink has enough partitions  (more than source), create it if does not exist
//	- remove all messages in sink
func ensureTopics(client sarama.Client, topicSource string, intermediateTopic string) error {
	SourcePartitions, err := client.Partitions(topicSource)
	if err != nil {
		return err
	}
	if len(SourcePartitions) <= 0 {
		return errors.New("topicSource does not exist?")
	}

	// Need to list topics instead of just requesting partition because Partition request actually create the topicSource if not exist
	topics, err := client.Topics()
	if err != nil {
		return err
	}

	if linq.From(topics).Contains(intermediateTopic) {
		SinkPartitions, err := client.Partitions(intermediateTopic)
		if err != nil {
			return err
		}

		if len(SinkPartitions) < len(SourcePartitions) {
			return errors.New("intermediateTopic already exist and does not have enough partitions")
		}
	} else {
		// Sink does not exist
		// Create it using same number of partition
		createTopicReq := sarama.CreateTopicsRequest{
			Version: 2,
			Timeout: 10 * time.Second,
			TopicDetails: map[string]*sarama.TopicDetail{
				intermediateTopic: {
					NumPartitions:     20,
					ReplicationFactor: 1, //TODO
				},
			},
		}

		controller, err := client.Controller()
		if err != nil {
			return err
		}
		createTopicResp, err := controller.CreateTopics(&createTopicReq)
		if err != nil {
			return err
		}

		tErr, ok := createTopicResp.TopicErrors[intermediateTopic]
		if ok && tErr != nil && tErr.Err != sarama.ErrNoError {
			return errors.Wrap(tErr.Err, getStringOrDefault(tErr.ErrMsg))
		}
	}

	err = ktbox.CleanTopic(client, intermediateTopic)
	if err != nil {
		return err
	}

	return nil
}
