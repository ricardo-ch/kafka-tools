package lib

import (
	"github.com/Shopify/sarama"
	"github.com/ahmetb/go-linq"
	"github.com/pkg/errors"
	"time"
)

var intermediateTopic = "kafka-hitman-work"

func SetIntermediateTopic(topic string) {
	intermediateTopic = topic
}

func KillMessage(brokers []string, topic string, contract KillContract) error {
	client, err := newClient(brokers)
	if err != nil {
		return err
	}

	consumerGroupsOffsets, err := GetConsumerGroup(client, topic)
	if err != nil {
		return err
	}

	// Do that at the beginning for fast-fail.
	// Do that again at the last moment for best effort check
	err = EnsureConsumerGroupsInactive(client, getConsumerListFromOffsetList(consumerGroupsOffsets))
	if err != nil {
		return err
	}
	//TODO use acl to block write on topicSource

	err = ensureTopics(client, topic, intermediateTopic)
	if err != nil {
		return err
	}

	newConsumerGroupOffset, err := CloneTopic(client, topic, intermediateTopic, contract, consumerGroupsOffsets)
	if err != nil {
		return err
	}

	err = CleanTopic(client, topic)
	if err != nil {
		return err
	}

	newConsumerGroupOffset, err = CloneTopic(client, intermediateTopic, topic, NoKillContract, newConsumerGroupOffset)
	if err != nil {
		return err
	}

	err = CleanTopic(client, intermediateTopic)
	if err != nil {
		return err
	}

	err = UpdateConsumerGroupOffset(client, topic, newConsumerGroupOffset)
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

	err = CleanTopic(client, intermediateTopic)
	if err != nil {
		return err
	}

	return nil
}
