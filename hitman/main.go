package main

import (
	"log"
)

var (
	topicSource       = "test-francois"
	intermediateTopic = "kafka-hitman-work" //TODO use specific topicSource with loads of partition in there
	bootstrapserver   = []string{"kafka:9092"}

	contract = func(partition int32, offset int64) bool {
		return offset == 3 && partition == 0
	}
)

func main() {
	client, err := NewClient(bootstrapserver)
	if err != nil {
		log.Fatal(err)
	}

	consumerGroupsOffsets, err := getConsumerGroup(client, topicSource)
	if err != nil {
		log.Fatal(err)
	}

	// Do that at the beginning for fast-fail.
	// Do that again at the last moment for best effort check
	err = ensureConsumerGroupsInactive(client, GetConsumerListFromOffsetList(consumerGroupsOffsets))
	if err != nil {
		log.Fatal(err)
	}
	//TODO use acl to block write on topicSource

	err = EnsureTopics(client, topicSource, intermediateTopic)
	if err != nil {
		log.Fatal(err)
	}

	newConsumerGroupOffset, err := CloneTopic(client, topicSource, intermediateTopic, contract, consumerGroupsOffsets)
	if err != nil {
		log.Fatal(err)
	}

	err = cleanTopic(client, topicSource)
	if err != nil {
		log.Fatal(err)
	}

	newConsumerGroupOffset, err = CloneTopic(client, intermediateTopic, topicSource, NoKillContract, newConsumerGroupOffset)
	if err != nil {
		log.Fatal(err)
	}

	// TODO should I delete topicSource instead of cleaning it?
	err = cleanTopic(client, intermediateTopic)
	if err != nil {
		log.Fatal(err)
	}

	err = updateConsumerGroupOffset(client, topicSource, newConsumerGroupOffset)
	if err != nil {
		log.Fatal(err)
	}
}
