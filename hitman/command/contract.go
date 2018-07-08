package command

import (
	"errors"
	"github.com/Shopify/sarama"
	hitman "github.com/ricardo-ch/kafka-tools/hitman/lib"
	"regexp"
	"strconv"
	"strings"
)

func GetContract(key, offset []string, regex string) (hitman.KillContract, error) {
	if len(key) > 0 {
		return getContractFromKey(key)
	}
	if len(offset) > 0 {
		return getContractFromOffset(offset)
	}
	if regex != "" {
		return getContractFromRegex(regex)
	}
	return nil, errors.New("no contract")
}

type offsetPartition struct {
	Offset    int64
	Partition int32
}

func getContractFromOffset(offsets []string) (hitman.KillContract, error) {
	var targets []offsetPartition
	for _, partitionOffset := range offsets {
		couple := strings.Split(partitionOffset, ":")
		if len(couple) != 2 {
			return nil, errors.New("invalid couple partition:offsets")
		}
		partition, err := strconv.Atoi(couple[0])
		if err != nil {
			return nil, errors.New("invalid couple partition:offsets")
		}
		offset, err := strconv.ParseInt(couple[1], 10, 64)
		if err != nil {
			return nil, errors.New("invalid couple partition:offsets")
		}
		targets = append(targets, offsetPartition{Offset: offset, Partition: int32(partition)})
	}

	return func(msg *sarama.ConsumerMessage) bool {
		for _, target := range targets {
			if target.Partition == msg.Partition && target.Offset == msg.Offset {
				return true
			}
		}
		return false
	}, nil
}

func getContractFromKey(keyList []string) (hitman.KillContract, error) {
	return func(msg *sarama.ConsumerMessage) bool {
		for _, key := range keyList {
			if string(msg.Key) == key {
				return true
			}
		}
		return false
	}, nil

}

func getContractFromRegex(reg string) (hitman.KillContract, error) {
	regex, err := regexp.Compile(reg)
	if err != nil {
		return nil, errors.New("invalid regex")
	}

	return func(msg *sarama.ConsumerMessage) bool {
		return regex.Match(msg.Value)
	}, nil
}
