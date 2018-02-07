package kafka

import (
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/raintank/worldping-api/pkg/log"
)

// returns elements that are in a but not in b
func DiffPartitions(a []int32, b []int32) []int32 {
	var diff []int32
Iter:
	for _, eA := range a {
		for _, eB := range b {
			if eA == eB {
				continue Iter
			}
		}
		diff = append(diff, eA)
	}
	return diff
}

func GetPartitions(client *confluent.Consumer, topics []string, retries, backoff, timeout int) (map[string][]int32, error) {
	partitions := make(map[string][]int32, 0)
	var ok bool
	var tm confluent.TopicMetadata
	for _, topic := range topics {
		partitions[topic] = make([]int32, 0)
		for i := retries; i > 0; i-- {
			metadata, err := client.GetMetadata(&topic, false, timeout)
			if err != nil {
				log.Warn("failed to get metadata from kafka client. %s, %d retries", err, i)
				time.Sleep(time.Duration(backoff) * time.Millisecond)
				continue
			}

			if tm, ok := metadata.Topics[topic]; !ok || tm.Error.Code() == confluent.ErrUnknownTopic {
				log.Warn("unknown topic %s, %d retries", topic, i)
				time.Sleep(time.Duration(backoff) * time.Millisecond)
				continue
			}

			if tm, ok = metadata.Topics[topic]; !ok || len(tm.Partitions) == 0 {
				log.Warn("0 partitions returned for %s, %d retries", topic, i)
				time.Sleep(time.Duration(backoff) * time.Millisecond)
				continue
			}

			for _, partitionMetadata := range tm.Partitions {
				partitions[topic] = append(partitions[topic], partitionMetadata.ID)
			}
		}
	}

	log.Info("returning partitions: %+v", partitions)
	return partitions, nil
}
