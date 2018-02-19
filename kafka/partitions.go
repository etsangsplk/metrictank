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
		for i := retries; i > 0; i-- {
			metadata, err := client.GetMetadata(&topic, false, timeout)
			if err != nil {
				log.Warn("kafka: failed to get metadata from kafka client. %s, %d retries", err, i)
				time.Sleep(time.Duration(backoff) * time.Millisecond)
				continue
			}

			// if kafka's auto.create.topics is enabled (default) then a topic will get created with the default
			// settings after our first GetMetadata call for it. But because the topic creation can take a moment
			// we'll need to retry a fraction of second later in order to actually get the according metadata.
			if tm, ok := metadata.Topics[topic]; !ok || tm.Error.Code() == confluent.ErrUnknownTopic {
				log.Warn("kafka: unknown topic %s, %d retries", topic, i)
				time.Sleep(time.Duration(backoff) * time.Millisecond)
				continue
			}

			if tm, ok = metadata.Topics[topic]; !ok || len(tm.Partitions) == 0 {
				log.Warn("kafka: 0 partitions returned for %s, %d retries", topic, i)
				time.Sleep(time.Duration(backoff) * time.Millisecond)
				continue
			}

			partitions[topic] = make([]int32, len(tm.Partitions))
			for _, partitionMetadata := range tm.Partitions {
				partitions[topic] = append(partitions[topic], partitionMetadata.ID)
			}
		}
	}

	log.Info("kafka: partitions by topic: %+v", partitions)
	return partitions, nil
}
