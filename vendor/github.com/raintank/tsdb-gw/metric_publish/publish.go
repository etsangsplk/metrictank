package metric_publish

import (
	"errors"
	"flag"
	"hash"
	"hash/fnv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	p "github.com/raintank/metrictank/cluster/partitioner"
	"github.com/raintank/metrictank/stats"
	"github.com/raintank/tsdb-gw/usage"
	"github.com/raintank/tsdb-gw/util"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
)

var (
	config   *kafka.ConfigMap
	producer *kafka.Producer
	brokers  []string

	metricsPublished = stats.NewCounter32("metrics.published")
	messagesSize     = stats.NewMeter32("metrics.message_size", false)
	publishDuration  = stats.NewLatencyHistogram15s32("metrics.publish")
	sendErrProducer  = stats.NewCounter32("metrics.send_error.producer")
	sendErrOther     = stats.NewCounter32("metrics.send_error.other")

	topic           string
	codec           string
	enabled         bool
	partitionScheme string
	flushFreq       time.Duration
	partitioner     Partitioner
	maxInFlight     int

	bufferPool = util.NewBufferPool()
)

type Partitioner interface {
	partition(schema.PartitionedMetric) (int32, []byte, error)
}

func NewPartitioner(pCount int32) Partitioner {
	kafka, err := p.NewKafka(partitionScheme)
	if err != nil {
		log.Fatal(4, "failed to initialize partitioner. %s", err)
	}

	return &partitionerFnv1a{
		hasher: fnv.New32a(),
		pCount: pCount,
		kafka:  kafka,
	}
}

type partitionerFnv1a struct {
	kafka  *p.Kafka
	hasher hash.Hash32
	pCount int32
}

func (p *partitionerFnv1a) partition(m schema.PartitionedMetric) (int32, []byte, error) {
	key, err := p.kafka.GetPartitionKey(m, nil)
	if err != nil {
		return -1, nil, err
	}

	p.hasher.Reset()
	_, err = p.hasher.Write(key)
	if err != nil {
		return -1, nil, err
	}

	partition := int32(p.hasher.Sum32()) % p.pCount
	if partition < 0 {
		partition = -partition
	}

	return partition, key, nil
}

func init() {
	flag.StringVar(&topic, "metrics-topic", "mdm", "topic for metrics")
	flag.StringVar(&codec, "metrics-kafka-comp", "snappy", "compression: none|gzip|snappy")
	flag.BoolVar(&enabled, "metrics-publish", false, "enable metric publishing")
	flag.StringVar(&partitionScheme, "metrics-partition-scheme", "bySeries", "method used for paritioning metrics. (byOrg|bySeries)")
	flag.DurationVar(&flushFreq, "metrics-flush-freq", time.Millisecond*50, "The best-effort frequency of flushes to kafka")
	flag.IntVar(&maxInFlight, "metrics-max-in-flight", 1000000, "The maximum number of messages in flight per broker connection")
}

func Init(broker string) {
	if !enabled {
		return
	}
	var err error

	config := kafka.ConfigMap{}
	config.SetKey("request.required.acks", "all")
	config.SetKey("message.send.max.retries", "10")
	config.SetKey("bootstrap.servers", broker)
	config.SetKey("compression.codec", codec)
	config.SetKey("max.in.flight", maxInFlight)

	producer, err = kafka.NewProducer(&config)
	if err != nil {
		log.Fatal(4, "failed to initialize kafka producer. %s", err)
	}

	meta, err := producer.GetMetadata(&topic, false, 30000)
	if err != nil {
		log.Fatal(4, "failed to initialize kafka partitioner. %s", err)
	}

	var t kafka.TopicMetadata
	var ok bool
	if t, ok = meta.Topics[topic]; !ok {
		log.Fatal(4, "failed to get metadata about topic %s", topic)
	}

	partitioner = NewPartitioner(int32(len(t.Partitions)))
}

func Publish(metrics []*schema.MetricData) error {
	if producer == nil {
		log.Debug("dropping %d metrics as publishing is disabled", len(metrics))
		return nil
	}
	if len(metrics) == 0 {
		return nil
	}
	var err error

	payload := make([]*kafka.Message, len(metrics))
	pre := time.Now()
	deliveryChan := make(chan kafka.Event)

	for i, metric := range metrics {
		data := bufferPool.Get()
		data, err = metric.MarshalMsg(data)
		if err != nil {
			return err
		}

		part, key, err := partitioner.partition(metric)
		if err != nil {
			return err
		}

		payload[i] = &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: part},
			Value:          data,
			Key:            key,
		}

		messagesSize.Value(len(data))

		err = producer.Produce(payload[i], deliveryChan)
		if err != nil {
			return err
		}
	}

	// return buffers to the bufferPool
	defer func() {
		var buf []byte
		for _, msg := range payload {
			buf = msg.Value
			bufferPool.Put(buf)
		}
	}()

	msgCount := 0
	var errCount int
	var firstErr error
	for e := range deliveryChan {
		msgCount++

		err = nil
		m, ok := e.(*kafka.Message)
		if !ok || e == nil {
			log.Error(4, "unexpected delivery report of type %T: %v", e, e)
			err = errors.New("Invalid acknowledgement")
		} else if m.TopicPartition.Error != nil {
			err = m.TopicPartition.Error
		}

		if err != nil {
			errCount++
			sendErrOther.Inc()
			if firstErr == nil {
				firstErr = err
			}
		}

		if msgCount >= len(metrics) {
			close(deliveryChan)
		}
	}

	if firstErr != nil {
		log.Error(4, "Got %d errors when sending %d messages, the first was: %s", errCount, len(metrics), firstErr)
		return firstErr
	}

	publishDuration.Value(time.Since(pre))
	metricsPublished.Add(len(metrics))
	log.Debug("published %d metrics", len(metrics))
	for _, metric := range metrics {
		usage.LogDataPoint(metric.Id)
	}
	return nil
}
