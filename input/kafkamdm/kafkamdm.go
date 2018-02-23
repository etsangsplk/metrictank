package kafkamdm

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/input"
	"github.com/grafana/metrictank/kafka"
	"github.com/grafana/metrictank/stats"
	"gopkg.in/raintank/schema.v1"
)

// metric input.kafka-mdm.metrics_per_message is how many metrics per message were seen.
var metricsPerMessage = stats.NewMeter32("input.kafka-mdm.metrics_per_message", false)

// metric input.kafka-mdm.metrics_decode_err is a count of times an input message failed to parse
var metricsDecodeErr = stats.NewCounter32("input.kafka-mdm.metrics_decode_err")

type KafkaMdm struct {
	input.Handler
	consumer   *confluent.Consumer
	lagMonitor *LagMonitor
	wg         sync.WaitGroup

	// signal to PartitionConsumers to shutdown
	stopConsuming chan struct{}
	// signal to caller that it should shutdown
	fatal chan struct{}
}

func (k *KafkaMdm) Name() string {
	return "kafka-mdm"
}

var DataDir string
var Enabled bool
var LogLevel int
var batchNumMessages int
var brokerStr string
var bufferMaxMs int
var channelBufferSize int
var currentOffsets map[string]map[int32]*int64
var fetchMin int
var maxWaitMs int
var metadataBackoffTime int
var metadataRetries int
var metadataTimeout int
var netMaxOpenRequests int
var offsetCommitInterval time.Duration
var offsetDuration time.Duration
var offsetStr string
var partitionLag map[int32]*stats.Gauge64
var partitionLogSize map[int32]*stats.Gauge64
var partitionOffset map[int32]*stats.Gauge64
var partitionStr string
var partitions []int32
var sessionTimeout int
var topicStr string
var topics []string

func ConfigSetup() {
	inKafkaMdm := flag.NewFlagSet("kafka-mdm-in", flag.ExitOnError)
	inKafkaMdm.BoolVar(&Enabled, "enabled", false, "")
	inKafkaMdm.DurationVar(&offsetCommitInterval, "offset-commit-interval", time.Second*5, "Interval at which offsets should be saved.")
	inKafkaMdm.IntVar(&batchNumMessages, "batch-num-messages", 10000, "Maximum number of messages batched in one MessageSet")
	inKafkaMdm.IntVar(&bufferMaxMs, "metrics-buffer-max-ms", 100, "Delay in milliseconds to wait for messages in the producer queue to accumulate before constructing message batches (MessageSets) to transmit to brokers")
	inKafkaMdm.IntVar(&channelBufferSize, "channel-buffer-size", 1000000, "Maximum number of messages allowed on the producer queue")
	inKafkaMdm.IntVar(&fetchMin, "consumer-fetch-min", 1, "Minimum number of bytes the broker responds with. If fetch.wait.max.ms expires the accumulated data will be sent to the client regardless of this setting")
	inKafkaMdm.IntVar(&maxWaitMs, "consumer-max-wait-ms", 100, "Maximum time the broker may wait to fill the response with fetch.min.bytes")
	inKafkaMdm.IntVar(&metadataBackoffTime, "metadata-backoff-time", 500, "Time to wait between attempts to fetch metadata in ms")
	inKafkaMdm.IntVar(&metadataRetries, "metadata-retries", 5, "Number of retries to fetch metadata in case of failure")
	inKafkaMdm.IntVar(&metadataTimeout, "consumer-metadata-timeout-ms", 10000, "Maximum time to wait for the broker to reply to metadata queries in ms")
	inKafkaMdm.IntVar(&netMaxOpenRequests, "net-max-open-requests", 100, "Maximum number of in-flight requests per broker connection. This is a generic property applied to all broker communication, however it is primarily relevant to produce requests.")
	inKafkaMdm.IntVar(&sessionTimeout, "consumer-session-timeout", 30000, "Client group session and failure detection timeout in ms")
	inKafkaMdm.StringVar(&DataDir, "data-dir", "", "Directory to store partition offsets index")
	inKafkaMdm.StringVar(&brokerStr, "brokers", "kafka:9092", "tcp address for kafka (may be be given multiple times as a comma-separated list)")
	inKafkaMdm.StringVar(&offsetStr, "offset", "oldest", "Set the offset to start consuming from. Can be one of newest, oldest or a time duration")
	inKafkaMdm.StringVar(&partitionStr, "partitions", "*", "kafka partitions to consume. use '*' or a comma separated list of id's")
	inKafkaMdm.StringVar(&topicStr, "topics", "mdm", "kafka topic (may be given multiple times as a comma-separated list)")
	globalconf.Register("kafka-mdm-in", inKafkaMdm)
}

func ConfigProcess(instance string) {
	if !Enabled {
		return
	}

	if offsetCommitInterval == 0 {
		log.Fatal(4, "kafkamdm: offset-commit-interval must be greater then 0")
	}

	if maxWaitMs == 0 {
		log.Fatal(4, "kafkamdm: consumer-max-wait-time must be greater then 0")
	}

	var err error
	switch offsetStr {
	case "oldest":
	case "newest":
	default:
		offsetDuration, err = time.ParseDuration(offsetStr)
		if err != nil {
			log.Fatal(4, "kafkamdm: invalid offest format. %s", err)
		}
	}

	config := kafka.GetConfig(brokerStr, "none", batchNumMessages, bufferMaxMs, channelBufferSize, fetchMin, netMaxOpenRequests, maxWaitMs, sessionTimeout)
	client, err := confluent.NewConsumer(config)
	if err != nil {
		log.Fatal(4, "failed to initialize kafka client. %s", err)
	}
	defer client.Close()

	topics = strings.Split(topicStr, ",")
	availPartsByTopic, err := kafka.GetPartitions(client, topics, metadataRetries, metadataBackoffTime, metadataTimeout)
	if err != nil {
		log.Fatal(4, "kafka-mdm: %s", err.Error())
	}

	var availParts []int32
	for _, topic := range topics {
		for _, part := range availPartsByTopic[topic] {
			availParts = append(availParts, part)
		}
	}

	log.Info("kafka-mdm: available partitions %v", availPartsByTopic)
	if partitionStr == "*" {
		partitions = availParts
	} else {
		parts := strings.Split(partitionStr, ",")
		for _, part := range parts {
			i, err := strconv.Atoi(part)
			if err != nil {
				log.Fatal(4, "could not parse partition %q. partitions must be '*' or a comma separated list of id's", part)
			}
			partitions = append(partitions, int32(i))
		}
		missing := kafka.DiffPartitions(partitions, availParts)
		if len(missing) > 0 {
			log.Fatal(4, "kafka-mdm: configured partitions not in list of available partitions. missing %v", missing)
		}
	}

	// record our partitions so others (MetricIdx) can use the partitioning information.
	// but only if the manager has been created (e.g. in metrictank), not when this input plugin is used in other contexts
	if cluster.Manager != nil {
		cluster.Manager.SetPartitions(partitions)
	}

	// initialize our offset metrics
	partitionOffset = make(map[int32]*stats.Gauge64)
	partitionLogSize = make(map[int32]*stats.Gauge64)
	partitionLag = make(map[int32]*stats.Gauge64)
	for _, part := range partitions {
		partitionOffset[part] = stats.NewGauge64(fmt.Sprintf("input.kafka-mdm.partition.%d.offset", part))
		partitionLogSize[part] = stats.NewGauge64(fmt.Sprintf("input.kafka-mdm.partition.%d.log_size", part))
		partitionLag[part] = stats.NewGauge64(fmt.Sprintf("input.kafka-mdm.partition.%d.lag", part))
	}
}

func New() *KafkaMdm {
	config := kafka.GetConfig(brokerStr, "none", batchNumMessages, bufferMaxMs, channelBufferSize, fetchMin, netMaxOpenRequests, maxWaitMs, sessionTimeout)
	consumer, err := confluent.NewConsumer(config)
	if err != nil {
		log.Fatal(4, "failed to initialize kafka consumer. %s", err)
	}
	log.Info("kafka-mdm consumer created without error")
	k := KafkaMdm{
		consumer:      consumer,
		lagMonitor:    NewLagMonitor(10, partitions),
		stopConsuming: make(chan struct{}),
	}

	return &k
}

func (k *KafkaMdm) startConsumer() error {
	currentOffsets = make(map[string]map[int32]*int64)
	var offset confluent.Offset
	var err error
	var topicPartitions confluent.TopicPartitions
	for _, topic := range topics {
		currentOffsets[topic] = make(map[int32]*int64)
		for _, partition := range partitions {
			var currentOffset int64
			switch offsetStr {
			case "oldest":
				currentOffset, _, err = k.tryGetOffset(topic, partition, int64(confluent.OffsetBeginning), 3, time.Second)
				if err != nil {
					return err
				}
			case "newest":
				_, currentOffset, err = k.tryGetOffset(topic, partition, int64(confluent.OffsetEnd), 3, time.Second)
				if err != nil {
					return err
				}
			default:
				currentOffset = time.Now().Add(-1*offsetDuration).UnixNano() / int64(time.Millisecond)
				currentOffset, _, err = k.tryGetOffset(topic, partition, currentOffset, 3, time.Second)
				if err != nil {
					return err
				}
			}

			offset, err = confluent.NewOffset(currentOffset)
			if err != nil {
				return err
			}

			topicPartitions = append(topicPartitions, confluent.TopicPartition{
				Topic:     &topic,
				Partition: partition,
				Offset:    offset,
			})

			currentOffsets[topic][partition] = &currentOffset
		}
	}

	return k.consumer.Assign(topicPartitions)
}

func (k *KafkaMdm) Start(handler input.Handler, fatal chan struct{}) error {
	k.Handler = handler
	k.fatal = fatal

	err := k.startConsumer()
	if err != nil {
		log.Error(4, "kafka-mdm: Failed to start consumer: %q", err)
		return err
	}

	go k.monitorLag()

	for i := 0; i < len(topics)*len(partitions); i++ {
		k.wg.Add(1)
		go k.consume()
	}

	return nil
}

func (k *KafkaMdm) monitorLag() {
	storeOffsets := func(ts time.Time) {
		for topic, partitions := range currentOffsets {
			for partition := range partitions {
				offset := atomic.LoadInt64(currentOffsets[topic][partition])
				k.lagMonitor.StoreOffset(partition, offset, ts)
				partitionOffset[partition].Set(int(offset))
				newest, _, err := k.tryGetOffset(topic, partition, int64(confluent.OffsetEnd), 3, time.Second)
				if err == nil {
					partitionLogSize[partition].Set(int(newest))
					lag := int(newest - offset)
					partitionLag[partition].Set(lag)
					k.lagMonitor.StoreLag(partition, lag)
				} else {
					log.Error(4, "kafka-mdm: Error getting offset: %s", err)
				}
			}
		}
	}

	storeOffsets(time.Now())
	ticker := time.NewTicker(offsetCommitInterval)
	for {
		select {
		case ts := <-ticker.C:
			storeOffsets(ts)
		case <-k.stopConsuming:
			k.consumer.Close()
			storeOffsets(time.Now())
			return
		}
	}
}

func (k *KafkaMdm) consume() {
	defer k.wg.Done()
	var ok bool
	var offsetPtr *int64
	var offset int64
	var currentTopicOffsets map[int32]*int64
	var topic string
	var partition int32

	events := k.consumer.Events()
	for {
		select {
		case ev := <-events:
			switch e := ev.(type) {
			case confluent.AssignedPartitions:
				k.consumer.Assign(e.Partitions)
				log.Info("kafka-mdm: Assigned partitions: %+v", e)
			case confluent.RevokedPartitions:
				k.consumer.Unassign()
				log.Info("kafka-mdm: Revoked partitions: %+v", e)
			case confluent.PartitionEOF:
			case *confluent.Message:
				topic = *e.TopicPartition.Topic
				partition = e.TopicPartition.Partition
				offset = int64(e.TopicPartition.Offset)
				if LogLevel < 2 {
					log.Debug("kafka-mdm: received message: Topic %s, Partition: %d, Offset: %d, Key: %x", topic, partition, offset, e.Key)
				}

				k.handleMsg(e.Value, partition)

				if currentTopicOffsets, ok = currentOffsets[topic]; !ok {
					log.Error(3, "kafka-mdm: received message of unexpected topic: %s", topic)
					continue
				}

				if offsetPtr, ok = currentTopicOffsets[partition]; !ok || offsetPtr == nil {
					log.Error(3, "kafka-mdm received message of unexpected partition: %s:%d", topic, partition)
					continue
				}

				atomic.StoreInt64(offsetPtr, offset)
			case *confluent.Error:
				log.Error(3, "kafka-mdm: kafka consumer error: %s", e.String())
				return
			case confluent.OffsetsCommitted:
				if e.Error != nil {
					log.Error(3, "kafka-mdmd: Error committing offsets for %+v: %s", e.Offsets, e.Error)
				}
				log.Debug("kafka-mdmd: Committed Offsets: %+v", e.Offsets)
			default:
				log.Warn("Unexpected kafka message: %+v", ev)
			}
		case <-k.stopConsuming:
			log.Info("Stopping consumer thread")
			return
		}
	}
}

// tryGetOffset will to query kafka repeatedly for the requested offset and give up after attempts unsuccesfull attempts.
// - if the given offset is <0 (f.e. confluent.OffsetBeginning) then the first & second returned values will be the
//   oldest & newest offset for the given topic and partition.
// - if it is >=0 then the first returned value is the earliest offset whose timestamp is greater than or equal to the
//   given timestamp and the second returned value will be 0
func (k *KafkaMdm) tryGetOffset(topic string, partition int32, offsetI int64, attempts int, sleep time.Duration) (int64, int64, error) {
	offset, err := confluent.NewOffset(offsetI)
	if err != nil {
		return 0, 0, err
	}

	var val1, val2 int64

	attempt := 1
	for {
		if offset == confluent.OffsetBeginning || offset == confluent.OffsetEnd {
			val1, val2, err = k.consumer.QueryWatermarkOffsets(topic, partition, metadataTimeout)
		} else {
			times := []confluent.TopicPartition{{Topic: &topic, Partition: partition, Offset: offset}}
			times, err = k.consumer.OffsetsForTimes(times, metadataTimeout)
			if err == nil {
				if len(times) == 0 {
					err = fmt.Errorf("Got 0 topics returned from broker")
				} else {
					val1 = int64(times[0].Offset)
				}
			}
		}

		if err == nil {
			return val1, val2, err
		}

		if attempt >= attempts {
			break
		}

		log.Warn("kafka-mdm %s", err)
		attempt += 1
		time.Sleep(sleep)
	}

	return 0, 0, fmt.Errorf("failed to get offset %s of partition %s:%d. %s (attempt %d/%d)", offset.String(), topic, partition, err, attempt, attempts)
}

func (k *KafkaMdm) handleMsg(data []byte, partition int32) {
	md := schema.MetricData{}
	_, err := md.UnmarshalMsg(data)
	if err != nil {
		metricsDecodeErr.Inc()
		log.Error(3, "kafka-mdm decode error, skipping message. %s", err)
		return
	}
	metricsPerMessage.ValueUint32(1)
	k.Handler.Process(&md, partition)
}

// Stop will initiate a graceful stop of the Consumer (permanent)
// and block until it stopped.
func (k *KafkaMdm) Stop() {
	log.Info("kafka-mdm: stopping kafka input")
	close(k.stopConsuming)
	k.wg.Wait()
	k.consumer.Close()
}

func (k *KafkaMdm) MaintainPriority() {
	go func() {
		ticker := time.NewTicker(time.Second * 10)
		for {
			select {
			case <-k.stopConsuming:
				return
			case <-ticker.C:
				cluster.Manager.SetPriority(k.lagMonitor.Metric())
			}
		}
	}()
}
