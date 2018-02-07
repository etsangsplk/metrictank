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
	"github.com/twinj/uuid"
	"gopkg.in/raintank/schema.v1"
)

// metric input.kafka-mdm.metrics_per_message is how many metrics per message were seen.
var metricsPerMessage = stats.NewMeter32("input.kafka-mdm.metrics_per_message", false)

// metric input.kafka-mdm.metrics_decode_err is a count of times an input message failed to parse
var metricsDecodeErr = stats.NewCounter32("input.kafka-mdm.metrics_decode_err")

const OffsetUndefined = -100

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

var LogLevel int
var Enabled bool
var brokerStr string
var topicStr string
var topics []string
var partitionStr string
var partitions []int32
var offsetStr string
var DataDir string
var channelBufferSize int
var consumerFetchMin int
var consumerFetchDefault int
var consumerMaxWaitTime time.Duration
var metadataTimeout int
var metadataBackoffTime int
var metadataRetries int
var currentOffsets map[string]map[int32]*int64
var netMaxOpenRequests int
var offsetMgr *kafka.OffsetMgr
var offsetDuration time.Duration
var offsetCommitInterval time.Duration
var partitionOffset map[int32]*stats.Gauge64
var partitionLogSize map[int32]*stats.Gauge64
var partitionLag map[int32]*stats.Gauge64

func ConfigSetup() {
	inKafkaMdm := flag.NewFlagSet("kafka-mdm-in", flag.ExitOnError)
	inKafkaMdm.BoolVar(&Enabled, "enabled", false, "")
	inKafkaMdm.StringVar(&brokerStr, "brokers", "kafka:9092", "tcp address for kafka (may be be given multiple times as a comma-separated list)")
	inKafkaMdm.StringVar(&topicStr, "topics", "mdm", "kafka topic (may be given multiple times as a comma-separated list)")
	inKafkaMdm.StringVar(&offsetStr, "offset", "last", "Set the offset to start consuming from. Can be one of newest, oldest,last or a time duration")
	inKafkaMdm.StringVar(&partitionStr, "partitions", "*", "kafka partitions to consume. use '*' or a comma separated list of id's")
	inKafkaMdm.DurationVar(&offsetCommitInterval, "offset-commit-interval", time.Second*5, "Interval at which offsets should be saved.")
	inKafkaMdm.StringVar(&DataDir, "data-dir", "", "Directory to store partition offsets index")
	inKafkaMdm.IntVar(&channelBufferSize, "channel-buffer-size", 1000000, "Maximum number of messages allowed on the producer queue")
	inKafkaMdm.IntVar(&consumerFetchMin, "consumer-fetch-min", 1, "Minimum number of bytes the broker responds with. If fetch.wait.max.ms expires the accumulated data will be sent to the client regardless of this setting")
	inKafkaMdm.DurationVar(&consumerMaxWaitTime, "consumer-max-wait-time", time.Second, "Maximum time the broker may wait to fill the response with fetch.min.bytes")
	inKafkaMdm.IntVar(&metadataBackoffTime, "metadata-backoff-time", 500, "Time to wait between attempts to fetch metadata in ms")
	inKafkaMdm.IntVar(&metadataTimeout, "consumer-metadata-timeout-ms", 10000, "Maximum time to wait for the broker to send its metadata in ms")
	inKafkaMdm.IntVar(&metadataRetries, "metadata-retries", 5, "Number of retries to fetch metadata in case of failure")
	inKafkaMdm.IntVar(&netMaxOpenRequests, "net-max-open-requests", 100, "Maximum number of in-flight requests per broker connection. This is a generic property applied to all broker communication, however it is primarily relevant to produce requests.")
	globalconf.Register("kafka-mdm-in", inKafkaMdm)
}

func getConfig() *confluent.ConfigMap {
	return &confluent.ConfigMap{
		// according to this we need to generated a uuid: https://github.com/edenhill/librdkafka/issues/1210
		"group.id": uuid.NewV4().String(),
		//"client.id":                             "abc-mdm",
		"bootstrap.servers":                     brokerStr,
		"session.timeout.ms":                    6000,
		"queue.buffering.max.messages":          channelBufferSize,
		"fetch.min.bytes":                       consumerFetchMin,
		"fetch.wait.max.ms":                     consumerMaxWaitTime,
		"max.in.flight.requests.per.connection": netMaxOpenRequests,
		"go.application.rebalance.enable":       true,
		"go.events.channel.enable":              true,
	}
}

func ConfigProcess(instance string) {
	if !Enabled {
		return
	}

	if offsetCommitInterval == 0 {
		log.Fatal(4, "kafkamdm: offset-commit-interval must be greater then 0")
	}
	if consumerMaxWaitTime == 0 {
		log.Fatal(4, "kafkamdm: consumer-max-wait-time must be greater then 0")
	}
	var err error
	switch offsetStr {
	case "last":
	case "oldest":
	case "newest":
	default:
		offsetDuration, err = time.ParseDuration(offsetStr)
		if err != nil {
			log.Fatal(4, "kafkamdm: invalid offest format. %s", err)
		}
	}

	offsetMgr, err = kafka.NewOffsetMgr(DataDir)
	if err != nil {
		log.Fatal(4, "kafka-mdm couldnt create offsetMgr. %s", err)
	}
	topics = strings.Split(topicStr, ",")

	fmt.Println("configuring consumer")
	client, err := confluent.NewConsumer(getConfig())
	if err != nil {
		log.Fatal(4, "failed to initialize kafka client. %s", err)
	}

	availPartsByTopic, err := kafka.GetPartitions(client, topics, metadataRetries, metadataBackoffTime, metadataTimeout)
	if err != nil {
		log.Fatal(4, "kafka-mdm: %s", err.Error())
	}

	var availParts []int32
	for _, topicPartitions := range availPartsByTopic {
		for _, part := range topicPartitions {
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
	consumer, err := confluent.NewConsumer(getConfig())
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
	var currentOffset int64
	var err error
	var topicPartitions confluent.TopicPartitions
	for _, topic := range topics {
		currentOffsets[topic] = make(map[int32]*int64)
		for _, partition := range partitions {
			switch offsetStr {
			case "oldest":
				offset = confluent.OffsetBeginning
			case "newest":
				offset = confluent.OffsetEnd
			case "last":
				currentOffset, err := offsetMgr.Last(topic, partition)
				if err != nil {
					return err
				}
				offset, err = confluent.NewOffset(currentOffset)
				if err != nil {
					return err
				}
			default:
				currentOffset = time.Now().Add(-1*offsetDuration).UnixNano() / int64(time.Millisecond)
				currentOffset, _, err = k.tryGetOffset(topic, partition, currentOffset, 7, time.Second)
				if err != nil {
					return err
				}
				offset = confluent.Offset(currentOffset)
			}

			topicPartitions = append(topicPartitions, confluent.TopicPartition{
				Topic:     &topic,
				Partition: partition,
				Offset:    offset,
			})

			newest, oldest, err := k.tryGetOffset(topic, partition, int64(confluent.OffsetBeginning), 3, 0)
			if err != nil {
				return err
			}

			if offset == confluent.OffsetEnd {
				currentOffset = newest
			} else if offset == confluent.OffsetBeginning {
				currentOffset = oldest
			} else {
				currentOffset = int64(offset)
			}

			currentOffsets[topic][partition] = &currentOffset
		}
	}

	fmt.Println(fmt.Sprintf("assigning %+v", topicPartitions))
	return k.consumer.Assign(topicPartitions)
}

func (k *KafkaMdm) Start(handler input.Handler, fatal chan struct{}) error {
	k.Handler = handler
	k.fatal = fatal

	fmt.Println("starting kafka input")
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
				if err := offsetMgr.Commit(topic, partition, offset); err != nil {
					log.Error(3, "kafka-mdm failed to commit offset for %s:%d, %s", topic, partition, err)
				}
				k.lagMonitor.StoreOffset(partition, offset, ts)
				newest, _, err := k.tryGetOffset(topic, partition, int64(confluent.OffsetEnd), 1, 0)
				partitionOffset[partition].Set(int(offset))
				if err != nil {
					partitionLogSize[partition].Set(int(newest))
					lag := int(newest - offset)
					partitionLag[partition].Set(lag)
				}
			}
		}
	}

	ticker := time.NewTicker(time.Second * 5)
	storeOffsets(time.Now())
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
			fmt.Println(fmt.Sprintf("packet: %+v", ev))
			switch e := ev.(type) {
			case confluent.AssignedPartitions:
				k.consumer.Assign(e.Partitions)
			case confluent.RevokedPartitions:
				fmt.Println(fmt.Sprintf("%% %v\n", e))
				k.consumer.Unassign()
			case confluent.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case *confluent.Message:
				topic = *e.TopicPartition.Topic
				partition = e.TopicPartition.Partition
				offset = int64(e.TopicPartition.Offset)
				if LogLevel < 2 {
					log.Debug("kafka-mdm received message: Topic %s, Partition: %d, Offset: %d, Key: %x", topic, partition, offset, e.Key)
				}

				k.handleMsg(e.Value, partition)

				if currentTopicOffsets, ok = currentOffsets[topic]; !ok {
					log.Error(3, "kafka-mdm received message of unexpected topic: %s", topic)
					continue
				}

				if offsetPtr, ok = currentTopicOffsets[partition]; !ok || offsetPtr == nil {
					log.Error(3, "kafka-mdm received message of unexpected partition: %s:%d", topic, partition)
					continue
				}

				atomic.StoreInt64(offsetPtr, offset)
			case *confluent.Error:
				fmt.Println(fmt.Sprintf("error: %+v", e))
				log.Error(3, "kafka-mdm: kafka consumer error: %s", e.String())
				return
			default:
				fmt.Println(fmt.Sprintf("weird: %+v", ev))
			}

		case <-k.stopConsuming:
			fmt.Println("stopping")
			return
		}
	}
}

// tryGetOffset will to query kafka repeatedly for the requested offset and give up after attempts unsuccesfull attempts.
// - if the given offset is <0 (f.e. confluent.OffsetStored) then the first & second returned values will be the
//   oldest & newest offset for the given topic and partition.
// - if it is >=0 then the first returned value is the earliest offset whose timestamp is greater than or equal to the
//   given timestamp and the second returned value can be ignored.
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
			break
		}

		err = fmt.Errorf("failed to get offset %s of partition %s:%d. %s (attempt %d/%d)", offset.String(), topic, partition, err, attempt, attempts)
		if attempt == attempts {
			break
		}

		log.Warn("kafka-mdm %s", err)
		attempt += 1
		time.Sleep(sleep)
	}

	return val1, val2, err
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
	fmt.Println("stopping kafka input")
	// closes notifications and messages channels, amongst others
	close(k.stopConsuming)
	k.wg.Wait()
	k.consumer.Close()
	offsetMgr.Close()
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
