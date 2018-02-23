package notifierKafka

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	part "github.com/grafana/metrictank/cluster/partitioner"
	"github.com/grafana/metrictank/kafka"
	"github.com/grafana/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
	"github.com/twinj/uuid"
)

var Enabled bool
var brokerStr string
var topic string
var offsetStr string
var dataDir string
var config confluent.ConfigMap
var offsetDuration time.Duration
var offsetCommitInterval time.Duration
var partitionStr string
var partitions []int32
var partitioner *part.Kafka
var partitionScheme string
var bootTimeOffsets map[int32]int64
var backlogProcessTimeout time.Duration
var backlogProcessTimeoutStr string
var partitionOffset map[int32]*stats.Gauge64
var partitionLogSize map[int32]*stats.Gauge64
var partitionLag map[int32]*stats.Gauge64
var metadataTimeout int
var metadataBackoffTime int
var metadataRetries int

// metric cluster.notifier.kafka.messages-published is a counter of messages published to the kafka cluster notifier
var messagesPublished = stats.NewCounter32("cluster.notifier.kafka.messages-published")

// metric cluster.notifier.kafka.message_size is the sizes seen of messages through the kafka cluster notifier
var messagesSize = stats.NewMeter32("cluster.notifier.kafka.message_size", false)

func init() {
	fs := flag.NewFlagSet("kafka-cluster", flag.ExitOnError)
	fs.BoolVar(&Enabled, "enabled", false, "")
	fs.StringVar(&brokerStr, "brokers", "kafka:9092", "tcp address for kafka (may be given multiple times as comma separated list)")
	fs.StringVar(&topic, "topic", "metricpersist", "kafka topic")
	fs.StringVar(&partitionStr, "partitions", "*", "kafka partitions to consume. use '*' or a comma separated list of id's. This should match the partitions used for kafka-mdm-in")
	fs.StringVar(&partitionScheme, "partition-scheme", "bySeries", "method used for partitioning metrics. This should match the settings of tsdb-gw. (byOrg|bySeries)")
	fs.StringVar(&offsetStr, "offset", "oldest", "Set the offset to start consuming from. Can be one of newest, oldest or a time duration")
	fs.StringVar(&dataDir, "data-dir", "", "Directory to store partition offsets index")
	fs.DurationVar(&offsetCommitInterval, "offset-commit-interval", time.Second*5, "Interval at which offsets should be saved.")
	fs.StringVar(&backlogProcessTimeoutStr, "backlog-process-timeout", "60s", "Maximum time backlog processing can block during metrictank startup.")
	fs.IntVar(&metadataBackoffTime, "metadata-backoff-time", 500, "Time to wait between attempts to fetch metadata in ms")
	fs.IntVar(&metadataTimeout, "consumer-metadata-timeout-ms", 10000, "Maximum time to wait for the broker to send its metadata in ms")
	fs.IntVar(&metadataRetries, "metadata-retries", 5, "Number of retries to fetch metadata in case of failure")
	globalconf.Register("kafka-cluster", fs)
}

func ConfigProcess(instance string) {
	if !Enabled {
		return
	}
	var err error
	switch offsetStr {
	case "oldest":
	case "newest":
	default:
		offsetDuration, err = time.ParseDuration(offsetStr)
		if err != nil {
			log.Fatal(4, "kafka-cluster: invalid offest format. %s", err)
		}
	}

	config = confluent.ConfigMap{}
	config.SetKey("client.id", instance+"-cluster")
	config.SetKey("bootstrap.servers", brokerStr)
	config.SetKey("compression.codec", "snappy")
	config.SetKey("retries", 10)
	config.SetKey("acks", "all")
	// according to this we need to generate a uuid: https://github.com/edenhill/librdkafka/issues/1210
	config.SetKey("group.id", uuid.NewV4().String())

	backlogProcessTimeout, err = time.ParseDuration(backlogProcessTimeoutStr)
	if err != nil {
		log.Fatal(4, "kafka-cluster: unable to parse backlog-process-timeout. %s", err)
	}

	partitioner, err = part.NewKafka(partitionScheme)
	if err != nil {
		log.Fatal(4, "kafka-cluster: failed to initialize partitioner. %s", err)
	}

	if partitionStr != "*" {
		parts := strings.Split(partitionStr, ",")
		for _, part := range parts {
			i, err := strconv.Atoi(part)
			if err != nil {
				log.Fatal(4, "kafka-cluster: could not parse partition %q. partitions must be '*' or a comma separated list of id's", part)
			}
			partitions = append(partitions, int32(i))
		}
	}
	// validate our partitions
	client, err := confluent.NewConsumer(&config)
	if err != nil {
		log.Fatal(4, "failed to initialize kafka client. %s", err)
	}
	defer client.Close()

	availPartsByTopic, err := kafka.GetPartitions(client, []string{topic}, metadataRetries, metadataBackoffTime, metadataTimeout)
	if err != nil {
		log.Fatal(4, "kafka-mdm: %s", err.Error())
	}

	var availParts []int32
	for _, part := range availPartsByTopic[topic] {
		availParts = append(availParts, part)
	}

	if err != nil {
		log.Fatal(4, "kafka-cluster: %s", err.Error())
	}
	if partitionStr == "*" {
		partitions = availParts
	} else {
		missing := kafka.DiffPartitions(partitions, availParts)
		if len(missing) > 0 {
			log.Fatal(4, "kafka-cluster: configured partitions not in list of available partitions. missing %v", missing)
		}
	}

	// initialize our offset metrics
	partitionOffset = make(map[int32]*stats.Gauge64)
	partitionLogSize = make(map[int32]*stats.Gauge64)
	partitionLag = make(map[int32]*stats.Gauge64)

	// get the "newest" offset for all partitions.
	// when booting up, we will delay consuming metrics until we have
	// caught up to these offsets.
	bootTimeOffsets = make(map[int32]int64)
	for _, part := range partitions {
		_, offset, err := client.QueryWatermarkOffsets(topic, part, metadataTimeout)
		if err != nil {
			log.Fatal(4, "kakfa-cluster: failed to get newest offset for topic %s part %d: %s", topic, part, err)
		}
		bootTimeOffsets[part] = offset
		partitionOffset[part] = stats.NewGauge64(fmt.Sprintf("cluster.notifier.kafka.partition.%d.offset", part))
		partitionLogSize[part] = stats.NewGauge64(fmt.Sprintf("cluster.notifier.kafka.partition.%d.log_size", part))
		partitionLag[part] = stats.NewGauge64(fmt.Sprintf("cluster.notifier.kafka.partition.%d.lag", part))
	}
	//fmt.Println(fmt.Sprintf("got bootTimeOffsets: %+v", bootTimeOffsets))
	log.Info("kafka-cluster: consuming from partitions %v", partitions)
}
