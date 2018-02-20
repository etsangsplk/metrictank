package notifierKafka

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/util"
	"github.com/raintank/worldping-api/pkg/log"
)

type NotifierKafka struct {
	instance string
	in       chan mdata.SavedChunk
	buf      []mdata.SavedChunk
	wg       sync.WaitGroup
	idx      idx.MetricIndex
	metrics  mdata.Metrics
	bPool    *util.BufferPool
	consumer *confluent.Consumer
	producer *confluent.Producer
	StopChan chan int

	// signal to PartitionConsumers to shutdown
	stopConsuming chan struct{}
}

var currentOffsets map[int32]*int64

func New(instance string, metrics mdata.Metrics, idx idx.MetricIndex) *NotifierKafka {
	consumer, err := confluent.NewConsumer(&config)
	if err != nil {
		log.Fatal(2, "kafka-cluster failed to initialize consumer: %s", err)
	}

	producer, err := confluent.NewProducer(&config)
	if err != nil {
		log.Fatal(2, "kafka-cluster failed to initialize producer: %s", err)
	}

	c := NotifierKafka{
		instance: instance,
		in:       make(chan mdata.SavedChunk),
		idx:      idx,
		metrics:  metrics,
		bPool:    util.NewBufferPool(),
		consumer: consumer,
		producer: producer,

		StopChan:      make(chan int),
		stopConsuming: make(chan struct{}),
	}

	c.start()
	go c.produce()

	return &c
}

func (c *NotifierKafka) start() {
	pre := time.Now()
	processBacklog := new(sync.WaitGroup)

	err := c.startConsumer()
	if err != nil {
		log.Fatal(4, "kafka-cluster: Failed to start consumer: %s", err)
	}
	processBacklog.Add(len(partitions))

	// wait for our backlog to be processed before returning.  This will block metrictank from consuming metrics until
	// we have processed old metricPersist messages. The end result is that we wont overwrite chunks in cassandra that
	// have already been previously written.
	// We don't wait more than backlogProcessTimeout for the backlog to be processed.
	log.Info("kafka-cluster: waiting for metricPersist backlog to be processed.")
	backlogProcessed := make(chan struct{}, 1)
	go func() {
		processBacklog.Wait()
		fmt.Println("backlog processed")
		backlogProcessed <- struct{}{}
	}()

	go c.monitorLag(processBacklog)

	for _ = range partitions {
		go c.consume()
	}

	select {
	case <-time.After(backlogProcessTimeout):
		log.Warn("kafka-cluster: Processing metricPersist backlog has taken too long, giving up lock after %s.", backlogProcessTimeout)
	case <-backlogProcessed:
		log.Info("kafka-cluster: metricPersist backlog processed in %s.", time.Since(pre))
	}
}

func (c *NotifierKafka) startConsumer() error {
	var offset confluent.Offset
	var err error
	var topicPartitions confluent.TopicPartitions
	currentOffsets = make(map[int32]*int64, len(partitions))
	for _, partition := range partitions {
		var currentOffset int64
		switch offsetStr {
		case "oldest":
			currentOffset, _, err = c.tryGetOffset(topic, partition, int64(confluent.OffsetBeginning), 3, time.Second)
			if err != nil {
				return err
			}
		case "newest":
			_, currentOffset, err = c.tryGetOffset(topic, partition, int64(confluent.OffsetEnd), 3, time.Second)
			if err != nil {
				return err
			}
		default:
			currentOffset = time.Now().Add(-1*offsetDuration).UnixNano() / int64(time.Millisecond)
			currentOffset, _, err = c.tryGetOffset(topic, partition, currentOffset, 3, time.Second)
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

		currentOffsets[partition] = &currentOffset
	}

	fmt.Println(fmt.Sprintf("assigning %+v", topicPartitions))
	return c.consumer.Assign(topicPartitions)
}

func (c *NotifierKafka) tryGetOffset(topic string, partition int32, offsetI int64, attempts int, sleep time.Duration) (int64, int64, error) {
	offset, err := confluent.NewOffset(offsetI)
	if err != nil {
		return 0, 0, err
	}

	var val1, val2 int64

	attempt := 1
	for {
		if offset == confluent.OffsetBeginning || offset == confluent.OffsetEnd {
			val1, val2, err = c.consumer.QueryWatermarkOffsets(topic, partition, metadataTimeout)
		} else {
			times := []confluent.TopicPartition{{Topic: &topic, Partition: partition, Offset: offset}}
			times, err = c.consumer.OffsetsForTimes(times, metadataTimeout)
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

func (c *NotifierKafka) monitorLag(processBacklog *sync.WaitGroup) {
	completed := make(map[int32]bool, len(partitions))
	for _, partition := range partitions {
		completed[partition] = false
	}

	storeOffsets := func(ts time.Time) {
		for partition := range currentOffsets {
			currentOffset := atomic.LoadInt64(currentOffsets[partition])
			partitionOffset[partition].Set(int(currentOffset))
			if !completed[partition] && currentOffset >= bootTimeOffsets[partition]-1 {
				processBacklog.Done()
				completed[partition] = true
				delete(bootTimeOffsets, partition)
				if len(bootTimeOffsets) == 0 {
					bootTimeOffsets = nil
				}
			}

			_, newest, err := c.consumer.QueryWatermarkOffsets(topic, partition, metadataTimeout)
			if err == nil {
				partitionLogSize[partition].Set(int(newest))
			}

			if currentOffset < 0 {
				// we have not yet consumed any messages.
				continue
			}

			partitionOffset[partition].Set(int(currentOffset))
			if err == nil {
				partitionLag[partition].Set(int(newest - currentOffset))
			}
		}
	}

	ticker := time.NewTicker(offsetCommitInterval)
	for {
		select {
		case ts := <-ticker.C:
			storeOffsets(ts)
		case <-c.stopConsuming:
			storeOffsets(time.Now())
			return
		}
	}
}

func (c *NotifierKafka) consume() {
	c.wg.Add(1)
	defer c.wg.Done()

	events := c.consumer.Events()
	for {
		select {
		case ev := <-events:
			switch e := ev.(type) {
			case confluent.AssignedPartitions:
				c.consumer.Assign(e.Partitions)
				log.Info("notifier-kafka: Assigned partitions: %+v", e)
			case confluent.RevokedPartitions:
				c.consumer.Unassign()
				log.Info("notifier-kafka: Revoked partitions: %+v", e)
			case confluent.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case *confluent.Message:
				if mdata.LogLevel < 2 {
					tp := e.TopicPartition
					log.Debug("notifier-kafka: received message: Topic %s, Partition: %d, Offset: %d, Key: %x", tp.Topic, tp.Partition, tp.Offset, e.Key)
				}
				mdata.Handle(c.metrics, e.Value, c.idx)
			case *confluent.Error:
				log.Error(3, "notifier-kafka: kafka consumer error: %s", e.String())
				return
			}
		case <-c.stopConsuming:
			log.Info("notifier-kafka: consumer ended.")
			return
		}
	}
}

// Stop will initiate a graceful stop of the Consumer (permanent)
//
// NOTE: receive on StopChan to block until this process completes
func (c *NotifierKafka) Stop() {
	// closes notifications and messages channels, amongst others
	close(c.stopConsuming)
	c.producer.Close()

	go func() {
		c.wg.Wait()
		close(c.StopChan)
	}()
}

func (c *NotifierKafka) Send(sc mdata.SavedChunk) {
	c.in <- sc
}

func (c *NotifierKafka) produce() {
	ticker := time.NewTicker(time.Second)
	max := 5000
	for {
		select {
		case chunk := <-c.in:
			c.buf = append(c.buf, chunk)
			if len(c.buf) == max {
				c.flush()
			}
		case <-ticker.C:
			c.flush()
		}
	}
}

// flush makes sure the batch gets sent, asynchronously.
func (c *NotifierKafka) flush() {
	if len(c.buf) == 0 {
		return
	}

	hasher := fnv.New32a()

	// In order to correctly route the saveMessages to the correct partition,
	// we cant send them in batches anymore.
	payload := make([]*confluent.Message, 0, len(c.buf))
	var pMsg mdata.PersistMessageBatch
	for i, msg := range c.buf {
		def, ok := c.idx.Get(strings.SplitN(msg.Key, "_", 2)[0])
		if !ok {
			log.Error(3, "kafka-cluster: failed to lookup metricDef with id %s", msg.Key)
			continue
		}
		buf := bytes.NewBuffer(c.bPool.Get())
		binary.Write(buf, binary.LittleEndian, uint8(mdata.PersistMessageBatchV1))
		encoder := json.NewEncoder(buf)
		pMsg = mdata.PersistMessageBatch{Instance: c.instance, SavedChunks: c.buf[i : i+1]}
		err := encoder.Encode(&pMsg)
		if err != nil {
			log.Fatal(4, "kafka-cluster failed to marshal persistMessage to json.")
		}
		messagesSize.Value(buf.Len())
		key := c.bPool.Get()
		key, err = partitioner.GetPartitionKey(&def, key)
		if err != nil {
			log.Fatal(4, "Unable to get partitionKey for metricDef with id %s. %s", def.Id, err)
		}

		hasher.Reset()
		_, err = hasher.Write(key)
		partition := int32(hasher.Sum32()) % int32(len(partitions))
		if partition < 0 {
			partition = -partition
		}

		kafkaMsg := &confluent.Message{
			TopicPartition: confluent.TopicPartition{
				Topic: &topic, Partition: partition,
			},
			Value: []byte(buf.Bytes()),
			Key:   []byte(key),
		}
		payload = append(payload, kafkaMsg)
	}

	c.buf = nil

	go func() {
		log.Debug("kafka-cluster sending %d batch metricPersist messages", len(payload))
		producerCh := c.producer.ProduceChannel()
		for _, msg := range payload {
			producerCh <- msg
		}
		sent := 0

	EVENTS:
		for e := range c.producer.Events() {
			switch ev := e.(type) {
			case *confluent.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition.Error)
					time.Sleep(time.Second)
					ev.TopicPartition.Error = nil
					producerCh <- ev
				} else {
					sent++
				}
				if sent == len(payload) {
					break EVENTS
					return
				}
			default:
				fmt.Printf("Ignored unexpected event: %s\n", ev)
			}
		}

		messagesPublished.Add(sent)

		// put our buffers back in the bufferPool
		for _, msg := range payload {
			c.bPool.Put(msg.Key)
			c.bPool.Put(msg.Value)
		}
	}()
}
