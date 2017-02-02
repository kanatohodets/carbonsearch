package kafka

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/dgryski/carbonzipper/mlog"
	m "github.com/kanatohodets/carbonsearch/consumer/message"
	"github.com/kanatohodets/carbonsearch/database"
	"github.com/kanatohodets/carbonsearch/util"

	"github.com/Shopify/sarama"
)

var logger mlog.Level

// Config holds the contents of kafka.yaml
type Config struct {
	WarmThreshold float32           `yaml:"warm_threshold"`
	Offset        string            `yaml:"offset"`
	BrokerList    []string          `yaml:"broker_list"`
	TopicMapping  map[string]string `yaml:"topic_mapping"`
}

// Consumer represents a carbonsearch kafka data source: it subscribes to a set
// of topics in kafka, and uses the messages from those topics to populate the
// carbonsearch Database.
type Consumer struct {
	stats             *util.Stats
	warmThreshold     float32
	initialOffset     int64
	consumer          sarama.Consumer
	partitionsByTopic map[string][]int32
	topicMapping      map[string]string
	shutdown          chan bool

	progress    map[string]map[int32]float32
	progressMut sync.Mutex
}

// New reads the kafka consumer config at the given path, and returns an initialized consumer, ready to Start.
func New(configPath string, stats *util.Stats) (*Consumer, error) {
	config := &Config{}
	err := util.ReadConfig(configPath, config)
	if err != nil {
		return nil, err
	}

	var initialOffset int64
	switch config.Offset {
	case "oldest":
		initialOffset = sarama.OffsetOldest
	case "newest":
		initialOffset = sarama.OffsetNewest
	default:
		return nil, fmt.Errorf("kafka consumer: offset should be `oldest` or `newest`")
	}

	c, err := sarama.NewConsumer(config.BrokerList, nil)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: Failed to create a consumer: %s", err)
	}

	if config.WarmThreshold > 0.01 {
		logger.Logf("kafka consumer: warm threshold set to %v", config.WarmThreshold)
	} else {
		logger.Logf("kafka consumer: warning, warm_threshold is very low or unset (value: %v). Carbonsearch may start serving requests before much data has been indexed from the kafka topics", config.WarmThreshold)
	}

	// map[topic]map[partition]progress%
	progress := map[string]map[int32]float32{}
	partitionsByTopic := make(map[string][]int32)
	for topic := range config.TopicMapping {
		//NOTE(btyler) always fetching all partitions
		partitionList, err := c.Partitions(topic)
		if err != nil {
			return nil, err
		}
		partitionsByTopic[topic] = partitionList

		progress[topic] = map[int32]float32{}
		for _, partition := range partitionList {
			progress[topic][partition] = 0
		}
	}

	return &Consumer{
		stats:             stats,
		warmThreshold:     config.WarmThreshold,
		initialOffset:     initialOffset,
		consumer:          c,
		partitionsByTopic: partitionsByTopic,
		topicMapping:      config.TopicMapping,
		shutdown:          make(chan bool),

		progress:    progress,
		progressMut: sync.Mutex{},
	}, nil
}

func (k *Consumer) WaitUntilWarm(wg *sync.WaitGroup) error {
	warmTopics := map[string]bool{}
	for {
		k.progressMut.Lock()
		for topic, partitionProgress := range k.progress {
			var progressSum float32 = 0
			for _, progress := range partitionProgress {
				progressSum += progress
			}

			avgPartitionProgress := progressSum / float32(len(partitionProgress))
			_, ok := warmTopics[topic]
			if !ok {
				if avgPartitionProgress >= k.warmThreshold {
					logger.Logf("kafka consumer: topic %v now considered warm (%.2f%% meets or exceeds threshold of %.2f%%)", topic, avgPartitionProgress*100, k.warmThreshold*100)
					warmTopics[topic] = true
				} else {
					logger.Logf("kafka consumer: topic %v %.2f%% warm (threshold is %.2f%%)", topic, avgPartitionProgress*100, k.warmThreshold*100)
				}
			}
		}
		k.progressMut.Unlock()
		if len(warmTopics) == len(k.topicMapping) {
			logger.Logf("kafka consumer: all topics reached warmup threshold (%v)", k.warmThreshold)
			wg.Done()
			return nil
		}
		time.Sleep(10 * time.Second)
	}
}

// Start begins reading from the configured kafka topics, inserting messages into Database as they're consumed.
func (k *Consumer) Start(db *database.Database) error {
	for topic, partitionList := range k.partitionsByTopic {
		for _, partition := range partitionList {
			pc, err := k.consumer.ConsumePartition(topic, partition, k.initialOffset)
			if err != nil {
				close(k.shutdown)
				return fmt.Errorf("kafka consumer: Failed to start consumer of topic %s for partition %d: %s", topic, partition, err)
			}

			go func(pc sarama.PartitionConsumer) {
				<-k.shutdown
				//TODO(btyler) AsyncClose and wait on pc.Messages/pc.Errors?
				err := pc.Close()
				if err != nil {
					logger.Logf("kafka consumer: Failed to close partition %v: %v", partition, err)
				}
			}(pc)

			k.trackPosition(topic, partition, 0, 0, pc.HighWaterMarkOffset())
			switch k.topicMapping[topic] {
			case "metric":
				go k.readMetric(pc, db)
			case "tag":
				go k.readTag(pc, db)
			case "custom":
				go k.readCustom(pc, db)
			default:
				panic(fmt.Sprintf("There's no topic mapping for %s in the kafka consumer config file. Topic mappings can be 'metric', 'tag', or 'custom'", topic))
			}
		}
	}
	return nil
}

// Stop halts the consumer. Note: calling Stop and then later calling Start on the same consumer is undefined.
func (k *Consumer) Stop() error {
	close(k.shutdown)
	return k.consumer.Close()
}

// Name returns the name of the consumer
func (k *Consumer) Name() string {
	return "kafka"
}

func (k *Consumer) readMetric(pc sarama.PartitionConsumer, db *database.Database) {
	var initialOffset int64 = sarama.OffsetOldest
	for kafkaMsg := range pc.Messages() {
		if initialOffset == sarama.OffsetOldest {
			initialOffset = kafkaMsg.Offset
		}
		k.trackPosition(kafkaMsg.Topic, kafkaMsg.Partition, initialOffset, kafkaMsg.Offset, pc.HighWaterMarkOffset())
		var msg *m.KeyMetric
		if err := json.Unmarshal(kafkaMsg.Value, &msg); err != nil {
			logger.Logln("ermg decoding problem :( ", err)
			continue
		}
		// TODO(btyler): fix malformed messages and let this get caught by database validation
		if msg.Value != "" && len(msg.Metrics) != 0 {
			err := db.InsertMetrics(msg)
			if err != nil {
				logger.Logf("kafka consumer: could not insert metrics: %v", err)
			}
		}
	}
}

func (k *Consumer) readTag(pc sarama.PartitionConsumer, db *database.Database) {
	var initialOffset int64 = sarama.OffsetOldest
	for kafkaMsg := range pc.Messages() {
		if initialOffset == sarama.OffsetOldest {
			initialOffset = kafkaMsg.Offset
		}
		k.trackPosition(kafkaMsg.Topic, kafkaMsg.Partition, initialOffset, kafkaMsg.Offset, pc.HighWaterMarkOffset())
		var msg *m.KeyTag
		if err := json.Unmarshal(kafkaMsg.Value, &msg); err != nil {
			logger.Logln("ermg decoding problem :( ", err)
			continue
		}

		// TODO(btyler): fix malformed messages and let this get caught by database validation
		if msg.Value != "" && len(msg.Tags) != 0 {
			err := db.InsertTags(msg)
			if err != nil {
				logger.Logf("kafka consumer: could not insert tags: %v", err)
			}
		}
	}
}

func (k *Consumer) readCustom(pc sarama.PartitionConsumer, db *database.Database) {
	var initialOffset int64 = sarama.OffsetOldest
	for kafkaMsg := range pc.Messages() {
		if initialOffset == sarama.OffsetOldest {
			initialOffset = kafkaMsg.Offset
		}
		k.trackPosition(kafkaMsg.Topic, kafkaMsg.Partition, initialOffset, kafkaMsg.Offset, pc.HighWaterMarkOffset())
		var msg *m.TagMetric
		if err := json.Unmarshal(kafkaMsg.Value, &msg); err != nil {
			logger.Logln("ermg decoding problem :( ", err)
			continue
		}

		// TODO(btyler): fix malformed messages and let this get caught by database validation
		if len(msg.Tags) != 0 && len(msg.Metrics) != 0 {
			err := db.InsertCustom(msg)
			if err != nil {
				logger.Logf("kafka consumer: could not insert custom associations: %v", err)
			}
		}
	}
}

// trackPosition allows kafka consumers to report their `cur` position
func (k *Consumer) trackPosition(topic string, p int32, initial, cur, highWaterMark int64) {
	scaledCurrentOffset := cur - initial
	scaledHighOffset := highWaterMark - initial
	k.progressMut.Lock()
	if highWaterMark == 0 {
		k.progress[topic][p] = 1
	} else {
		k.progress[topic][p] = float32(scaledCurrentOffset) / float32(scaledHighOffset)
	}
	k.progressMut.Unlock()

	k.stats.Progress.Set(fmt.Sprintf("%s-%d-relative-current", topic, p), util.ExpInt(scaledCurrentOffset))
	k.stats.Progress.Set(fmt.Sprintf("%s-%d-relative-newest", topic, p), util.ExpInt(scaledHighOffset))
	k.stats.Progress.Set(fmt.Sprintf("%s-%d-absolute-current", topic, p), util.ExpInt(cur))
	k.stats.Progress.Set(fmt.Sprintf("%s-%d-absolute-newest", topic, p), util.ExpInt(highWaterMark))
}
