package kafka

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/dgryski/carbonzipper/mlog"
	m "github.com/kanatohodets/carbonsearch/consumer/message"
	"github.com/kanatohodets/carbonsearch/database"
	"github.com/kanatohodets/carbonsearch/util"

	"github.com/Shopify/sarama"
)

var logger mlog.Level

type KafkaConfig struct {
	Offset       string            `yaml:"offset"`
	BrokerList   []string          `yaml:"broker_list"`
	TopicMapping map[string]string `yaml:"topic_mapping"`
}

type KafkaConsumer struct {
	initialOffset     int64
	consumer          sarama.Consumer
	partitionsByTopic map[string][]int32
	topicMapping      map[string]string
	shutdown          chan bool
}

func New(configPath string) (*KafkaConsumer, error) {
	config := &KafkaConfig{}
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

	partitionsByTopic := make(map[string][]int32)
	for topic := range config.TopicMapping {
		//NOTE(btyler) always fetching all partitions
		partitionList, err := c.Partitions(topic)
		if err != nil {
			return nil, err
		}
		partitionsByTopic[topic] = partitionList
	}

	return &KafkaConsumer{
		initialOffset:     initialOffset,
		consumer:          c,
		partitionsByTopic: partitionsByTopic,
		topicMapping:      config.TopicMapping,
		shutdown:          make(chan bool),
	}, nil
}

func (k *KafkaConsumer) Start(wg *sync.WaitGroup, db *database.Database) error {
	for topic, partitionList := range k.partitionsByTopic {
		for _, partition := range partitionList {
			pc, err := k.consumer.ConsumePartition(topic, partition, k.initialOffset)
			if err != nil {
				close(k.shutdown)
				return fmt.Errorf("kafka consumer: Failed to start consumer of topic %s for partition %d: %s", topic, partition, err)
			}

			wg.Add(1)
			go func(pc sarama.PartitionConsumer) {
				defer wg.Done()
				<-k.shutdown
				//TODO(btyler) AsyncClose and wait on pc.Messages/pc.Errors?
				pc.Close()
			}(pc)

			switch k.topicMapping[topic] {
			case "metric":
				go readMetric(pc, db)
			case "tag":
				go readTag(pc, db)
			case "custom":
				go readCustom(pc, db)
			default:
				panic(fmt.Sprintf("what are you even doing? there's no topic mapping for %s in the config file", topic))
			}
		}
	}
	return nil
}

func (k *KafkaConsumer) Stop() error {
	close(k.shutdown)
	if err := k.consumer.Close(); err != nil {
		return err
	}
	return nil
}

func (k *KafkaConsumer) Name() string {
	return "kafka"
}

func readMetric(pc sarama.PartitionConsumer, db *database.Database) {
	for kafkaMsg := range pc.Messages() {
		db.TrackPosition(kafkaMsg.Topic, kafkaMsg.Partition, kafkaMsg.Offset, pc.HighWaterMarkOffset())
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

func readTag(pc sarama.PartitionConsumer, db *database.Database) {
	for kafkaMsg := range pc.Messages() {
		db.TrackPosition(kafkaMsg.Topic, kafkaMsg.Partition, kafkaMsg.Offset, pc.HighWaterMarkOffset())
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

func readCustom(pc sarama.PartitionConsumer, db *database.Database) {
	for kafkaMsg := range pc.Messages() {
		db.TrackPosition(kafkaMsg.Topic, kafkaMsg.Partition, kafkaMsg.Offset, pc.HighWaterMarkOffset())
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
