package propel

import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

type confluentConsumer interface {
	Poll(timeout int) kafka.Event
	Subscribe(topics string, cb kafka.RebalanceCb) error
	StoreMessage(m *kafka.Message) (storedOffsets []kafka.TopicPartition, err error)
	Close() error
	Pause([]kafka.TopicPartition) error
	Resume([]kafka.TopicPartition) error
	Logs() chan kafka.LogEvent
}
