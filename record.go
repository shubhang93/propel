package epcons

import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

type Records []*kafka.Message
type Record *kafka.Message
type TopicPart struct {
	Topic     string
	Partition int32
	Offset    int64
}

func TopicPartFor(recs Records) TopicPart {
	rec := recs[0]
	return TopicPart{
		Topic:     *rec.TopicPartition.Topic,
		Partition: rec.TopicPartition.Partition,
		Offset:    int64(rec.TopicPartition.Offset),
	}
}

func LastOffset(recs Records) TopicPart {
	rec := recs[len(recs)-1]
	return TopicPart{
		Topic:     *rec.TopicPartition.Topic,
		Partition: rec.TopicPartition.Partition,
		Offset:    int64(rec.TopicPartition.Offset),
	}
}
