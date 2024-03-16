package propel

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

type MockConsumer struct {
	PollFunc         func(timeout int) kafka.Event
	SubscribeFunc    func(topics string, cb kafka.RebalanceCb) error
	StoreMessageFunc func(m *kafka.Message) (storedOffsets []kafka.TopicPartition, err error)
	LogsFunc         func() chan kafka.LogEvent
	PauseFunc        func([]kafka.TopicPartition) error
	ResumeFunc       func([]kafka.TopicPartition) error
	CloseFunc        func() error
}

func (m *MockConsumer) Poll(timeout int) kafka.Event {
	if m.PollFunc == nil {
		return nil
	}
	return m.PollFunc(timeout)
}

func (m *MockConsumer) Subscribe(topics string, cb kafka.RebalanceCb) error {
	if m.StoreMessageFunc == nil {
		return nil
	}
	return m.SubscribeFunc(topics, cb)
}

func (m *MockConsumer) StoreMessage(msg *kafka.Message) (storedOffsets []kafka.TopicPartition, err error) {
	if m.StoreMessageFunc == nil {
		return []kafka.TopicPartition{}, nil
	}
	return m.StoreMessageFunc(msg)
}

func (m *MockConsumer) Close() error {
	if m.CloseFunc == nil {
		return nil
	}
	return m.CloseFunc()
}

func (m *MockConsumer) Logs() chan kafka.LogEvent {
	return m.LogsFunc()
}

func (m *MockConsumer) Pause(tps []kafka.TopicPartition) error {
	if m.PauseFunc == nil {
		return nil
	}
	return m.PauseFunc(tps)
}

func (m *MockConsumer) Resume(tps []kafka.TopicPartition) error {
	if m.ResumeFunc == nil {
		return nil
	}
	return m.ResumeFunc(tps)
}

func TestPartitionConsumer_pollBatch(t *testing.T) {
	var offset int64
	mc := MockConsumer{
		PollFunc: func(timeout int) kafka.Event {
			if offset > 150 {
				return nil
			}
			offset++
			return &kafka.Message{TopicPartition: kafka.TopicPartition{Offset: kafka.Offset(offset)}}
		},
		SubscribeFunc: func(topics string, cb kafka.RebalanceCb) error {
			return nil
		},
		StoreMessageFunc: func(m *kafka.Message) (storedOffsets []kafka.TopicPartition, err error) {
			return nil, err
		},
		CloseFunc: func() error {
			return nil
		},
	}

	pc := PartitionConsumer{
		c:      &mc,
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})),
	}

	batch := make([]*kafka.Message, 100)
	n, err := pc.pollBatch(context.Background(), 100, batch)
	if err != nil {
		t.Errorf("expected error nil got %v\n", err)
	}
	if len(batch) != n {
		t.Errorf("expected %d got %d\n", n, len(batch))
	}
	t.Logf("%v\n", batch)

	n, err = pc.pollBatch(context.Background(), 100, batch)
	if len(batch[:n]) != n {
		t.Errorf("expected %d got %d\n", n, len(batch[:n]))
	}
	t.Logf("%v\n", batch[:n])

}

func TestPartitionConsumer_Run(t *testing.T) {

	consumerTimeout := time.Duration(0)
	timeoutStr := os.Getenv("CONSUMER_TIMEOUT_MS")
	if timeoutStr != "" {
		parsed, err := time.ParseDuration(timeoutStr)
		if err != nil {
			t.Error(err)
			return
		}
		consumerTimeout = parsed
	} else {
		consumerTimeout = 15000 * time.Millisecond
	}

	t.Logf("using consumer timeout of %s\n", consumerTimeout)
	mockCluster, err := kafka.NewMockCluster(3)
	if err != nil {
		t.Errorf("error creating mock cluster:%v\n", err)
		return
	}
	messageCount := 10000
	defer mockCluster.Close()
	topic := "test"

	brokers := mockCluster.BootstrapServers()

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		t.Errorf("error creating producer:%v\n", err)
		return
	}

	delivery := make(chan kafka.Event)

	done := make(chan struct{})
	go func() {
		for range delivery {
		}
		close(done)
	}()

	var readCount int32
	batchSize := 500
	pc := PartitionConsumer{
		BatchSize: batchSize,
		BatchHandler: BatchHandlerFunc(func(records Records) {
			atomic.AddInt32(&readCount, int32(len(records)))
		}),
		Config: &Config{BoostrapServers: brokers, GroupID: "test", AutoOffsetReset: "earliest"},
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), consumerTimeout)
		defer cancel()
		_ = pc.Run(ctx, "test")

		close(delivery)
	}()

	for i := 0; i < messageCount; i++ {
		err := p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte("test"),
			Key:            []byte("test"),
			Timestamp:      time.Now(),
		}, delivery)
		if err != nil {
			t.Errorf("error producing:%v\n", err)
			return
		}
	}

	<-done

	if readCount != int32(messageCount) {
		t.Errorf("expected message count to be %d got %d\n", messageCount, readCount)
	}
}
