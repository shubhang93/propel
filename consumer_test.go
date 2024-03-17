package propel

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/mock"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

type MockConsumer struct {
	mock.Mock
}

func (m *MockConsumer) Poll(timeout int) kafka.Event {
	args := m.Mock.Called(timeout)
	return args.Get(0).(kafka.Event)
}

func (m *MockConsumer) Subscribe(topics string, cb kafka.RebalanceCb) error {
	args := m.Mock.Called(topics, cb)
	return args.Error(0)
}

func (m *MockConsumer) StoreMessage(msg *kafka.Message) (storedOffsets []kafka.TopicPartition, err error) {
	args := m.Mock.Called(msg)
	return args.Get(0).([]kafka.TopicPartition), args.Error(1)
}

func (m *MockConsumer) Close() error {
	args := m.Mock.Called()
	return args.Error(0)
}

func (m *MockConsumer) Logs() chan kafka.LogEvent {
	args := m.Mock.Called()
	return args.Get(0).(chan kafka.LogEvent)
}

func (m *MockConsumer) Pause(tps []kafka.TopicPartition) error {
	args := m.Mock.Called(tps)
	return args.Error(0)
}

func (m *MockConsumer) Resume(tps []kafka.TopicPartition) error {
	args := m.Mock.Called(tps)
	return args.Error(0)
}

func TestPartitionConsumer_pollBatch(t *testing.T) {
	t.Run("Confluent consumer returns a *kafka.Message", func(t *testing.T) {
		mc := MockConsumer{}

		tc := ThrottledConsumer{
			c:      &mc,
			Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})),
		}

		mc.On("Poll", 100).Return(&kafka.Message{})

		batch := make([]*kafka.Message, 100)
		n, err := tc.pollBatch(context.Background(), 100, batch)
		if err != nil {
			t.Errorf("expected error nil got %v\n", err)
			return
		}
		if len(batch) != n {
			t.Errorf("expected %d got %d\n", n, len(batch))
			return
		}

		n, err = tc.pollBatch(context.Background(), 100, batch)
		if len(batch[:n]) != n {
			t.Errorf("expected %d got %d\n", n, len(batch[:n]))
		}
	})

	t.Run("Confluent consumer returns a non fatal error", func(t *testing.T) {
		mc := MockConsumer{}

		tc := ThrottledConsumer{
			c:      &mc,
			Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})),
		}

		mc.On("Poll", 100).Return(kafka.Error{})

		batch := make([]*kafka.Message, 100)
		n, err := tc.pollBatch(context.Background(), 100, batch)
		if err != nil {
			t.Errorf("expected nil error got %v\n", err)
			return
		}

		expectedCount := 0
		if expectedCount != n {
			t.Errorf("expected %d got %d\n", expectedCount, n)
			return
		}

	})

	t.Run("Confluent consumer returns a fatal error", func(t *testing.T) {
		mc := MockConsumer{}

		tc := ThrottledConsumer{
			c:      &mc,
			Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})),
		}

		mc.On("Poll", 100).Return(kafka.NewError(kafka.ErrAllBrokersDown, "error kafka", true))

		batch := make([]*kafka.Message, 100)
		n, err := tc.pollBatch(context.Background(), 100, batch)
		if err == nil {
			t.Errorf("expected error got %v\n", err)
			return
		}

		expectedCount := 0
		if expectedCount != n {
			t.Errorf("expected %d got %d\n", expectedCount, n)
			return
		}

	})

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
	tc := ThrottledConsumer{
		BatchSize: batchSize,
		BatchHandlerFunc: func(records Records) {
			atomic.AddInt32(&readCount, int32(len(records)))
		},
		KafkaConsumerConf: &ConsumerConfig{
			BoostrapServers: brokers,
			GroupID:         "test",
			AutoOffsetReset: "earliest",
		},
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), consumerTimeout)
		defer cancel()
		_ = tc.Run(ctx, "test")

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

	if readCount != atomic.LoadInt32(&readCount) {
		t.Errorf("expected message count to be %d got %d\n", messageCount, readCount)
	}
}
