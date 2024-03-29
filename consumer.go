package propel

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log/slog"
	"os"
	"time"
)

const defaultWorkerStopTimeoutMS = 2000
const defaultPollTimeoutMS = 100

// for all practical use cases a single consumer instance should not be handling more than 256 partitions
const maxPartitionCount = 256

type topicPart struct {
	Topic string
	Part  int32
}

type progress struct {
	m      *kafka.Message
	resume bool
}

func (t topicPart) String() string {
	return fmt.Sprintf("%s#%d", t.Topic, t.Part)
}

type ThrottledConsumer struct {
	c       confluentConsumer
	workers map[topicPart]*batchWorker

	BatchSize int

	BatchHandler     BatchHandler
	Handler          Handler
	BatchHandlerFunc func(records Records)
	HandlerFunc      func(record Record)

	committable chan progress

	consumerInitFunc  func(c *kafka.ConfigMap) (confluentConsumer, error)
	KafkaConsumerConf *ConsumerConfig
	rebalanceCallback kafka.RebalanceCb

	WorkerStopTimeoutMS time.Duration
	Logger              *slog.Logger

	lastPoll time.Time
}

func (tc *ThrottledConsumer) Run(ctx context.Context, topics string) error {
	// init all required states
	tc.mustInit()

	c, err := tc.consumerInitFunc(tc.KafkaConsumerConf.toConfigMap())
	if err != nil {
		return err
	}
	tc.c = c
	logs := tc.c.Logs()
	go func() {
		for evt := range logs {
			tc.Logger.Info(evt.Message, slog.Int("level", evt.Level))
		}
	}()

	defer func() {
		for _, w := range tc.workers {
			tc.stopWorker(w, defaultWorkerStopTimeoutMS*time.Millisecond)
		}
		err := tc.c.Close()
		if err != nil {
			tc.Logger.Error("consumer close error", "err", err)
		}
		tc.Logger.Info("consumer closed without errors")
	}()

	err = tc.c.Subscribe(topics, rebalanceCallback(tc))
	if err != nil {
		return err
	}

	msgGroup := make(map[topicPart][]*kafka.Message, maxPartitionCount)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		batch := make([]*kafka.Message, tc.BatchSize)
		n, err := tc.pollBatch(ctx, defaultPollTimeoutMS, batch)
		tc.Logger.Debug("poll diff", "diff-ms", time.Now().Sub(tc.lastPoll).Milliseconds())
		tc.lastPoll = time.Now()
		if err != nil {
			tc.Logger.Error("poll batch err", slog.String("err", err.Error()))
			return err
		}

		tc.Logger.Debug("poll fetched", slog.Int("count", n))
		if n < tc.BatchSize {
			clear(batch[n+1:])
		}

		clear(msgGroup)
		var pauseable []kafka.TopicPartition
		if n > 0 {
			groupMsgs(batch[:n], msgGroup)
			for tp := range msgGroup {
				if tc.workers[tp] == nil {
					continue
				}
				pauseable = append(pauseable, kafka.TopicPartition{Topic: &tp.Topic, Partition: tp.Part})
				tc.Logger.Debug("sending", "tp", tp, "count", len(msgGroup[tp]))
				tc.workers[tp].records <- msgGroup[tp]
			}
		}

		tc.Logger.Debug("pausing", "parts", pauseable)
		if err := tc.c.Pause(pauseable); err != nil {
			return fmt.Errorf("pause failed:%w", err)
		}

		tc.Logger.Debug("enqueuing committable messages", slog.Int("count", len(tc.committable)))
		if err := tc.enqueueCommits(); err != nil {
			return err
		}

	}
}

func (tc *ThrottledConsumer) stopWorker(worker *batchWorker, timeout time.Duration) {
	close(worker.stop)
	after := time.After(timeout)
	select {
	case <-worker.done:
		return
	case <-after:
		tc.Logger.Info("worker shutdown timed out", "worker", worker)
	}
}

func (tc *ThrottledConsumer) pollBatch(ctx context.Context, timeoutMS int, batch []*kafka.Message) (int, error) {
	remainingTime := time.Duration(timeoutMS) * time.Millisecond
	endTime := time.Now().Add(time.Duration(timeoutMS) * time.Millisecond)

	pollLogger := tc.Logger.WithGroup("poll")
	done := ctx.Done()
	var i int
	for i < cap(batch) {
		select {
		case <-done:
			pollLogger.Info("starting shutdown context done")
			return 0, ctx.Err()
		default:
			e := tc.c.Poll(timeoutMS)
			switch event := e.(type) {
			case kafka.Error:
				if event.IsFatal() {
					return 0, event
				}
			case *kafka.Message:
				batch[i] = event
				i++
			}
		}
		remainingTime = endTime.Sub(time.Now())
		if remainingTime < 0 {
			return i, nil
		}
	}
	return i, nil
}

func (tc *ThrottledConsumer) mustInit() {
	if tc.Logger == nil {
		tc.Logger = slog.New(slog.NewTextHandler(os.Stdout, nil))
		tc.Logger = tc.Logger.WithGroup("propel")
	}

	if tc.consumerInitFunc == nil {
		tc.consumerInitFunc = func(c *kafka.ConfigMap) (confluentConsumer, error) {
			return kafka.NewConsumer(c)
		}
	}

	if tc.rebalanceCallback == nil {
		tc.rebalanceCallback = rebalanceCallback(tc)
	}

	if tc.BatchSize < 1 {
		tc.BatchSize = 500
	}

	if (tc.BatchHandler == nil && tc.Handler == nil) && (tc.BatchHandlerFunc == nil && tc.HandlerFunc == nil) {
		panic("one of [BatchHandler,Handler,BatchHandlerFunc,HandlerFunc] must be set")
	}

	if tc.WorkerStopTimeoutMS == 0 {
		tc.WorkerStopTimeoutMS = defaultWorkerStopTimeoutMS
	}

	if tc.committable == nil {
		tc.committable = make(chan progress, maxPartitionCount)
	}

	if tc.workers == nil {
		tc.workers = make(map[topicPart]*batchWorker, maxPartitionCount)
	}

}

func (tc *ThrottledConsumer) enqueueCommits() error {
	var resumeable []kafka.TopicPartition
	for i := 0; i < len(tc.committable); i++ {
		p := <-tc.committable
		msg := p.m
		tp := topicPart{Topic: *msg.TopicPartition.Topic, Part: msg.TopicPartition.Partition}

		if tc.workers[tp] == nil {
			continue
		}

		if _, err := tc.c.StoreMessage(msg); err != nil {
			tc.Logger.Error("error storing message", "error", err)
			return err
		}

		if p.resume {
			resumeable = append(resumeable, kafka.TopicPartition{Topic: &tp.Topic, Partition: tp.Part})
		}
	}

	tc.Logger.Debug("resumeable", "parts", resumeable)
	if err := tc.c.Resume(resumeable); err != nil {
		return fmt.Errorf("error resuming:%w", err)
	}

	return nil
}

func groupMsgs(msgs []*kafka.Message, group map[topicPart][]*kafka.Message) {
	for _, msg := range msgs {
		tp := topicPart{Topic: *msg.TopicPartition.Topic, Part: msg.TopicPartition.Partition}
		group[tp] = append(group[tp], msg)
	}
}

func (tc *ThrottledConsumer) oneOfBatchHandlers() BatchHandler {
	if tc.BatchHandlerFunc != nil {
		return BatchHandlerFunc(tc.BatchHandlerFunc)
	}
	return tc.BatchHandler
}

func (tc *ThrottledConsumer) oneOfHandlers() Handler {
	if tc.HandlerFunc != nil {
		return HandlerFunc(tc.HandlerFunc)
	}
	return tc.Handler
}
