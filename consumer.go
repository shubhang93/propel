package epcons

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log/slog"
	"os"
	"sync"
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

type ConfluentConsumer interface {
	Poll(timeout int) kafka.Event
	Subscribe(topics string, cb kafka.RebalanceCb) error
	StoreMessage(m *kafka.Message) (storedOffsets []kafka.TopicPartition, err error)
	Close() error
	Pause([]kafka.TopicPartition) error
	Resume([]kafka.TopicPartition) error
	Logs() chan kafka.LogEvent
}

type PartitionConsumer struct {
	c                   ConfluentConsumer
	workers             map[topicPart]*batchWorker
	BatchSize           int
	BatchHandler        BatchHandler
	Handler             Handler
	Config              *Config
	WorkerStopTimeoutMS time.Duration
	Logger              *slog.Logger
	lastPoll            time.Time
}

func groupMsgs(msgs []*kafka.Message, group map[topicPart][]*kafka.Message) {
	for _, msg := range msgs {
		tp := topicPart{Topic: *msg.TopicPartition.Topic, Part: msg.TopicPartition.Partition}
		group[tp] = append(group[tp], msg)
	}
}

func (pc *PartitionConsumer) Run(ctx context.Context, topics string) error {
	pc.setDefaults()

	c, err := kafka.NewConsumer(pc.Config.toConfigMap())
	if err != nil {
		return err
	}
	pc.c = c
	logs := pc.c.Logs()
	go func() {
		for evt := range logs {
			pc.Logger.Info(evt.Message, slog.Int("level", evt.Level))
		}
	}()

	defer func() {
		err := pc.c.Close()
		if err != nil {
			pc.Logger.Error("consumer close error", "err", err)
		}
		pc.Logger.Info("consumer closed without errors")

	}()

	committable := make(chan progress, maxPartitionCount)
	err = pc.c.Subscribe(topics, func(consumer *kafka.Consumer, event kafka.Event) error {
		// Locks are not required when we
		// modify the workers map because
		// rebalance callabck blocks the poll loop

		rbLogger := pc.Logger.WithGroup("rb-callback")
		pc.Logger.Info("rebalance callback invoked")

		switch e := event.(type) {
		case kafka.AssignedPartitions:
			for _, part := range e.Partitions {
				tp := topicPart{Topic: *part.Topic, Part: part.Partition}
				bw := &batchWorker{
					part:        tp.Part,
					topic:       tp.Topic,
					records:     make(chan []*kafka.Message, 1),
					stop:        make(chan struct{}),
					done:        make(chan struct{}, 1),
					logger:      pc.Logger.WithGroup("worker"),
					committable: committable,
				}
				pc.workers[tp] = bw
				if pc.BatchHandler != nil {
					go bw.consumeBatch(pc.BatchHandler)
					continue
				}
				go bw.consumeSingle(pc.Handler)
			}
		case kafka.RevokedPartitions:
			pc.Logger.Info("assignment status", "lost", c.AssignmentLost())
			var wg sync.WaitGroup
			tps := make([]topicPart, 0, len(e.Partitions))
			for _, part := range e.Partitions {
				tp := topicPart{Topic: *part.Topic, Part: part.Partition}
				if pc.workers[tp] == nil {
					continue
				}
				wg.Add(1)
				go func() {
					w := pc.workers[tp]
					pc.stopWorker(w, pc.WorkerStopTimeoutMS*time.Millisecond)
					rbLogger.Info("stopped worker", slog.String("tp", tp.String()))
					wg.Done()
				}()
				tps = append(tps, tp)
			}
			wg.Wait()
			for _, tp := range tps {
				delete(pc.workers, tp)
			}

			rbLogger.Info("stopped all revoked workers")
		}

		return nil
	})
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

		batch := make([]*kafka.Message, pc.BatchSize)
		n, err := pc.pollBatch(ctx, defaultPollTimeoutMS, batch)
		pc.Logger.Debug("poll diff", "diff-ms", time.Now().Sub(pc.lastPoll).Milliseconds())
		pc.lastPoll = time.Now()
		if err != nil {
			pc.Logger.Error("poll batch err", slog.String("err", err.Error()))
			return err
		}

		pc.Logger.Debug("poll fetched", slog.Int("count", n))
		if n < pc.BatchSize {
			clear(batch[n+1:])
		}

		clear(msgGroup)
		var pauseable []kafka.TopicPartition
		if n > 0 {
			groupMsgs(batch[:n], msgGroup)
			for tp := range msgGroup {
				if pc.workers[tp] == nil {
					continue
				}
				pauseable = append(pauseable, kafka.TopicPartition{Topic: &tp.Topic, Partition: tp.Part})
				pc.Logger.Debug("sending", "tp", tp, "count", len(msgGroup[tp]))
				pc.workers[tp].records <- msgGroup[tp]
			}
		}

		pc.Logger.Debug("pausing", "parts", pauseable)
		if err := pc.c.Pause(pauseable); err != nil {
			return fmt.Errorf("pause failed:%w", err)
		}

		pc.Logger.Debug("enqueuing committable messages", slog.Int("count", len(committable)))
		if err := pc.enqueueCommits(committable); err != nil {
			return err
		}

	}
}

func (pc *PartitionConsumer) stopWorker(worker *batchWorker, timeout time.Duration) {
	close(worker.stop)
	after := time.After(timeout)
	select {
	case <-worker.done:
		return
	case <-after:
		pc.Logger.Info("worker shutdown timed out", "worker", worker)
	}
}

func (pc *PartitionConsumer) pollBatch(ctx context.Context, timeoutMS int, batch []*kafka.Message) (int, error) {
	remainingTime := time.Duration(timeoutMS) * time.Millisecond
	endTime := time.Now().Add(time.Duration(timeoutMS) * time.Millisecond)

	pollLogger := pc.Logger.WithGroup("poll")
	done := ctx.Done()
	var i int
	for i < cap(batch) {
		select {
		case <-done:
			pollLogger.Info("starting shutdown context done")
			return 0, ctx.Err()
		default:
			e := pc.c.Poll(timeoutMS)
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

func (pc *PartitionConsumer) setDefaults() {
	if pc.Logger == nil {
		pc.Logger = slog.New(slog.NewTextHandler(os.Stdout, nil))
		pc.Logger = pc.Logger.WithGroup("propel")
	}

	if pc.BatchSize < 1 {
		pc.BatchSize = 500
	}

	if pc.BatchHandler == nil && pc.Handler == nil {
		panic("batch handler and handler cannot be nil")
	}

	if pc.WorkerStopTimeoutMS == 0 {
		pc.WorkerStopTimeoutMS = defaultWorkerStopTimeoutMS
	}

	if pc.workers == nil {
		pc.workers = make(map[topicPart]*batchWorker, maxPartitionCount)
	}
}

func (pc *PartitionConsumer) enqueueCommits(committable chan progress) error {
	var resumeable []kafka.TopicPartition
	for i := 0; i < len(committable); i++ {
		p := <-committable
		msg := p.m
		tp := topicPart{Topic: *msg.TopicPartition.Topic, Part: msg.TopicPartition.Partition}

		if pc.workers[tp] == nil {
			continue
		}

		if _, err := pc.c.StoreMessage(msg); err != nil {
			pc.Logger.Error("error storing message", "error", err)
			return err
		}

		if p.resume {
			resumeable = append(resumeable, kafka.TopicPartition{Topic: &tp.Topic, Partition: tp.Part})
		}
	}

	pc.Logger.Debug("resumeable", "parts", resumeable)
	if err := pc.c.Resume(resumeable); err != nil {
		return fmt.Errorf("error resuming:%w", err)
	}

	return nil
}
