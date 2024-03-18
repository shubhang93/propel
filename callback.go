package propel

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log/slog"
	"sync"
	"time"
)

func rebalanceCallback(tc *ThrottledConsumer) kafka.RebalanceCb {
	return func(_ *kafka.Consumer, event kafka.Event) error {
		rbLogger := tc.Logger.WithGroup("rb-callback")
		tc.Logger.Info("rebalance callback invoked")

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
					logger:      tc.Logger.WithGroup("worker"),
					committable: tc.committable,
				}
				tc.workers[tp] = bw
				if handler := tc.oneOfBatchHandlers(); handler != nil {
					go bw.consumeBatch(handler)
					continue
				}
				go bw.consumeSingle(tc.oneOfHandlers())
			}
		case kafka.RevokedPartitions:
			tc.Logger.Info("assignment status", "lost", e.Partitions)
			var wg sync.WaitGroup
			tps := make([]topicPart, 0, len(e.Partitions))
			for _, part := range e.Partitions {
				tp := topicPart{Topic: *part.Topic, Part: part.Partition}
				if tc.workers[tp] == nil {
					continue
				}
				wg.Add(1)
				go func() {
					w := tc.workers[tp]
					tc.stopWorker(w, tc.WorkerStopTimeoutMS*time.Millisecond)
					rbLogger.Info("stopped worker", slog.String("tp", tp.String()))
					wg.Done()
				}()
				tps = append(tps, tp)
			}
			wg.Wait()
			for _, tp := range tps {
				delete(tc.workers, tp)
			}

			rbLogger.Info("stopped all revoked workers")
		}

		return nil

	}
}
