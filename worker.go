package propel

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log/slog"
)

type batchWorker struct {
	part        int32
	topic       string
	records     chan []*kafka.Message
	committable chan progress
	stop        chan struct{}
	done        chan struct{}
	logger      *slog.Logger
}

func (b *batchWorker) String() string {
	return fmt.Sprintf("worker#%s#%d", b.topic, b.part)
}

func (b *batchWorker) consumeSingle(handler Handler) {
	b.logger.Info("starting worker", "worker", b.String())
	for {
		select {
		case <-b.stop:
			close(b.done)
			return
		case recs := <-b.records:
			b.logger.Debug("processing records", slog.Int("size", len(recs)), slog.String("worker", b.String()))
			resume := false
			for i := range recs {
				handler.Handle(recs[i])
				if i == len(recs)-1 {
					resume = true
				}
				b.logger.Debug("sending progress", slog.Int("rec-index", i), slog.String("worker", b.String()))
				b.committable <- progress{m: recs[i], resume: resume}
			}
		}
	}
}

func (b *batchWorker) consumeBatch(handler BatchHandler) {
	b.logger.Info("starting worker", "worker", b.String())
	for {
		select {
		case <-b.stop:
			close(b.done)
			return
		case recs := <-b.records:
			b.logger.Debug("processing records", "size", len(recs), "worker", b.String())
			handler.Handle(recs)
			b.committable <- progress{m: recs[len(recs)-1], resume: true}
		}
	}
}
