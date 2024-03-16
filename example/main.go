package main

import (
	"context"
	"github.com/shubhang93/propel"
	"log"
	"log/slog"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	pc := propel.PartitionConsumer{
		BatchHandler: propel.BatchHandlerFunc(func(records propel.Records) {
			slog.Info("processing records", "tp", propel.TopicPartFor(records))
			time.Sleep(10 * time.Second)
		}),
		Config: &propel.Config{
			BoostrapServers: "localhost:9092",
			GroupID:         "test_part_cons",
		},
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	err := pc.Run(ctx, "test-topic")
	if err != nil {
		log.Fatal(err)
	}
}
