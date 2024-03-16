//go:build ignore

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
	tc := propel.ThrottledConsumer{
		BatchHandlerFunc: func(records propel.Records) {
			slog.Info("processing records", "tp", propel.TopicPartFor(records))
			time.Sleep(10 * time.Second)
		},
		Config: &propel.ConsumerConfig{
			BoostrapServers: "localhost:9092",
			GroupID:         "test_part_cons",
		},
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	err := tc.Run(ctx, "test-topic")
	if err != nil {
		log.Fatal(err)
	}
}
