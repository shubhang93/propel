//go:build exclude

package main

import (
	"context"
	"log"
	"log/slog"
	"os/signal"
	"source.golabs.io/engineering-platforms/epcons"
	"syscall"
	"time"
)

func main() {
	pc := epcons.PartitionConsumer{
		BatchHandler: epcons.BatchHandlerFunc(func(records epcons.Records) {
			slog.Info("processing records", "tp", epcons.TopicPartFor(records))
			time.Sleep(10 * time.Second)
		}),
		Config: &epcons.Config{
			BoostrapServers: "g-gojek-id-mainstream.golabs.io:6668",
			GroupID:         "test_part_cons",
		},
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	err := pc.Run(ctx, "driver-location-ping-3")
	if err != nil {
		log.Fatal(err)
	}
}
