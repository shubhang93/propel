#  [ Propel ]

### propel is a thin wrapper over the Confluent kafka Go library

### Features

- Non blocking message polling
- Batch consumption and Single message consumption
- Per partition goroutine processing
- Small API footprint

## Config

```go
&epcon.Config{
	BoostrapServers: "localhost:9092", // Comma separated list of servers
	GroupID:         "consumer_group_id",
}
```

### Example

```go
package main

import (
	"context"
	"log"
	"os/signal"
	"source.golabs.io/engineering-platforms/propel"
	"syscall"
	"time"
)

func main() {
	pc := epcon.PartitionConsumer{
		BatchSize: 500,
		BatchHandler: propel.BatchHandler(func(records propel.Records) {
			time.Sleep(500 * time.Millisecond)
		}),
		Config: &epcon.Config{
			BoostrapServers: "localhost:9092",
			GroupID:         "test_part_cons",
		},
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	// comma separated value of list of topics
	err := pc.Run(ctx, "driver-location-ping-3")
	if err != nil {
		log.Fatal(err)
	}

```

## FAQs

### Why retries aren't a part of the library ?

Retry logic can be implemented in your handler if the message processing fails. Retry logic is not baked into the
library to keep the consumption separate from retries, this makes the library extremely complicated and difficult to
change. Middleware contributions are always welcome.


### How are offsets committed

If you are using a Batch handler then the last offset in the batch is committed once the batch handler returns

If you are using a single Handler then each offset is committed after the handler returns.

### If my handler takes too long to process messages will it get kicked out of the group ?

No, your handler can take as long as it wants to process the messages, the poll loop is non blocking and does not wait
for messages to finish processing before calling the poll again.

### How do I scale my consumers ? I do not see any config for setting the thread count or consumer count

Each partition is processed in its own goroutine. If your topic contains 100 partitions, there are 100 goroutines
concurrently calling your handler. Scaling up your consumer is as simple as adding more machines/pods. If you have 4
machines there would 25 goroutines running on each of the machine. This can be optimized further by load testing your
application.

### How can I cancel running my consumer from the handler

Call the `cancel` function returned by the `Context`, this will kill the consumer.

### Does my handler need to be idempotent ?

Yes, the Partition consumer implements the at least once delivery semantics, which means that you can consume the same
message again.

### Are there any tradeoffs ?

Yes, the PartitionConsumer automatically throttles messages by adapting to your processing speed, which means the
partitions are paused and resumed frequently and this can lead to reduced throughput but what is the point of having a
high throughput if your handler is unable to keep up with it.

# TODO

- [ ] Observability



