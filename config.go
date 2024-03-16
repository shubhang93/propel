package propel

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"maps"
)

// https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md

const defaultAutoCommitIntervalMS = 5000

type ConsumerConfig struct {
	BoostrapServers      string
	GroupID              string
	ConsumerDebug        string
	AutoOffsetReset      string
	AutoCommitIntervalMS int
	ConfigMapOverride    kafka.ConfigMap
}

func (c *ConsumerConfig) toConfigMap() *kafka.ConfigMap {

	if c.AutoOffsetReset == "" {
		c.AutoOffsetReset = "latest"
	}

	if c.AutoCommitIntervalMS == 0 {
		c.AutoCommitIntervalMS = defaultAutoCommitIntervalMS
	}

	if c.ConsumerDebug == "" {
		c.ConsumerDebug = "consumer"
	}

	cm := kafka.ConfigMap{}
	maps.Copy(cm, c.ConfigMapOverride)
	cm["bootstrap.servers"] = c.BoostrapServers
	cm["group.id"] = c.GroupID
	cm["enable.auto.offset.store"] = false
	cm["auto.offset.reset"] = c.AutoOffsetReset
	cm["auto.commit.interval.ms"] = c.AutoCommitIntervalMS
	cm["go.logs.channel.enable"] = true
	cm["debug"] = c.ConsumerDebug

	return &cm
}
