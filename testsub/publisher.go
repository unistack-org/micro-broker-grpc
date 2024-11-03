package main

import (
	"context"
	"fmt"

	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/logger"
)

type Pub struct {
	b   broker.Broker
	Log logger.Logger
}

func (p *Pub) Publish(ctx context.Context) {
	topics := []string{"topic-1", "topic-2", "topic-3", "topic-4", "topic-5"}

	header := make(map[string]string, 0)
	header["Header1"] = "1"
	header["Header2"] = "2"
	header["Header3"] = "3"
	body := `
	{
		"micro-broker-grpc": "success"
	}
`
	msg := &broker.Message{
		Header: header,
		Body:   []byte(body),
	}

	for _, t := range topics {
		p.Log.Info(ctx, fmt.Sprintf("publisher: topic: %s, err: %s", t, p.b.Publish(ctx, t, msg)))
	}
}
