package client

import (
	"context"
	"sync"

	pbMicro "go.unistack.org/micro-broker-grpc/v3/proto/proto"
	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/metadata"
)

var _ broker.Subscriber = (*subscriber)(nil)

type subscriber struct {
	c       pbMicro.Broker_SubscribeClient
	topic   string
	opts    broker.SubscribeOptions
	handler broker.Handler
	closed  bool
	done    chan struct{}
	rw      sync.RWMutex
}

func (s *subscriber) Options() broker.SubscribeOptions {
	return s.opts
}

func (s *subscriber) Topic() string {
	return s.topic
}

func (s *subscriber) Unsubscribe(ctx context.Context) error {
	if s.closed {
		return nil
	}
	close(s.done)
	s.closed = true
	return nil
}

func (s *subscriber) poll(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		default:
			msg, err := s.c.Recv()
			if err != nil {
				return
			}
			ev := new(event)
			ev.msg = &broker.Message{}
			ev.msg.Header = metadata.New(len(msg.Headers))
			for key, val := range msg.Headers {
				ev.msg.Header.Set(key, val)
			}
			err = s.handler(ev)
			if err != nil {
				return
			}

		}
	}
}
