package micro_broker_grpc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	pb "go.unistack.org/micro-broker-grpc/v3/proto"
	pbMicro "go.unistack.org/micro-broker-grpc/v3/proto/proto"
	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/metadata"
	"go.unistack.org/micro/v3/semconv"
)

var (

	// DefaultCommitInterval specifies how fast send commit offsets to kafka
	DefaultCommitInterval = 5 * time.Second

	// DefaultStatsInterval specifies how fast check consumer lag
	DefaultStatsInterval = 5 * time.Second

	// DefaultSubscribeMaxInflight specifies how much messages keep inflight
	DefaultSubscribeMaxInflight = 100
)

var _ broker.Broker = (*Broker)(nil)

type Broker struct {
	init bool
	rw   sync.RWMutex
	opts broker.Options
	cli  pbMicro.BrokerClient
	subs []*subscriber
}

func NewBroker(cli pbMicro.BrokerClient, opts ...broker.Option) *Broker {
	options := broker.NewOptions(opts...)

	return &Broker{
		opts: options,
		cli:  cli,
		subs: make([]*subscriber, 0),
	}
}

func (b *Broker) Name() string {
	return b.opts.Name
}

func (b *Broker) Init(opts ...broker.Option) error {
	b.rw.Lock()
	defer b.rw.Unlock()

	if err := b.opts.Tracer.Init(); err != nil {
		return err
	}
	if err := b.opts.Logger.Init(); err != nil {
		return err
	}
	if err := b.opts.Meter.Init(); err != nil {
		return err
	}

	b.init = true

	return nil
}

func (b *Broker) Options() broker.Options {
	return b.opts
}

func (b *Broker) Address() string {
	return strings.Join(b.opts.Addrs, ",")
}

func (b *Broker) Connect(ctx context.Context) error {
	return nil
}

func (b *Broker) Disconnect(ctx context.Context) error {
	return nil
}

func (b *Broker) BatchPublish(ctx context.Context, msgs []*broker.Message, opts ...broker.PublishOption) error {
	return b.publish(ctx, msgs, opts...)
}

func (b *Broker) Publish(ctx context.Context, topic string, msg *broker.Message, opts ...broker.PublishOption) error {
	msg.Header.Set(metadata.HeaderTopic, topic)
	return b.publish(ctx, []*broker.Message{msg}, opts...)
}

func (b *Broker) publish(ctx context.Context, msgs []*broker.Message, opts ...broker.PublishOption) error {
	for _, v := range msgs {
		t, _ := v.Header.Get(metadata.HeaderTopic)
		b.opts.Meter.Counter(semconv.PublishMessageInflight, "endpoint", t, "topic", t).Inc()
		newMsg := &pb.Message{
			Headers: v.Header,
			Body:    v.Body,
		}
		ts := time.Now()
		rsp, err := b.cli.Publish(ctx, newMsg)
		te := time.Since(ts)
		b.opts.Meter.Summary(semconv.PublishMessageLatencyMicroseconds, "endpoint", t, "topic", t).Update(te.Seconds())
		b.opts.Meter.Histogram(semconv.PublishMessageDurationSeconds, "endpoint", t, "topic", t).Update(te.Seconds())
		b.opts.Meter.Counter(semconv.PublishMessageInflight, "endpoint", t, "topic", t).Dec()
		if err != nil {
			b.opts.Meter.Counter(semconv.PublishMessageTotal, "endpoint", t, "topic", t, "status", "failure").Inc()
			b.opts.Logger.Error(ctx, fmt.Sprintf("micro-broker-grpc: error publish msg: error %s, topic %s",
				err,
				newMsg.Headers[metadata.HeaderTopic],
			))
			return err
		}
		if rsp.GetError() != "" {
			b.opts.Logger.Error(ctx, fmt.Sprintf("micro-broker-grpc: error response %s", rsp.GetError()))
			return errors.New(rsp.GetError())
		}
		b.opts.Meter.Counter(semconv.PublishMessageTotal, "endpoint", t, "topic", t, "status", "success").Inc()
	}
	return nil
}

func (b *Broker) Subscribe(ctx context.Context, topic string, h broker.Handler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	options := broker.NewSubscribeOptions(opts...)

	req := &pb.SubscribeRequest{Topic: topic}
	subcli, err := b.cli.Subscribe(ctx, req)
	if err != nil {
		b.opts.Logger.Error(ctx, fmt.Sprintf("micro-broker-grpc: error to subscribe req: %v topic: %s", req, err))
		return nil, err
	}

	sub := &subscriber{
		c:       subcli,
		topic:   topic,
		opts:    options,
		bopts:   b.opts,
		handler: h,
	}

	go sub.poll(ctx)

	b.rw.Lock()
	b.subs = append(b.subs, sub)
	b.rw.Unlock()
	return sub, nil
}

func (b *Broker) BatchSubscribe(ctx context.Context, topic string, h broker.BatchHandler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	return nil, nil
}

func (b *Broker) String() string {
	return "grpc"
}
