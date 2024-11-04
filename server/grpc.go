package server

import (
	"context"
	"fmt"
	"time"

	"go.unistack.org/micro-broker-grpc/v3/internal"
	pb "go.unistack.org/micro-broker-grpc/v3/proto"
	pbMicro "go.unistack.org/micro-broker-grpc/v3/proto/proto"
	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/metadata"
	"go.unistack.org/micro/v3/semconv"
	"go.unistack.org/micro/v3/store"
	"go.unistack.org/micro/v3/tracer"
)

var (
	_ pbMicro.BrokerServer = (*brokerServiceServer)(nil)
)

type brokerServiceServer struct {
	opts  broker.Options
	store *internal.TopicStorage
}

func NewBrokerServer(s store.Store, opts ...broker.Option) (pbMicro.BrokerServer, error) {
	options := broker.NewOptions(opts...)

	return &brokerServiceServer{
		opts:  options,
		store: internal.NewTopicStorage(),
	}, nil
}

func (b *brokerServiceServer) Publish(ctx context.Context, req *pb.Message, rsp *pb.Response) error {
	ts := time.Now()
	topic := req.Headers[metadata.HeaderTopic]
	ctx, sp := b.opts.Tracer.Start(ctx, "server.publish")

	b.opts.Logger.Debug(ctx, "micro-broker-grpc: start publish")
	b.opts.Meter.Counter(semconv.ServerRequestTotal, "publish", topic).Inc()
	defer func() {
		te := time.Since(ts)
		b.opts.Meter.Summary(semconv.ServerRequestLatencyMicroseconds, "event", "publish", "endpoint", topic, "topic", topic).Update(float64(te.Microseconds()))
		b.opts.Meter.Histogram(semconv.ServerRequestDurationSeconds, "event", "publish", "endpoint", topic, "topic", topic).Update(te.Seconds())
	}()

	sp.AddEvent("check topic start")
	t, err := b.checkTopic(ctx, topic)
	sp.AddEvent("check topic stop")
	if err != nil {
		sp.SetStatus(tracer.SpanStatusError, err.Error())
		b.opts.Meter.Counter(semconv.ServerRequestTotal, "publish", topic, "status", "failure").Inc()
		return err
	}

	msg := broker.Message{
		Header: req.Headers,
		Body:   req.Body,
	}

	b.opts.Logger.Debug(ctx, fmt.Sprintf("micro-broker-grpc: send msg %v to %s", msg, t.Name))
	sp.AddEvent("publish msg start")
	err = t.PublishMessage(ctx, msg)
	sp.AddEvent("publish msg stop")
	if err != nil {
		sp.SetStatus(tracer.SpanStatusError, err.Error())
		b.opts.Meter.Counter(semconv.ServerRequestTotal, "publish", topic, "status", "failure").Inc()
		b.opts.Logger.Error(ctx, fmt.Sprintf("micro-broker-grpc: error with publish msg, err: %s", err))
		rsp.Error = err.Error()
		return err
	}

	b.opts.Meter.Counter(semconv.ServerRequestTotal, "publish", topic, "status", "success").Inc()
	sp.Finish()
	return nil
}

func (b *brokerServiceServer) Subscribe(ctx context.Context, req *pb.SubscribeRequest, stream pbMicro.Broker_SubscribeStream) error {
	b.opts.Logger.Debug(ctx, "micro-broker-grpc: start subscribe")
	b.opts.Meter.Counter(semconv.ServerRequestTotal, "subscribe", req.Topic).Inc()
	ctx, sp := b.opts.Tracer.Start(ctx, "server.subscribe")

	sp.AddEvent("check topic start")
	t, err := b.checkTopic(ctx, req.Topic)
	sp.AddEvent("check topic stop")
	if err != nil {
		sp.SetStatus(tracer.SpanStatusError, err.Error())
		return err
	}

	sp.AddEvent("register subscriber start")
	ch := t.RegisterSubscriber(ctx)
	sp.AddEvent("register subscriber start")

	for {
		select {
		case <-ctx.Done():
			b.opts.Logger.Debug(ctx, "micro-broker-grpc:  unsubscribe")
			sp.Finish()
			return nil
		case msg := <-ch:
			ts := time.Now()
			sp.AddEvent("send msg start")
			b.opts.Logger.Debug(ctx, fmt.Sprintf("micro-broker-grpc: get msg %v from topic %s", msg, t.Name))
			messageResponse := &pb.Message{Headers: msg.Header, Body: msg.Body}
			if err := stream.Send(messageResponse); err != nil {
				sp.SetStatus(tracer.SpanStatusError, err.Error())
				b.opts.Meter.Counter(semconv.ServerRequestTotal, "publish", req.Topic, "status", "failure").Inc()
				b.opts.Logger.Error(ctx, fmt.Sprintf("micro-broker-grpc: error with send msg to handler, err: %s", err))
				return err
			}
			te := time.Since(ts)
			b.opts.Meter.Summary(semconv.ServerRequestLatencyMicroseconds, "event", "subscribe", "endpoint", req.Topic, "topic", req.Topic).Update(float64(te.Microseconds()))
			b.opts.Meter.Histogram(semconv.ServerRequestDurationSeconds, "event", "subscribe", "endpoint", req.Topic, "topic", req.Topic).Update(te.Seconds())
			sp.AddEvent("send msg stop")
		}
	}
}

func (b *brokerServiceServer) checkTopic(ctx context.Context, topic string) (*internal.Topic, error) {
	var t *internal.Topic
	/*if err := b.store.Exists(ctx, topic); err != nil {
		b.opts.Logger.Debug(ctx, fmt.Sprintf("micro-broker-grpc: topic %s is not exist, start create topic", topic))
		t = internal.NewTopic(ctx, topic, b.opts.Logger)
		if err = b.store.Write(ctx, topic, t); err != nil {
			b.opts.Logger.Error(ctx, "micro-broker-grpc: error with create topic", err)
			return nil, err
		}
	} else {
		if err = b.store.Read(ctx, topic, t); err != nil {
			b.opts.Logger.Error(ctx, "micro-broker-grpc: error read topic", err)
			return nil, err
		}
	}*/
	t, ok := b.store.GetTopic(topic)
	if !ok {
		b.opts.Logger.Debug(ctx, fmt.Sprintf("micro-broker-grpc: topic %s is not exist, start create topic", topic))
		t = b.store.CreateTopic(ctx, topic, b.opts.Logger)
	}
	return t, nil
}
