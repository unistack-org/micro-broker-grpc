package server

import (
	"context"
	"fmt"

	"go.unistack.org/micro-broker-grpc/v3/internal"
	pb "go.unistack.org/micro-broker-grpc/v3/proto"
	pbMicro "go.unistack.org/micro-broker-grpc/v3/proto/proto"
	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/logger"
	"go.unistack.org/micro/v3/metadata"
)

var (
	_ pbMicro.BrokerServer = (*brokerServiceServer)(nil)
)

type brokerServiceServer struct {
	log   logger.Logger
	store *internal.TopicStorage
}

func NewBrokerServer(l logger.Logger) *brokerServiceServer {
	return &brokerServiceServer{
		log:   l,
		store: internal.NewTopicStorage(),
	}
}

func (b *brokerServiceServer) Publish(ctx context.Context, req *pb.Message, rsp *pb.Response) error {
	b.log.Debug(ctx, "micro-broker-grpc: start publish")
	topic := req.Headers[metadata.HeaderTopic]

	var t *internal.Topic
	t, ok := b.store.GetTopic(topic)
	if !ok {
		b.log.Debug(ctx, fmt.Sprintf("micro-broker-grpc: topic %s is not exist, start create topic", topic))
		t = b.store.CreateTopic(ctx, topic, b.log)
	}

	msg := broker.Message{
		Header: req.Headers,
		Body:   req.Body,
	}

	b.log.Debug(ctx, fmt.Sprintf("micro-broker-grpc: send msg %v to %s", msg, t.Name))
	err := t.PublishMessage(ctx, msg)
	if err != nil {
		b.log.Error(ctx, fmt.Sprintf("micro-broker-grpc: error with publish msg, err: %s", err))
		rsp.Error = err.Error()
		return err
	}

	return nil
}

func (b *brokerServiceServer) Subscribe(ctx context.Context, req *pb.SubscribeRequest, stream pbMicro.Broker_SubscribeStream) error {
	b.log.Debug(ctx, "micro-broker-grpc: start subscribe")
	topic, ok := b.store.GetTopic(req.Topic)
	if !ok {
		b.log.Debug(ctx, fmt.Sprintf("micro-broker-grpc: topic %s is not exist, start create topic", topic))
		topic = b.store.CreateTopic(ctx, req.Topic, b.log)
	}
	ch := topic.RegisterSubscriber(ctx)

	for {
		select {
		case <-ctx.Done():
			b.log.Debug(ctx, "micro-broker-grpc:  unsubscribe")
			return nil
		case msg := <-ch:
			b.log.Debug(ctx, fmt.Sprintf("micro-broker-grpc: get msg %v from topic %s", msg, topic.Name))
			messageResponse := &pb.Message{Headers: msg.Header, Body: msg.Body}
			if err := stream.Send(messageResponse); err != nil {
				b.log.Error(ctx, fmt.Sprintf("micro-broker-grpc: error with send msg to handler, err: %s", err))
				return err
			}
		}
	}
}
