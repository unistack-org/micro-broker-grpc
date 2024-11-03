package main

import (
	"context"

	bclient "go.unistack.org/micro-broker-grpc/v3/client"
	pbMicro "go.unistack.org/micro-broker-grpc/v3/proto/proto"
	cgrpc "go.unistack.org/micro-client-grpc/v3"
	codec "go.unistack.org/micro-codec-json/v3"
	sgrpc "go.unistack.org/micro-server-grpc/v3"
	"go.unistack.org/micro/v3"
	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/client"
	"go.unistack.org/micro/v3/logger"
	"go.unistack.org/micro/v3/logger/slog"
	mserver "go.unistack.org/micro/v3/server"
	"go.unistack.org/micro/v3/tracer"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := slog.NewLogger(logger.WithLevel(logger.DebugLevel))
	if err := log.Init(); err != nil {
		log.Fatal(ctx, "logger init error", err)
	}
	logger.DefaultLogger = log
	tr := tracer.NewTracer()
	tracer.DefaultTracer = tr

	c := cgrpc.NewClient(
		client.Context(ctx),
		client.Tracer(tr),
		client.Logger(log),
		client.Name("testserver.broker.client-sub"),
		client.Retries(2),
		client.ContentType("application/grpc"),
		client.Proxy("localhost:8888"),
		client.Codec("application/grpc", codec.NewCodec()),
	)

	cli := pbMicro.NewBrokerClient(
		"testserver.broker.client-sub",
		c,
	)

	b := bclient.NewBroker(cli, broker.Logger(log))

	srv := sgrpc.NewServer(
		mserver.Name("testserver.broker.server-sub"),
		mserver.ID("1"),
		mserver.Address("localhost:8899"),
		mserver.Logger(log),
		mserver.Codec("application/grpc", codec.NewCodec()),
		mserver.Broker(b),
	)

	svc := micro.NewService(
		micro.Context(ctx),
		micro.Broker(b),
		micro.Logger(log),
		micro.Server(srv),
		micro.Client(c),
	)

	h := &Handler{Log: log}

	/*err := micro.RegisterSubscriber("topic-1", svc.Server(), h.Handle)
	if err != nil {
		log.Fatal(ctx, "client: subscribe error", err)
	}*/
	_, err := b.Subscribe(ctx, "topic-1", h.Handle)
	if err != nil {
		log.Fatal(ctx, "client: subscribe error", err)
	}
	svc.Init()

	if err = svc.Run(); err != nil {
		log.Fatal(ctx, "run broker service error", err)
	}
}
