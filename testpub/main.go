package main

import (
	"context"

	bclient "go.unistack.org/micro-broker-grpc/v3/client"
	pbMicro "go.unistack.org/micro-broker-grpc/v3/proto/proto"
	cgrpc "go.unistack.org/micro-client-grpc/v3"
	codec "go.unistack.org/micro-codec-json/v3"
	"go.unistack.org/micro/v3"
	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/client"
	"go.unistack.org/micro/v3/logger"
	"go.unistack.org/micro/v3/logger/slog"
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
		client.Name("testserver.broker.client-pub"),
		client.Retries(2),
		client.ContentType("application/grpc"),
		client.Proxy("localhost:8888"),
		client.Codec("application/grpc", codec.NewCodec()),
	)

	cli := pbMicro.NewBrokerClient(
		"testserver.broker.client-pub",
		c,
	)

	b := bclient.NewBroker(cli,
		broker.Logger(log),
	)

	svc := micro.NewService(
		micro.Context(ctx),
		micro.Broker(b),
		micro.Logger(log),
	)

	pub := &Pub{Log: log, b: b}
	pub.Publish(ctx)

	svc.Init()

	if err := svc.Run(); err != nil {
		log.Fatal(ctx, "run broker service error", err)
	}
}
