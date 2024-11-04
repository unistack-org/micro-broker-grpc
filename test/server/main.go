package main

import (
	"context"

	"go.unistack.org/micro-broker-grpc/v3/internal"
	pbMicro "go.unistack.org/micro-broker-grpc/v3/proto/proto"
	"go.unistack.org/micro-broker-grpc/v3/server"
	codec "go.unistack.org/micro-codec-json/v3"
	sgrpc "go.unistack.org/micro-server-grpc/v3"
	"go.unistack.org/micro/v3"
	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/logger"
	"go.unistack.org/micro/v3/logger/slog"
	"go.unistack.org/micro/v3/meter"
	mserver "go.unistack.org/micro/v3/server"
	"go.unistack.org/micro/v3/tracer"
)

const appName = "Broker"

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
	m := meter.NewMeter()
	meter.DefaultMeter = m

	srv := sgrpc.NewServer(
		mserver.Name(appName),
		mserver.Address("localhost:8888"),
		mserver.Logger(log),
		mserver.Codec("application/grpc", codec.NewCodec()),
		mserver.Tracer(tr),
		mserver.Meter(m),
	)

	svc := micro.NewService(
		micro.Name(appName),
		micro.Context(ctx),
		micro.Server(srv),
		micro.Logger(log),
		micro.Tracer(tr),
		micro.Meter(m),
	)

	handler, err := server.NewBrokerServer(
		internal.NewStorage(log),
		broker.Logger(log),
		broker.Tracer(tr),
		broker.Meter(m),
	)
	if err != nil {
		log.Fatal(ctx, "create broker server error", err)
	}

	if err := pbMicro.RegisterBrokerServer(svc.Server(), handler); err != nil {
		log.Fatal(ctx, "register broker server error", err)
	}

	svc.Init()

	if err := svc.Run(); err != nil {
		log.Fatal(ctx, "run broker service error", err)
	}
}
