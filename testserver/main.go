package main

import (
	"context"

	pbMicro "go.unistack.org/micro-broker-grpc/v3/proto/proto"
	"go.unistack.org/micro-broker-grpc/v3/server"
	codec "go.unistack.org/micro-codec-json/v3"
	sgrpc "go.unistack.org/micro-server-grpc/v3"
	"go.unistack.org/micro/v3"
	"go.unistack.org/micro/v3/logger"
	"go.unistack.org/micro/v3/logger/slog"
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

	srv := sgrpc.NewServer(
		mserver.Name(appName),
		mserver.Address("localhost:8888"),
		mserver.Logger(log),
		mserver.Codec("application/grpc", codec.NewCodec()),
	)

	svc := micro.NewService(
		micro.Name(appName),
		micro.Context(ctx),
		micro.Server(srv),
		micro.Logger(log),
	)

	handler := server.NewBrokerServer(log)

	if err := pbMicro.RegisterBrokerServer(svc.Server(), handler); err != nil {
		log.Fatal(ctx, "register broker server error", err)
	}

	svc.Init()

	if err := svc.Run(); err != nil {
		log.Fatal(ctx, "run broker service error", err)
	}
}
