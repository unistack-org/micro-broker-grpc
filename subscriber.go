package micro_broker_grpc

import (
	"context"
	"sync"
	"time"

	pbMicro "go.unistack.org/micro-broker-grpc/v3/proto/proto"
	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/metadata"
	"go.unistack.org/micro/v3/semconv"
	"go.unistack.org/micro/v3/tracer"
)

var _ broker.Subscriber = (*subscriber)(nil)

type subscriber struct {
	c       pbMicro.Broker_SubscribeClient
	topic   string
	opts    broker.SubscribeOptions
	bopts   broker.Options
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
	eh := s.bopts.ErrorHandler
	if s.opts.ErrorHandler != nil {
		eh = s.opts.ErrorHandler
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		default:
			ctx, sp := s.bopts.Tracer.Start(ctx, "recv.msg")
			ts := time.Now()
			msg, err := s.c.Recv()
			if err != nil {
				sp.SetStatus(tracer.SpanStatusError, err.Error())
				return
			}
			t := msg.GetHeaders()[metadata.HeaderTopic]
			s.bopts.Meter.Counter(semconv.SubscribeMessageInflight, "endpoint", t, "topic", t).Inc()
			ev := eventPool.Get().(*event)
			ev.msg.Header = nil
			ev.msg.Body = msg.Body
			ev.msg.Header = metadata.New(len(msg.Headers))
			ev.ctx = ctx
			for key, val := range msg.Headers {
				ev.msg.Header.Set(key, val)
			}
			sp.AddEvent("handler start")
			err = s.handler(ev)
			sp.AddEvent("handler stop")
			if err != nil {
				te := time.Since(ts)
				sp.SetStatus(tracer.SpanStatusError, err.Error())
				s.bopts.Meter.Counter(semconv.SubscribeMessageTotal, "endpoint", t, "topic", t, "status", "failure").Inc()
				s.bopts.Meter.Summary(semconv.SubscribeMessageLatencyMicroseconds, "endpoint", t, "topic", t).Update(float64(te.Microseconds()))
				s.bopts.Meter.Histogram(semconv.SubscribeMessageDurationSeconds, "endpoint", t, "topic", t).Update(te.Seconds())
				if eh != nil {
					sp.AddEvent("error handler start")
					_ = eh(ev)
					sp.AddEvent("error handler stop")
				}
				eventPool.Put(ev)
				return
			}
			eventPool.Put(ev)
			te := time.Since(ts)
			s.bopts.Meter.Counter(semconv.SubscribeMessageTotal, "endpoint", t, "topic", t, "status", "success").Inc()
			s.bopts.Meter.Summary(semconv.SubscribeMessageLatencyMicroseconds, "endpoint", t, "topic", t).Update(float64(te.Microseconds()))
			s.bopts.Meter.Histogram(semconv.SubscribeMessageDurationSeconds, "endpoint", t, "topic", t).Update(te.Seconds())
			sp.Finish()
		}
	}
}
