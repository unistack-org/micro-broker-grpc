package internal

import (
	"context"
	"fmt"

	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/logger"
)

var subscriberId = NewAutoId()

type Subscriber struct {
	Id              string
	Channel         chan broker.Message
	ctx             context.Context
	unSubSignal     chan *Subscriber
	RegisterChannel chan *broker.Message // TODO change to broker_proto.Message
	messages        []*broker.Message
	log             logger.Logger
}

func (s *Subscriber) SendMessages() {
	for {
		select {
		case <-s.ctx.Done():
			go func() { s.unSubSignal <- s }()
			return
		case msg := <-s.RegisterChannel:
			s.log.Debug(s.ctx, fmt.Sprintf("subscriber: got msg %v for subscriber %s", msg, s.Id))
			s.Channel <- *msg
		}
	}
}

func NewSubscriber(ctx context.Context, ch chan broker.Message, unSubSignal chan *Subscriber, log logger.Logger) *Subscriber {
	newSub := &Subscriber{
		Id:              subscriberId.GetID(),
		Channel:         ch,
		ctx:             ctx,
		unSubSignal:     unSubSignal,
		RegisterChannel: make(chan *broker.Message),
		messages:        make([]*broker.Message, 10),
		log:             log,
	}
	go newSub.SendMessages()
	return newSub
}
