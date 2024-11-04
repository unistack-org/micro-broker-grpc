package internal

import (
	"context"
	"fmt"
	"sync"

	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/logger"
)

var (
	messageID = NewAutoId()

	HeaderMessageId = "Micro-Message-Id"
)

type Topic struct {
	m             sync.Mutex
	Name          string
	Subscribers   map[string]*Subscriber
	subDeleteChan chan *Subscriber
	subAddChan    chan *Subscriber
	msgPubChan    chan *broker.Message
	limitMessage  int
	log           logger.Logger
	ctx           context.Context
}

func (t *Topic) RegisterSubscriber(ctx context.Context) chan broker.Message {
	ch := make(chan broker.Message, t.limitMessage)
	newSub := NewSubscriber(ctx, ch, t.subDeleteChan, t.log)
	t.subAddChan <- newSub
	return ch
}

func (t *Topic) PublishMessage(ctx context.Context, msg broker.Message) error {
	messageId := messageID.GetID()
	t.log.Debug(ctx, fmt.Sprintf("topic: get id for message: %s", messageID))
	msg.Header.Set(HeaderMessageId, messageId)

	t.msgPubChan <- &msg
	t.log.Debug(ctx, "topic: send msg to queue")
	return nil
}

func (t *Topic) actionListener() {
	for {
		select {
		case newSub := <-t.subAddChan:
			//При появлении нового подписчика на топик добавляем его в мапу
			t.log.Debug(t.ctx, fmt.Sprintf("topic: new subscriber %s", newSub.Id))
			t.Subscribers[newSub.Id] = newSub
		case subscriber := <-t.subDeleteChan:
			//Удаляем подписчика
			t.log.Debug(t.ctx, fmt.Sprintf("topic: delete subscriber %s", subscriber.Id))
			delete(t.Subscribers, subscriber.Id)
		case msg := <-t.msgPubChan:
			//Вычитываем сообщения из канала и перекладываем в очереди подписчиков
			t.log.Debug(t.ctx, fmt.Sprintf("topic: new msg %v to topic %s", msg, t.Name))
			var wg sync.WaitGroup
			for _, sub := range t.Subscribers {
				wg.Add(1)
				s := sub
				go func() {
					t.log.Debug(t.ctx, "topic: send msg to register channel ")
					s.RegisterChannel <- msg
					wg.Done()
				}()
			}
			wg.Wait()
		}
	}
}

func NewTopic(ctx context.Context, name string, log logger.Logger) *Topic {
	newTopic := &Topic{
		Name:          name,
		Subscribers:   map[string]*Subscriber{},
		subDeleteChan: make(chan *Subscriber, 3),
		subAddChan:    make(chan *Subscriber),
		msgPubChan:    make(chan *broker.Message),
		limitMessage:  50,
		log:           log,
		ctx:           ctx,
	}
	go newTopic.actionListener()
	return newTopic
}
