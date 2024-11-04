package internal

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"go.unistack.org/micro/v3/logger"
	"go.unistack.org/micro/v3/store"
)

var (
	ErrNoExist = errors.New("message not exist")
)

var _ store.Store = (*topicStorage)(nil)

type topicStorage struct {
	rw     sync.RWMutex
	logger logger.Logger
	topics map[string]*Topic
}

func NewStorage(l logger.Logger) store.Store {
	return &topicStorage{
		rw:     sync.RWMutex{},
		logger: l,
		topics: map[string]*Topic{},
	}
}

func (m *topicStorage) Name() string {
	return "topic-storage"
}

func (m *topicStorage) Init(opts ...store.Option) error {
	m.logger.Debug(context.Background(), "not implement")
	return nil
}

func (m *topicStorage) Connect(ctx context.Context) error {
	m.logger.Debug(ctx, "not implement")
	return nil
}

func (m *topicStorage) Options() store.Options {
	m.logger.Debug(context.Background(), "not implement")
	return store.NewOptions()
}

func (m *topicStorage) Exists(ctx context.Context, key string, opts ...store.ExistsOption) error {
	m.rw.RLock()
	defer m.rw.RUnlock()
	_, ok := m.topics[key]
	if ok {
		return nil
	}
	return ErrNoExist
}

func (m *topicStorage) Read(ctx context.Context, key string, val interface{}, opts ...store.ReadOption) error {
	m.rw.RLock()
	defer m.rw.RUnlock()
	msg, ok := m.topics[key]
	if ok {
		val = msg
		return nil
	}
	return ErrNoExist
}

func (m *topicStorage) Write(ctx context.Context, key string, val interface{}, opts ...store.WriteOption) error {
	m.rw.Lock()
	defer m.rw.Unlock()
	msg, ok := val.(*Topic)
	if !ok {
		return errors.New("value should be broker.Message")
	}
	m.topics[key] = msg
	return nil
}

func (m *topicStorage) Delete(ctx context.Context, key string, opts ...store.DeleteOption) error {
	m.rw.Lock()
	defer m.rw.Unlock()
	delete(m.topics, key)
	return nil
}

func (m *topicStorage) List(ctx context.Context, opts ...store.ListOption) ([]string, error) {
	m.rw.Lock()
	defer m.rw.Unlock()
	rsp := make([]string, 0, 2*len(m.topics))
	for k, v := range m.topics {
		buf, err := json.Marshal(v)
		if err != nil {
			return rsp, err
		}
		rsp = append(rsp, k, string(buf))
	}
	return rsp, nil
}

func (m *topicStorage) Disconnect(ctx context.Context) error {
	m.logger.Debug(ctx, "not implement")
	return nil
}

func (m *topicStorage) String() string {
	return "TopicStorage"
}

type TopicStorage struct {
	topics map[string]*Topic
	rw     sync.RWMutex
}

func (ts *TopicStorage) GetTopic(name string) (*Topic, bool) {
	ts.rw.RLock()
	defer ts.rw.RUnlock()
	topic, ok := ts.topics[name]
	return topic, ok
}

func (ts *TopicStorage) CreateTopic(ctx context.Context, name string, log logger.Logger) *Topic {
	ts.rw.Lock()
	defer ts.rw.Unlock()
	newTopic := NewTopic(ctx, name, log)
	ts.topics[name] = newTopic
	return newTopic
}

func NewTopicStorage() *TopicStorage {
	return &TopicStorage{
		topics: map[string]*Topic{},
	}
}
