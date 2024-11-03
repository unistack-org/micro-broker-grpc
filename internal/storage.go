package internal

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/logger"
	"go.unistack.org/micro/v3/store"
)

var (
	ErrNoExist = errors.New("message not exist")
)

var _ store.Store = (*mapStorage)(nil)

type mapStorage struct {
	rw       sync.RWMutex
	logger   logger.Logger
	messages map[string]*broker.Message
}

func NewStorage(l logger.Logger) store.Store {
	return &mapStorage{
		rw:       sync.RWMutex{},
		logger:   l,
		messages: make(map[string]*broker.Message, 0),
	}
}

func (m *mapStorage) Name() string {
	return "in-memory-storage"
}

func (m *mapStorage) Init(opts ...store.Option) error {
	m.logger.Debug(context.Background(), "not implement")
	return nil
}

func (m *mapStorage) Connect(ctx context.Context) error {
	m.logger.Debug(ctx, "not implement")
	return nil
}

func (m *mapStorage) Options() store.Options {
	m.logger.Debug(context.Background(), "not implement")
	return store.NewOptions()
}

func (m *mapStorage) Exists(ctx context.Context, key string, opts ...store.ExistsOption) error {
	m.rw.Lock()
	defer m.rw.Unlock()
	_, ok := m.messages[key]
	if ok {
		return nil
	}
	return ErrNoExist
}

func (m *mapStorage) Read(ctx context.Context, key string, val interface{}, opts ...store.ReadOption) error {
	m.rw.Lock()
	defer m.rw.Unlock()
	msg, ok := m.messages[key]
	if ok {
		val = *msg
		return nil
	}
	return ErrNoExist
}

func (m *mapStorage) Write(ctx context.Context, key string, val interface{}, opts ...store.WriteOption) error {
	m.rw.Lock()
	defer m.rw.Unlock()
	msg, ok := val.(broker.Message)
	if !ok {
		return errors.New("value should be broker.Message")
	}
	m.messages[key] = &msg
	return nil
}

func (m *mapStorage) Delete(ctx context.Context, key string, opts ...store.DeleteOption) error {
	m.rw.Lock()
	defer m.rw.Unlock()
	delete(m.messages, key)
	return nil
}

func (m *mapStorage) List(ctx context.Context, opts ...store.ListOption) ([]string, error) {
	m.rw.Lock()
	defer m.rw.Unlock()
	rsp := make([]string, 0, 2*len(m.messages))
	for k, v := range m.messages {
		buf, err := json.Marshal(v)
		if err != nil {
			return rsp, err
		}
		rsp = append(rsp, k, string(buf))
	}
	return rsp, nil
}

func (m *mapStorage) Disconnect(ctx context.Context) error {
	m.logger.Debug(ctx, "not implement")
	return nil
}

func (m *mapStorage) String() string {
	return "mapStorage"
}
