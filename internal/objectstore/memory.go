package objectstore

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
)

type Memory struct {
	mu      sync.Mutex
	objects map[string]Object
	seq     int
}

func NewMemory() *Memory {
	return &Memory{objects: map[string]Object{}}
}

func (m *Memory) Get(_ context.Context, key string) (Object, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok := m.objects[key]
	if !ok {
		return Object{}, ErrNotFound
	}
	return obj, nil
}

func (m *Memory) PutIfAbsent(_ context.Context, key string, data []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.objects[key]; exists {
		return "", ErrPreconditionFailed
	}
	return m.store(key, data), nil
}

func (m *Memory) PutIfMatch(_ context.Context, key string, data []byte, etag string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, exists := m.objects[key]
	if !exists || current.ETag != etag {
		return "", ErrPreconditionFailed
	}
	return m.store(key, data), nil
}

func (m *Memory) store(key string, data []byte) string {
	m.seq++
	etag := fmt.Sprintf("\"%d\"", m.seq)
	m.objects[key] = Object{Data: append([]byte(nil), data...), ETag: etag}
	return etag
}

func (m *Memory) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return nil
}

func (m *Memory) List(_ context.Context, prefix string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var keys []string
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (m *Memory) DeletePrefix(_ context.Context, prefix string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			delete(m.objects, k)
		}
	}
	return nil
}
