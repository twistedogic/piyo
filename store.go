package main

import (
	"sync"
	"time"
)

type Store interface {
	Read(from, to time.Time) ([]Event, error)
	Write(Event) error
	Delete(string) error
}

type inMemoryStore struct {
	mu    *sync.RWMutex
	store *eventStream
}

func NewInMemoryStore() Store {
	return inMemoryStore{mu: &sync.RWMutex{}, store: NewEventStream()}
}

func (s inMemoryStore) Write(event Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store.upsert(event)
	return nil
}

func (s inMemoryStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store.delete(id)
	return nil
}

func (s inMemoryStore) Read(start, end time.Time) ([]Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	events := s.store.query(start.Unix(), end.Unix())
	return events, nil
}
