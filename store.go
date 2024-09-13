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

func NewInMemoryStoreWithEventStream(s *eventStream) Store {
	return inMemoryStore{mu: &sync.RWMutex{}, store: s}
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

type walStore struct {
	wal *WAL
	Store
}

func NewWALStore(path string) (Store, error) {
	wal, err := NewWAL(path)
	if err != nil {
		return nil, err
	}
	stream, err := wal.Stream()
	if err != nil {
		return nil, err
	}
	return walStore{wal: wal, Store: NewInMemoryStoreWithEventStream(stream)}, err
}

func (w walStore) Write(e Event) error {
	if err := w.wal.Write(e); err != nil {
		return err
	}
	return w.Store.Write(e)
}

func (w walStore) Delete(id string) error {
	if err := w.wal.Delete(id); err != nil {
		return err
	}
	return w.Store.Delete(id)
}
