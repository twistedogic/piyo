package main

import (
	"encoding/json"
	"io"
	"os"
	"sync"
)

type Ops uint

const (
	ADD Ops = iota
	DELETE
)

type Log struct {
	Ops   Ops
	Event Event
}

type WAL struct {
	path string
	mu   *sync.Mutex
	f    *os.File
}

func NewWAL(name string) (*WAL, error) {
	wal := &WAL{
		path: name,
		mu:   &sync.Mutex{},
	}
	if err := wal.Compact(); err != nil {
		return nil, err
	}
	return wal, nil
}

func (w *WAL) open() error {
	f, err := os.OpenFile(w.path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	w.f = f
	return nil
}

func (w *WAL) Stream() (*eventStream, error) {
	// move offset to beginning of file
	if _, err := w.f.Seek(0, 0); err != nil {
		return nil, err
	}
	dec := json.NewDecoder(w.f)
	stream := NewEventStream()
	for {
		var l Log
		if err := dec.Decode(&l); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		switch l.Ops {
		case ADD:
			stream.upsert(l.Event)
		case DELETE:
			stream.delete(l.Event.ID)
		}
	}
	return stream, nil
}

func (w *WAL) compact() error {
	defer w.open()
	if err := w.f.Sync(); err != nil {
		return err
	}
	stream, err := w.Stream()
	if err != nil {
		return err
	}
	if err := w.f.Close(); err != nil {
		return err
	}
	compacted := w.path + ".compact"
	f, err := os.Create(compacted)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	for _, event := range stream.list() {
		if err := enc.Encode(Log{Ops: ADD, Event: event}); err != nil {
			return err
		}
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(compacted, w.path); err != nil {
		return err
	}
	return nil
}

func (w *WAL) Append(l Log) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	// move offset to end of file
	if _, err := w.f.Seek(0, 2); err != nil {
		return err
	}
	return json.NewEncoder(w.f).Encode(l)
}

func (w *WAL) Compact() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		if err := w.open(); err != nil {
			return err
		}
	}
	return w.compact()
}

func (w *WAL) Write(e Event) error    { return w.Append(Log{Ops: ADD, Event: e}) }
func (w *WAL) Delete(id string) error { return w.Append(Log{Ops: DELETE, Event: Event{ID: id}}) }
