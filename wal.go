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
	mu *sync.Mutex
	f  *os.File
}

func NewWAL(name string) (WAL, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0644)
	return WAL{
		mu: &sync.Mutex{},
		f:  f,
	}, err
}

func (w WAL) Append(l Log) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return json.NewEncoder(w.f).Encode(l)
}

func (w WAL) Compact() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	info, err := w.f.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	dec := json.NewDecoder(w.f)
	stream := NewEventStream()
	for {
		var l Log
		if err := dec.Decode(&l); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		switch l.Ops {
		case ADD:
			stream.upsert(l.Event)
		case DELETE:
			stream.delete(l.Event.ID)
		}
	}
	if err := w.f.Truncate(size); err != nil {
		return err
	}
	enc := json.NewEncoder(w.f)
	for _, event := range stream.list() {
		if err := enc.Encode(Log{Ops: ADD, Event: event}); err != nil {
			return err
		}
	}
	return w.f.Sync()
}
