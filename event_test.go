package main

import (
	"bytes"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_Event(t *testing.T) {
	cases := map[string]struct {
		now   time.Time
		event Event
		want  string
	}{
		"base": {
			now: time.Date(2024, 8, 1, 18, 0, 0, 0, time.Local),
			event: Event{
				Type:    "Formula",
				StartAt: time.Date(2024, 8, 1, 17, 0, 0, 0, time.Local).Unix(),
				Value:   2,
				Unit:    "ml",
			},
			want: "1h0m0s ago\tFormula\t2\tml",
		},
		"with duration": {
			now: time.Date(2024, 8, 1, 18, 0, 0, 0, time.Local),
			event: Event{
				Type:     "Sleep",
				StartAt:  time.Date(2024, 7, 28, 18, 0, 0, 0, time.Local).Unix(),
				Value:    2,
				Unit:     "nope",
				Duration: time.Hour,
			},
			want: "4d ago\tSleep\t60\tminutes",
		},
		"older than 1 day": {
			now: time.Date(2024, 8, 1, 18, 0, 0, 0, time.Local),
			event: Event{
				Type:    "Formula",
				StartAt: time.Date(2024, 7, 28, 18, 0, 0, 0, time.Local).Unix(),
				Value:   2,
				Unit:    "ml",
			},
			want: "4d ago\tFormula\t2\tml",
		},
	}
	for name := range cases {
		tc := cases[name]
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.event.Display(tc.now))
		})
	}
}

func Test_insert(t *testing.T) {
	cases := map[string]struct {
		events, want []Event
		event        Event
	}{
		"empty": {
			want: []Event{
				{StartAt: 1},
			},
			event: Event{StartAt: 1},
		},
		"insert": {
			events: []Event{
				{StartAt: 0},
				{StartAt: 2},
			},
			want: []Event{
				{StartAt: 0},
				{StartAt: 1},
				{StartAt: 2},
			},
			event: Event{StartAt: 1},
		},
		"prepend": {
			events: []Event{
				{StartAt: 1},
				{StartAt: 2},
			},
			want: []Event{
				{StartAt: 0},
				{StartAt: 1},
				{StartAt: 2},
			},
			event: Event{StartAt: 0},
		},
		"append": {
			events: []Event{
				{StartAt: 0},
				{StartAt: 1},
			},
			want: []Event{
				{StartAt: 0},
				{StartAt: 1},
				{StartAt: 2},
			},
			event: Event{StartAt: 2},
		},
		"replace": {
			events: []Event{
				{StartAt: 0},
				{StartAt: 1},
				{StartAt: 2},
			},
			want: []Event{
				{StartAt: 0},
				{StartAt: 1},
				{StartAt: 2, Unit: "h"},
			},
			event: Event{StartAt: 2, Unit: "h"},
		},
	}
	for name := range cases {
		tc := cases[name]
		t.Run(name, func(t *testing.T) {
			got := insert(tc.events, tc.event)
			require.Equal(t, tc.want, got)
		})
	}
}

func checkLast(t *testing.T, stream *eventStream, want Event) {
	last, ok := stream.last()
	require.True(t, ok, "get last event")
	require.Equal(t, want, last, "validate last event")
}

func Test_eventSteam(t *testing.T) {
	stream := NewEventStream()
	_, ok := stream.last()
	require.False(t, ok)
	stream.upsert(Event{StartAt: 0})
	stream.upsert(Event{StartAt: 0})
	checkLast(t, stream, Event{ID: "0", StartAt: 0})
	require.Len(t, stream.events, 1)
	stream.upsert(Event{StartAt: 0, Type: "a"})
	stream.upsert(Event{StartAt: 0, Type: "b"})
	require.Len(t, stream.events, 3)
	checkLast(t, stream, Event{ID: "0b", StartAt: 0, Type: "b"})
	stream.delete("1")
	require.Len(t, stream.events, 3)
	stream.delete("0b")
	require.Len(t, stream.events, 2)
	checkLast(t, stream, Event{ID: "0a", StartAt: 0, Type: "a"})
	stream.upsert(Event{StartAt: 3})
	stream.upsert(Event{StartAt: 4})
	stream.upsert(Event{StartAt: 2})
	stream.upsert(Event{StartAt: 1})
	require.Len(t, stream.events, 6)
	require.True(t, slices.IsSortedFunc(stream.events, ascendOrder))
	require.Equal(t, []Event{
		{ID: "3", StartAt: 3},
		{ID: "4", StartAt: 4},
	}, stream.query(3, 4))
	require.Equal(t, []Event{
		{ID: "3", StartAt: 3},
		{ID: "4", StartAt: 4},
	}, stream.query(3, 10))
	require.Equal(t, []Event{
		{ID: "0", StartAt: 0},
		{ID: "0a", StartAt: 0, Type: "a"},
		{ID: "1", StartAt: 1},
	}, stream.query(0, 1))
	stream.delete("0c")
	require.Equal(t, []Event{
		{ID: "0", StartAt: 0},
		{ID: "0a", StartAt: 0, Type: "a"},
		{ID: "1", StartAt: 1},
	}, stream.query(0, 1))
	stream.delete("0a")
	stream.delete("0")
	require.Equal(t, []Event{{ID: "1", StartAt: 1}}, stream.query(0, 1))
	buf := &bytes.Buffer{}
	require.NoError(t, stream.flush(buf))
	newStream, err := LoadEventStream(buf)
	require.NoError(t, err)
	require.Equal(t, stream.events, newStream.events)
}
