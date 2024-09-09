package main

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strconv"
	"time"
)

type Event struct {
	ID       string        `json:"id"`
	Who      string        `json:"who,omitempty"`
	Type     string        `json:"type,omitempty"`  // event type
	StartAt  int64         `json:"when,omitempty"`  // start time unix
	Value    int           `json:"value,omitempty"` // quantity
	Unit     string        `json:"unit,omitempty"`  // if unit is s, this is a range event
	Duration time.Duration `json:"duration,omitempty"`
}

func setID(e Event) Event {
	e.ID = e.id()
	return e
}

func (e Event) id() string {
	if e.ID != "" {
		return e.ID
	}
	return strconv.FormatInt(e.StartAt, 10) + e.Type + e.Who
}

func (e Event) value() (int, string) {
	v, u := e.Value, e.Unit
	if e.Duration > 0 {
		v = int(e.Duration.Minutes())
		u = "minutes"
	}
	return v, u
}

func (e Event) String() string {
	v, u := e.value()
	return fmt.Sprintf(
		"%s\t%s\t%s\t%d\t%s",
		time.Unix(e.StartAt, 0).Format(time.DateTime),
		e.Who, e.Type, v, u,
	)
}

func (e Event) Display(now time.Time) string {
	at := time.Unix(e.StartAt, 0)
	when := at.Format(time.DateTime)
	diff := now.Sub(at).Truncate(time.Minute)
	switch {
	case diff < 5*time.Hour:
		when = diff.String() + " ago"
	default:
		when = fmt.Sprintf("%dd ago", int(diff.Hours())/24)
	}
	v, u := e.value()
	return fmt.Sprintf("%s\t%s\t%d\t%s", when, e.Type, v, u)
}

func byWho(left, right Event) int {
	switch {
	case left.Who > right.Who:
		return 1
	case left.Who < right.Who:
		return -1
	}
	return 0
}

func byType(left, right Event) int {
	switch {
	case left.Type > right.Type:
		return 1
	case left.Type < right.Type:
		return -1
	}
	return 0
}

func byTime(left, right Event) int {
	switch {
	case left.StartAt > right.StartAt:
		return 1
	case left.StartAt < right.StartAt:
		return -1
	}
	return 0
}

func byID(left, right Event) int {
	switch {
	case left.ID > right.ID:
		return 1
	case left.ID < right.ID:
		return -1
	}
	return 0
}

type EventCompare func(Event, Event) int

func chainCompare(cmps ...EventCompare) EventCompare {
	return func(left, right Event) int {
		for _, c := range cmps {
			if i := c(left, right); i != 0 {
				return i
			}
		}
		return 0
	}
}

func ascendOrder(left, right Event) int {
	return chainCompare(byTime, byWho, byType)(left, right)
}

func insert(events []Event, event Event) []Event {
	if len(events) == 0 {
		return []Event{event}
	}
	idx, found := slices.BinarySearchFunc(events, event, ascendOrder)
	if !found {
		events = slices.Insert(events, idx, event)
	}
	events[idx] = event
	return events
}

type eventStream struct {
	events []Event
}

func LoadEventStream(r io.Reader) (*eventStream, error) {
	dec := json.NewDecoder(r)
	events := []Event{}
	for {
		var e Event
		if err := dec.Decode(&e); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return NewEventStream(events...), nil
}

func NewEventStream(events ...Event) *eventStream {
	if !slices.IsSortedFunc(events, ascendOrder) {
		slices.SortFunc(events, ascendOrder)
	}
	return &eventStream{events: events}
}

func (e *eventStream) delete(id string) {
	if idx := slices.IndexFunc(e.events, func(evt Event) bool {
		return evt.ID == id
	}); idx >= 0 {
		e.events = slices.Delete(e.events, idx, idx+1)
	}
}

func (e *eventStream) update(event Event) {
	e.delete(event.ID)
	e.events = insert(e.events, event)
}

func (e *eventStream) insert(event Event) { e.events = insert(e.events, event) }

func (e *eventStream) upsert(event Event) {
	if event.ID != "" {
		e.update(event)
	} else {
		e.insert(setID(event))
	}
}

func (e *eventStream) last() (Event, bool) {
	if len(e.events) == 0 {
		return Event{}, false
	}
	return e.events[len(e.events)-1], true
}

func (e *eventStream) query(from, to int64) []Event {
	start, end := Event{StartAt: from}, Event{StartAt: to}
	if to < from {
		start, end = end, start
	}
	startIdx, _ := slices.BinarySearchFunc(e.events, start, ascendOrder)
	endIdx, _ := slices.BinarySearchFunc(e.events, end, ascendOrder)
	if endIdx >= len(e.events) {
		endIdx = len(e.events) - 1
	}
	return e.events[startIdx : endIdx+1]
}

func (e *eventStream) flush(w io.Writer) error {
	enc := json.NewEncoder(w)
	for _, e := range e.events {
		if err := enc.Encode(&e); err != nil {
			return err
		}
	}
	return nil
}

func (e *eventStream) list() []Event { return e.events }

type Select struct {
	Who, Type map[string]struct{}
}

func NewSelect(w, t []string) Select {
	wm := make(map[string]struct{})
	tm := make(map[string]struct{})
	for _, s := range w {
		wm[s] = struct{}{}
	}
	for _, s := range t {
		tm[s] = struct{}{}
	}
	return Select{Who: wm, Type: tm}
}

func (s Select) match(e Event) bool {
	if len(s.Who) != 0 {
		if _, ok := s.Who[e.Who]; !ok {
			return false
		}
	}
	if len(s.Type) != 0 {
		if _, ok := s.Type[e.Type]; !ok {
			return false
		}
	}
	return true
}

func (s Select) Filter(events []Event) []Event {
	filtered := make([]Event, 0, len(events))
	for _, e := range events {
		if s.match(e) {
			filtered = append(filtered, e)
		}
	}
	return filtered
}
