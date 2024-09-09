package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

func readBody(w http.ResponseWriter, r *http.Request) (Event, error) {
	var e Event
	b, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return e, err
	}
	if err := json.Unmarshal(b, &e); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
	return e, err
}

func parseDateQuery(w http.ResponseWriter, r *http.Request) (from, to time.Time, err error) {
	query := r.URL.Query()
	from, err = time.Parse(time.DateOnly, query.Get("from"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	to, err = time.Parse(time.DateOnly, query.Get("to"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if to.Before(from) {
		err = fmt.Errorf("'from' needs to be before 'to'")
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
	return
}

func upsertEvent(s Store) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if e, err := readBody(w, r); err == nil {
			if err := s.Write(e); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			message := fmt.Sprintf("added entry: %s", e)
			w.Write([]byte(message))
		}
	})
}

func deleteEvent(s Store) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if err := s.Delete(id); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

func queryEvents(s Store) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		from, to, err := parseDateQuery(w, r)
		if err != nil {
			return
		}
		events, err := s.Read(from, to)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(events); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

func StartService(s Store, port int) error {
	mux := http.NewServeMux()
	mux.Handle("DELETE /event/{id}", deleteEvent(s))
	mux.Handle("POST /event", upsertEvent(s))
	mux.Handle("GET /query", queryEvents(s))
	return http.ListenAndServe(fmt.Sprintf(":%d", port), mux)
}
