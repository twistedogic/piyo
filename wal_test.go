package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func setupTempFile(t *testing.T) (string, func()) {
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	name := filepath.Join(dir, "wal")
	_, err = os.Create(name)
	require.NoError(t, err)
	return name, func() {
		os.RemoveAll(dir)
	}
}

func checkContent(t *testing.T, path string, content []byte) {
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, string(content), string(b))
}

func Test_WAL(t *testing.T) {
	path, cleanup := setupTempFile(t)
	defer cleanup()
	wal, err := NewWAL(path)
	require.NoError(t, err)
	require.NotNil(t, wal)
	checkContent(t, path, []byte(""))
	addA := Log{Ops: ADD, Event: Event{ID: "a", Type: "a"}}
	require.NoError(t, wal.Append(addA))
	require.NoError(t, wal.Append(addA))
	require.NoError(t, wal.Append(addA))
	checkContent(t, path, []byte(`{"Ops":0,"Event":{"id":"a","type":"a"}}
{"Ops":0,"Event":{"id":"a","type":"a"}}
{"Ops":0,"Event":{"id":"a","type":"a"}}
`))
	require.NoError(t, wal.Compact())
	checkContent(t, path, []byte(`{"Ops":0,"Event":{"id":"a","type":"a"}}
`))
	deleteA := Log{Ops: DELETE, Event: Event{ID: "a", Type: "a"}}
	require.NoError(t, wal.Append(deleteA))
	checkContent(t, path, []byte(`{"Ops":0,"Event":{"id":"a","type":"a"}}
{"Ops":1,"Event":{"id":"a","type":"a"}}
`))
	require.NoError(t, wal.Compact())
	checkContent(t, path, []byte(""))
}
