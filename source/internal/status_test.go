package internal

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/burnside-project/pg-cdc-core/internal/config"
	"github.com/burnside-project/pg-cdc-core/internal/ports"
)

// fakeRow lets Status's slot query fail gracefully (slot not found branch).
type fakeRow struct{}

func (fakeRow) Scan(...any) error { return errors.New("no rows") }

type statusSource struct{ fakeSource }

func (statusSource) QueryRow(context.Context, string, ...any) ports.Row { return fakeRow{} }

// captureStdout runs fn with os.Stdout redirected and returns what it wrote.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	defer func() { os.Stdout = old }()

	done := make(chan string)
	go func() {
		out, _ := io.ReadAll(r)
		done <- string(out)
	}()

	fn()
	_ = w.Close()
	return <-done
}

// The status view must locate the original column of a blocked release
// batch: table, column, declared type, value, and reason.
func TestStatusShowsBlockedBatch(t *testing.T) {
	m := testManifest()
	snk := newFakeSink(m)
	st := &fakeStore{blocked: &ports.BlockedBatch{
		EpochNumber:  3,
		TableName:    "public.orders",
		ColumnName:   "amount",
		PgType:       "numeric",
		ValuePreview: `"1234567890123.45"`,
		Reason:       "digit count 15 exceeds precision of numeric(10,2)",
		FirstSeen:    time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC),
		LastSeen:     time.Date(2026, 11, 23, 10, 5, 0, 0, time.UTC),
		Occurrences:  31,
	}}

	cfg := config.Config{}
	cfg.Storage.Path = "/tmp/data"

	out := captureStdout(t, func() {
		if err := Status(context.Background(), cfg, &statusSource{}, snk, st); err != nil {
			t.Errorf("Status: %v", err)
		}
	})

	for _, want := range []string{
		"BLOCKED: epoch 3 is not published",
		"Table:      public.orders",
		"Column:     amount (numeric)",
		`"1234567890123.45"`,
		"digit count 15 exceeds precision of numeric(10,2)",
		"occurrences: 31",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("status output missing %q:\n%s", want, out)
		}
	}
}

// Without a blocked batch the status view stays quiet.
func TestStatusNoBlockedBatch(t *testing.T) {
	m := testManifest()
	snk := newFakeSink(m)
	st := &fakeStore{}

	cfg := config.Config{}
	out := captureStdout(t, func() {
		if err := Status(context.Background(), cfg, &statusSource{}, snk, st); err != nil {
			t.Errorf("Status: %v", err)
		}
	})
	if strings.Contains(out, "BLOCKED") {
		t.Errorf("no block recorded, output must not mention BLOCKED:\n%s", out)
	}
}
