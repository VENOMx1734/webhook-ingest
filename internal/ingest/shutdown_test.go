package ingest_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/config"
	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/redisclient"
	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// TestShutdownWaitsForInFlightRecordingWork simulates a deploy: a webhook
// lands, and — the way main.go now does it — Shutdown is called right
// after, the way it would be moments after a SIGTERM. Recording processing
// must still complete instead of being silently abandoned.
func TestShutdownWaitsForInFlightRecordingWork(t *testing.T) {
	st := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, st)

	cfg := config.Load()
	rdb, err := redisclient.New(context.Background(), cfg.RedisAddr)
	if err != nil {
		t.Fatalf("connect to redis (is `docker compose up` running?): %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := ingest.New(st, stats.NewCache(), rdb, log)

	evt := ingest.Event{
		EventID:      eventID,
		CallID:       callID,
		AccountID:    accountID,
		Status:       "completed",
		DurationSec:  5,
		RecordingURL: "https://recordings.example.com/" + callID + ".wav",
	}
	if err := svc.Ingest(context.Background(), evt); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	// Deliberately called immediately, with no sleep — the whole point is
	// that Shutdown itself must wait, not that we got lucky with timing.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := svc.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	var processed bool
	row := st.Pool().QueryRow(context.Background(),
		`SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !processed {
		t.Fatal("recording was not processed before Shutdown returned — in-flight work was dropped")
	}
}
