// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// recordingTimeout bounds how long background recording processing is
// allowed to run. It is intentionally independent of the HTTP request that
// triggered it — the request may finish (and its context be canceled) long
// before this background work is done.
const recordingTimeout = 10 * time.Second

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger

	// wg tracks recording-processing goroutines started by Ingest, so
	// Shutdown can wait for in-flight work instead of abandoning it when
	// the process exits.
	wg sync.WaitGroup
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log}
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	// IngestEvent is one atomic write: it stores the event, upserts the
	// call, and increments the account aggregate together, and it is safe
	// to call twice with the same event_id — a redelivery is a no-op.
	inserted, err := s.store.IngestEvent(ctx, rec)
	if err != nil {
		return err
	}
	if !inserted {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}

	s.cache.Record(rec.AccountID, rec.DurationSec)

	// Recordings are slow to fetch, so that part does not block the
	// provider. It deliberately does NOT reuse the request's context: once
	// this handler returns, net/http cancels r.Context(), which would kill
	// this work before it had a chance to run. It gets its own bounded
	// context instead, and wg lets Shutdown wait for it to finish on
	// deploy rather than abandoning it when the process exits.
	if rec.RecordingURL != "" {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			bgCtx, cancel := context.WithTimeout(context.Background(), recordingTimeout)
			defer cancel()
			if err := s.processRecording(bgCtx, rec); err != nil {
				s.log.Error("process recording failed",
					"call_id", rec.CallID, "event_id", rec.EventID, "err", err)
			}
		}()
	}

	return nil
}

// Shutdown waits for in-flight recording processing to finish, or for ctx
// to be done, whichever comes first. Call it after the HTTP server has
// stopped accepting new requests, so no new background work can start while
// this is draining.
func (s *Service) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	time.Sleep(recordingWork)
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}
