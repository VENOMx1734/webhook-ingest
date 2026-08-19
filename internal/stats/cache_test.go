package stats_test

import (
	"sync"
	"testing"

	"github.com/convin/webhook-ingest/internal/stats"
)

func TestCacheRecordAccumulates(t *testing.T) {
	c := stats.NewCache()

	c.Record("acc_1", 30)
	c.Record("acc_1", 12)
	c.Record("acc_2", 5)

	got := c.Get("acc_1")
	if got.CallCount != 2 || got.TotalDurationSec != 42 {
		t.Fatalf("acc_1: got %+v, want CallCount=2 TotalDurationSec=42", got)
	}

	other := c.Get("acc_2")
	if other.CallCount != 1 || other.TotalDurationSec != 5 {
		t.Fatalf("acc_2: got %+v, want CallCount=1 TotalDurationSec=5", other)
	}
}

func TestCacheGetUnknownAccountIsZero(t *testing.T) {
	c := stats.NewCache()
	if got := c.Get("nobody"); got.CallCount != 0 || got.TotalDurationSec != 0 {
		t.Fatalf("got %+v, want zero value", got)
	}
}

// TestCacheRecordIsSafeForConcurrentUse catches the missing lock in Record.
// Every real call comes through concurrent HTTP handlers, so this is not a
// hypothetical: run this with `go test -race` against the unfixed code and
// it reports a data race — several goroutines can race to create the very
// first entry for an accountID via the unsynchronized `c.m[accountID] = s`,
// which Go's map implementation detects as a fatal, unrecoverable
// "concurrent map writes" error, not a normal test failure. Even without
// -race, lost increments can quietly undercount CallCount.
func TestCacheRecordIsSafeForConcurrentUse(t *testing.T) {
	c := stats.NewCache()

	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Record("acc_1", 1)
		}()
	}
	wg.Wait()

	got := c.Get("acc_1")
	if got.CallCount != n {
		t.Fatalf("CallCount = %d, want %d (lost updates under concurrent access)", got.CallCount, n)
	}
}
