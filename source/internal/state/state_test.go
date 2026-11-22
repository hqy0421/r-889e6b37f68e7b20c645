package state

import (
	"context"
	"path/filepath"
	"testing"
)

func TestOpenAndMigrate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()
}

func TestOpenCreatesParentDir(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "dir", "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open with nested dir: %v", err)
	}
	defer func() { _ = s.Close() }()
}

func TestLockStateTable(t *testing.T) {
	s := openTestDB(t)
	// lock_state table should exist after migration — the systemd
	// ExecStartPre runs "DELETE FROM lock_state" before starting.
	_, err := s.db.Exec("DELETE FROM lock_state")
	if err != nil {
		t.Fatalf("lock_state table should exist: %v", err)
	}
}

func TestReplicationState(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	// Initially nil
	rs, err := s.GetReplicationState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rs != nil {
		t.Fatal("expected nil")
	}

	// Save
	err = s.SaveReplicationState(ctx, &ReplicationState{
		SlotName:        "pg_cdc_slot",
		PublicationName: "pg_cdc_pub",
		ConfirmedLSN:    "0/16B3740",
		Status:          "streaming",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Read back
	rs, err = s.GetReplicationState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rs.ConfirmedLSN != "0/16B3740" {
		t.Errorf("LSN = %q", rs.ConfirmedLSN)
	}
	if rs.Status != "streaming" {
		t.Errorf("status = %q", rs.Status)
	}

	// Update
	rs.ConfirmedLSN = "0/16B4000"
	rs.Status = "stopped"
	if err := s.SaveReplicationState(ctx, rs); err != nil {
		t.Fatal(err)
	}

	rs2, _ := s.GetReplicationState(ctx)
	if rs2.ConfirmedLSN != "0/16B4000" {
		t.Errorf("updated LSN = %q", rs2.ConfirmedLSN)
	}
}

func TestEpochState(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	// No epochs initially
	latest, err := s.GetLatestEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if latest != nil {
		t.Fatal("expected nil")
	}

	// Open epoch
	ep, err := s.OpenEpoch(ctx, 1, "0/1000")
	if err != nil {
		t.Fatal(err)
	}
	if ep.EpochNumber != 1 {
		t.Errorf("epoch = %d", ep.EpochNumber)
	}
	if ep.Status != "open" {
		t.Errorf("status = %q", ep.Status)
	}

	// Commit
	if err := s.CommitEpoch(ctx, ep.ID, "0/2000", 42); err != nil {
		t.Fatal(err)
	}

	// Get latest
	latest, _ = s.GetLatestEpoch(ctx)
	if latest.Status != "committed" {
		t.Errorf("status = %q", latest.Status)
	}
	if latest.RowCount != 42 {
		t.Errorf("row_count = %d", latest.RowCount)
	}
}

func TestTableState(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	// Save
	err := s.SaveTableState(ctx, &TableState{
		TableName:       "public.orders",
		LastSnapshotLSN: "0/1000",
		RowCount:        100,
		SchemaVersion:   1,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Read
	ts, err := s.GetTableState(ctx, "public.orders")
	if err != nil {
		t.Fatal(err)
	}
	if ts.RowCount != 100 {
		t.Errorf("row_count = %d", ts.RowCount)
	}

	// Not found
	ts2, _ := s.GetTableState(ctx, "nonexistent")
	if ts2 != nil {
		t.Error("expected nil for nonexistent")
	}
}

func TestCompactionState(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	err := s.SaveCompactionState(ctx, &CompactionStateRecord{
		TableName:      "public.orders",
		LastBaseEpoch:  5,
		TombstoneCount: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBlockedBatch(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	// Initially none
	bb, err := s.GetBlockedBatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if bb != nil {
		t.Fatal("expected no blocked batch")
	}

	// Save
	err = s.SaveBlockedBatch(ctx, &BlockedBatch{
		EpochNumber:  7,
		TableName:    "public.orders",
		ColumnName:   "amount",
		PgType:       "numeric",
		ValuePreview: `"12345678901234567890.99"`,
		Reason:       "digit count 22 exceeds precision of numeric(10,2)",
	})
	if err != nil {
		t.Fatal(err)
	}

	bb, err = s.GetBlockedBatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if bb == nil {
		t.Fatal("expected blocked batch")
	}
	if bb.EpochNumber != 7 || bb.TableName != "public.orders" || bb.ColumnName != "amount" {
		t.Errorf("unexpected blocked batch: %+v", bb)
	}
	if bb.Occurrences != 1 {
		t.Errorf("occurrences = %d, want 1", bb.Occurrences)
	}
	if bb.FirstSeen.IsZero() || bb.LastSeen.IsZero() {
		t.Error("first/last seen should be set")
	}

	// Same block again → occurrence counter bumps, first_seen retained.
	if err := s.SaveBlockedBatch(ctx, &BlockedBatch{
		EpochNumber: 7, TableName: "public.orders", ColumnName: "amount",
		PgType: "numeric", Reason: "same",
	}); err != nil {
		t.Fatal(err)
	}
	bb2, _ := s.GetBlockedBatch(ctx)
	if bb2.Occurrences != 2 {
		t.Errorf("occurrences = %d, want 2", bb2.Occurrences)
	}
	if !bb2.FirstSeen.Equal(bb.FirstSeen) {
		t.Errorf("first_seen changed on repeat block: %v → %v", bb.FirstSeen, bb2.FirstSeen)
	}

	// A different block resets the window.
	if err := s.SaveBlockedBatch(ctx, &BlockedBatch{
		EpochNumber: 9, TableName: "public.orders", ColumnName: "qty",
		PgType: "integer", Reason: "overflow",
	}); err != nil {
		t.Fatal(err)
	}
	bb3, _ := s.GetBlockedBatch(ctx)
	if bb3.Occurrences != 1 || bb3.ColumnName != "qty" || bb3.EpochNumber != 9 {
		t.Errorf("window should reset for a different block: %+v", bb3)
	}

	// Clear
	if err := s.ClearBlockedBatch(ctx); err != nil {
		t.Fatal(err)
	}
	bb4, err := s.GetBlockedBatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if bb4 != nil {
		t.Error("expected cleared blocked batch")
	}
}

func openTestDB(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
