package internal

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/burnside-project/burnside-go/manifest"

	"github.com/burnside-project/pg-cdc-core/internal/ports"
)

// --- fakes ---

type fakeSink struct {
	files    map[string][]byte
	manifest *manifest.Manifest
}

func newFakeSink(m *manifest.Manifest) *fakeSink {
	return &fakeSink{files: map[string][]byte{}, manifest: m}
}

func (f *fakeSink) WriteFile(_ context.Context, path string, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	f.files[path] = data
	return nil
}

func (f *fakeSink) ReadFile(_ context.Context, path string) (io.ReadCloser, error) {
	data, ok := f.files[path]
	if !ok {
		return nil, io.EOF
	}
	return io.NopCloser(strings.NewReader(string(data))), nil
}

func (f *fakeSink) ListFiles(_ context.Context, prefix string) ([]string, error) {
	var out []string
	for p := range f.files {
		if strings.HasPrefix(p, prefix) {
			out = append(out, p)
		}
	}
	return out, nil
}

func (f *fakeSink) DeleteFile(_ context.Context, path string) error {
	delete(f.files, path)
	return nil
}

func (f *fakeSink) ReadManifest(context.Context) (*manifest.Manifest, error) { return f.manifest, nil }
func (f *fakeSink) WriteManifest(_ context.Context, m *manifest.Manifest) error {
	f.manifest = m
	return nil
}
func (f *fakeSink) Backend() string { return "fake" }

type fakeStore struct {
	blocked *ports.BlockedBatch
}

func (f *fakeStore) GetReplicationState(context.Context) (*ports.ReplicationState, error) {
	return nil, nil
}
func (f *fakeStore) SaveReplicationState(context.Context, *ports.ReplicationState) error {
	return nil
}
func (f *fakeStore) OpenEpoch(_ context.Context, n int64, lsn string) (*ports.EpochState, error) {
	return &ports.EpochState{EpochNumber: n, StartLSN: lsn}, nil
}
func (f *fakeStore) CommitEpoch(context.Context, int64, string, int64) error { return nil }
func (f *fakeStore) GetLatestEpoch(context.Context) (*ports.EpochState, error) {
	return nil, nil
}
func (f *fakeStore) SaveTableState(context.Context, *ports.TableState) error { return nil }
func (f *fakeStore) GetTableState(context.Context, string) (*ports.TableState, error) {
	return nil, nil
}
func (f *fakeStore) SaveCompactionState(context.Context, *ports.CompactionStateRecord) error {
	return nil
}
func (f *fakeStore) SaveBlockedBatch(_ context.Context, bb *ports.BlockedBatch) error {
	cp := *bb
	f.blocked = &cp
	return nil
}
func (f *fakeStore) GetBlockedBatch(context.Context) (*ports.BlockedBatch, error) {
	return f.blocked, nil
}
func (f *fakeStore) ClearBlockedBatch(context.Context) error {
	f.blocked = nil
	return nil
}
func (f *fakeStore) Close() error { return nil }

type fakeSource struct {
	confirmedLSN string
}

func (f *fakeSource) ListTables(context.Context, string) ([]string, error) { return nil, nil }
func (f *fakeSource) GetPrimaryKeys(context.Context, string, string) ([]string, error) {
	return nil, nil
}
func (f *fakeSource) GetTableSchema(context.Context, string, string) ([]ports.ColumnInfo, error) {
	return nil, nil
}
func (f *fakeSource) Version(context.Context) (string, error) { return "test", nil }
func (f *fakeSource) CreateReplicationSlot(context.Context, string) (ports.Snapshot, error) {
	return ports.Snapshot{}, nil
}
func (f *fakeSource) ReleaseSnapshot(context.Context)                           {}
func (f *fakeSource) CreatePublication(context.Context, string, []string) error { return nil }
func (f *fakeSource) CopyTable(context.Context, string, string, string) ([]map[string]any, []ports.ColumnInfo, error) {
	return nil, nil, nil
}
func (f *fakeSource) Stream(context.Context, string, string, string, chan<- ports.CDCEvent) error {
	return nil
}
func (f *fakeSource) ConfirmFlushedLSN(lsn string) error {
	f.confirmedLSN = lsn
	return nil
}
func (f *fakeSource) BuildRoleProfiles(context.Context, []string) (map[string]ports.RoleProfile, error) {
	return nil, nil
}
func (f *fakeSource) DropReplicationSlot(context.Context, string) error { return nil }
func (f *fakeSource) DropPublication(context.Context, string) error     { return nil }
func (f *fakeSource) QueryRow(context.Context, string, ...any) ports.Row {
	return nil
}
func (f *fakeSource) Close() {}

// --- helpers ---

func testManifest() *manifest.Manifest {
	return &manifest.Manifest{
		Version: 1,
		Source:  manifest.Source{Name: "test", InitLSN: "0/100", InitTimestamp: time.Now()},
		Tables: map[string]manifest.Table{
			"public.orders": {
				Status: "active", Path: "public.orders", PrimaryKey: []string{"id"},
			},
		},
		Schemas: map[string]manifest.Schema{
			"public.orders": {
				Version: 1,
				Columns: []manifest.Column{
					{Name: "id", PgType: "bigint", ParquetType: "INT64", Nullable: false},
					{Name: "amount", PgType: "numeric", ParquetType: "DECIMAL", Precision: 10, Scale: 2, Nullable: true},
				},
			},
		},
	}
}

func newTestBuffer(src *fakeSource, snk *fakeSink, st *fakeStore, m *manifest.Manifest) *eventBuffer {
	return &eventBuffer{
		source:   src,
		sink:     snk,
		store:    st,
		manifest: m,
		tables:   m.ActiveTables(),
	}
}

func orderEvent(lsn, amount string) ports.CDCEvent {
	return ports.CDCEvent{
		Table: "public.orders",
		Op:    "I",
		Row:   map[string]any{"id": "1", "amount": amount},
		LSN:   lsn,
	}
}

// --- tests ---

// A batch whose records all fit the current schema version lands: delta file
// uploaded, manifest advanced, LSN confirmed, no block recorded.
func TestFlushValidBatchLands(t *testing.T) {
	m := testManifest()
	src := &fakeSource{}
	snk := newFakeSink(m)
	st := &fakeStore{}
	b := newTestBuffer(src, snk, st, m)

	b.add(orderEvent("0/200", "99999999.99"))
	if err := b.flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if _, ok := snk.files["public.orders/deltas/epoch=000001.parquet"]; !ok {
		t.Errorf("delta file not uploaded; files: %v", snk.files)
	}
	if got := m.Tables["public.orders"].LatestDeltaEpoch; got != 1 {
		t.Errorf("LatestDeltaEpoch = %d, want 1", got)
	}
	if src.confirmedLSN != "0/200" {
		t.Errorf("confirmed LSN = %q, want 0/200", src.confirmedLSN)
	}
	if st.blocked != nil {
		t.Errorf("no block should be recorded: %+v", st.blocked)
	}
	if b.count() != 0 {
		t.Errorf("buffer should be drained, has %d events", b.count())
	}
}

// A record exceeding the manifest schema version's column type blocks its
// whole release batch: nothing lands, the manifest does not advance, the LSN
// is not confirmed, events stay buffered for retry, and the state store
// pinpoints the source column.
func TestFlushBlockedOnOversizedRecord(t *testing.T) {
	m := testManifest()
	src := &fakeSource{}
	snk := newFakeSink(m)
	st := &fakeStore{}
	b := newTestBuffer(src, snk, st, m)

	// amount has 13 integer digits — far beyond numeric(10,2).
	b.add(orderEvent("0/200", "99999999.99"))
	b.add(orderEvent("0/210", "1234567890123.45"))

	err := b.flush(context.Background())
	if err == nil {
		t.Fatal("expected flush to fail")
	}
	if !strings.Contains(err.Error(), "amount") || !strings.Contains(err.Error(), "epoch 1") {
		t.Errorf("error should name epoch and column: %v", err)
	}

	if len(snk.files) != 0 {
		t.Errorf("blocked batch must not land; files: %v", snk.files)
	}
	if got := m.Tables["public.orders"].LatestDeltaEpoch; got != 0 {
		t.Errorf("manifest must not advance, LatestDeltaEpoch = %d", got)
	}
	if src.confirmedLSN != "" {
		t.Errorf("LSN must not be confirmed while blocked, got %q", src.confirmedLSN)
	}
	if b.count() != 2 {
		t.Errorf("events must be rebuffered for retry, count = %d", b.count())
	}

	bb := st.blocked
	if bb == nil {
		t.Fatal("blocked batch must be recorded in the state store")
	}
	if bb.EpochNumber != 1 || bb.TableName != "public.orders" || bb.ColumnName != "amount" {
		t.Errorf("blocked batch should pinpoint the column: %+v", bb)
	}
	if !strings.Contains(bb.Reason, "numeric(10,2)") {
		t.Errorf("reason should name the declared type: %q", bb.Reason)
	}
}

// After the operator widens the manifest column type, the retried batch
// lands and the block clears.
func TestFlushRetryAfterSchemaFix(t *testing.T) {
	m := testManifest()
	src := &fakeSource{}
	snk := newFakeSink(m)
	st := &fakeStore{}
	b := newTestBuffer(src, snk, st, m)

	b.add(orderEvent("0/200", "1234567890123.45"))
	if err := b.flush(context.Background()); err == nil {
		t.Fatal("expected initial block")
	}
	if st.blocked == nil {
		t.Fatal("expected recorded block")
	}

	// Operator action: advance the schema version to the widened type.
	schema := m.Schemas["public.orders"]
	schema.Version++
	schema.Columns[1].Precision = 20
	schema.Columns[1].Scale = 2
	m.Schemas["public.orders"] = schema

	if err := b.flush(context.Background()); err != nil {
		t.Fatalf("retry after schema fix: %v", err)
	}
	if _, ok := snk.files["public.orders/deltas/epoch=000001.parquet"]; !ok {
		t.Error("retried batch should land")
	}
	if st.blocked != nil {
		t.Error("block should clear after a successful flush")
	}
	if src.confirmedLSN != "0/200" {
		t.Errorf("confirmed LSN = %q, want 0/200", src.confirmedLSN)
	}
}

// A bad record in one table blocks the whole epoch — no other table's delta
// lands either.
func TestFlushBlockIsAtomicAcrossTables(t *testing.T) {
	m := testManifest()
	m.Tables["public.customers"] = manifest.Table{
		Status: "active", Path: "public.customers", PrimaryKey: []string{"id"},
	}
	m.Schemas["public.customers"] = manifest.Schema{
		Version: 1,
		Columns: []manifest.Column{
			{Name: "id", PgType: "bigint", ParquetType: "INT64", Nullable: false},
		},
	}

	src := &fakeSource{}
	snk := newFakeSink(m)
	st := &fakeStore{}
	b := newTestBuffer(src, snk, st, m)

	b.add(ports.CDCEvent{Table: "public.customers", Op: "I", Row: map[string]any{"id": "7"}, LSN: "0/200"})
	b.add(orderEvent("0/210", "1234567890123.45")) // exceeds numeric(10,2)

	if err := b.flush(context.Background()); err == nil {
		t.Fatal("expected block")
	}
	if len(snk.files) != 0 {
		t.Errorf("no table's file may land when the epoch is blocked: %v", snk.files)
	}
	if got := m.Tables["public.customers"].LatestDeltaEpoch; got != 0 {
		t.Errorf("customers epoch must not advance: %d", got)
	}
	if b.count() != 2 {
		t.Errorf("all events rebuffered, count = %d", b.count())
	}
}

func TestMaxLSNString(t *testing.T) {
	events := []ports.CDCEvent{
		{LSN: "0/FF"},  // 255
		{LSN: "0/100"}, // 256
		{LSN: "0/200"}, // 512 — string sort would pick "0/FF" instead
		{LSN: "junk"},  // ignored
		{LSN: "1/0"},   // 2^32, greatest
	}
	if got := maxLSNString(events); got != "1/0" {
		t.Errorf("maxLSNString = %q, want 1/0", got)
	}
	if got := maxLSNString(nil); got != "" {
		t.Errorf("empty events = %q, want empty", got)
	}
}
