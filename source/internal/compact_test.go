package internal

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/burnside-project/burnside-go/manifest"

	"github.com/burnside-project/pg-cdc-core/internal/writer"
)

// writeTempDelta writes a delta parquet file and returns its bytes.
func writeTempDelta(t *testing.T, rows []writer.Row, columns []manifest.Column, epochNum int64) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "delta.parquet")
	if err := writer.WriteDeltaParquet(path, rows, columns, epochNum); err != nil {
		t.Fatalf("WriteDeltaParquet: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// DECIMAL columns written by the streaming path must read back as exact
// decimal text so compaction can re-encode them without precision loss.
func TestReadParquetRows_DecimalRoundTrip(t *testing.T) {
	columns := []manifest.Column{
		{Name: "id", PgType: "bigint", ParquetType: "INT64", Nullable: false},
		{Name: "amount", PgType: "numeric", ParquetType: "DECIMAL", Precision: 38, Scale: 10, Nullable: true},
	}
	rows := []writer.Row{
		{"__op": "I", "id": "1", "amount": "1234567890123456789012345678.1234567891"},
		{"__op": "I", "id": "2", "amount": "-0.0000000001"},
		{"__op": "I", "id": "3", "amount": nil},
	}
	data := writeTempDelta(t, rows, columns, 1)

	snk := newFakeSink(testManifest())
	snk.files["public.orders/deltas/epoch=000001.parquet"] = data

	got, err := readParquetRows(context.Background(), snk, "public.orders/deltas/epoch=000001.parquet")
	if err != nil {
		t.Fatalf("readParquetRows: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("rows = %d, want 3", len(got))
	}

	want := []any{"1234567890123456789012345678.1234567891", "-0.0000000001", nil}
	for i, w := range want {
		if got[i]["amount"] != w {
			t.Errorf("row %d amount = %#v, want %#v", i, got[i]["amount"], w)
		}
	}
	// id comes back as int64 for the PK keying path.
	if got[0]["id"] != int64(1) {
		t.Errorf("row 0 id = %#v, want int64(1)", got[0]["id"])
	}
}

// Full compaction over exact-decimal deltas produces a base file whose
// amounts survive exactly.
func TestCompactTable_ExactDecimal(t *testing.T) {
	m := testManifest()
	m.Tables["public.orders"] = manifest.Table{
		Status: "active", Path: "public.orders", PrimaryKey: []string{"id"},
		BaseEpoch: 0, LatestDeltaEpoch: 1,
	}
	schema := m.Schemas["public.orders"]
	schema.Columns[1].Precision = 38
	schema.Columns[1].Scale = 10
	m.Schemas["public.orders"] = schema

	snk := newFakeSink(m)
	snk.files["public.orders/deltas/epoch=000001.parquet"] = writeTempDelta(t,
		[]writer.Row{
			{"__op": "I", "id": "1", "amount": "98765432109876543210.0987654321"},
			{"__op": "I", "id": "2", "amount": "1.5"},
		},
		schema.Columns, 1)

	if err := compactTable(context.Background(), snk, m, "public.orders", m.Tables["public.orders"], m.Schemas["public.orders"]); err != nil {
		t.Fatalf("compactTable: %v", err)
	}

	base, err := readParquetRows(context.Background(), snk, "public.orders/base/base_000001.parquet")
	if err != nil {
		t.Fatalf("read new base: %v", err)
	}
	if len(base) != 2 {
		t.Fatalf("base rows = %d, want 2", len(base))
	}
	amounts := map[int64]string{}
	for _, r := range base {
		id, _ := r["id"].(int64)
		amt, _ := r["amount"].(string)
		amounts[id] = amt
	}
	if amounts[1] != "98765432109876543210.0987654321" {
		t.Errorf("amount 1 = %q, want exact", amounts[1])
	}
	if amounts[2] != "1.5000000000" {
		t.Errorf("amount 2 = %q, want 1.5000000000 (column scale)", amounts[2])
	}
}
