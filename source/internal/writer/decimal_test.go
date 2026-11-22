package writer

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/burnside-project/burnside-go/manifest"
	goparquet "github.com/parquet-go/parquet-go"
)

// readBack opens a written parquet file and returns its rows keyed by column
// name, plus the logical-type decimal annotations per column.
func readBack(t *testing.T, path string, nRows int) ([]goparquet.Row, map[string]int) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	stat, _ := f.Stat()
	pf, err := goparquet.OpenFile(f, stat.Size())
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}
	colIdx := map[string]int{}
	for _, field := range pf.Schema().Fields() {
		leaf, _ := pf.Schema().Lookup(field.Name())
		colIdx[field.Name()] = leaf.ColumnIndex
	}
	pqRows := make([]goparquet.Row, nRows)
	for i := range pqRows {
		pqRows[i] = make(goparquet.Row, len(colIdx))
	}
	rgReader := pf.RowGroups()[0].Rows()
	n, _ := rgReader.ReadRows(pqRows)
	_ = rgReader.Close()
	if n != nRows {
		t.Fatalf("read %d rows, want %d", n, nRows)
	}
	return pqRows, colIdx
}

// TestWriteDeltaParquet_ExactDecimal widened-precision amounts (e.g. after
// ALTER COLUMN ... TYPE numeric(38,10)) must round-trip exactly — a DOUBLE
// cannot represent these values.
func TestWriteDeltaParquet_ExactDecimal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "deltas", "epoch=000001.parquet")

	columns := []manifest.Column{
		{Name: "id", PgType: "bigint", ParquetType: "INT64", Nullable: false},
		{Name: "amount", PgType: "numeric", ParquetType: "DECIMAL", Precision: 38, Scale: 10, Nullable: true},
	}

	rows := []Row{
		{"__op": "I", "id": "1", "amount": "1234567890123456789012345678.1234567891"},
		{"__op": "U", "id": "2", "amount": "-0.0000000001"},
		{"__op": "I", "id": "3", "amount": nil},
	}

	if err := WriteDeltaParquet(path, rows, columns, 1); err != nil {
		t.Fatalf("WriteDeltaParquet: %v", err)
	}

	pqRows, colIdx := readBack(t, path, 3)
	amountIdx := colIdx["amount"]

	want := []string{"1234567890123456789012345678.1234567891", "-0.0000000001"}
	for i, w := range want {
		got := DecimalString(pqRows[i][amountIdx].ByteArray(), 10)
		if got != w {
			t.Errorf("row %d amount = %s, want %s (not exact)", i, got, w)
		}
	}
	if !pqRows[2][amountIdx].IsNull() {
		t.Errorf("row 2 amount should be NULL")
	}
}

// TestColumnError_PrecisionExceeded: a record wider than the manifest schema
// version's declared type must fail the write with a column-attributed error.
func TestColumnError_PrecisionExceeded(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "deltas", "epoch=000002.parquet")

	columns := []manifest.Column{
		{Name: "id", PgType: "bigint", ParquetType: "INT64", Nullable: false},
		// Manifest still at numeric(10,2) while the source was widened.
		{Name: "amount", PgType: "numeric", ParquetType: "DECIMAL", Precision: 10, Scale: 2, Nullable: false},
	}
	rows := []Row{
		{"__op": "I", "id": "1", "amount": "99999999.99"},      // fits numeric(10,2)
		{"__op": "I", "id": "2", "amount": "1234567890123.45"}, // 15 digits > precision 10
	}

	err := WriteDeltaParquet(path, rows, columns, 2)
	if err == nil {
		t.Fatal("expected ColumnError, got nil")
	}
	var colErr *ColumnError
	if !errors.As(err, &colErr) {
		t.Fatalf("want *ColumnError, got %T: %v", err, err)
	}
	if colErr.Column != "amount" {
		t.Errorf("Column = %q, want %q", colErr.Column, "amount")
	}
	if colErr.Row != 1 {
		t.Errorf("Row = %d, want 1", colErr.Row)
	}
	if !strings.Contains(colErr.Error(), "numeric(10,2)") {
		t.Errorf("error should name the declared type: %v", colErr)
	}
	// A blocked batch leaves no publishable file behind.
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Errorf("partial file must be removed on conversion failure")
	}
}

// TestColumnError_ScaleExceeded: fractional digits beyond the column scale
// would require rounding — rejected, not rounded.
func TestColumnError_ScaleExceeded(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "deltas", "epoch=000003.parquet")

	columns := []manifest.Column{
		{Name: "amount", PgType: "numeric", ParquetType: "DECIMAL", Precision: 10, Scale: 2, Nullable: false},
	}
	rows := []Row{{"__op": "I", "amount": "9.999"}}

	var colErr *ColumnError
	if err := WriteDeltaParquet(path, rows, columns, 3); !errors.As(err, &colErr) {
		t.Fatalf("want *ColumnError, got %v", err)
	}
	if !strings.Contains(colErr.Err.Error(), "fractional digits 3") {
		t.Errorf("unexpected reason: %v", colErr.Err)
	}
}

// TestColumnError_NonFinite: NaN/Infinity have no exact decimal form.
func TestColumnError_NonFinite(t *testing.T) {
	dir := t.TempDir()
	columns := []manifest.Column{
		{Name: "amount", PgType: "numeric", ParquetType: "DECIMAL", Precision: 38, Scale: 10, Nullable: false},
	}
	for _, v := range []string{"NaN", "Infinity", "-Infinity"} {
		rows := []Row{{"__op": "I", "amount": v}}
		var colErr *ColumnError
		if err := WriteDeltaParquet(filepath.Join(dir, "e.parquet"), rows, columns, 4); !errors.As(err, &colErr) {
			t.Fatalf("value %q: want *ColumnError, got %v", v, err)
		}
	}
}

// TestColumnError_IntOverflow covers non-decimal strict conversion: an
// integer that overflows the declared column type is a column-level failure.
func TestColumnError_IntOverflow(t *testing.T) {
	dir := t.TempDir()
	columns := []manifest.Column{
		{Name: "qty", PgType: "integer", ParquetType: "INT32", Nullable: false},
	}
	rows := []Row{{"__op": "I", "qty": "3000000000"}}
	var colErr *ColumnError
	if err := WriteDeltaParquet(filepath.Join(dir, "e.parquet"), rows, columns, 5); !errors.As(err, &colErr) {
		t.Fatalf("want *ColumnError, got %v", err)
	}
	if colErr.Column != "qty" {
		t.Errorf("Column = %q, want qty", colErr.Column)
	}
}

// TestColumnError_BadBool: unparseable booleans fail instead of becoming false.
func TestColumnError_BadBool(t *testing.T) {
	dir := t.TempDir()
	columns := []manifest.Column{
		{Name: "active", PgType: "boolean", ParquetType: "BOOLEAN", Nullable: false},
	}
	rows := []Row{{"__op": "I", "active": "maybe"}}
	var colErr *ColumnError
	if err := WriteDeltaParquet(filepath.Join(dir, "e.parquet"), rows, columns, 6); !errors.As(err, &colErr) {
		t.Fatalf("want *ColumnError, got %v", err)
	}
}

// TestDecimalSpec_DefaultForUnboundedNumeric: plain "numeric" without typmod
// uses this version's default DECIMAL(38,18).
func TestDecimalSpec_DefaultForUnboundedNumeric(t *testing.T) {
	p, s := decimalSpec(manifest.Column{PgType: "numeric"})
	if p != 38 || s != 18 {
		t.Errorf("default spec = (%d,%d), want (38,18)", p, s)
	}
	p, s = decimalSpec(manifest.Column{PgType: "numeric(12,4)"})
	if p != 12 || s != 4 {
		t.Errorf("typmod spec = (%d,%d), want (12,4)", p, s)
	}
	p, s = decimalSpec(manifest.Column{PgType: "numeric", Precision: 50, Scale: 8})
	if p != 38 || s != 8 {
		t.Errorf("capped spec = (%d,%d), want (38,8)", p, s)
	}
}

// TestParseDecimal covers the exact-decimal text parser.
func TestParseDecimal(t *testing.T) {
	cases := []struct {
		in        string
		unscaled  string
		scale     int
		wantError bool
	}{
		{"9.99", "999", 2, false},
		{"-9.99", "-999", 2, false},
		{"1.2300", "123", 2, false}, // trailing zeros stripped
		{"0", "0", 0, false},        //
		{"0.000", "0", 0, false},    // zero normalizes
		{"-0.00", "0", 0, false},    // negative zero is zero
		{"1.23E+5", "123000", 0, false},
		{"1.23E-2", "123", 4, false},
		{"5.", "5", 0, false},
		{".5", "5", 1, false},
		{" 12.5 ", "125", 1, false},
		{"12345678901234567890123456789012345678.1234567891", "123456789012345678901234567890123456781234567891", 10, false},
		{"NaN", "", 0, true},
		{"Infinity", "", 0, true},
		{"-inf", "", 0, true},
		{"abc", "", 0, true},
		{"", "", 0, true},
		{".", "", 0, true},
		{"1.2.3", "", 0, true},
		{"1eX", "", 0, true},
	}
	for _, c := range cases {
		u, scale, err := parseDecimal(c.in)
		if c.wantError {
			if err == nil {
				t.Errorf("parseDecimal(%q): want error, got %s/%d", c.in, u, scale)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseDecimal(%q): %v", c.in, err)
			continue
		}
		if u.Text(10) != c.unscaled || scale != c.scale {
			t.Errorf("parseDecimal(%q) = %s/%d, want %s/%d", c.in, u, scale, c.unscaled, c.scale)
		}
	}
}

// TestDecimalRoundTrip verifies the byte encoding against DecimalString.
func TestDecimalRoundTrip(t *testing.T) {
	cases := []struct {
		text      string
		precision int
		scale     int
		want      string
	}{
		{"9.99", 10, 2, "9.99"},
		{"-9.99", 10, 2, "-9.99"},
		{"0", 10, 2, "0.00"},
		{"1", 10, 4, "1.0000"},
		{"1234567890123456789012345678.1234567891", 38, 10, "1234567890123456789012345678.1234567891"},
		{"-1234567890123456789012345678.1234567891", 38, 10, "-1234567890123456789012345678.1234567891"},
		{"99999999999999999999999999999999999999", 38, 0, "99999999999999999999999999999999999999"},
		{"-99999999999999999999999999999999999999", 38, 0, "-99999999999999999999999999999999999999"},
	}
	for _, c := range cases {
		b, err := toDecimalBytes(c.text, c.precision, c.scale)
		if err != nil {
			t.Errorf("toDecimalBytes(%q): %v", c.text, err)
			continue
		}
		if len(b) != 16 {
			t.Errorf("toDecimalBytes(%q): len = %d, want 16", c.text, len(b))
		}
		if got := DecimalString(b, c.scale); got != c.want {
			t.Errorf("round-trip %q = %q, want %q", c.text, got, c.want)
		}
	}
}

// TestFitDecimal_Boundary checks precision/scale edge cases against the
// declared column type.
func TestFitDecimal_Boundary(t *testing.T) {
	// numeric(10,2): 8 integer digits fit, 9 do not.
	if _, err := toDecimalBytes("99999999.99", 10, 2); err != nil {
		t.Errorf("99999999.99 should fit numeric(10,2): %v", err)
	}
	if _, err := toDecimalBytes("999999999.99", 10, 2); err == nil {
		t.Error("999999999.99 must not fit numeric(10,2)")
	}
	// Leading zeros do not count as precision.
	if _, err := toDecimalBytes("0.05", 2, 2); err != nil {
		t.Errorf("0.05 should fit numeric(2,2): %v", err)
	}
	// Widening scale is exact; narrowing is not.
	if _, err := toDecimalBytes("1.2", 10, 4); err != nil {
		t.Errorf("1.2 should fit numeric(10,4): %v", err)
	}
	if _, err := toDecimalBytes("1.2345", 10, 2); err == nil {
		t.Error("1.2345 must not fit numeric(10,2)")
	}
	// Beyond this version's physical limit.
	if _, err := toDecimalBytes("123456789012345678901234567890123456789", 38, 0); err == nil {
		t.Error("39 digits must not fit DECIMAL(38,0)")
	}
}
