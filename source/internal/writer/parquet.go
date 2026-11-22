// Package writer provides a typed Parquet file writer for CDC output.
// Uses parquet-go (pure Go, no CGO).
package writer

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/burnside-project/burnside-go/manifest"
	btypes "github.com/burnside-project/burnside-go/types"

	goparquet "github.com/parquet-go/parquet-go"
)

// Row is a generic row type for Parquet writing.
type Row = map[string]any

// Decimal limits for this version. Postgres numeric is unbounded, but the
// exact-decimal Parquet encoding used here (16-byte fixed-length byte array,
// two's complement) holds at most 38 decimal digits. A value that does not
// fit the effective column type is rejected with a ColumnError — never
// silently rounded or zeroed.
const (
	// MaxDecimalPrecision is the largest precision representable in the
	// 16-byte fixed-length decimal encoding (floor(log10(2^127)) ≈ 38).
	MaxDecimalPrecision = 38
	// DefaultDecimalPrecision/DefaultDecimalScale apply to numeric columns
	// whose declared type carries no typmod (plain "numeric").
	DefaultDecimalPrecision = 38
	DefaultDecimalScale     = 18

	decimalBytes = 16
)

// ColumnError reports a single value that could not be converted to the
// column's declared Parquet type. Conversion is strict: when a value cannot
// be represented exactly (e.g. a numeric whose precision exceeds the column
// type recorded in the current manifest schema version), the write fails
// instead of silently producing an approximate value. The release batch
// containing the row must be blocked — see the flush path in
// internal/stream.go, which surfaces the column in `pg-cdc status`.
type ColumnError struct {
	Row    int    // index of the row within the batch
	Column string // name of the source column
	PgType string // declared Postgres type from the manifest schema
	Value  string // preview of the offending value (truncated)
	Err    error  // why the value does not fit
}

func (e *ColumnError) Error() string {
	return fmt.Sprintf("column %q (%s), batch row %d: value %s: %v",
		e.Column, e.PgType, e.Row, e.Value, e.Err)
}

func (e *ColumnError) Unwrap() error { return e.Err }

// WriteBaseParquet writes rows to a base Parquet file using parquet-go.
// Numeric columns are written as exact Parquet DECIMAL; temporal/text-like
// columns round-trip as strings. A value that does not fit its declared
// column type fails the whole write with a *ColumnError.
func WriteBaseParquet(path string, rows []Row, columns []manifest.Column) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}

	// Filter out CDC metadata columns
	var dataCols []manifest.Column
	for _, col := range columns {
		if col.Name == btypes.ColOp || col.Name == btypes.ColEpoch || col.Name == btypes.ColDeletedAt {
			continue
		}
		dataCols = append(dataCols, col)
	}

	return writeParquetFile(path, rows, dataCols)
}

// WriteDeltaParquet writes CDC delta rows with __op and __epoch columns.
// Returns a *ColumnError when any value does not fit its declared column
// type; no file is left behind in that case.
func WriteDeltaParquet(path string, rows []Row, columns []manifest.Column, epochNum int64) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	// Prepend __op and __epoch columns
	allCols := []manifest.Column{
		{Name: btypes.ColOp, PgType: "text", ParquetType: "UTF8", Nullable: false},
		{Name: btypes.ColEpoch, PgType: "bigint", ParquetType: "INT64", Nullable: false},
	}
	allCols = append(allCols, columns...)

	// Inject __epoch into each row
	for i := range rows {
		rows[i][btypes.ColEpoch] = epochNum
	}

	return writeParquetFile(path, rows, allCols)
}

func writeParquetFile(path string, rows []Row, columns []manifest.Column) error {
	schema := goparquet.NewSchema("table", makeGroupNode(columns))

	// Build a name→schema column index map. Group is a map so the schema
	// may reorder columns vs. our slice; we must use the schema's indices.
	colIndex := make(map[string]int, len(columns))
	for _, col := range columns {
		leaf, ok := schema.Lookup(col.Name)
		if !ok {
			return fmt.Errorf("column %q not found in parquet schema", col.Name)
		}
		colIndex[col.Name] = leaf.ColumnIndex
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	w := goparquet.NewWriter(f, schema)

	for rowIdx, row := range rows {
		pqRow := make(goparquet.Row, 0, len(columns))
		for _, col := range columns {
			idx := colIndex[col.Name]
			val, err := toParquetValue(row[col.Name], col)
			if err != nil {
				// Remove the partial file: a blocked batch must not
				// leave a publishable artifact behind.
				_ = os.Remove(path)
				return &ColumnError{
					Row:    rowIdx,
					Column: col.Name,
					PgType: col.PgType,
					Value:  valuePreview(row[col.Name]),
					Err:    err,
				}
			}
			defLevel := 0
			if col.Nullable && row[col.Name] != nil {
				defLevel = 1
			}
			pqRow = append(pqRow, val.Level(0, defLevel, idx))
		}
		// Row.Range assumes values are sorted by column index.
		sort.Slice(pqRow, func(i, j int) bool {
			return pqRow[i].Column() < pqRow[j].Column()
		})
		if _, err := w.WriteRows([]goparquet.Row{pqRow}); err != nil {
			_ = os.Remove(path)
			return fmt.Errorf("write row: %w", err)
		}
	}

	if err := w.Close(); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("close parquet writer: %w", err)
	}
	return nil
}

func makeGroupNode(columns []manifest.Column) goparquet.Node {
	group := goparquet.Group{}
	for _, col := range columns {
		var node goparquet.Node
		switch btypes.ParquetType(col.PgType) {
		case "INT32":
			node = goparquet.Leaf(goparquet.Int32Type)
		case "INT64":
			node = goparquet.Leaf(goparquet.Int64Type)
		case "FLOAT":
			node = goparquet.Leaf(goparquet.FloatType)
		case "DOUBLE":
			node = goparquet.Leaf(goparquet.DoubleType)
		case "DECIMAL":
			// Exact decimal: unscaled two's-complement integer in a
			// 16-byte fixed-length array, annotated DECIMAL(p, s).
			precision, scale := decimalSpec(col)
			node = goparquet.Decimal(scale, precision, goparquet.FixedLenByteArrayType(decimalBytes))
		case "BOOLEAN":
			node = goparquet.Leaf(goparquet.BooleanType)
		case "BYTE_ARRAY":
			// bytea: raw binary, no logical type annotation.
			node = goparquet.Leaf(goparquet.ByteArrayType)
		default:
			// UTF8, JSON, UUID, TIMESTAMP_MICROS, DATE, TIME_MICROS and
			// anything else all round-trip as strings in v0.1 (see
			// WriteBaseParquet doc comment). Annotating them as STRING
			// makes DuckDB read them as VARCHAR instead of BLOB.
			node = goparquet.String()
		}
		if col.Nullable {
			node = goparquet.Optional(node)
		}
		group[col.Name] = node
	}
	return group
}

// decimalSpec resolves the effective (precision, scale) of a numeric column:
// the manifest column's recorded typmod first, then a typmod embedded in the
// PgType string ("numeric(10,2)"), then this version's default for unbounded
// numerics. Precision is capped at MaxDecimalPrecision — the physical limit
// of the 16-byte encoding — so a column declared wider than this version can
// represent rejects oversized values instead of truncating them.
func decimalSpec(col manifest.Column) (precision, scale int) {
	p, s := col.Precision, col.Scale
	if p <= 0 {
		if tp, ts, ok := parseNumericTypmod(col.PgType); ok {
			p, s = tp, ts
		}
	}
	if p <= 0 {
		p, s = DefaultDecimalPrecision, DefaultDecimalScale
	}
	if p > MaxDecimalPrecision {
		p = MaxDecimalPrecision
	}
	if s < 0 {
		s = 0
	}
	if s > p {
		s = p
	}
	return p, s
}

// parseNumericTypmod extracts (precision, scale) from a type name like
// "numeric(10,2)". Returns ok=false when no typmod is present.
func parseNumericTypmod(pgType string) (precision, scale int, ok bool) {
	i := strings.IndexByte(pgType, '(')
	if i < 0 {
		return 0, 0, false
	}
	rest := pgType[i+1:]
	j := strings.IndexByte(rest, ')')
	if j < 0 {
		return 0, 0, false
	}
	parts := strings.Split(rest[:j], ",")
	if len(parts) != 2 {
		return 0, 0, false
	}
	p, err1 := strconv.Atoi(strings.TrimSpace(parts[0]))
	s, err2 := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return p, s, true
}

func toParquetValue(val any, col manifest.Column) (goparquet.Value, error) {
	if val == nil {
		return goparquet.NullValue(), nil
	}

	pqType := btypes.ParquetType(col.PgType)
	switch pqType {
	case "INT32":
		n, err := toInt32(val)
		if err != nil {
			return goparquet.Value{}, err
		}
		return goparquet.ValueOf(n), nil
	case "INT64":
		n, err := toInt64(val)
		if err != nil {
			return goparquet.Value{}, err
		}
		return goparquet.ValueOf(n), nil
	case "FLOAT":
		f, err := toFloat32(val)
		if err != nil {
			return goparquet.Value{}, err
		}
		return goparquet.ValueOf(f), nil
	case "DOUBLE":
		f, err := toFloat64(val)
		if err != nil {
			return goparquet.Value{}, err
		}
		return goparquet.ValueOf(f), nil
	case "DECIMAL":
		precision, scale := decimalSpec(col)
		b, err := toDecimalBytes(val, precision, scale)
		if err != nil {
			return goparquet.Value{}, err
		}
		return goparquet.FixedLenByteArrayValue(b), nil
	case "BOOLEAN":
		b, err := toBool(val)
		if err != nil {
			return goparquet.Value{}, err
		}
		return goparquet.ValueOf(b), nil
	case "BYTE_ARRAY":
		return goparquet.ByteArrayValue(toBytes(val)), nil
	default:
		return goparquet.ByteArrayValue([]byte(toString(val))), nil
	}
}

func toInt32(v any) (int32, error) {
	switch val := v.(type) {
	case int32:
		return val, nil
	case int64:
		if val < math.MinInt32 || val > math.MaxInt32 {
			return 0, fmt.Errorf("integer %d overflows int32", val)
		}
		return int32(val), nil
	case int:
		return toInt32(int64(val))
	case float64:
		if val != math.Trunc(val) || val < math.MinInt32 || val > math.MaxInt32 {
			return 0, fmt.Errorf("value %v does not fit int32", val)
		}
		return int32(val), nil
	case string:
		// CDC decoder emits all values as strings (postgres text format).
		n, err := strconv.ParseInt(strings.TrimSpace(val), 10, 32)
		if err != nil {
			return 0, fmt.Errorf("cannot parse %q as int32: %v", val, err)
		}
		return int32(n), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int32", v)
	}
}

func toInt64(v any) (int64, error) {
	switch val := v.(type) {
	case int64:
		return val, nil
	case int32:
		return int64(val), nil
	case int:
		return int64(val), nil
	case float64:
		if val != math.Trunc(val) || val < math.MinInt64 || val > math.MaxInt64 {
			return 0, fmt.Errorf("value %v does not fit int64", val)
		}
		return int64(val), nil
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(val), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("cannot parse %q as int64: %v", val, err)
		}
		return n, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

func toFloat32(v any) (float32, error) {
	switch val := v.(type) {
	case float32:
		return val, nil
	case float64:
		return float32(val), nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(val), 32)
		if err != nil {
			return 0, fmt.Errorf("cannot parse %q as float32: %v", val, err)
		}
		return float32(f), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float32", v)
	}
}

func toFloat64(v any) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case json.Number:
		f, err := val.Float64()
		if err != nil {
			return 0, fmt.Errorf("cannot parse %q as float64: %v", string(val), err)
		}
		return f, nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(val), 64)
		if err != nil {
			return 0, fmt.Errorf("cannot parse %q as float64: %v", val, err)
		}
		return f, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}

func toBool(v any) (bool, error) {
	switch val := v.(type) {
	case bool:
		return val, nil
	case string:
		// Postgres text format for bool is "t" / "f"; ParseBool handles both.
		b, err := strconv.ParseBool(strings.TrimSpace(val))
		if err != nil {
			return false, fmt.Errorf("cannot parse %q as boolean: %v", val, err)
		}
		return b, nil
	default:
		return false, fmt.Errorf("cannot convert %T to boolean", v)
	}
}

func toBytes(v any) []byte {
	switch val := v.(type) {
	case []byte:
		return val
	case string:
		return []byte(val)
	default:
		return []byte(fmt.Sprintf("%v", val))
	}
}

func toString(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case time.Time:
		return val.Format(time.RFC3339Nano)
	default:
		return fmt.Sprintf("%v", val)
	}
}

// valuePreview renders a value for error messages, truncated to keep log
// lines and the state store bounded.
func valuePreview(v any) string {
	s := fmt.Sprintf("%v", v)
	const max = 128
	if len(s) > max {
		s = s[:max] + "…"
	}
	return strconv.Quote(s)
}

// --- Exact decimal ---

// toDecimalBytes converts a value to the 16-byte big-endian two's-complement
// unscaled integer encoding of Parquet DECIMAL(precision, scale). The value
// must fit exactly: excess precision or fractional digits are an error, not
// a rounding opportunity.
func toDecimalBytes(v any, precision, scale int) ([]byte, error) {
	text, err := decimalText(v)
	if err != nil {
		return nil, err
	}
	unscaled, valScale, err := parseDecimal(text)
	if err != nil {
		return nil, err
	}
	fitted, err := fitDecimal(unscaled, valScale, precision, scale)
	if err != nil {
		return nil, err
	}
	return bigIntToBE(fitted, decimalBytes), nil
}

// decimalText renders a value as decimal text for parseDecimal. CDC events
// arrive as strings; COPY and compaction may deliver numbers, and pgx's
// pgtype.Numeric satisfies driver.Valuer.
func decimalText(v any) (string, error) {
	switch val := v.(type) {
	case string:
		return val, nil
	case []byte:
		return string(val), nil
	case json.Number:
		return val.String(), nil
	case float64:
		// Shortest round-trip form; used when re-writing legacy
		// DOUBLE-encoded decimals during compaction.
		return strconv.FormatFloat(val, 'g', -1, 64), nil
	case float32:
		return strconv.FormatFloat(float64(val), 'g', -1, 32), nil
	case int:
		return strconv.Itoa(val), nil
	case int32:
		return strconv.FormatInt(int64(val), 10), nil
	case int64:
		return strconv.FormatInt(val, 10), nil
	case driver.Valuer:
		dv, err := val.Value()
		if err != nil {
			return "", fmt.Errorf("read numeric driver value: %w", err)
		}
		s, ok := dv.(string)
		if !ok {
			return "", fmt.Errorf("unexpected numeric driver value of type %T", dv)
		}
		return s, nil
	default:
		return "", fmt.Errorf("cannot convert %T to decimal", v)
	}
}

// parseDecimal parses a Postgres numeric text value into an unscaled integer
// and a scale (number of fractional digits). Trailing fractional zeros are
// stripped, so the returned scale is the smallest exact one ("1.2300" →
// 123, 2). Postgres emits plain decimal notation; scientific notation is
// also accepted since COPY/pgx may produce it. NaN and Infinity have no
// exact decimal representation and are rejected.
func parseDecimal(s string) (unscaled *big.Int, scale int, err error) {
	t := strings.TrimSpace(s)
	if t == "" {
		return nil, 0, errors.New("empty numeric value")
	}
	switch strings.ToLower(t) {
	case "nan", "inf", "+inf", "-inf", "infinity", "+infinity", "-infinity":
		return nil, 0, fmt.Errorf("non-finite numeric %q has no exact decimal representation", t)
	}

	neg := false
	if t[0] == '+' {
		t = t[1:]
	} else if t[0] == '-' {
		neg = true
		t = t[1:]
	}

	exp := 0
	if i := strings.IndexAny(t, "eE"); i >= 0 {
		e, convErr := strconv.Atoi(t[i+1:])
		if convErr != nil {
			return nil, 0, fmt.Errorf("invalid numeric %q: bad exponent", s)
		}
		exp = e
		t = t[:i]
	}

	intPart, fracPart := t, ""
	if i := strings.IndexByte(t, '.'); i >= 0 {
		intPart, fracPart = t[:i], t[i+1:]
	}
	if intPart == "" && fracPart == "" {
		return nil, 0, fmt.Errorf("invalid numeric %q", s)
	}
	for i := 0; i < len(intPart)+len(fracPart); i++ {
		var c byte
		if i < len(intPart) {
			c = intPart[i]
		} else {
			c = fracPart[i-len(intPart)]
		}
		if c < '0' || c > '9' {
			return nil, 0, fmt.Errorf("invalid numeric %q", s)
		}
	}

	digits := intPart + fracPart
	scale = len(fracPart) - exp
	if scale < 0 {
		digits += strings.Repeat("0", -scale)
		scale = 0
	}
	// Strip trailing fractional zeros: "1.2300" is exactly "1.23".
	for scale > 0 && len(digits) > 0 && digits[len(digits)-1] == '0' {
		digits = digits[:len(digits)-1]
		scale--
	}
	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		digits = "0"
		scale = 0
	}

	unscaled, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return nil, 0, fmt.Errorf("invalid numeric %q", s)
	}
	if neg && unscaled.Sign() != 0 {
		unscaled.Neg(unscaled)
	}
	return unscaled, scale, nil
}

// fitDecimal validates that (unscaled, valScale) fits numeric(precision,
// colScale) exactly and returns the unscaled integer at the column scale.
// Widening the scale (appending zeros) is exact; anything else is an error.
func fitDecimal(unscaled *big.Int, valScale, precision, colScale int) (*big.Int, error) {
	if valScale > colScale {
		return nil, fmt.Errorf("fractional digits %d exceed scale of numeric(%d,%d)",
			valScale, precision, colScale)
	}
	u := unscaled
	if d := colScale - valScale; d > 0 {
		u = new(big.Int).Mul(unscaled, pow10(d))
	}
	if digits := decimalDigits(u); digits > precision {
		return nil, fmt.Errorf("digit count %d exceeds precision of numeric(%d,%d)",
			digits, precision, colScale)
	}
	return u, nil
}

func decimalDigits(n *big.Int) int {
	if n.Sign() == 0 {
		return 1
	}
	return len(new(big.Int).Abs(n).Text(10))
}

func pow10(n int) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}

// bigIntToBE encodes n as a size-byte big-endian two's-complement integer.
// Callers must have range-checked n via fitDecimal (precision ≤ 38 always
// fits 16 bytes).
func bigIntToBE(n *big.Int, size int) []byte {
	out := make([]byte, size)
	m := n
	if n.Sign() < 0 {
		m = new(big.Int).Add(new(big.Int).Lsh(big.NewInt(1), uint(size*8)), n)
	}
	b := m.Bytes()
	copy(out[size-len(b):], b)
	if n.Sign() < 0 {
		for i := 0; i < size-len(b); i++ {
			out[i] = 0xFF
		}
	}
	return out
}

// DecimalString converts a Parquet DECIMAL byte encoding (big-endian
// two's-complement unscaled integer) back to exact decimal text. Used when
// reading decimals back for compaction.
func DecimalString(b []byte, scale int) string {
	if len(b) == 0 {
		return "0"
	}
	n := new(big.Int).SetBytes(b)
	if b[0]&0x80 != 0 {
		n.Sub(n, new(big.Int).Lsh(big.NewInt(1), uint(len(b)*8)))
	}
	neg := n.Sign() < 0
	digits := new(big.Int).Abs(n).Text(10)
	if scale > 0 {
		for len(digits) <= scale {
			digits = "0" + digits
		}
		digits = digits[:len(digits)-scale] + "." + digits[len(digits)-scale:]
	}
	if neg {
		return "-" + digits
	}
	return digits
}
