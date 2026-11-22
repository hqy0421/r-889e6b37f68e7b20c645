package internal

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/burnside-project/burnside-go/epoch"
	"github.com/burnside-project/burnside-go/manifest"
	btypes "github.com/burnside-project/burnside-go/types"

	"github.com/burnside-project/pg-cdc-core/internal/config"
	"github.com/burnside-project/pg-cdc-core/internal/ports"
	"github.com/burnside-project/pg-cdc-core/internal/writer"
)

// Start begins WAL streaming and writes delta Parquet files.
//
// A flush publishes one epoch (the release batch): per-table delta files are
// converted and uploaded, then the manifest advances. Conversion is strict —
// a record that does not fit the column type of the current manifest schema
// version blocks its whole epoch: nothing is published, the LSN is not
// confirmed (so Postgres retains the WAL), and the offending column is
// recorded in the state store where `pg-cdc status` surfaces it. The batch
// is retried on every flush tick until the schema or data issue is resolved.
func Start(ctx context.Context, cfg config.Config, source ports.Source, sink ports.Sink, store ports.StateStore) error {
	defer source.Close()
	if store != nil {
		defer func() { _ = store.Close() }()
	}

	// Load manifest
	m, err := sink.ReadManifest(ctx)
	if err != nil {
		return fmt.Errorf("read manifest (run 'pg-cdc init' first): %w", err)
	}

	slotName := cfg.Replication.Slot
	if slotName == "" {
		slotName = "pg_cdc_slot"
	}
	pubName := cfg.Replication.Publication
	if pubName == "" {
		pubName = "pg_cdc_pub"
	}

	flushInterval := time.Duration(cfg.Flush.IntervalSec) * time.Second
	if flushInterval == 0 {
		flushInterval = 10 * time.Second
	}
	flushMaxRows := cfg.Flush.MaxRows
	if flushMaxRows == 0 {
		flushMaxRows = 1000
	}

	fmt.Printf("Streaming from slot=%s pub=%s (flush: %s / %d rows)\n", slotName, pubName, flushInterval, flushMaxRows)
	fmt.Printf("Starting from LSN: %s\n", m.Source.InitLSN)

	// Start streaming
	events := make(chan ports.CDCEvent, 1000)
	var streamErr error
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		streamErr = source.Stream(ctx, slotName, pubName, m.Source.InitLSN, events)
	}()

	// Buffer and flush
	buffer := &eventBuffer{
		source:   source,
		sink:     sink,
		store:    store,
		manifest: m,
		tables:   m.ActiveTables(),
	}

	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()

	for {
		select {
		case evt, ok := <-events:
			if !ok {
				// Stream closed — flush remaining
				if err := buffer.flush(ctx); err != nil {
					fmt.Printf("flush error: %v\n", err)
				}
				wg.Wait()
				return streamErr
			}
			buffer.add(evt)
			if buffer.count() >= flushMaxRows {
				if err := buffer.flush(ctx); err != nil {
					fmt.Printf("flush error: %v\n", err)
				}
			}

		case <-flushTicker.C:
			if buffer.count() > 0 {
				if err := buffer.flush(ctx); err != nil {
					fmt.Printf("flush error: %v\n", err)
				}
			}

		case <-ctx.Done():
			if err := buffer.flush(ctx); err != nil {
				fmt.Printf("flush error: %v\n", err)
			}
			wg.Wait()
			return ctx.Err()
		}
	}
}

// eventBuffer collects CDC events and flushes them as epoch Parquet files.
type eventBuffer struct {
	source   ports.Source
	sink     ports.Sink
	store    ports.StateStore // nil: blocked batches are logged, not persisted
	manifest *manifest.Manifest
	tables   map[string]manifest.Table
	events   []ports.CDCEvent
	mu       sync.Mutex
}

// evolveSchema checks events for columns not present in the manifest schema
// and refreshes the schema from the source if any are found. New columns get
// appended to preserve existing column ordering in downstream Parquet files.
// Returns true if the schema was updated.
func (b *eventBuffer) evolveSchema(ctx context.Context, tableName string, events []ports.CDCEvent) (bool, error) {
	schema, ok := b.manifest.Schemas[tableName]
	if !ok {
		return false, nil
	}

	known := make(map[string]struct{}, len(schema.Columns))
	for _, c := range schema.Columns {
		known[c.Name] = struct{}{}
	}

	// Collect unknown column names, skipping CDC metadata.
	unknown := make(map[string]struct{})
	for _, evt := range events {
		for k := range evt.Row {
			if k == btypes.ColOp || k == btypes.ColEpoch || k == btypes.ColDeletedAt {
				continue
			}
			if _, ok := known[k]; !ok {
				unknown[k] = struct{}{}
			}
		}
	}
	if len(unknown) == 0 {
		return false, nil
	}

	// Refresh schema from source.
	parts := strings.SplitN(tableName, ".", 2)
	if len(parts) != 2 {
		return false, fmt.Errorf("invalid table name %q", tableName)
	}
	cols, err := b.source.GetTableSchema(ctx, parts[0], parts[1])
	if err != nil {
		return false, fmt.Errorf("refresh schema for %s: %w", tableName, err)
	}

	// Append new columns in source order, preserving existing column order.
	added := 0
	for _, c := range cols {
		if _, ok := known[c.Name]; ok {
			continue
		}
		if _, ok := unknown[c.Name]; !ok {
			continue
		}
		schema.Columns = append(schema.Columns, manifest.Column{
			Name:        c.Name,
			PgType:      c.Type,
			ParquetType: btypes.ParquetType(c.Type),
			Precision:   c.Precision,
			Scale:       c.Scale,
			Nullable:    c.Nullable,
		})
		added++
	}
	if added == 0 {
		return false, nil
	}

	schema.Version++
	b.manifest.Schemas[tableName] = schema
	fmt.Printf("  schema evolved: %s (+%d cols, v=%d)\n", tableName, added, schema.Version)
	return true, nil
}

func (b *eventBuffer) add(evt ports.CDCEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, evt)
}

func (b *eventBuffer) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.events)
}

func (b *eventBuffer) flush(ctx context.Context) error {
	b.mu.Lock()
	if len(b.events) == 0 {
		b.mu.Unlock()
		return nil
	}
	events := b.events
	b.events = nil
	b.mu.Unlock()

	if err := b.publish(ctx, events); err != nil {
		// Put the events back ahead of anything buffered since, so the
		// batch is retried in order on the next flush tick. Nothing was
		// confirmed to the source, so no event is lost.
		b.mu.Lock()
		b.events = append(events, b.events...)
		b.mu.Unlock()
		return err
	}
	return nil
}

// publish flushes one epoch: convert every table's rows and write the delta
// files locally, then upload them, then advance the manifest. Conversion
// happens for all tables before anything is uploaded, so a column-level
// conversion failure blocks the whole epoch before any part of it lands.
func (b *eventBuffer) publish(ctx context.Context, events []ports.CDCEvent) error {
	// Group events by table
	byTable := make(map[string][]ports.CDCEvent)
	for _, evt := range events {
		byTable[evt.Table] = append(byTable[evt.Table], evt)
	}

	// Determine next epoch number
	var maxEpoch int64
	for _, t := range b.manifest.Tables {
		if t.LatestDeltaEpoch > maxEpoch {
			maxEpoch = t.LatestDeltaEpoch
		}
	}
	nextEpoch := maxEpoch + 1

	type pendingFile struct {
		tableName  string
		remotePath string
		tmpPath    string
		rows       int
	}

	// Phase 1: convert and write local delta files for every table.
	var files []pendingFile
	for tableName, tableEvents := range byTable {
		table, ok := b.tables[tableName]
		if !ok {
			fmt.Printf("  skip: %s (not in manifest)\n", tableName)
			continue
		}

		// Detect and absorb schema additions (new columns via ALTER TABLE).
		if _, err := b.evolveSchema(ctx, tableName, tableEvents); err != nil {
			fmt.Printf("  schema refresh failed for %s: %v\n", tableName, err)
		}

		// Build rows with __op
		rows := make([]writer.Row, len(tableEvents))
		for i, evt := range tableEvents {
			row := make(writer.Row, len(evt.Row)+1)
			row[btypes.ColOp] = evt.Op
			for k, v := range evt.Row {
				row[k] = v
			}
			rows[i] = row
		}

		// Get column definitions from manifest schema
		schema, ok := b.manifest.Schemas[tableName]
		if !ok {
			fmt.Printf("  skip: %s (no schema in manifest)\n", tableName)
			continue
		}

		// Write to temp; upload happens only after every table converts.
		deltaFilename := epoch.FormatFilename(nextEpoch)
		remotePath := filepath.Join(table.Path, "deltas", deltaFilename)
		tmpPath := filepath.Join("/tmp", "pgcdc-stream", remotePath)

		if err := writer.WriteDeltaParquet(tmpPath, rows, schema.Columns, nextEpoch); err != nil {
			var colErr *writer.ColumnError
			if errors.As(err, &colErr) {
				b.recordBlocked(ctx, nextEpoch, tableName, colErr)
				return fmt.Errorf("epoch %d blocked: %s: %w", nextEpoch, tableName, colErr)
			}
			return fmt.Errorf("write delta for %s: %w", tableName, err)
		}
		files = append(files, pendingFile{tableName: tableName, remotePath: remotePath, tmpPath: tmpPath, rows: len(tableEvents)})
	}

	// Phase 2: upload every converted file and advance in-memory table state.
	for _, f := range files {
		if err := uploadFile(ctx, b.sink, f.remotePath, f.tmpPath); err != nil {
			return fmt.Errorf("upload delta for %s: %w", f.tableName, err)
		}

		t := b.manifest.Tables[f.tableName]
		t.LatestDeltaEpoch = nextEpoch
		b.manifest.Tables[f.tableName] = t
		b.tables[f.tableName] = t

		fmt.Printf("  epoch %d: %s (%d events)\n", nextEpoch, f.tableName, f.rows)
	}

	// Phase 3: persist the manifest.
	b.manifest.UpdatedAt = time.Now().UTC()
	if err := b.sink.WriteManifest(ctx, b.manifest); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	// Phase 4: the epoch is durable — confirm the batch's highest LSN so
	// Postgres may release its WAL, and clear any previous block.
	if lsn := maxLSNString(events); lsn != "" {
		if err := b.source.ConfirmFlushedLSN(lsn); err != nil {
			fmt.Printf("  warning: confirm flushed LSN: %v\n", err)
		}
	}
	if b.store != nil {
		if err := b.store.ClearBlockedBatch(ctx); err != nil {
			fmt.Printf("  warning: clear blocked batch: %v\n", err)
		}
	}

	return nil
}

// recordBlocked persists the blocked batch with the offending column so the
// operator can locate it from `pg-cdc status`.
func (b *eventBuffer) recordBlocked(ctx context.Context, epochNum int64, tableName string, colErr *writer.ColumnError) {
	fmt.Printf("  epoch %d BLOCKED: %s: %v (batch not published; will retry)\n",
		epochNum, tableName, colErr)
	if b.store == nil {
		return
	}
	bb := &ports.BlockedBatch{
		EpochNumber:  epochNum,
		TableName:    tableName,
		ColumnName:   colErr.Column,
		PgType:       colErr.PgType,
		ValuePreview: colErr.Value,
		Reason:       colErr.Err.Error(),
	}
	if err := b.store.SaveBlockedBatch(ctx, bb); err != nil {
		fmt.Printf("  warning: record blocked batch: %v\n", err)
	}
}

// maxLSNString returns the numerically greatest LSN among events. LSNs are
// Postgres "X/Y" hex positions; string comparison would misorder them.
func maxLSNString(events []ports.CDCEvent) string {
	var best string
	var bestV uint64
	for _, e := range events {
		v, ok := parseLSN(e.LSN)
		if !ok {
			continue
		}
		if best == "" || v > bestV {
			best, bestV = e.LSN, v
		}
	}
	return best
}

func parseLSN(s string) (uint64, bool) {
	hi, lo, ok := strings.Cut(s, "/")
	if !ok {
		return 0, false
	}
	hiV, err1 := strconv.ParseUint(hi, 16, 32)
	loV, err2 := strconv.ParseUint(lo, 16, 32)
	if err1 != nil || err2 != nil {
		return 0, false
	}
	return hiV<<32 | loV, true
}
