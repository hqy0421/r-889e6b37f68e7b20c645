package internal

import (
	"context"
	"fmt"

	"github.com/burnside-project/pg-cdc-core/internal/config"
	"github.com/burnside-project/pg-cdc-core/internal/ports"
)

// Status shows replication health: slot status, lag, LSN, epochs, tables.
// When a release batch is blocked by a column-level conversion failure, the
// offending source column is shown so the operator can locate and fix it.
// store may be nil (state store unavailable); the blocked-batch section is
// then skipped.
func Status(ctx context.Context, cfg config.Config, source ports.Source, sink ports.Sink, store ports.StateStore) error {
	defer source.Close()
	if store != nil {
		defer func() { _ = store.Close() }()
	}

	m, err := sink.ReadManifest(ctx)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}

	slotName := cfg.Replication.Slot
	if slotName == "" {
		slotName = "pg_cdc_slot"
	}

	// Query slot status
	var slotActive bool
	var confirmedLSN, restartLSN string
	err = source.QueryRow(ctx,
		`SELECT active, confirmed_flush_lsn::text, restart_lsn::text
		 FROM pg_replication_slots WHERE slot_name = $1`, slotName).
		Scan(&slotActive, &confirmedLSN, &restartLSN)

	fmt.Println("pg-cdc status")
	fmt.Println()

	if err != nil {
		fmt.Printf("  Slot: %s (not found)\n", slotName)
	} else {
		activeStr := "inactive"
		if slotActive {
			activeStr = "streaming"
		}
		fmt.Printf("  Slot:          %s (%s)\n", slotName, activeStr)
		fmt.Printf("  Confirmed LSN: %s\n", confirmedLSN)
		fmt.Printf("  Restart LSN:   %s\n", restartLSN)
	}

	fmt.Printf("  Init LSN:      %s\n", m.Source.InitLSN)
	fmt.Printf("  Source:         %s\n", m.Source.Name)
	fmt.Println()

	var active, excluded int
	var maxDelta int64
	for _, t := range m.Tables {
		if t.Status == "active" {
			active++
			if t.LatestDeltaEpoch > maxDelta {
				maxDelta = t.LatestDeltaEpoch
			}
		} else {
			excluded++
		}
	}

	fmt.Printf("  Tables:         %d active, %d excluded\n", active, excluded)
	fmt.Printf("  Latest epoch:   %d\n", maxDelta)
	fmt.Printf("  Storage:        %s (%s)\n", cfg.Storage.Path, sink.Backend())

	if !m.Compaction.LastCompactedAt.IsZero() {
		fmt.Printf("  Last compacted: %s\n", m.Compaction.LastCompactedAt.Format("2006-01-02 15:04:05"))
	}

	if len(m.Roles) > 0 {
		fmt.Printf("  Role profiles:  %d\n", len(m.Roles))
	}

	if store != nil {
		printBlockedBatch(ctx, store)
	}

	return nil
}

// printBlockedBatch renders the currently blocked release batch, if any.
func printBlockedBatch(ctx context.Context, store ports.StateStore) {
	bb, err := store.GetBlockedBatch(ctx)
	if err != nil {
		fmt.Printf("  Blocked batch:  (state unreadable: %v)\n", err)
		return
	}
	if bb == nil {
		return
	}

	fmt.Println()
	fmt.Printf("  BLOCKED: epoch %d is not published\n", bb.EpochNumber)
	fmt.Printf("    Table:      %s\n", bb.TableName)
	fmt.Printf("    Column:     %s (%s)\n", bb.ColumnName, bb.PgType)
	fmt.Printf("    Value:      %s\n", bb.ValuePreview)
	fmt.Printf("    Reason:     %s\n", bb.Reason)
	fmt.Printf("    First seen: %s\n", bb.FirstSeen.Format("2006-01-02 15:04:05"))
	fmt.Printf("    Last seen:  %s (occurrences: %d)\n", bb.LastSeen.Format("2006-01-02 15:04:05"), bb.Occurrences)
	fmt.Println("    The batch is retried every flush tick; it publishes once the")
	fmt.Println("    column type in the manifest schema covers the value (or the")
	fmt.Println("    source value is fixed). WAL is retained while blocked.")
}
