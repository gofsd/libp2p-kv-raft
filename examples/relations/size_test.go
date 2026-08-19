package relations_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/examples/relations"
)

// bookSize totals every key and value a book occupies, across all three
// of this package's namespaces -- what the replicated store actually
// carries, before SQLite's own per-row overhead.
func bookSize(t *testing.T, be relations.Backend) (records, bytes int) {
	t.Helper()
	for _, ns := range []byte{relations.NamespaceRelation, relations.NamespaceIndex, relations.NamespacePresence} {
		start, end := relations.NamespaceBounds(ns)
		pairs, err := be.Scan(context.Background(), start, end, 0)
		if err != nil {
			t.Fatalf("Scan: %v", err)
		}
		for _, p := range pairs {
			records++
			bytes += len(p.Key) + len(p.Value)
		}
	}
	return records, bytes
}

// TestBookSizeStaysBounded measures what a line of this log costs and
// holds that cost still.
//
// A book is capped at 65025 lines, so its size is settled entirely by the
// per-line cost, and that cost is worth defending: every record carries a
// 64-byte signature on top of a 9-byte key, so roughly seven bytes in ten
// of a full book are Ed25519 signatures, and anything that quietly adds a
// second record per cell doubles a book on every device replicating it --
// phones included, where a book is not obviously affordable.
//
// The record count is asserted exactly, because it is a structural fact
// rather than a measurement: two records per cell (the forward record and
// its mirror), plus the line's own declaration and its two chain records.
// The byte figure gets generous headroom, since it is only meant to catch
// a doubling, not to freeze the record layout.
func TestBookSizeStaysBounded(t *testing.T) {
	ctx := context.Background()
	const (
		terms = 20
		lines = 600
	)

	for _, columns := range []int{3, 5, 10} {
		t.Run(fmt.Sprintf("%dcol", columns), func(t *testing.T) {
			be := relations.Memory()
			book := openBook(t, be, 1)

			fields := make([]relations.Entity, columns)
			for c := range fields {
				field, err := book.DefineField(ctx, fmt.Sprintf("col%02d", c), relations.InputTerm)
				if err != nil {
					t.Fatalf("DefineField: %v", err)
				}
				fields[c] = field
				for v := 0; v < terms; v++ {
					if _, err := book.Term(ctx, field, fmt.Sprintf("c%02d-value-%02d", c, v)); err != nil {
						t.Fatalf("Term: %v", err)
					}
				}
			}
			schemaRecords, schemaBytes := bookSize(t, be)

			for i := 0; i < lines; i++ {
				cells := make([]relations.Cell, 0, columns)
				for c, field := range fields {
					cells = append(cells, relations.TermCell(field, fmt.Sprintf("c%02d-value-%02d", c, i%terms)))
				}
				if _, err := book.Append(ctx, cells...); err != nil {
					t.Fatalf("Append line %d: %v", i, err)
				}
			}
			totalRecords, totalBytes := bookSize(t, be)

			addedRecords := totalRecords - schemaRecords
			perLineRecords := float64(addedRecords) / float64(lines)
			perLineBytes := float64(totalBytes-schemaBytes) / float64(lines)

			// A full book, for whoever is sizing a device: the schema is
			// paid once, the per-line cost 65025 times.
			const maxLines = 255 * 255
			t.Logf("%d columns: schema %d records / %d bytes; line %.1f records / %.0f bytes; full book %.0f MiB",
				columns, schemaRecords, schemaBytes, perLineRecords, perLineBytes,
				(float64(schemaBytes)+perLineBytes*maxLines)/(1024*1024))

			// Two fixed costs ride along: the running head, written once
			// and overwritten thereafter, and one allocator counter per
			// entry page the lines spilled across.
			pages := (lines + 254) / 255
			if want := lines*(3+2*columns) + 1 + pages; addedRecords != want {
				t.Errorf("%d lines cost %d records, want exactly %d (%d per line -- 2 per cell, a declaration and 2 chain records -- plus the running head and %d page counters)",
					lines, addedRecords, want, 3+2*columns, pages)
			}
			// Measured at ~362 + 186*columns bytes; this allows well over
			// a third again on both terms before it complains.
			if ceiling := float64(500 + 250*columns); perLineBytes > ceiling {
				t.Errorf("a line costs %.0f bytes, over the %.0f-byte ceiling: a full book would be %.0f MiB",
					perLineBytes, ceiling, perLineBytes*maxLines/(1024*1024))
			}
		})
	}
}
