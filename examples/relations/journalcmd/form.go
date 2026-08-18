package journalcmd

import (
	"context"
	"fmt"

	"github.com/gofsd/libp2p-kv-raft/examples/relations"
)

// Form is what a client needs to draw the page in front of somebody: the
// columns of this log, in the order they were declared, what each one
// holds, and -- for a column whose vocabulary is closed -- exactly which
// values it will accept.
//
// It is generated from the journal's own schema rather than written
// alongside it, which is the point: the form a phone draws and the rules
// the log enforces cannot drift apart, because they are the same
// declarations. A column closed after the form was fetched refuses the
// value at submit time too (see relations.ErrFieldClosed); a form is a
// description, never a promise.
type Form struct {
	// Log and Page are which book this is and which page a line
	// submitted now would land on.
	Log  uint8 `json:"log"`
	Page uint8 `json:"page"`
	// PageClosed says the page has been signed off, so a line submitted
	// now starts the next one.
	PageClosed bool     `json:"page_closed,omitempty"`
	Columns    []Column `json:"columns"`
}

// Column is one field of the form.
type Column struct {
	// Field is the column's entity, in the form ParseEntity reads back;
	// Name is its heading, and what a submitted line keys its values by.
	Field string `json:"field"`
	Name  string `json:"name"`
	// Input is "term", "number" or "text" -- a dropdown, a number box, a
	// free-text box.
	Input string `json:"input"`
	// Closed reports a vocabulary that admits nothing new, in which case
	// Options is the whole of what this column accepts. An open term
	// column's Options are what has been used so far: worth offering,
	// but not a restriction.
	Closed  bool     `json:"closed,omitempty"`
	Options []Option `json:"options,omitempty"`
}

// Option is one admissible value of a term column.
type Option struct {
	Term string `json:"term"`
	Text string `json:"text"`
}

// Form reads the journal's schema and returns the form for it.
//
// Provenance columns are left out: a submitter does not fill in who they
// are or which request this was, because the service records those
// itself from the dispatch it is answering (see Service).
func (s *Service) Form(ctx context.Context) (Form, error) {
	j := s.Journal
	fields, err := j.Fields(ctx)
	if err != nil {
		return Form{}, err
	}

	page, closed, err := s.currentPage(ctx)
	if err != nil {
		return Form{}, err
	}
	form := Form{Log: j.Store().Log, Page: page, PageClosed: closed}

	for _, field := range fields {
		name, err := j.FieldName(ctx, field)
		if err != nil {
			return Form{}, err
		}
		if s.isProvenanceColumn(name) {
			continue
		}
		input, err := j.FieldInput(ctx, field)
		if err != nil {
			return Form{}, err
		}
		column := Column{Field: field.String(), Name: name, Input: input.String()}
		if input == relations.InputTerm {
			if column.Closed, err = j.FieldIsClosed(ctx, field); err != nil {
				return Form{}, err
			}
			vocab, err := j.Vocabulary(ctx, field)
			if err != nil {
				return Form{}, err
			}
			for _, term := range vocab {
				column.Options = append(column.Options, Option{Term: term.Term.String(), Text: term.Text})
			}
		}
		form.Columns = append(form.Columns, column)
	}
	return form, nil
}

// currentPage is which page a line submitted now would land on, and
// whether the page the log is otherwise on has been signed off.
func (s *Service) currentPage(ctx context.Context) (page uint8, closed bool, err error) {
	st := s.Journal.Store()
	for p := int(relations.FirstEntryPage); p <= 0xFF; p++ {
		shut, err := st.PageIsClosed(ctx, uint8(p), relations.TypeEntry)
		if err != nil {
			return 0, false, err
		}
		if shut {
			closed = true
			continue
		}
		last, _, err := st.PageAllocated(ctx, uint8(p), relations.TypeEntry)
		if err != nil {
			return 0, false, err
		}
		if last == 0xFF {
			continue // full
		}
		return uint8(p), closed && p > int(relations.FirstEntryPage), nil
	}
	return 0, closed, fmt.Errorf("journalcmd: this log has no page left to write on")
}
