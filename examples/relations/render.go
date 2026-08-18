package relations

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// A log book's whole point is that somebody can be handed it. Everything
// else in this package is about writing the record correctly; this is
// about being able to read it without a Go program -- the page laid out
// the way the paper original was, with its columns, its line numbers,
// its struck-through lines still legible, and the signatures at the
// foot.
//
// It is a reading path, not a hot one: rendering a page reads that
// page's lines and their records. Rendering a book means rendering its
// pages.

// timeLayout is how every timestamp in a rendered page is written --
// minute resolution in UTC, which is what a log book's own columns held.
const timeLayout = "2006-01-02 15:04"

// RenderPage lays one page out as text: a heading, the ruled columns in
// the order they were declared, one row per line, and a foot recording
// how the page was closed.
//
// A struck line is shown, not hidden -- its number in parentheses and
// what happened to it in the last column -- because that is what a
// strike-through is for. Countersignatures appear there too.
func (j *Journal) RenderPage(ctx context.Context, page uint8) (string, error) {
	fields, err := j.Fields(ctx)
	if err != nil {
		return "", err
	}
	lines, err := j.Page(ctx, page)
	if err != nil {
		return "", err
	}

	headings := []string{"#"}
	for _, field := range fields {
		name, err := j.fieldName(ctx, field)
		if err != nil {
			return "", err
		}
		headings = append(headings, name)
	}
	headings = append(headings, "signed", "at", "notes")

	rows := make([][]string, 0, len(lines))
	names := make(map[Entity]string)
	for _, line := range lines {
		row, err := j.renderLine(ctx, line, fields, names)
		if err != nil {
			return "", err
		}
		rows = append(rows, row)
	}

	var out strings.Builder
	fmt.Fprintf(&out, "log %d, page %d -- %s\n\n", j.st.Log, page, plural(len(lines), "line"))
	if len(rows) == 0 {
		out.WriteString("(no lines)\n")
	} else {
		writeTable(&out, headings, rows, numericColumns(rows))
	}

	signoff, closed, err := j.PageStatus(ctx, page)
	if err != nil {
		return "", err
	}
	out.WriteString("\n")
	if closed {
		who := signoff.Name
		if who == "" {
			who = signoff.By.String()
		}
		fmt.Fprintf(&out, "page closed by %s at %s, %s\n", who, signoff.At.UTC().Format(timeLayout), plural(int(signoff.Lines), "line"))
	} else {
		out.WriteString("page open\n")
	}
	return out.String(), nil
}

// renderLine turns one line into its row of cells: line number, one cell
// per declared column, who signed it and when, and what has happened to
// it since.
func (j *Journal) renderLine(ctx context.Context, line Entity, fields []Entity, names map[Entity]string) ([]string, error) {
	row, err := j.Row(ctx, line)
	if err != nil {
		return nil, err
	}
	byField := make(map[Entity]string, len(row))
	for _, cell := range row {
		switch {
		case cell.Numeric:
			byField[cell.Field] = strconv.FormatInt(cell.Number, 10)
		default:
			byField[cell.Field] = cell.Text
		}
	}

	decl, found, err := j.st.Declaration(ctx, line)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("relations: render: line %s is not declared", line)
	}
	author, err := j.actorName(ctx, decl.Record.Author, names)
	if err != nil {
		return nil, err
	}

	status, err := j.Status(ctx, line)
	if err != nil {
		return nil, err
	}
	notes, err := j.renderNotes(ctx, line, status, names)
	if err != nil {
		return nil, err
	}

	number := strconv.Itoa(int(line.ID))
	if !status.Live() {
		number = "(" + number + ")"
	}
	cells := make([]string, 0, len(fields)+4)
	cells = append(cells, number)
	for _, field := range fields {
		cells = append(cells, byField[field])
	}
	return append(cells, author, decl.Record.Created.UTC().Format(timeLayout), strings.Join(notes, "; ")), nil
}

// renderNotes is the last column: what happened to this line after it
// was written.
func (j *Journal) renderNotes(ctx context.Context, line Entity, status EntryStatus, names map[Entity]string) ([]string, error) {
	var notes []string
	switch status.State {
	case StateSuperseded:
		notes = append(notes, "superseded by "+j.lineRef(line, status.Replacement))
	case StateVoided:
		note := "voided"
		if !status.Reason.IsZero() {
			info, err := j.resolveTerm(ctx, status.Reason)
			if err != nil {
				return nil, err
			}
			note += " (" + info.Text + ")"
		}
		notes = append(notes, note)
	}

	signatures, err := j.Countersignatures(ctx, line)
	if err != nil {
		return nil, err
	}
	if len(signatures) > 0 {
		who := make([]string, 0, len(signatures))
		for _, signature := range signatures {
			name := signature.Name
			if name == "" {
				name = signature.Actor.String()
			}
			who = append(who, name)
		}
		sort.Strings(who)
		notes = append(notes, "countersigned by "+strings.Join(who, ", "))
	}

	// A line that supersedes an earlier one says so, so a reader
	// following a correction backwards does not have to guess.
	rels, err := j.st.Relations(ctx, line)
	if err != nil {
		return nil, err
	}
	for _, rel := range OfKind(rels, KindSupersedes) {
		notes = append(notes, "replaces "+j.lineRef(line, rel.B))
	}
	return notes, nil
}

// lineRef names another line as briefly as it can: by number if it is on
// the same page, by page and number otherwise.
func (j *Journal) lineRef(from, to Entity) string {
	if from.Page == to.Page {
		return "line " + strconv.Itoa(int(to.ID))
	}
	return fmt.Sprintf("page %d line %d", to.Page, to.ID)
}

// actorName resolves an actor to what their declaration calls them,
// caching within one render.
func (j *Journal) actorName(ctx context.Context, actor Entity, names map[Entity]string) (string, error) {
	if name, ok := names[actor]; ok {
		return name, nil
	}
	decl, found, err := j.st.Declaration(ctx, actor)
	if err != nil {
		return "", err
	}
	name := actor.String()
	if found && decl.Record.Name != "" {
		name = decl.Record.Name
	}
	names[actor] = name
	return name, nil
}

// numericColumns reports which columns to right-align: the line number,
// and any column whose cells are all numbers. Quantities read as
// quantities only when their digits line up, and working that out from
// the cells rather than from the schema keeps a column of numbers
// aligned whatever it was declared as.
func numericColumns(rows [][]string) func(col int) bool {
	numeric := map[int]bool{0: true}
	seen := map[int]bool{}
	for _, row := range rows {
		for col, cell := range row {
			if col == 0 || cell == "" {
				continue
			}
			if _, err := strconv.ParseInt(cell, 10, 64); err != nil {
				numeric[col] = false
				continue
			}
			if !seen[col] {
				seen[col], numeric[col] = true, true
			}
		}
	}
	return func(col int) bool { return numeric[col] }
}

// writeTable lays rows out under headings, padding every column to its
// widest cell. right reports which columns to right-align.
func writeTable(out *strings.Builder, headings []string, rows [][]string, right func(int) bool) {
	widths := make([]int, len(headings))
	for i, heading := range headings {
		widths[i] = len(heading)
	}
	for _, row := range rows {
		for i, cell := range row {
			if i < len(widths) && len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}

	// Every line is padded to the full width, the rule included, so the
	// heading, the rule and every row are the same length and the
	// columns line up whatever is in them.
	writeRow := func(cells []string) {
		for i, cell := range cells {
			if i > 0 {
				out.WriteString(" | ")
			}
			out.WriteString(pad(cell, widths[i], right(i)))
		}
		out.WriteString("\n")
	}

	writeRow(headings)
	rule := make([]string, len(headings))
	for i := range rule {
		rule[i] = strings.Repeat("-", widths[i])
	}
	out.WriteString(strings.Join(rule, "-+-") + "\n")
	for _, row := range rows {
		writeRow(row)
	}
}

func pad(s string, width int, right bool) string {
	if len(s) >= width {
		return s
	}
	fill := strings.Repeat(" ", width-len(s))
	if right {
		return fill + s
	}
	return s + fill
}

func plural(n int, noun string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, noun)
	}
	return fmt.Sprintf("%d %ss", n, noun)
}

// RenderBook lays out every page of the log in order, followed by the
// state of the chain -- which is the difference between handing somebody
// a printout and handing them a record they can check.
func (j *Journal) RenderBook(ctx context.Context) (string, error) {
	var out strings.Builder
	for page := int(FirstEntryPage); page <= 0xFF; page++ {
		_, reached, err := j.st.PageAllocated(ctx, uint8(page), TypeEntry)
		if err != nil {
			return "", err
		}
		if !reached {
			break
		}
		text, err := j.RenderPage(ctx, uint8(page))
		if err != nil {
			return "", err
		}
		if out.Len() > 0 {
			out.WriteString("\n")
		}
		out.WriteString(text)
	}
	if out.Len() == 0 {
		out.WriteString("(empty log)\n")
	}

	events, err := j.VerifyChain(ctx)
	out.WriteString("\n")
	if err != nil {
		fmt.Fprintf(&out, "chain: BROKEN after %s -- %v\n", plural(events, "event"), err)
		return out.String(), nil
	}
	fmt.Fprintf(&out, "chain: %s, verified %s\n", plural(events, "event"), j.st.now().UTC().Format(timeLayout))
	return out.String(), nil
}
