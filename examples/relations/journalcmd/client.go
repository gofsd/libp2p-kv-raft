package journalcmd

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
)

// This is the submitting side: what a phone, a browser bridge or a
// script does. It is deliberately thin -- build the request, submit it
// through the ordinary catalog path so the FSM checks the submitter's
// standing, then wait for the answer the service records.
//
// Nothing here needs write access to the journal's keyspace, which is
// the entire point: a submitter that cannot write a journal key can
// still write a line, because the node that owns the log does it on the
// strength of a permission-checked request.

// defaultAwaitPoll and defaultAwaitTimeout pace Await. The dispatcher is
// poked over Execute the moment a request lands, so the usual wait is
// one poll interval, not one timeout.
const (
	defaultAwaitPoll    = 100 * time.Millisecond
	defaultAwaitTimeout = 30 * time.Second
)

// Submit sends one request to commandID and returns the instance id it
// was dispatched under. The submitter's standing is checked inside the
// FSM (see the package doc), so a peer without it gets an error here and
// writes nothing.
func Submit(commandID string, req Request) (string, error) {
	inputs, err := json.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("journalcmd: encode request: %w", err)
	}
	instanceID, err := kvctl.SubmitCommand(commandID, string(inputs))
	if err != nil {
		return "", err
	}
	return instanceID, nil
}

// Await waits for the service's answer to instanceID and returns it.
//
// It returns the Result even when the service refused, with Error set --
// a refusal is an answer, not a failure to get one. The error return is
// for not getting an answer at all: a timeout, or a log entry that does
// not parse.
func Await(instanceID string, timeout time.Duration) (Result, error) {
	if timeout <= 0 {
		timeout = defaultAwaitTimeout
	}
	deadline := time.Now().Add(timeout)
	for {
		entry, err := kvctl.LatestCommandLog(instanceID)
		if err == nil && entry.Fields[FieldStatus] != kvctl.CommandStatusRunning {
			var result Result
			if raw := entry.Fields[FieldResult]; raw != "" {
				if err := json.Unmarshal([]byte(raw), &result); err != nil {
					return Result{}, fmt.Errorf("journalcmd: answer to %s does not parse: %w", instanceID, err)
				}
				return result, nil
			}
			return Result{Error: entry.Narrative}, nil
		}
		if time.Now().After(deadline) {
			return Result{}, fmt.Errorf("journalcmd: no answer to %s within %s", instanceID, timeout)
		}
		time.Sleep(defaultAwaitPoll)
	}
}

// Do submits a request and waits for its answer -- the one call a
// form-driven client actually makes.
func Do(commandID string, req Request, timeout time.Duration) (Result, error) {
	instanceID, err := Submit(commandID, req)
	if err != nil {
		return Result{}, err
	}
	return Await(instanceID, timeout)
}

// FetchForm asks commandID for the schema to draw a form from.
func FetchForm(commandID string, timeout time.Duration) (Form, error) {
	result, err := Do(commandID, Request{Op: OpForm}, timeout)
	if err != nil {
		return Form{}, err
	}
	if result.Error != "" {
		return Form{}, fmt.Errorf("journalcmd: %s", result.Error)
	}
	if result.Form == nil {
		return Form{}, fmt.Errorf("journalcmd: %s answered with no form", commandID)
	}
	return *result.Form, nil
}

// AppendLine submits a filled form -- column heading to value as text,
// exactly the shape a form posts -- and returns where the line landed.
func AppendLine(commandID string, cells map[string]string, timeout time.Duration) (string, error) {
	return writeLine(commandID, Request{Op: OpAppend, Cells: cells}, timeout)
}

// CorrectLine submits a replacement for line.
func CorrectLine(commandID, line string, cells map[string]string, timeout time.Duration) (string, error) {
	return writeLine(commandID, Request{Op: OpCorrect, Line: line, Cells: cells}, timeout)
}

// VoidLine strikes line through, with an optional reason drawn from the
// service's reason vocabulary.
func VoidLine(commandID, line, reason string, timeout time.Duration) error {
	result, err := Do(commandID, Request{Op: OpVoid, Line: line, Reason: reason}, timeout)
	if err != nil {
		return err
	}
	if result.Error != "" {
		return fmt.Errorf("journalcmd: %s", result.Error)
	}
	return nil
}

// RenderPage asks for a page as text; page 0 means the one being
// written.
func RenderPage(commandID string, page uint8, timeout time.Duration) (string, error) {
	result, err := Do(commandID, Request{Op: OpRender, Page: page}, timeout)
	if err != nil {
		return "", err
	}
	if result.Error != "" {
		return "", fmt.Errorf("journalcmd: %s", result.Error)
	}
	return result.Text, nil
}

func writeLine(commandID string, req Request, timeout time.Duration) (string, error) {
	result, err := Do(commandID, req, timeout)
	if err != nil {
		return "", err
	}
	if result.Error != "" {
		return "", fmt.Errorf("journalcmd: %s", result.Error)
	}
	return result.Line, nil
}
