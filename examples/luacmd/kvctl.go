package luacmd

import (
	"context"
	"fmt"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
)

// The desktop adapter: Cluster and Registrar over pkg/kvctl, against
// whichever node `mage use` selected. mobile/kvmobile has its own, against
// its in-process daemon; everything above these two is shared.
//
// pkg/kvctl's functions take no context and apply their own IPC timeout
// internally, so what these can honour is cancellation up to the point of
// the call rather than during it -- the same accommodation
// examples/croncmd's own Kvctl submitter makes, and for the same reason.

// Registrar is the catalog half of registering a Lua command: writing the
// Command record that names a script and pinning its hash, and linking it
// to the group that decides who may run it. Separate from Cluster because
// a runner never needs it -- a runner serves commands, it does not create
// them -- and because these two calls are the voter-gated ones.
type Registrar interface {
	// PutCommand creates or updates the Command record, spec included.
	PutCommand(ctx context.Context, id, name, peerID, spec string) error
	// LinkGroup links commandID to groupID.
	LinkGroup(ctx context.Context, commandID, groupID string) error
}

// Device is both halves of what one machine offers this package: the
// Cluster a runner serves through, and the Registrar a command is created
// with. The desktop and mobile adapters each implement the pair. (Not
// "Node" -- that name is already the journal constructor in store.go, and
// this is a different thing.)
type Device interface {
	Cluster
	Registrar
}

// kvctlCluster implements Device through pkg/kvctl.
type kvctlCluster struct{}

// Kvctl returns the Device that talks to whichever daemon `mage use`
// selected.
func Kvctl() Device { return &kvctlCluster{} }

// CurrentCatalog returns a script Catalog over that same node, alongside
// its peer id -- the pair every desktop entry point starts from.
func CurrentCatalog() (*Catalog, string, error) {
	journal, peerID, err := CurrentNode()
	if err != nil {
		return nil, "", err
	}
	return NewCatalog(journal, peerID), peerID, nil
}

func (*kvctlCluster) SelfPeerID(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	reg, err := registry.Open()
	if err != nil {
		return "", err
	}
	return reg.Current()
}

func (*kvctlCluster) ListCommands(ctx context.Context) ([]Command, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	found, err := kvctl.ListCommands()
	if err != nil {
		return nil, err
	}
	commands := make([]Command, 0, len(found))
	for _, c := range found {
		commands = append(commands, Command{ID: c.ID, Name: c.Name, PeerID: c.PeerID, Spec: c.Spec})
	}
	return commands, nil
}

func (*kvctlCluster) ListRequests(ctx context.Context, commandID string) ([]Request, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	found, err := kvctl.ListCommandRequests(commandID)
	if err != nil {
		return nil, err
	}
	requests := make([]Request, 0, len(found))
	for _, r := range found {
		requests = append(requests, Request{
			InstanceID:  r.InstanceID,
			CommandID:   r.CommandID,
			RequestedBy: r.RequestedBy,
			Inputs:      r.Inputs,
			RequestedAt: r.RequestedAt,
		})
	}
	return requests, nil
}

func (*kvctlCluster) QueryLog(ctx context.Context, instanceID string) ([]LogEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	records, err := kvctl.QueryCommandLog(instanceID, time.Unix(0, 0), time.Now(), 0)
	if err != nil {
		return nil, err
	}
	return entriesFromRecords(instanceID, records), nil
}

func (*kvctlCluster) Progress(ctx context.Context, requesterPeerID, instanceID string, fields map[string]string, narrative string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return kvctl.ReportProgress(requesterPeerID, instanceID, fields, narrative)
}

func (*kvctlCluster) Append(ctx context.Context, requesterPeerID, instanceID string, fields map[string]string, narrative string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return kvctl.AppendCommandLog(requesterPeerID, instanceID, fields, narrative)
}

func (*kvctlCluster) Submit(ctx context.Context, commandID, inputsJSON string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return kvctl.SubmitCommand(commandID, inputsJSON)
}

func (*kvctlCluster) PutCommand(ctx context.Context, id, name, peerID, spec string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return kvctl.PutCommandWithSpec(id, name, peerID, spec)
}

func (*kvctlCluster) LinkGroup(ctx context.Context, commandID, groupID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return kvctl.CreateGroupCommand(commandID, groupID)
}

// entriesFromRecords converts a command log read into what a script and
// the runner both work in. The instance id comes from the caller because a
// record's own UnitID is the instance id it was keyed under -- passing it
// in keeps this honest even for a record that predates the field.
func entriesFromRecords(instanceID string, records []logrecord.Record) []LogEntry {
	entries := make([]LogEntry, 0, len(records))
	for _, r := range records {
		entries = append(entries, LogEntry{
			InstanceID: instanceID,
			Timestamp:  r.Timestamp,
			Fields:     r.Fields,
			Narrative:  r.Narrative,
		})
	}
	return entries
}

// Register stores script and registers it as a catalog Command that runs
// on peerID, pinning the hash the script just got, and links it to groupID
// if one is given.
//
// The order matters and is the safe one: the script is written first, so
// the Command a voter creates never points at bytes that are not there
// yet. The reverse would leave a window in which a submitted request finds
// a command whose script does not exist.
//
// Both catalog writes are voter-gated by the daemon. A learner (a phone,
// usually) gets the daemon's own refusal from PutCommand, after its script
// is already stored -- which is recoverable: a voter can register the
// command later against the same script id, and nothing else has to be
// redone.
func Register(ctx context.Context, scripts *Catalog, reg Registrar, peerID string, script Script, groupID string) (Script, error) {
	if peerID == "" {
		return Script{}, fmt.Errorf("luacmd: a Lua command needs a target peer id -- the device that will run it")
	}
	stored, err := scripts.Put(ctx, script)
	if err != nil {
		return Script{}, err
	}
	spec, err := NewSpec(stored).Encode()
	if err != nil {
		return Script{}, err
	}
	if err := reg.PutCommand(ctx, stored.ID, stored.Name, peerID, spec); err != nil {
		return Script{}, fmt.Errorf("luacmd: register command %s (only a raft voter may do this): %w", stored.ID, err)
	}
	if groupID != "" {
		if err := reg.LinkGroup(ctx, stored.ID, groupID); err != nil {
			return Script{}, fmt.Errorf("luacmd: link command %s to group %s: %w", stored.ID, groupID, err)
		}
	}
	return stored, nil
}
