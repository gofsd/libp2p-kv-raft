// Package e2edata implements the single JSON file that records everything
// the end-to-end test/deploy pipeline needs: a version history stamped
// with this repo's own semver (shared across every platform's
// implementation -- one monorepo, one release version), the deterministic
// node identities used across platforms (desktop, android, web, and the
// one SSH-deployed remote leader), and a durable, human-readable log of
// test rows (one shmevent, one node, one recorded pass/fail) run against
// those identities.
//
// The file is meant to be committed to the repo (test/e2e/testdata.json)
// and read/edited by a human, not just tooling: identities are
// deterministic (see identity.go) so every checkout/deploy reproduces the
// exact same peer ids and keys ("predictable deploy" from the design
// brief), events are recorded by name with plain-text values rather than
// raw wire bytes (see Event's doc comment), and PublishedVersion tracks
// how far the recorded test history has been confirmed passing, so mage
// e2e:current only re-runs what's new since the last time this file's
// version count moved ("version increment based on new tests").
package e2edata

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
	"unicode/utf8"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// Platform identifies which client implementation a Node's identity is
// deployed under -- each has its own way of being handed a deterministic
// seed (see cmd/kvctl-cli's -key-path file for desktop, mobile/kvmobile's
// identitySeedHex ldflag for android, web-app's worker_main_with_seed for
// web), except PlatformRemote, which identifies the one SSH-deployed
// bootstrap/leader node (see pkg/e2erun.EnsureBootstrap and its doc
// comment on BootstrapHost) that every other platform's test node joins.
type Platform string

const (
	PlatformDesktop Platform = "desktop"
	PlatformAndroid Platform = "android"
	PlatformWeb     Platform = "web"
	PlatformRemote  Platform = "remote"
)

// Node is one deterministic identity recorded in the file, keyed by a small
// integer id in File.Nodes. PublicKey/PrivateKey are hex-encoded stdlib
// crypto/ed25519 keys (32 and 64 raw bytes respectively) -- the same format
// pkg/shmevent.PublicKey/PrivateKey and EventGetPublicKey/EventGetPrivateKey
// use, so a row's expectations can be checked against a live node's actual
// keys with no conversion.
type Node struct {
	Platform   Platform `json:"platform"`
	PeerID     string   `json:"peer_id"`
	PublicKey  string   `json:"public_key"`
	PrivateKey string   `json:"private_key"`
}

// Event is the JSON form of pkg/shmevent.Msg used inside a test Row's
// "event" field and as kvctl-cli sendevent's argument/output shape --
// human-readable on purpose, without changing pkg/shmevent's capnp wire
// structure at all: this is purely a JSON presentation layer over the same
// Msg{EventType, SourceID, DestinationID, Value, ID} fields.
//
//   - "event" is the name pkg/shmevent.EventName prints ("set_field",
//     "get_public_key", ...), not the raw byte -- parsed back via
//     shmevent.EventFromName.
//   - "value" is a plain string when the underlying bytes are valid UTF-8
//     (true for every KV test key/value a human actually authors), or a
//     "0x"-prefixed hex string when they're not (a raw Ed25519 key from a
//     GetPublicKey/GetPrivateKey response, or a deliberately-corrupt test
//     value) -- still unambiguous to parse back (see valueToJSON/
//     valueFromJSON), just not pretending binary data is text.
//
// See MarshalJSON/UnmarshalJSON for the actual (de)serialization; the
// exported fields below are for in-Go construction/inspection only.
type Event struct {
	EventType     uint8
	SourceID      uint16
	DestinationID uint16
	RawValue      []byte
	ID            uint16
}

// NewEvent builds an Event carrying value as its raw value bytes.
func NewEvent(eventType uint8, sourceID, destinationID uint16, value []byte, id uint16) Event {
	return Event{
		EventType:     eventType,
		SourceID:      sourceID,
		DestinationID: destinationID,
		RawValue:      value,
		ID:            id,
	}
}

// Value returns e's raw value bytes.
func (e Event) Value() []byte { return e.RawValue }

// ToMsg converts e to the wire struct pkg/ipc.Call/pkg/shmevent.Encode need.
func (e Event) ToMsg() shmevent.Msg {
	return shmevent.Msg{
		EventType:     e.EventType,
		SourceID:      e.SourceID,
		DestinationID: e.DestinationID,
		Value:         e.RawValue,
		ID:            e.ID,
	}
}

// EventFromMsg converts a decoded response back to the JSON-friendly Event
// shape, for recording/printing.
func EventFromMsg(m shmevent.Msg) Event {
	return NewEvent(m.EventType, m.SourceID, m.DestinationID, m.Value, m.ID)
}

// eventJSON is Event's on-disk shape -- see Event's doc comment.
type eventJSON struct {
	Event         string `json:"event"`
	SourceID      uint16 `json:"source_id,omitempty"`
	DestinationID uint16 `json:"destination_id,omitempty"`
	Value         string `json:"value,omitempty"`
	ID            uint16 `json:"id,omitempty"`
}

func (e Event) MarshalJSON() ([]byte, error) {
	return json.Marshal(eventJSON{
		Event:         shmevent.EventName(e.EventType),
		SourceID:      e.SourceID,
		DestinationID: e.DestinationID,
		Value:         valueToJSON(e.RawValue),
		ID:            e.ID,
	})
}

func (e *Event) UnmarshalJSON(data []byte) error {
	var j eventJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return err
	}
	eventType, ok := shmevent.EventFromName(j.Event)
	if !ok {
		return fmt.Errorf("e2edata: unknown event name %q", j.Event)
	}
	value, err := valueFromJSON(j.Value)
	if err != nil {
		return fmt.Errorf("e2edata: event %q: %w", j.Event, err)
	}
	*e = Event{
		EventType:     eventType,
		SourceID:      j.SourceID,
		DestinationID: j.DestinationID,
		RawValue:      value,
		ID:            j.ID,
	}
	return nil
}

// valueToJSON renders raw as plain text if it's valid UTF-8 (every KV test
// value in practice), or as a "0x"-prefixed hex string otherwise (a raw
// Ed25519 key, or deliberately-corrupt test bytes) -- see Event's doc
// comment.
func valueToJSON(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	if utf8.Valid(raw) {
		return string(raw)
	}
	return "0x" + hex.EncodeToString(raw)
}

// valueFromJSON is valueToJSON's inverse.
func valueFromJSON(s string) ([]byte, error) {
	if s == "" {
		return nil, nil
	}
	if rest, ok := hexPrefix(s); ok {
		raw, err := hex.DecodeString(rest)
		if err != nil {
			return nil, fmt.Errorf("decode 0x-prefixed value: %w", err)
		}
		return raw, nil
	}
	return []byte(s), nil
}

func hexPrefix(s string) (rest string, ok bool) {
	if len(s) >= 2 && s[0] == '0' && s[1] == 'x' {
		return s[2:], true
	}
	return "", false
}

// UIExpectSucceeded/UIExpectRejected/UIExpectNoCrash are UICase.Expect's
// valid values -- the on-disk names for UiCommandE2ETest.kt's own
// assertSucceeded/assertRejected/assertNoCrash assertion functions (see
// that file's doc comment for what each means: a rejection is expected
// when this device isn't currently a raft voter, no_crash tolerates either
// outcome for a case whose result depends on state this pipeline doesn't
// control). Empty/unrecognized defaults to UIExpectSucceeded on the Kotlin
// side, matching Case's own Kotlin-side default before this file existed.
const (
	UIExpectSucceeded = "succeeded"
	UIExpectRejected  = "rejected"
	UIExpectNoCrash   = "no_crash"
)

// UICase is one android UiCommandE2ETest test plan entry, keyed by command
// label ("$category: $name", matching CommandSpec.label in
// android-app/app/src/main/java/com/gofsd/kvdemo/CommandCatalog.kt) in
// File.UICases. This is the single source of truth for what
// UiCommandE2ETest actually does with each catalog command -- moved here
// (see git history) from a hardcoded Kotlin map so the whole e2e test
// surface (cross-platform wire rows and the Android UI command walk alike)
// lives in one reviewable file instead of being split across a JSON file
// and a Kotlin source file.
//
// Inputs may contain the literal tokens "{{selfPeerID}}", "{{leaderPeerID}}",
// "{{testKey}}", and "{{testValue}}" -- substituted on-device at test run
// time (see UiCommandE2ETest.kt's substituteTokens) for values that can
// only be known live (this device's own peer id, the cluster's
// currently-observed leader, and a randomized-per-run KV test key/value) --
// there is no way to freeze those into a committed file the way every other
// field here is frozen.
//
// A catalog command with no entry in File.UICases still gets full
// navigation-only coverage (screen reachable, param field count verified)
// via UiCommandE2ETest's own default case, just never has Run tapped --
// see that file's own doc comment for exactly which commands are
// deliberately left out this way (destructive to this shared cluster, or
// needing a second cooperating device) and why.
type UICase struct {
	Inputs        []string `json:"inputs,omitempty"`
	Execute       bool     `json:"execute,omitempty"`
	Expect        string   `json:"expect,omitempty"`
	RetryBudgetMs int64    `json:"retry_budget_ms,omitempty"`
}

// StatusPass/StatusFail/StatusSkipped are the Row.Status conventions this
// package's runner uses. Any other non-zero value is still "failed" as far
// as File methods are concerned; StatusSkipped exists only so a platform
// this pipeline can't yet drive for real (see e2erun's android gap) doesn't
// get reported as a false failure or a false pass.
const (
	StatusPass    = 0
	StatusFail    = 1
	StatusSkipped = 2
)

// Row is one recorded test: send Event to the node identified by Node
// (against File.Nodes), as it stood the last time this version's tests
// ran, with the outcome in Status/Error.
type Row struct {
	Version int    `json:"version"`
	Node    int    `json:"node"`
	Event   Event  `json:"event"`
	Status  int    `json:"status"`
	Error   string `json:"error,omitempty"`

	// Platform is Node's platform at the time this row was recorded (see
	// AddTest), kept independently of whatever File.Nodes[Node] currently
	// holds -- unlike Node, this survives that node later being deleted
	// (mage e2e:deletenode/destroyall). It's what lets Load's backfill (for
	// rows predating this field) and Run's EnsureNode recover from exactly
	// that: a row whose node id no longer resolves still knows what
	// platform to re-provision under that same id, instead of being
	// permanently orphaned the way "unknown node id" used to mean.
	Platform Platform `json:"platform,omitempty"`
}

// UICaseResult is one command's outcome from the most recent
// UiCommandE2ETest walk -- the same shape UiCommandE2ETest.kt itself
// writes to the device's ui_e2e_results.json, pulled back into this file
// so it survives past the next run overwriting/deleting that on-device
// file (see pkg/e2erun/android.go's runUICommandTest).
type UICaseResult struct {
	Command string `json:"command"`
	Pass    bool   `json:"pass"`
	Error   string `json:"error,omitempty"`
}

// AndroidUIResult is the most recent outcome of the catalog-driven
// UiCommandE2ETest walk (see UICases). Unlike Rows, this isn't
// versioned/replayed history -- there's one of these, overwritten by every
// run that includes the androidui type (E2E_TYPES=androidui or the
// unfiltered default), not appended to. nil (the field entirely absent)
// means it has never run since this field was introduced, same meaning as
// a Row.Platform backfill finding nothing to recover: no data, not "it
// failed."
//
// Before this existed, this walk's result was never persisted anywhere --
// only ever printed to that one run's console output, with nothing on
// disk to check afterward (not even the transient on-device
// ui_e2e_results.json survives to the next run, since runUICommandTest
// deletes it first). A caller wanting to know "did the UI walk pass last
// time" had no way to answer that except having watched the run happen.
type AndroidUIResult struct {
	RanAt  time.Time      `json:"ran_at"`
	Status int            `json:"status"`
	Error  string         `json:"error,omitempty"`
	Cases  []UICaseResult `json:"cases,omitempty"`
}

// File is the on-disk shape of test/e2e/testdata.json.
type File struct {
	// Versions maps a version id to this repo's semver at the time that
	// version was created (see mage e2e:newversion, which stamps it from
	// the same git-tag-derived semver `mage patch`/`minor`/`major` manage)
	// -- one shared version across every platform's implementation, not a
	// separate number per platform. CurrentVersion is always the highest
	// key.
	Versions map[int]string `json:"versions"`
	// PublishedVersion is the highest version id confirmed passing and
	// pushed. e2e:current runs every row with Version > PublishedVersion;
	// once they all pass, the pipeline advances PublishedVersion to
	// CurrentVersion.
	PublishedVersion int          `json:"published_version"`
	Nodes            map[int]Node `json:"nodes"`
	Rows             []Row        `json:"rows"`
	// UICases is the Android UiCommandE2ETest test plan, keyed by command
	// label -- see UICase's doc comment. Deliberately not versioned/rowed
	// the way Rows is: it always describes how to exercise whatever
	// commands the *current* CommandCatalog.kt has, not a historical
	// regression log of past runs.
	UICases map[string]UICase `json:"android_ui_cases,omitempty"`
	// AndroidUIResult is the most recent UiCommandE2ETest walk's outcome --
	// see that type's own doc comment.
	AndroidUIResult *AndroidUIResult `json:"android_ui_result,omitempty"`
}

// DefaultPath is where the testdata file lives relative to the repo root.
const DefaultPath = "test/e2e/testdata.json"

// Load reads and parses the file at path. A missing file is not an error --
// it returns a freshly initialized empty File, since the very first
// `mage e2e:newversion` run has nothing to load yet.
func Load(path string) (*File, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &File{Versions: map[int]string{}, Nodes: map[int]Node{}}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("e2edata: read %s: %w", path, err)
	}
	var f File
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("e2edata: parse %s: %w", path, err)
	}
	if f.Versions == nil {
		f.Versions = map[int]string{}
	}
	if f.Nodes == nil {
		f.Nodes = map[int]Node{}
	}
	if f.UICases == nil {
		f.UICases = map[string]UICase{}
	}
	// Backfill Row.Platform for files predating that field: only possible
	// while the referenced node still exists, which is exactly the state
	// every pre-existing testdata.json is in the first time it's loaded
	// after this field was introduced. A row whose node was already gone
	// by then has no platform to recover -- see EnsureNode's doc comment
	// for what that means for it going forward.
	for i, r := range f.Rows {
		if r.Platform == "" {
			if n, ok := f.Nodes[r.Node]; ok {
				f.Rows[i].Platform = n.Platform
			}
		}
	}
	return &f, nil
}

// Save writes f to path, creating parent directories as needed, formatted
// for a readable diff in version control.
func (f *File) Save(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("e2edata: create %s: %w", filepath.Dir(path), err)
	}
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return fmt.Errorf("e2edata: encode: %w", err)
	}
	data = append(data, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// CurrentVersion returns the highest version id in Versions -- the
// not-yet-published version any new AddTest row targets -- or 0 if none
// exists yet.
func (f *File) CurrentVersion() int {
	max := 0
	for v := range f.Versions {
		if v > max {
			max = v
		}
	}
	return max
}

// NewVersion records a new version stamped with semver (see File.Versions'
// doc comment on where that string comes from) and returns its id
// (CurrentVersion()+1). Called by `mage e2e:newversion`, or lazily by
// AddTest if no version exists yet.
func (f *File) NewVersion(semver string) int {
	id := f.CurrentVersion() + 1
	if f.Versions == nil {
		f.Versions = map[int]string{}
	}
	f.Versions[id] = semver
	return id
}

// nextNodeID returns the smallest node id greater than every id currently
// in Nodes *and* every id any Row still references -- the latter matters
// once a node has been deleted (mage e2e:deletenode/destroyall) while its
// rows remain: without also considering Rows here, a freshly (re-)added
// node (e.g. EnsureBootstrap's remote node, provisioned before
// pkg/e2erun.Run gets a chance to call EnsureNode for orphaned rows) could
// claim an id an orphaned row is about to need back, silently stealing it
// instead of leaving it free to be recovered under its original platform.
func (f *File) nextNodeID() int {
	max := 0
	for id := range f.Nodes {
		if id > max {
			max = id
		}
	}
	for _, r := range f.Rows {
		if r.Node > max {
			max = r.Node
		}
	}
	return max + 1
}

// AddNode generates a fresh deterministic identity for platform, records it
// under a new node id, and returns that id.
func (f *File) AddNode(platform Platform) (int, Node, error) {
	pub, priv, err := GenerateIdentity()
	if err != nil {
		return 0, Node{}, err
	}
	peerID, err := PeerIDFromPrivateKey(priv)
	if err != nil {
		return 0, Node{}, err
	}
	if f.Nodes == nil {
		f.Nodes = map[int]Node{}
	}
	id := f.nextNodeID()
	n := Node{
		Platform:   platform,
		PeerID:     peerID,
		PublicKey:  encodeHex(pub),
		PrivateKey: encodeHex(priv),
	}
	f.Nodes[id] = n
	return id, n, nil
}

// RemoteNode returns the id and Node of the (at most one expected)
// PlatformRemote entry in Nodes -- the SSH-deployed bootstrap/leader every
// other test node joins (see pkg/e2erun.EnsureBootstrap). ok is false if
// none has been provisioned yet.
func (f *File) RemoteNode() (id int, node Node, ok bool) {
	for id, n := range f.Nodes {
		if n.Platform == PlatformRemote {
			return id, n, true
		}
	}
	return 0, Node{}, false
}

// DeleteNode removes nodeID from Nodes and returns it, along with how many
// rows still reference it (left in place -- see this package's doc comment
// on the file being a durable, human-reviewed log; a caller wanting those
// gone too should say so explicitly rather than have deletion silently
// rewrite history). pkg/e2erun.DeleteNode wraps this with the real
// process/data teardown for the node's platform before calling it.
func (f *File) DeleteNode(nodeID int) (Node, int, error) {
	node, ok := f.Nodes[nodeID]
	if !ok {
		return Node{}, 0, fmt.Errorf("e2edata: unknown node id %d", nodeID)
	}
	delete(f.Nodes, nodeID)
	affected := 0
	for _, r := range f.Rows {
		if r.Node == nodeID {
			affected++
		}
	}
	return node, affected, nil
}

// AddTest appends a row against CurrentVersion (creating version 1 first if
// none exists yet), targeting nodeID with event ev.
func (f *File) AddTest(nodeID int, ev Event) (Row, error) {
	node, ok := f.Nodes[nodeID]
	if !ok {
		return Row{}, fmt.Errorf("e2edata: unknown node id %d", nodeID)
	}
	v := f.CurrentVersion()
	if v == 0 {
		v = f.NewVersion("0.0.0")
	}
	row := Row{Version: v, Node: nodeID, Event: ev, Platform: node.Platform}
	f.Rows = append(f.Rows, row)
	return row, nil
}

// EnsureNode returns the existing node at id if present, or provisions a
// fresh identity for platform and records it under exactly id -- not the
// next available id the way AddNode picks -- if missing. This is how
// pkg/e2erun.Run recovers a row whose node was deleted (mage
// e2e:deletenode/destroyall) while the row itself remained: the row
// carries its own Platform (see Row's doc comment) precisely so this
// recovery doesn't need to remember what id N "used to be" from anywhere
// else. The freshly generated identity is unrelated to whatever the
// original one was -- there's no way to recover that once deleted, and
// none of this package's callers need it to be the same, only valid.
func (f *File) EnsureNode(id int, platform Platform) (Node, error) {
	if n, ok := f.Nodes[id]; ok {
		return n, nil
	}
	pub, priv, err := GenerateIdentity()
	if err != nil {
		return Node{}, err
	}
	peerID, err := PeerIDFromPrivateKey(priv)
	if err != nil {
		return Node{}, err
	}
	if f.Nodes == nil {
		f.Nodes = map[int]Node{}
	}
	n := Node{
		Platform:   platform,
		PeerID:     peerID,
		PublicKey:  encodeHex(pub),
		PrivateKey: encodeHex(priv),
	}
	f.Nodes[id] = n
	return n, nil
}

// PendingRows returns every row whose Version is newer than
// PublishedVersion -- what `mage e2e:current` runs.
func (f *File) PendingRows() []int {
	var idx []int
	for i, r := range f.Rows {
		if r.Version > f.PublishedVersion {
			idx = append(idx, i)
		}
	}
	return idx
}

// AllRowIndices returns every row index -- what `mage e2e:all` runs.
func (f *File) AllRowIndices() []int {
	idx := make([]int, len(f.Rows))
	for i := range f.Rows {
		idx[i] = i
	}
	return idx
}

// MarkPublished advances PublishedVersion to CurrentVersion. The e2e
// runner calls this once every pending row has Status == StatusPass.
func (f *File) MarkPublished() {
	f.PublishedVersion = f.CurrentVersion()
}
