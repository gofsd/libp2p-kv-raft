package luacmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
)

// MaxScriptBytes bounds a script's source. Generous next to anything
// anybody will type into the Android editor, and far under
// pkg/shmevent.MaxValueSize, which is the ceiling that actually applies to
// the record carrying it -- this is a "that is not a script, that is a
// payload" check rather than a wire limit.
const MaxScriptBytes = 64 << 10

// maxScriptIDLen bounds a script id, which becomes a pkg/logrecord unitID
// and so also decides how wide kindBounds' fixed upper bound has to be.
// Same value as pkg/kvctl's own maxCatalogIDLen, deliberately: a script id
// is normally reused as the catalog Command's id, and a limit a script
// could pass but its command could not would be a trap.
const maxScriptIDLen = 256

// maxScriptNameLen bounds the human name. Nothing scans on it, so this is
// only here to keep a pasted document out of a field meant to be read in a
// list.
const maxScriptNameLen = 256

// Record field names. Only what cannot be derived from the record itself:
// the id is its unitID, the author and revision time are the record's own
// AuthorPeerID/Timestamp, and the source is its Narrative.
const (
	fieldName    = "name"
	fieldSHA256  = "sha256"
	fieldDeleted = "deleted"
)

// ErrNotFound is what Get and Delete return for a script id that has no
// revisions at all, and for one whose latest revision is a tombstone --
// deleted and never-existed are deliberately the same answer to a reader,
// while History tells the two apart.
var ErrNotFound = errors.New("luacmd: no such script")

// endOfTime is the upper bound every scan here uses.
//
// pkg/kvctl's equivalent scans use time.Now(), which is right for records
// that node wrote itself. These are not: a script is typically written on
// a phone and read by whichever device runs it, and if that phone's clock
// is even slightly ahead, a time.Now() bound would hide the revision it
// just wrote until the reader's own clock caught up -- a "my script did
// not save" that resolves itself in a few seconds and is unreproducible
// afterwards. Scanning to the end of the representable range costs nothing
// (the bound is a key, not a filter) and has no such failure mode.
var endOfTime = time.Unix(0, math.MaxInt64)

// Script is one revision of a Lua script as stored in the journal.
//
// Deliberately carries no group: which groups a command belongs to is the
// catalog's business (GroupCommand records, voter-gated), and a copy here
// would be a second answer to the same question, free to drift from the
// one that actually decides who may run it.
type Script struct {
	// ID names the script, and is what its catalog Command's spec points
	// at. Reused as that Command's own id by every caller in this repo.
	ID string `json:"id"`
	// Name is what a list shows a person.
	Name string `json:"name"`
	// Code is the Lua source. Empty on a tombstone.
	Code string `json:"code,omitempty"`
	// SHA256 is Sum(Code), hex -- what a Command's spec pins so the runner
	// can refuse to execute bytes nobody registered. See the package doc.
	SHA256 string `json:"sha256"`
	// Author is the peer id that wrote this revision, as claimed by the
	// node it was written through. Provenance for a reader, never an input
	// to a permission decision -- those are the catalog's, and are checked
	// against an identity raft actually authenticated.
	Author string `json:"author,omitempty"`
	// Rev is when this revision was written, and with ID identifies it.
	Rev time.Time `json:"rev"`
	// Deleted marks a tombstone revision (see Catalog.Delete).
	Deleted bool `json:"deleted,omitempty"`
}

// Sum returns code's SHA-256 as lowercase hex -- the value a Command's
// spec pins and the runner recomputes before executing anything.
func Sum(code string) string {
	sum := sha256.Sum256([]byte(code))
	return hex.EncodeToString(sum[:])
}

// Catalog reads and writes scripts in a Journal, attributing everything it
// writes to authorPeerID.
//
// The author is passed in rather than discovered because a Journal may be
// a session somebody else opened (Session/Sessions), and shmclient.Session
// doesn't expose the identity it was opened for. Callers already hold it:
// CurrentNode returns it, and mobile/kvmobile has its own PeerID.
type Catalog struct {
	Journal Journal
	Author  string
}

// NewCatalog returns a Catalog writing through j as authorPeerID.
func NewCatalog(j Journal, authorPeerID string) *Catalog {
	return &Catalog{Journal: j, Author: authorPeerID}
}

// Put stores a new revision of s. Create and update are the same call and
// the same record: the latest revision of an id is what Get answers with,
// and nothing is ever rewritten in place.
//
// That includes putting an id whose latest revision is a tombstone, which
// resurrects it -- an id is a name, not a grave, and refusing would mean a
// script deleted by mistake could never be restored under the name every
// catalog Command pointing at it already uses.
//
// Returns the revision as stored, with SHA256, Author and Rev filled in --
// the caller needs the hash immediately, since that is what goes into the
// Command's spec.
func (c *Catalog) Put(ctx context.Context, s Script) (Script, error) {
	if err := validateScript(s); err != nil {
		return Script{}, err
	}

	stored := Script{
		ID:     s.ID,
		Name:   s.Name,
		Code:   s.Code,
		SHA256: Sum(s.Code),
		Author: c.Author,
		Rev:    time.Now(),
	}
	fields := map[string]string{
		fieldName:   stored.Name,
		fieldSHA256: stored.SHA256,
	}
	if err := c.append(ctx, stored.ID, stored.Rev, fields, stored.Code); err != nil {
		return Script{}, err
	}
	return stored, nil
}

// Get returns the latest revision of id, or ErrNotFound if it has none or
// its latest revision is a tombstone.
func (c *Catalog) Get(ctx context.Context, id string) (Script, error) {
	if err := validateID(id); err != nil {
		return Script{}, err
	}
	revisions, err := c.History(ctx, id)
	if err != nil {
		return Script{}, err
	}
	if len(revisions) == 0 {
		return Script{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	latest := revisions[len(revisions)-1]
	if latest.Deleted {
		return Script{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return latest, nil
}

// History returns every revision of id in the order they were written,
// tombstones included -- the reason the source lives in an append-only log
// at all. Empty (not ErrNotFound) for an id nothing was ever written
// under, so that a caller asking "what happened to this" can tell "nothing
// ever did" from "it was deleted".
func (c *Catalog) History(ctx context.Context, id string) ([]Script, error) {
	if err := validateID(id); err != nil {
		return nil, err
	}
	lo, hi := logrecord.ScanBounds(ScriptKind, id, time.Unix(0, 0), endOfTime)
	pairs, err := c.Journal.Range(ctx, lo, hi)
	if err != nil {
		return nil, err
	}

	revisions := make([]Script, 0, len(pairs))
	for _, p := range pairs {
		s, err := decodeScript(p.Value)
		if err != nil {
			return nil, err
		}
		revisions = append(revisions, s)
	}
	return revisions, nil
}

// List returns the latest revision of every script that currently exists,
// in ascending id order, with deleted ones left out.
//
// One scan of the whole kind, folded in memory, rather than a scan per id:
// the alternative is a list scan plus a range scan per script, and every
// revision has to be read either way to find out which is latest.
func (c *Catalog) List(ctx context.Context) ([]Script, error) {
	lo, hi := kindBounds(ScriptKind)
	pairs, err := c.Journal.Range(ctx, lo, hi)
	if err != nil {
		return nil, err
	}

	// Keys sort by id then timestamp (see logrecord.BuildKey), so records
	// arrive grouped by id and chronological within a group: the last one
	// seen for an id is its latest revision, and ids come out already
	// ordered.
	var ids []string
	latest := map[string]Script{}
	for _, p := range pairs {
		s, err := decodeScript(p.Value)
		if err != nil {
			return nil, err
		}
		if _, seen := latest[s.ID]; !seen {
			ids = append(ids, s.ID)
		}
		latest[s.ID] = s
	}

	scripts := make([]Script, 0, len(ids))
	for _, id := range ids {
		if s := latest[id]; !s.Deleted {
			scripts = append(scripts, s)
		}
	}
	return scripts, nil
}

// Delete writes a tombstone revision for id, after which Get answers
// ErrNotFound and List leaves it out. The source stays readable through
// History, which is the whole difference between this and a store where
// delete means gone.
//
// Deleting a script does not touch the catalog Command pointing at it.
// That is a voter-gated record and this is not a voter-gated call, so the
// two cannot be made to happen together; the runner refusing a command
// whose script has gone is what covers the gap (see the package doc).
func (c *Catalog) Delete(ctx context.Context, id string) error {
	existing, err := c.Get(ctx, id)
	if err != nil {
		return err
	}
	fields := map[string]string{
		fieldName:    existing.Name,
		fieldDeleted: "true",
	}
	return c.append(ctx, id, time.Now(), fields, "")
}

// append is the shared tail of Put and Delete: build the key, encode the
// record, hand both to the journal.
func (c *Catalog) append(ctx context.Context, id string, ts time.Time, fields map[string]string, code string) error {
	rnd, err := logrecord.NewRand()
	if err != nil {
		return fmt.Errorf("luacmd: %w", err)
	}
	key, err := logrecord.BuildKey(ScriptKind, id, ts, rnd)
	if err != nil {
		return fmt.Errorf("luacmd: %w", err)
	}
	value, err := logrecord.Record{
		Kind:         ScriptKind,
		UnitID:       id,
		Timestamp:    ts,
		AuthorPeerID: c.Author,
		Fields:       fields,
		Narrative:    code,
	}.Encode()
	if err != nil {
		return fmt.Errorf("luacmd: encode record: %w", err)
	}
	return c.Journal.Append(ctx, key, value)
}

// decodeScript turns one stored record back into a Script, checking the
// stored hash against the stored source as it goes.
//
// That check catches corruption, not tampering: anyone able to write the
// record could rewrite both halves. What makes a substituted script
// detectable is the hash the *catalog Command* pins, which lives in a
// voter-gated record this one cannot reach -- see the package doc.
func decodeScript(value []byte) (Script, error) {
	rec, err := logrecord.Decode(value)
	if err != nil {
		return Script{}, fmt.Errorf("luacmd: decode record: %w", err)
	}
	s := Script{
		ID:      rec.UnitID,
		Name:    rec.Fields[fieldName],
		Code:    rec.Narrative,
		SHA256:  rec.Fields[fieldSHA256],
		Author:  rec.AuthorPeerID,
		Rev:     rec.Timestamp,
		Deleted: rec.Fields[fieldDeleted] == "true",
	}
	if !s.Deleted && s.SHA256 != Sum(s.Code) {
		return Script{}, fmt.Errorf("luacmd: script %s revision %s is corrupt: stored hash does not match stored source", s.ID, s.Rev.Format(time.RFC3339Nano))
	}
	return s, nil
}

// kindBounds returns the [lo, hi] range covering every record of kind,
// across every script id and timestamp -- the same fixed-width upper bound
// pkg/kvctl's kindPrefixBounds builds, sized from maxScriptIDLen so it is
// provably past every key any valid id could produce.
func kindBounds(kind string) (lo, hi []byte) {
	prefix := logrecord.KindPrefix(kind)
	lo = prefix
	hi = make([]byte, len(prefix)+2+maxScriptIDLen+8+logrecord.RandSize)
	copy(hi, prefix)
	for i := len(prefix); i < len(hi); i++ {
		hi[i] = 0xFF
	}
	return lo, hi
}

func validateScript(s Script) error {
	if err := validateID(s.ID); err != nil {
		return err
	}
	if s.Name == "" {
		return fmt.Errorf("luacmd: script name must not be empty")
	}
	if len(s.Name) > maxScriptNameLen {
		return fmt.Errorf("luacmd: script name exceeds %d bytes", maxScriptNameLen)
	}
	if len(s.Code) > MaxScriptBytes {
		return fmt.Errorf("luacmd: script exceeds %d bytes", MaxScriptBytes)
	}
	// Last, so that a script with several problems reports the cheap
	// structural ones before the compiler's.
	return Check(s.Code)
}

func validateID(id string) error {
	if id == "" {
		return fmt.Errorf("luacmd: script id must not be empty")
	}
	if len(id) > maxScriptIDLen {
		return fmt.Errorf("luacmd: script id exceeds %d bytes", maxScriptIDLen)
	}
	return nil
}
