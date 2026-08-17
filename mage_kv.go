//go:build mage

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
)

// Set stores key=value through raft on the current node.
// Usage: mage set <key> <value>
func Set(key, value string) error {
	return kvctl.Set(key, value)
}

// Get reads key from the current node's local state.
// Usage: mage get <key>
func Get(key string) error {
	value, err := kvctl.Get(key)
	if err != nil {
		return err
	}
	fmt.Println(value)
	return nil
}

// Txn atomically applies a space-separated list of ops through raft on the
// current node -- either all of them land, or none do. Each op is
// `<key>=<value>` (a Set), `del:<key>` (a Delete), `if:<key>=<value>` or
// `ifabsent:<key>` (preconditions -- the transaction applies only if every
// one of them holds); pass the whole list as one quoted argument, the same
// convention `mage kvrecover`'s voterMultiaddr list already uses.
//
// Usage: mage txn "<key1>=<value1> [key2=value2 ...] [del:key3 ...] [if:key4=value4 ...] [ifabsent:key5 ...]"
func Txn(ops string) error {
	return kvctl.Txn(ops)
}

// Cas writes value to key on the current node only if key currently holds
// expected, printing whether the swap happened. The compare runs inside the
// raft FSM, serialized against every other write in the cluster, so unlike a
// get-then-set there is no window another node can write in between.
//
// Usage: mage cas <key> <expected> <value>
func Cas(key, expected, value string) error {
	return kvctl.CompareAndSwap(key, expected, value, false)
}

// CasAbsent writes value to key on the current node only if key does not
// exist yet -- Cas's create-if-not-exists half, for the case no expected
// value can express ("not there" is not a value).
//
// Usage: mage casabsent <key> <value>
func CasAbsent(key, value string) error {
	return kvctl.CompareAndSwap(key, "", value, true)
}

// optionalCount parses one of RangeScan's own optional numeric arguments,
// where "" means "not given" (0) rather than being an error -- see
// RangeScan's usage line for why they are passed as explicit ""s.
func optionalCount(name, raw string) (int, error) {
	if raw == "" {
		return 0, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("rangescan: invalid %s %q: %w", name, raw, err)
	}
	return v, nil
}

// RangeScan lists every key/value pair between start and end (both
// inclusive, lexicographic byte order over the raw key bytes) on the
// current node's local state, one JSON object per line -- a generic
// counterpart to set/get for inspecting a whole range of keys at once
// instead of one at a time, covering any key in the store (ordinary data
// as well as this project's own reserved namespaces -- see
// kvctl.RangeScan's doc comment for why that's not a new privilege for a
// local caller).
//
// limit is a count or "" (unlimited); skip is a count or "" (none), dropped
// from the front of the requested order; order is "asc"/"" or "desc". Mage
// has no notion of an optional argument, so the trailing three are passed
// as explicit ""s when not wanted -- the same convention this repo's other
// optional-argument targets already use.
//
// Usage: mage rangescan <start> <end> [limit|""] [skip|""] [order|""]
func RangeScan(start, end, limit, skip, order string) error {
	n, err := optionalCount("limit", limit)
	if err != nil {
		return err
	}
	off, err := optionalCount("skip", skip)
	if err != nil {
		return err
	}
	results, err := kvctl.RangeScan(start, end, n, off, order)
	if err != nil {
		return err
	}
	for _, kv := range results {
		out, err := json.Marshal(kv)
		if err != nil {
			return err
		}
		fmt.Println(string(out))
	}
	return nil
}

// Kvhttp runs cmd/kvhttp -- a local, multi-tenant HTTP wrapper around
// kvctl-cli sendevent for a caller that can only do a plain fetch() (e.g. a
// browser sandbox with no SharedArrayBuffer/WebTransport; see that
// command's own doc comment) -- in the foreground, blocking until
// interrupted (Ctrl+C), the same as running `go run ./cmd/kvhttp` directly.
// One running kvhttp serves every node currently in the local registry;
// each request's `Authorization: Bearer <token>` (see `mage accesstoken
// <peerID>`) picks which one it targets, so there is no per-node flag here
// to set.
//
// addr may be "" to take cmd/kvhttp's own default, 127.0.0.1:8787.
// Usage: mage kvhttp [addr]
func Kvhttp(addr string) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	args := []string{"run", "./cmd/kvhttp"}
	if addr != "" {
		args = append(args, "-addr", addr)
	}
	cmd := exec.Command("go", args...)
	cmd.Dir = root
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
