//go:build mage

// This file is the entry point mage discovers targets from
// (package main, //go:build mage). Its own body is deliberately just
// Default + repoRoot, the two things every other file in this package
// needs; every actual target is split by domain into its own file,
// mirroring pkg/kvctl's own file layout:
//
//   - mage_version.go: git-tag semver bump targets (Status/Patch/Minor/...)
//   - mage_e2e.go: the E2E namespace (end-to-end test/deploy pipeline)
//   - mage_release.go: TLS/Githooks namespaces, Test/Integration/TestAll/
//     TestCurrent, Build*, Docker/Podman/Shell
//   - mage_cluster.go: node lifecycle (AddNode/AddFollower/.../DeleteNode/
//     Backup/Restore/ListClusters/ListNodes)
//   - mage_kv.go: Set/Get/Txn/Cas/CasAbsent/RangeScan/Kvhttp
//   - mage_permit.go: RequestPermit/ConfirmPermit/RevokePermit
//   - mage_relay.go: relay-node CRUD + BootstrapNodes
//   - mage_publicaccess.go: RequestPublicAccess/EnablePublicAccess/
//     DialSubmitCommand/DialQueryCommandLog
//   - mage_invite.go: join-invite/exec-invite + their signed-ticket forms
//   - mage_joinrequest.go: AddPending/join-request + its signed-ticket form
//   - mage_misc.go: GetOwnAddr/Version/Execute/PollExecute/channel/
//     logrecord one-shots
//   - mage_catalog.go: Group/Command/Station CRUD
//   - mage_dispatch.go: SubmitCommand/execution tracking/command log
//
// This is a pure reorganization (see the desktop refactor plan this split
// implements) -- `mage -l` lists the identical target set either way,
// since mage discovers exported top-level functions across every file in
// this package, not just this one.
package main

import (
	"fmt"
	"path/filepath"
	"runtime"
)

// Default target to run if none is specified
var Default = Status

// repoRoot returns the directory this magefile lives in, so AddNode can
// `go build ./cmd/kvnode` regardless of the directory mage was invoked from.
func repoRoot() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("addnode: cannot determine repo root")
	}
	return filepath.Dir(file), nil
}
