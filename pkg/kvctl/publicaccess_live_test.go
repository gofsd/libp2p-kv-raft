package kvctl_test

import (
	"testing"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
)

// TestRequestPublicAccessGrantsStandingOnAStrangerCluster drives
// RequestPublicAccess/EnablePublicAccess end to end through two
// independently-spawned, never-joined solo nodes -- the CLI-facing path
// behind `mage requestpublicaccess`/`mage enablepublicaccess`, previously
// exercised only by hand at this layer (pkg/daemon's own
// TestPublicAccessGrantsChannelRelayAccess covers the underlying
// mechanism in-process; this instead proves the two real spawned
// processes, real dialable addresses, and pkg/kvctl's own IPC/session
// plumbing actually connect end to end -- something an in-process daemon
// test can't).
func TestRequestPublicAccessGrantsStandingOnAStrangerCluster(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	fastRaftArgs := []string{
		"-raft-heartbeat-timeout", "300ms",
		"-raft-election-timeout", "300ms",
		"-raft-leader-lease-timeout", "250ms",
	}

	targetID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode (target): %v", err)
	}
	strangerID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode (stranger, separate solo cluster): %v", err)
	}

	// EnablePublicAccess is already seeded at bootstrap (pkg/daemon's
	// ensureDefaultPublicCommand) -- calling it again here exercises its
	// own documented idempotency (PutGroup/PutCommand/PutGroupCommand are
	// all plain Puts) rather than assuming that seeding happened to run.
	if err := kvctl.Use(targetID); err != nil {
		t.Fatalf("Use(target): %v", err)
	}
	if err := kvctl.EnablePublicAccess(); err != nil {
		t.Fatalf("EnablePublicAccess: %v", err)
	}
	targetAddr, err := kvctl.GetOwnAddr()
	if err != nil {
		t.Fatalf("GetOwnAddr(target): %v", err)
	}

	if err := kvctl.Use(strangerID); err != nil {
		t.Fatalf("Use(stranger): %v", err)
	}
	instanceID, err := kvctl.RequestPublicAccess(targetAddr, "kvctl-live-test")
	if err != nil {
		t.Fatalf("RequestPublicAccess: %v", err)
	}
	if instanceID == "" {
		t.Fatal("RequestPublicAccess returned an empty instance id")
	}
}
