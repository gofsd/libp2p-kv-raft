//go:build mage

package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
)

// AddNode spawns a new node process and bootstraps it as the cluster's sole
// leader. mage requires every target to take a fixed number of arguments
// (no optional/variadic CLI args), so the three addnode shapes described in
// the project's design (0, 1, or 2 peer-id arguments) are exposed as three
// targets -- AddNode, AddFollower, RejoinNode -- that all delegate to the
// same underlying kvctl.AddNode(repoRoot, peerIDs...).
//
// Usage: mage addnode
func AddNode() error {
	return runAddNode()
}

// AddFollower spawns a new node process and joins it to the cluster led by
// leaderPeerID.
//
// Usage: mage addfollower <leaderPeerID>
func AddFollower(leaderPeerID string) error {
	return runAddNode(leaderPeerID)
}

// RejoinNode restarts the existing node ownPeerID (reusing its data
// directory and identity) and (re)joins it to leaderPeerID. Use this when
// the node's address changed since it went down, or a different/new
// leader needs to be told about it; if neither is true, ResumeNode is
// simpler (no leader coordination at all).
//
// Usage: mage rejoinnode <leaderPeerID> <ownPeerID>
func RejoinNode(leaderPeerID, ownPeerID string) error {
	return runAddNode(leaderPeerID, ownPeerID)
}

// ResumeNode restarts the existing node ownPeerID in place -- reusing its
// data directory and identity -- with no leader coordination at all. The
// daemon recognizes it already has persisted raft state and resumes
// operating on it directly. Use this when the node's address hasn't
// changed since it went down (a pinned -listen-port makes that reliable);
// otherwise use RejoinNode.
//
// Usage: mage resumenode <ownPeerID>
func ResumeNode(ownPeerID string) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	peerID, err := kvctl.ResumeNode(root, ownPeerID)
	if err != nil {
		return err
	}
	fmt.Printf("✅ node %s resumed and selected as current\n", peerID)
	return nil
}

func runAddNode(peerIDs ...string) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	peerID, err := kvctl.AddNode(root, peerIDs...)
	if err != nil {
		return err
	}
	printNodeUp(peerID)
	return nil
}

// printNodeUp prints AddNode/AddNodeWithKey's shared success line, plus
// peerID's kvhttp access token (kvctl.AccessToken -- see that function's
// doc comment) so an operator has everything needed to drive the node
// through cmd/kvhttp immediately, without a separate `mage accesstoken`
// round trip. A token-derivation failure is reported but does not fail
// node creation itself -- the node is already up either way, and `mage
// accesstoken <peerID>` can be retried separately once whatever's wrong
// with reading its identity.key is fixed.
func printNodeUp(peerID string) {
	fmt.Printf("✅ node %s is up and selected as current\n", peerID)
	token, err := kvctl.AccessToken(peerID)
	if err != nil {
		fmt.Printf("   (could not derive kvhttp access token: %v)\n", err)
		return
	}
	fmt.Printf("   kvhttp access token: %s\n", token)
}

// AddNodeWithKey is the AddNode equivalent for provisioning a node under an
// existing Ed25519 identity (an identity.key file's hex-encoded format --
// e.g. one saved from a node created elsewhere) instead of generating a
// fresh one, bootstrapping it as the cluster's sole leader.
//
// Usage: mage addnodewithkey <keyFile>
func AddNodeWithKey(keyFile string) error {
	return runAddNodeWithKey(keyFile)
}

// AddFollowerWithKey is the AddFollower equivalent of AddNodeWithKey: joins
// the cluster led by leaderPeerID under an existing identity instead of
// generating a fresh one.
//
// Usage: mage addfollowerwithkey <keyFile> <leaderPeerID>
func AddFollowerWithKey(keyFile, leaderPeerID string) error {
	return runAddNodeWithKey(keyFile, leaderPeerID)
}

func runAddNodeWithKey(keyFile string, peerIDs ...string) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	peerID, err := kvctl.AddNodeWithKey(root, keyFile, peerIDs...)
	if err != nil {
		return err
	}
	printNodeUp(peerID)
	return nil
}

// AccessToken prints peerID's deterministic kvhttp bearer token -- see
// kvctl.AccessToken's doc comment. AddNode/AddNodeWithKey already print
// this once at creation time; use this to recover it again later (it's
// re-derived from identity.key, not stored separately, so there's nothing
// to have lost).
//
// Usage: mage accesstoken <peerID>
func AccessToken(peerID string) error {
	token, err := kvctl.AccessToken(peerID)
	if err != nil {
		return err
	}
	fmt.Println(token)
	return nil
}

// Join asks the raft cluster reachable through targetPeerID to admit the
// current node's own identity (see `mage use`), switching it onto a
// composite data dir dedicated to that cluster. If the target requires
// confirmation (-require-confirm-for-join), this only lodges a pending
// request -- ask a current raft voter on that cluster to run
// `mage confirmpermit cluster-join <peerID>` to actually admit it.
//
// Usage: mage join <targetPeerID>
func Join(targetPeerID string) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	peerID, err := kvctl.Join(root, targetPeerID)
	if err != nil {
		return err
	}
	fmt.Printf("✅ node %s is up and selected as current\n", peerID)
	return nil
}

// Leave asks the raft cluster the current node is joined to (see `mage
// use`) to remove it (raft.RemoveServer -- a graceful shrink, the
// remaining voters keep operating normally), then switches the node back
// onto its own default solo single-node cluster. The composite cluster
// data dir is left on disk untouched, so a later `mage join`/
// `mage rejoinnode` back to the same cluster can pick its local state
// back up -- see Rm for the variant that wipes it instead.
//
// Usage: mage leave <peerID>
func Leave(peerID string) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	if err := kvctl.Leave(root, peerID); err != nil {
		return err
	}
	fmt.Printf("✅ node %s left its cluster and resumed its solo db\n", peerID)
	return nil
}

// Rm does everything Leave does, plus revokes peerID's standing with the
// cluster it's leaving (so a later `mage join` attempt against the same
// cluster starts genuinely pending again, not auto-approved by a stale
// confirmed record) and deletes the composite cluster data dir outright.
//
// Usage: mage rm <peerID>
func Rm(peerID string) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	if err := kvctl.Rm(root, peerID); err != nil {
		return err
	}
	fmt.Printf("🗑️  node %s removed from its cluster and resumed its solo db\n", peerID)
	return nil
}

// Kick asks the raft cluster peerID is currently joined to to force-
// remove targetPeerID (raft.RemoveServer) without targetPeerID's own
// cooperation -- Leave/Rm's self-service counterpart for a voter that's
// gone down for good (crashed, wiped, never coming back) and isn't going
// to gracefully `mage leave` itself, potentially leaving the remaining
// voters unable to elect a leader at all until it's dropped from the
// configuration. peerID itself is untouched -- it must already be a raft
// voter (or able to forward to one) for this to take effect.
//
// Usage: mage kick <peerID> <targetPeerID>
func Kick(peerID, targetPeerID string) error {
	if err := kvctl.Kick(peerID, targetPeerID); err != nil {
		return err
	}
	fmt.Printf("✅ %s removed from %s's cluster\n", targetPeerID, peerID)
	return nil
}

// Kvrecover force-recovers a stopped node's raft configuration via
// cmd/kvrecover -- hashicorp/raft's offline raft.RecoverCluster, for the
// one situation Kick cannot reach: the cluster has already lost quorum
// (Kick relies on an ordinary raft.RemoveServer command committing through
// normal consensus, so it needs a majority to still exist -- see Kick's
// own doc comment). Run it with the target node's kvnode daemon stopped;
// see cmd/kvrecover's package doc comment for the full mechanics and,
// when recovering more than one surviving node at once, why every
// survivor needs the identical voters list before any of them restart.
//
// voters is one or more dialable multiaddrs (each including /p2p/<peer-id>)
// for the surviving voters to keep in the recovered configuration --
// space-separated in a single argument (mage targets can't take a
// variadic arg list), so pass them quoted as one shell word, e.g.
// "/ip4/.../p2p/<id1> /ip4/.../p2p/<id2>". The node being recovered must
// include itself.
//
// Usage: mage kvrecover <dataDir> <keyPath> "<voterMultiaddr> [voterMultiaddr...]"
func Kvrecover(dataDir, keyPath, voters string) error {
	voterList := strings.Fields(voters)
	if len(voterList) == 0 {
		return fmt.Errorf("kvrecover: at least one voter multiaddr is required (the recovering node itself, plus any other survivors)")
	}
	root, err := repoRoot()
	if err != nil {
		return err
	}
	args := []string{"run", "./cmd/kvrecover", "-data-dir", dataDir, "-key-path", keyPath}
	for _, v := range voterList {
		args = append(args, "-voter", v)
	}
	cmd := exec.Command("go", args...)
	cmd.Dir = root
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// Use selects which node Set/Get target, by peer id.
// Usage: mage use <peerID>
func Use(peerID string) error {
	if err := kvctl.Use(peerID); err != nil {
		return err
	}
	fmt.Printf("current node set to %s\n", peerID)
	return nil
}

// DeleteNode permanently deletes a node's on-disk data (identity, sqlite
// store, raft log/snapshots) and its registry entry, by peer id. Refuses
// while the node's daemon process still appears to be running -- stop it
// first (there is no automatic kill: this mirrors the e2e pipeline's
// deletenode, which likewise never tears anything down implicitly).
//
// Usage: mage deletenode <peerID>
func DeleteNode(peerID string) error {
	if err := kvctl.DeleteNode(peerID); err != nil {
		return err
	}
	fmt.Printf("🗑️  node %s deleted\n", peerID)
	return nil
}

// Backup writes a tar.gz snapshot of a stopped node's entire data
// directory (identity key, sqlite store, raft log/snapshots) to
// destArchive. Refuses while the node's daemon process still appears to be
// running -- see kvctl.BackupNode's doc comment on why a live copy risks
// being torn, and README's "Backup and restore" section for the full
// runbook (including how to back up a *running* voter without taking it
// down, using raft's own periodic snapshot files instead of this command).
//
// Usage: mage backup <peerID> <destArchive>
func Backup(peerID, destArchive string) error {
	if err := kvctl.BackupNode(peerID, destArchive); err != nil {
		return err
	}
	fmt.Printf("✅ %s backed up to %s\n", peerID, destArchive)
	return nil
}

// Restore extracts a `mage backup` archive to destDir, verbatim -- it does
// not touch the registry or start anything. Restoring into the exact
// directory registry.NodeDataDir/ClusterDataDir would compute for this
// node's peer id (see kvctl.RestoreNode's doc comment) is what lets a
// later `mage resumenode <peerID>` pick this data back up as that node
// again; see README's "Backup and restore" section for the full
// walkthrough of both that path and restoring under a fresh identity
// instead.
//
// Usage: mage restore <archive> <destDir>
func Restore(archivePath, destDir string) error {
	if err := kvctl.RestoreNode(archivePath, destDir); err != nil {
		return err
	}
	fmt.Printf("✅ %s extracted to %s\n", archivePath, destDir)
	return nil
}

// ListClusters shows every raft cluster known to this machine's registry
// (grouped by whichever peer id originally bootstrapped it -- see
// kvctl.ListClusters), and every locally-created node identity that
// belongs to each one. This is purely a local registry read: no daemon
// needs to be running, but for the same reason it only ever shows clusters
// this machine has itself created or joined a node into -- there is no
// network-wide cluster discovery. Pass any listed peer id (one whose
// daemon is currently running) to `mage listnodes` to see that cluster's
// full live raft membership instead.
//
// Usage: mage listclusters
func ListClusters() error {
	clusters, err := kvctl.ListClusters()
	if err != nil {
		return err
	}
	if len(clusters) == 0 {
		fmt.Println("no nodes registered on this machine")
		return nil
	}
	for _, c := range clusters {
		fmt.Printf("cluster %s\n", c.ClusterID)
		for _, m := range c.Members {
			running := "stopped"
			if m.Running {
				running = "running"
			}
			fmt.Printf("  %s  role=%s  %s\n", m.PeerID, m.Role, running)
		}
	}
	return nil
}

// ListNodes queries the already-running node localPeerID for its raft
// cluster's full live membership (every peer id currently a voter/learner/
// leader, including peers this machine never created and so has no
// registry entry for) -- read from that node's own locally-replicated
// shmevent.KindClusterMember records, see kvctl.ListClusterMembers.
// localPeerID would typically be one of the peer ids `mage listclusters`
// just printed, but it must actually be running: unlike raft's own
// AppendEntries/InstallSnapshot traffic, this query only ever reaches a
// daemon over local shmring IPC, never a remote peer directly.
//
// Usage: mage listnodes <localPeerID>
func ListNodes(localPeerID string) error {
	members, err := kvctl.ListClusterMembers(localPeerID)
	if err != nil {
		return err
	}
	if len(members) == 0 {
		fmt.Printf("no cluster-member records found via %s\n", localPeerID)
		return nil
	}
	for _, m := range members {
		fmt.Printf("%s  role=%s\n", m.PeerID, m.Role)
	}
	return nil
}
