package e2erun

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
)

// DeleteNode tears down whatever real process/data nodeID's platform
// actually has running (a local kvnode for PlatformDesktop, the SSH
// bootstrap daemon and its entire remote directory for PlatformRemote --
// see BootstrapRemoteDir's doc comment on why that's safe to fully remove
// without touching anything else on the host; PlatformWeb/PlatformAndroid
// have no persistent process this pipeline manages), then removes it from
// f. Nodes are never torn down automatically by Run -- see this package's
// doc comment -- this is the explicit, human-invoked counterpart (`mage
// e2e:deletenode`).
func DeleteNode(f *e2edata.File, nodeID int) error {
	node, ok := f.Nodes[nodeID]
	if !ok {
		return fmt.Errorf("e2erun: unknown node id %d", nodeID)
	}

	switch node.Platform {
	case e2edata.PlatformDesktop:
		if err := deleteLocalDesktopNode(node); err != nil {
			return err
		}
	case e2edata.PlatformRemote:
		if err := deleteRemoteNode(); err != nil {
			return err
		}
	}

	_, affectedRows, err := f.DeleteNode(nodeID)
	if err != nil {
		return err
	}
	if affectedRows > 0 {
		fmt.Fprintf(os.Stderr, "e2erun: warning: %d row(s) still reference deleted node %d; they will fail with \"unknown node id\" until removed too\n", affectedRows, nodeID)
	}
	return nil
}

// DeleteAllNodes tears down every node currently in f the same way
// DeleteNode does, one at a time in ascending node-id order (so output reads
// top-to-bottom the same way `mage e2e:deletenode` output would if run
// manually for each id) -- the explicit, human-invoked counterpart to
// wanting a clean slate across desktop/remote/android/web all at once
// (`mage e2e:destroyall`). Continues past a single node's teardown failure
// rather than aborting, collecting every error, since one node's local
// process being unkillable (say) shouldn't leave every other node's real
// process/data untouched -- returns a combined error afterward if anything
// failed, but every node that *could* be torn down still was.
func DeleteAllNodes(f *e2edata.File) error {
	ids := make([]int, 0, len(f.Nodes))
	for id := range f.Nodes {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	var errs []string
	for _, id := range ids {
		if err := DeleteNode(f, id); err != nil {
			errs = append(errs, fmt.Sprintf("node %d: %v", id, err))
			continue
		}
		fmt.Printf("✅ node %d destroyed\n", id)
	}
	if len(errs) > 0 {
		return fmt.Errorf("e2erun: failed to destroy %d node(s):\n%s", len(errs), strings.Join(errs, "\n"))
	}
	return nil
}

// deleteLocalDesktopNode kills node's local kvnode process (by the exact
// pid EnsureLocalDesktopNode recorded, never a name/pattern match -- see
// isalive_unix.go's doc comment on why pattern-based kills are avoided
// throughout this package) if it's still alive, then removes its entire
// local data directory.
func deleteLocalDesktopNode(node e2edata.Node) error {
	e2eHome, err := localE2EHome()
	if err != nil {
		return err
	}
	dataDir := desktopNodeDataDir(e2eHome, node)
	pidPath := filepath.Join(dataDir, "e2e.pid")
	if data, err := os.ReadFile(pidPath); err == nil {
		if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil && isAlive(pid) {
			if proc, err := os.FindProcess(pid); err == nil {
				_ = proc.Kill()
			}
		}
	}
	return os.RemoveAll(dataDir)
}

// deleteRemoteNode kills the bootstrap daemon (by the exact pid
// startIfNotRunning recorded in bootstrapPidFile) if it's still alive, then
// removes BootstrapRemoteDir entirely.
func deleteRemoteNode() error {
	out, _ := sshOutput(BootstrapHost, "cat "+bootstrapPidFile+" 2>/dev/null || true")
	if pid := strings.TrimSpace(out); pid != "" {
		// pid is read back from a file only startIfNotRunning ever writes
		// (always a bare integer), not attacker- or user-controlled input,
		// so interpolating it into the remote command is safe here.
		if err := sshRun(BootstrapHost, "kill "+pid+" 2>/dev/null || true"); err != nil {
			return err
		}
	}
	return sshRun(BootstrapHost, "rm -rf "+BootstrapRemoteDir)
}
