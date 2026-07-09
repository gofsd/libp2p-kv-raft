//go:build !windows

package kvctl

import (
	"os"
	"os/exec"
	"syscall"
)

// detach configures cmd to run in its own session, so it survives the
// spawning mage process exiting.
func detach(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
}

// isAlive reports whether pid refers to a running process. os.FindProcess
// always succeeds on Unix regardless of whether pid exists, so liveness has
// to be tested with a signal-0 probe (a standard Unix idiom: signal 0 does
// nothing but still reports ESRCH if the process is gone).
func isAlive(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}
