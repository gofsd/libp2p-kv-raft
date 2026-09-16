//go:build !android

package ipc

import (
	"io"
	"os"
	"sync"
	"testing"
)

// resetCallTiming re-reads the env var for one test -- the real one is read once, on purpose, so
// the hot path does not pay a getenv per round trip.
func resetCallTiming(t *testing.T, value string) {
	t.Helper()
	t.Setenv(CallTimingEnvVar, value)
	callTimingOnce = sync.Once{}
	callTimingDur, callTimingOn = 0, false
	t.Cleanup(func() {
		callTimingOnce = sync.Once{}
		callTimingDur, callTimingOn = 0, false
	})
}

// captureStderr returns whatever fn wrote to os.Stderr.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	orig := os.Stderr
	os.Stderr = w
	done := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	fn()
	w.Close()
	os.Stderr = orig
	return <-done
}
