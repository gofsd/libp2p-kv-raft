// Command kvhttp is a thin, local-only HTTP front door onto kvctl-cli
// sendevent, for callers that can do a plain fetch() but can't run this
// project's real clients (a browser wasm bundle needing SharedArrayBuffer/
// WebTransport, or a Go/Kotlin binary) -- e.g. an AI chat provider's
// in-browser JS canvas sandbox, which typically restricts outbound
// connections to ordinary HTTPS fetch calls only. See web-app/README.md
// and this project's top-level README for why the real clients can't run
// there directly.
//
// Exactly one endpoint, POST /command, accepting and returning the exact
// same human-readable event JSON kvctl-cli sendevent already speaks (see
// pkg/e2edata.Event's doc comment) -- e.g.
// {"event":"set_key","value":"hello","id":100} or {"event":"get_field",
// "value":"hello"}. This process never touches shmring/libp2p/raft itself;
// it just shells out to kvctl-cli sendevent (same as pkg/e2erun's own
// sendEventLocal) so all the real signing/IPC logic stays in one place.
//
// Not meant to be exposed beyond localhost: it has no auth of its own
// (anyone who can reach it can drive the target node exactly as if they
// were running kvctl-cli sendevent themselves), only permissive CORS so a
// browser-based caller on a different origin can reach it at all.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
)

// commandTimeout bounds one kvctl-cli sendevent subprocess call. Must
// comfortably exceed sendEventTimeout (cmd/kvctl-cli/main.go), which is
// itself the ctx deadline that process applies internally to the IPC call
// -- this is just the outer bound on the subprocess as a whole (process
// startup/exit overhead on top of that internal deadline), not a second
// independent budget.
const commandTimeout = 70 * time.Second

// maxBodyBytes caps the request body kvhttp will read before giving up --
// generous for any real event (the largest legitimate value here is a
// GetPrivateKey/GetPublicKey response's raw key material, nowhere close to
// this), just a backstop against an accidental multi-megabyte POST.
const maxBodyBytes = 1 << 20 // 1 MiB

func main() {
	addr := flag.String("addr", "127.0.0.1:8787", "listen address -- deliberately loopback-only by default, see this file's doc comment")
	peerID := flag.String("peer", "", "target peer id; defaults to whatever `mage use`/`kvctl-cli use` last selected")
	kvctlBin := flag.String("kvctl-cli", "", "path to the kvctl-cli binary; defaults to $PATH, falling back to `go build` into a temp dir")
	flag.Parse()

	resolvedPeer, err := resolvePeerID(*peerID)
	if err != nil {
		log.Fatalf("kvhttp: %v", err)
	}
	resolvedBin, err := resolveKvctlBin(*kvctlBin)
	if err != nil {
		log.Fatalf("kvhttp: %v", err)
	}

	h := &handler{peerID: resolvedPeer, kvctlBin: resolvedBin}
	mux := http.NewServeMux()
	mux.HandleFunc("/command", h.handleCommand)

	log.Printf("kvhttp: targeting peer %s via %s, listening on http://%s/command", resolvedPeer, resolvedBin, *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}

// resolvePeerID returns explicit if non-empty, else whatever `mage use`
// last recorded via pkg/registry -- same lookup kvctl.Set/Get already do,
// so this wrapper needs no config of its own beyond having already run
// `mage addnode`/`mage use <peerID>` once.
func resolvePeerID(explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	reg, err := registry.Open()
	if err != nil {
		return "", fmt.Errorf("open registry: %w", err)
	}
	peerID, err := reg.Current()
	if err != nil {
		return "", fmt.Errorf("resolve target peer: %w (pass -peer explicitly to skip this)", err)
	}
	return peerID, nil
}

// resolveKvctlBin returns explicit if non-empty, else the first of: a
// kvctl-cli already on $PATH, or a freshly `go build`-ed one in a temp
// dir -- mirrors pkg/e2erun.buildNativeBinaries's fallback build, just
// without that function's Android/cross-platform concerns.
func resolveKvctlBin(explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	if found, err := exec.LookPath("kvctl-cli"); err == nil {
		return found, nil
	}

	dir, err := os.MkdirTemp("", "kvhttp-build-")
	if err != nil {
		return "", fmt.Errorf("create build dir: %w", err)
	}
	out := dir + "/kvctl-cli"
	build := exec.Command("go", "build", "-o", out, "./cmd/kvctl-cli")
	build.Stdout = os.Stderr
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		return "", fmt.Errorf("build kvctl-cli (not found on $PATH, pass -kvctl-cli explicitly to skip this): %w", err)
	}
	return out, nil
}

type handler struct {
	peerID   string
	kvctlBin string
}

func (h *handler) handleCommand(w http.ResponseWriter, r *http.Request) {
	// Permissive by design -- see this file's doc comment on why this is
	// meant for localhost-only use, not a substitute for real auth.
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodPost {
		httpError(w, http.StatusMethodNotAllowed, "only POST is supported")
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil {
		httpError(w, http.StatusBadRequest, fmt.Sprintf("read body: %v", err))
		return
	}
	if len(body) > maxBodyBytes {
		httpError(w, http.StatusRequestEntityTooLarge, "body too large")
		return
	}
	if !json.Valid(body) {
		httpError(w, http.StatusBadRequest, "body is not valid JSON")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), commandTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, h.kvctlBin, "sendevent", h.peerID, string(body))
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()

	// kvctl-cli sendevent always prints the response JSON to stdout before
	// exiting -- including the EventError case, where it exits 1 *after*
	// printing (see cmd/kvctl-cli's cmdSendEvent) -- so parse stdout first
	// and only fall back to the raw process error when there's nothing to
	// parse, same reasoning as pkg/e2erun.interpretSendEventResult.
	out := strings.TrimSpace(stdout.String())
	if out == "" {
		httpError(w, http.StatusBadGateway, fmt.Sprintf("kvctl-cli sendevent: %v: %s", runErr, stderr.String()))
		return
	}

	var resp struct {
		Event string `json:"event"`
	}
	if err := json.Unmarshal([]byte(out), &resp); err != nil {
		httpError(w, http.StatusBadGateway, fmt.Sprintf("parse kvctl-cli sendevent output %q: %v", out, err))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if resp.Event == "error" {
		// Still a well-formed response body (the event JSON itself
		// carries the error message in "value") -- 422 marks it as a
		// request-level rejection, distinct from 502's "kvhttp/kvctl-cli
		// itself is broken".
		w.WriteHeader(http.StatusUnprocessableEntity)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	_, _ = w.Write([]byte(out))
}

func httpError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
