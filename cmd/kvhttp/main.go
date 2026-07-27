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
// One kvhttp process serves *every* node currently in the local registry
// (pkg/registry, whatever `mage addnode`/`addnodewithkey` has created on
// this machine) rather than being pinned to one at startup: each request
// must carry `Authorization: Bearer <token>` naming which node it targets,
// where <token> is that node's own deterministic access token
// (registry.AccessTokenForKeyFile -- printed automatically by AddNode/
// AddNodeWithKey, or recovered any time via `mage accesstoken <peerID>`).
// A request whose token doesn't match any registered node's is rejected
// outright, before kvctl-cli ever runs -- so holding one node's token lets
// a caller drive *that* node exactly as if running kvctl-cli sendevent
// themselves, same as before, but says nothing about any other node this
// machine happens to also have registered.
//
// /command is served over HTTPS only -- there is deliberately no plain-HTTP
// fallback (an earlier version had one; it's gone). Token comparison is
// constant-time (crypto/hmac.Equal), but that only protects against timing
// attacks on the comparison itself: without TLS, the token and every
// request/response body travel in the clear, which a plain Bearer scheme
// can't make up for on its own. -tls-cert/-tls-key default to the
// self-signed pair `mage tls:genselfsigned <hosts>` generates
// (pkg/tlscert) under the local registry directory; kvhttp refuses to
// start at all if they're missing, rather than silently falling back to
// plain HTTP. A self-signed cert has no CA behind it, so every caller
// (browser, curl, etc.) must explicitly trust this exact certificate
// first -- expected and fine for this project's own trusted-caller model,
// but note it for real deployment: swap in a CA-issued cert/key pair via
// the same two flags if callers need to trust it without doing that
// manually.
//
// Alongside /command, this process also always starts a second, unrelated
// plain-HTTP server on :80 serving whatever's on disk under staticDir --
// see serveStatic below.
package main

import (
	"context"
	"crypto/hmac"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
)

// defaultStaticDir is served verbatim at web root by serveStatic --
// whatever's on disk under it, at whatever relative path, is what gets
// served. Deliberately opaque to this server: its contents are
// deployment-specific, so they're never committed to the repo (see
// .gitignore) and never built into the binary either -- copied onto the
// deployed host by hand (e.g. scp), entirely outside both git and
// `go build`. The directory doesn't need to exist at all; a missing
// directory/file just 404s. Overridable via -static-dir.
const defaultStaticDir = "static"

// staticAddr is the plain-HTTP listen address serveStatic binds.
const staticAddr = ":80"

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
	kvctlBin := flag.String("kvctl-cli", "", "path to the kvctl-cli binary; defaults to $PATH, falling back to `go build` into a temp dir")
	staticDir := flag.String("static-dir", defaultStaticDir, "directory served verbatim at web root by the secondary listener on "+staticAddr)
	tlsCert := flag.String("tls-cert", "", "path to the TLS certificate (PEM); defaults to the self-signed pair `mage tls:genselfsigned` writes under the local registry directory -- required, see this file's doc comment")
	tlsKey := flag.String("tls-key", "", "path to the TLS private key (PEM); same default/requirement as -tls-cert")
	flag.Parse()

	reg, err := registry.Open()
	if err != nil {
		log.Fatalf("kvhttp: open registry: %v", err)
	}
	resolvedBin, err := resolveKvctlBin(*kvctlBin)
	if err != nil {
		log.Fatalf("kvhttp: %v", err)
	}
	certPath, keyPath, err := resolveTLSFiles(*tlsCert, *tlsKey, reg.Dir)
	if err != nil {
		log.Fatalf("kvhttp: %v", err)
	}

	h := &handler{reg: reg, kvctlBin: resolvedBin}
	mux := http.NewServeMux()
	mux.HandleFunc("/command", h.handleCommand)

	go serveStatic(*staticDir)

	log.Printf("kvhttp: multi-tenant (every node in %s), routed by Authorization: Bearer <token> -- see `mage accesstoken <peerID>` -- via %s, listening on https://%s/command (cert: %s)", reg.Dir, resolvedBin, *addr, certPath)
	log.Fatal(http.ListenAndServeTLS(*addr, certPath, keyPath, mux))
}

// defaultTLSDir must match magefile.go's kvhttpTLSDir exactly -- both
// resolve to <registry dir>/kvhttp-tls, the one being where `mage
// tls:genselfsigned` writes cert.pem/key.pem and the other being where
// kvhttp defaults to reading them from, so that generating the pair and
// then just running kvhttp with no -tls-cert/-tls-key flags at all works
// with no further wiring.
func defaultTLSDir(registryDir string) string {
	return filepath.Join(registryDir, "kvhttp-tls")
}

// resolveTLSFiles returns explicitCert/explicitKey if both are set,
// otherwise the default pair under defaultTLSDir(registryDir) -- and
// fails closed (a clear error, not a silent plain-HTTP fallback) if
// whichever pair applies doesn't actually exist on disk. There is no
// supported way to run kvhttp without TLS at all -- see this file's own
// doc comment on why an earlier plain-HTTP mode was removed rather than
// kept as a fallback.
func resolveTLSFiles(explicitCert, explicitKey, registryDir string) (certPath, keyPath string, err error) {
	certPath, keyPath = explicitCert, explicitKey
	if certPath == "" && keyPath == "" {
		dir := defaultTLSDir(registryDir)
		certPath = filepath.Join(dir, "cert.pem")
		keyPath = filepath.Join(dir, "key.pem")
	} else if certPath == "" || keyPath == "" {
		return "", "", fmt.Errorf("-tls-cert and -tls-key must both be set, or both left unset to use the default self-signed pair")
	}
	if _, statErr := os.Stat(certPath); statErr != nil {
		return "", "", fmt.Errorf("TLS certificate not found at %s (run `mage tls:genselfsigned <hosts>` first, or pass -tls-cert/-tls-key explicitly): %w", certPath, statErr)
	}
	if _, statErr := os.Stat(keyPath); statErr != nil {
		return "", "", fmt.Errorf("TLS private key not found at %s (run `mage tls:genselfsigned <hosts>` first, or pass -tls-cert/-tls-key explicitly): %w", keyPath, statErr)
	}
	return certPath, keyPath, nil
}

// serveStatic serves dir at web root, verbatim, over plain HTTP on
// staticAddr. Deliberately a separate, minimal *http.Server from the
// loopback -addr listener above -- this has nothing to do with the
// token-gated /command bridge, and failing/blocking here must never take
// that down. Blocks; run in its own goroutine.
func serveStatic(dir string) {
	log.Printf("kvhttp: secondary listener on %s, serving %s", staticAddr, dir)
	if err := http.ListenAndServe(staticAddr, http.FileServer(http.Dir(dir))); err != nil && err != http.ErrServerClosed {
		log.Fatalf("secondary listener on %s: %v", staticAddr, err)
	}
}

// resolvePeerFromToken finds the registered node whose own access token
// (registry.AccessTokenForKeyFile) matches token, re-reading the registry
// fresh on every call -- so a node created after kvhttp started (e.g. a
// fresh `mage addnodewithkey`) is reachable immediately, no restart needed.
// Comparison is constant-time (hmac.Equal) per candidate; the number of
// registered nodes on one machine is small enough that this being O(n)
// rather than an indexed lookup doesn't matter.
func (h *handler) resolvePeerFromToken(token string) (string, error) {
	if token == "" {
		return "", fmt.Errorf("missing bearer token")
	}
	nodes, err := h.reg.List()
	if err != nil {
		return "", fmt.Errorf("list registered nodes: %w", err)
	}
	for _, n := range nodes {
		want, err := registry.AccessTokenForKeyFile(n.KeyPath)
		if err != nil {
			// This node's own key file is unreadable/corrupt -- not this
			// request's problem, and not necessarily every other node's
			// either, so skip it rather than failing the whole lookup.
			continue
		}
		if hmac.Equal([]byte(want), []byte(token)) {
			return n.PeerID, nil
		}
	}
	return "", fmt.Errorf("no registered node matches this access token")
}

// bearerToken extracts the token from an `Authorization: Bearer <token>`
// header, or "" if the header is missing or a different scheme.
func bearerToken(r *http.Request) string {
	const prefix = "Bearer "
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, prefix) {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(h, prefix))
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
	reg      *registry.Registry
	kvctlBin string
}

func (h *handler) handleCommand(w http.ResponseWriter, r *http.Request) {
	// CORS is permissive by design -- see this file's doc comment for why
	// that's fine here (auth is the Authorization header below, checked
	// per request, not anything origin-based). "Authorization" must be
	// listed for a browser's CORS preflight to allow the real request
	// through with that header set.
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodPost {
		httpError(w, http.StatusMethodNotAllowed, "only POST is supported")
		return
	}

	peerID, err := h.resolvePeerFromToken(bearerToken(r))
	if err != nil {
		// Deliberately the same message/status for "no token" and "token
		// doesn't match any node" -- distinguishing them would let a
		// caller probe for whether *some* node's token looks like theirs.
		httpError(w, http.StatusUnauthorized, "missing or invalid access token (Authorization: Bearer <token>; see `mage accesstoken <peerID>`)")
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

	cmd := exec.CommandContext(ctx, h.kvctlBin, "sendevent", peerID, string(body))
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
