package tlscert

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestGenerateSelfSignedProducesAValidServableCert(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")

	if err := GenerateSelfSigned(certPath, keyPath, []string{"127.0.0.1", "localhost"}, DefaultValidFor); err != nil {
		t.Fatalf("GenerateSelfSigned: %v", err)
	}

	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatalf("LoadX509KeyPair: %v", err)
	}

	// Serve a real TLS listener with the generated pair and dial it as a
	// client would -- the closest thing to how cmd/kvhttp actually uses
	// this, not just checking the files parse.
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	srv.StartTLS()
	defer srv.Close()

	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("ParseCertificate: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)

	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET over TLS with the generated cert trusted: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	// Untrusted client (doesn't have the self-signed cert in its pool)
	// must be rejected -- proves this is a real cert requiring explicit
	// trust, not an accidentally-permissive one.
	untrusted := &http.Client{Timeout: 5 * time.Second}
	if _, err := untrusted.Get(srv.URL); err == nil {
		t.Fatal("expected an untrusted client to reject the self-signed cert, got no error")
	}
}

func TestGenerateSelfSignedRequiresAtLeastOneHost(t *testing.T) {
	dir := t.TempDir()
	err := GenerateSelfSigned(filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem"), nil, DefaultValidFor)
	if err == nil {
		t.Fatal("expected an error with zero hosts, got nil")
	}
}
