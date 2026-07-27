// Package tlscert generates a self-signed TLS certificate/key pair for
// cmd/kvhttp's HTTPS listener -- see that command's own doc comment on why
// it now refuses to serve /command over plain HTTP at all. Pure Go
// (crypto/x509/crypto/ecdsa), no external openssl dependency, matching this
// project's general "no external toolchain beyond Go itself" preference
// (see modernc.org/sqlite's own pure-Go rationale in the top-level README).
package tlscert

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// DefaultValidFor is how long a freshly generated certificate is valid --
// generous for a self-signed cert nobody's rotating automatically, short
// enough that a permanently-compromised key doesn't stay trusted forever
// if the host is ever rebuilt with a fresh one instead of regenerating in
// place.
const DefaultValidFor = 825 * 24 * time.Hour // ~2 years, under browsers' historical max cert lifetime norms

// GenerateSelfSigned writes a freshly generated ECDSA (P-256) private key
// and a self-signed X.509 certificate for it to keyPath/certPath (PEM,
// 0o600 for the key), valid for validFor from now. hosts is every
// hostname/IP the certificate should be valid for (SANs) -- e.g.
// "localhost", "127.0.0.1", and the real public IP/hostname callers will
// actually connect to; a self-signed cert with no matching SAN fails
// validation even with the cert otherwise trusted. Overwrites any existing
// files at those paths.
func GenerateSelfSigned(certPath, keyPath string, hosts []string, validFor time.Duration) error {
	if len(hosts) == 0 {
		return fmt.Errorf("tlscert: at least one host/IP is required for the certificate's SAN list")
	}

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("tlscert: generate key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("tlscert: generate serial number: %w", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: hosts[0], Organization: []string{"libp2p-kv-raft kvhttp (self-signed)"}},
		NotBefore:             time.Now().Add(-5 * time.Minute), // clock-skew slack
		NotAfter:              time.Now().Add(validFor),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA:                  true, // self-signed leaf acting as its own root, so ServerAuth chain-building accepts it
		BasicConstraintsValid: true,
	}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, h)
		}
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		return fmt.Errorf("tlscert: create certificate: %w", err)
	}

	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return fmt.Errorf("tlscert: marshal private key: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(certPath), 0o755); err != nil {
		return fmt.Errorf("tlscert: create cert directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(keyPath), 0o755); err != nil {
		return fmt.Errorf("tlscert: create key directory: %w", err)
	}

	certOut := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(certPath, certOut, 0o644); err != nil {
		return fmt.Errorf("tlscert: write certificate: %w", err)
	}
	keyOut := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyPath, keyOut, 0o600); err != nil {
		return fmt.Errorf("tlscert: write private key: %w", err)
	}
	return nil
}
