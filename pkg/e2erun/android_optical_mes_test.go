package e2erun

import (
	"os"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
)

func mesCase(id string, params ...string) e2edata.OpticalScanCase {
	return e2edata.OpticalScanCase{
		CaseID:   id,
		Generate: e2edata.OpticalGenerateSpec{Target: "run", Category: "Mes", Name: "InventoryVerify", Params: params},
	}
}

// TestResolveMesCasesSubstitutesTheConfiguredBackend: the whole point of the token is that a case
// can be written once and run against whatever backend a given rig stood up.
func TestResolveMesCasesSubstitutesTheConfiguredBackend(t *testing.T) {
	const addr = "/ip4/192.0.2.7/tcp/4001/p2p/12D3KooWTest"
	t.Setenv(mesBackendAddrEnvVar, addr)

	got := resolveMesCases([]e2edata.OpticalScanCase{
		mesCase("plain"),
		mesCase("mes", mesBackendAddrToken, "verify-alice"),
	})
	if len(got) != 2 {
		t.Fatalf("want both cases kept, got %d", len(got))
	}
	if got[1].Generate.Params[0] != addr {
		t.Errorf("want the token replaced with %q, got %q", addr, got[1].Generate.Params[0])
	}
	if got[1].Generate.Params[1] != "verify-alice" {
		t.Errorf("a param that is not the token should be untouched, got %q", got[1].Generate.Params[1])
	}
}

// TestResolveMesCasesDropsMesCasesWithNoBackend: a rig with no signal-cli backend running is
// still a good rig for every case that does not need one, so those must survive -- and the ones
// that do need one must be dropped rather than left to fail on a literal "{{mesBackendAddr}}"
// typed into a form.
func TestResolveMesCasesDropsMesCasesWithNoBackend(t *testing.T) {
	t.Setenv(mesBackendAddrEnvVar, "")
	// The file fallback is the operator's real home, which this test must not depend on the
	// contents of -- point HOME somewhere empty for the duration.
	t.Setenv("HOME", t.TempDir())

	got := resolveMesCases([]e2edata.OpticalScanCase{
		mesCase("plain"),
		mesCase("mes", mesBackendAddrToken),
		mesCase("plain_two"),
	})
	if len(got) != 2 {
		t.Fatalf("want the two backend-free cases kept, got %d: %+v", len(got), got)
	}
	for _, c := range got {
		if c.CaseID == "mes" {
			t.Fatal("a case naming the token should not survive with no backend to substitute")
		}
	}
}

// TestMesBackendAddrPrefersTheEnvironment pins the precedence: an operator pointing a run at a
// particular backend by hand must not be silently overridden by whatever a previous serve session
// left in the rendezvous file.
func TestMesBackendAddrPrefersTheEnvironment(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := os.MkdirAll(home+"/.libp2p-kv-raft", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(mesBackendAddrPath(), []byte("  /from/the/file  \n"), 0o644); err != nil {
		t.Fatal(err)
	}

	t.Setenv(mesBackendAddrEnvVar, "/from/the/env")
	if got := mesBackendAddr(); got != "/from/the/env" {
		t.Errorf("want the environment to win, got %q", got)
	}

	t.Setenv(mesBackendAddrEnvVar, "")
	if got := mesBackendAddr(); got != "/from/the/file" {
		t.Errorf("want the file read and trimmed when the environment is silent, got %q", got)
	}
}
