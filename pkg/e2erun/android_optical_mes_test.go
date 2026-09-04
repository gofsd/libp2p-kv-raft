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

// TestResolveMesCasesSubstitutesTheExpectationSideToo pins the half that is easy to forget: a
// "form" case names the backend address twice -- once as a param device A types, once as the
// value device B must find in the field -- and substituting only the first makes a correct run
// fail with a mismatch that reads like a dropped param.
func TestResolveMesCasesSubstitutesTheExpectationSideToo(t *testing.T) {
	t.Setenv(mesBackendAddrEnvVar, "/ip4/127.0.0.1/tcp/46217/p2p/12D3KooWFake")

	got := resolveMesCases([]e2edata.OpticalScanCase{{
		CaseID:   "form_case",
		Generate: e2edata.OpticalGenerateSpec{Target: "nav_form", Category: "Mes", Name: "LinkAccount", Params: []string{mesBackendAddrToken, "optical rig"}},
		Expect:   e2edata.OpticalExpectSpec{Kind: "form", ExpectParams: []string{mesBackendAddrToken, "optical rig"}},
	}})

	if len(got) != 1 {
		t.Fatalf("resolved %d cases, want 1", len(got))
	}
	if got[0].Generate.Params[0] != "/ip4/127.0.0.1/tcp/46217/p2p/12D3KooWFake" {
		t.Errorf("generate param = %q, want the real address", got[0].Generate.Params[0])
	}
	if got[0].Expect.ExpectParams[0] != "/ip4/127.0.0.1/tcp/46217/p2p/12D3KooWFake" {
		t.Errorf("expect param = %q, want the real address", got[0].Expect.ExpectParams[0])
	}
}

// TestResolveMesCasesSubstitutesTheEnrolmentToken: the enrolment cases are the only ones that
// need a second host-resolved value, and a case naming both must get both -- substituting one and
// leaving the other typed into a form as a literal "{{...}}" is a failure that names the wrong
// half.
func TestResolveMesCasesSubstitutesTheEnrolmentToken(t *testing.T) {
	const addr = "/ip4/192.0.2.7/tcp/4001/p2p/12D3KooWTest"
	const token = "MFRGGZDFMZTWQ2LK"
	t.Setenv(mesBackendAddrEnvVar, addr)
	t.Setenv(mesEnrolTokenEnvVar, token)

	got := resolveMesCases([]e2edata.OpticalScanCase{
		mesCase("enrol", mesBackendAddrToken, mesEnrolTokenToken, ""),
	})
	if len(got) != 1 {
		t.Fatalf("want the case kept, got %d", len(got))
	}
	if got[0].Generate.Params[0] != addr || got[0].Generate.Params[1] != token {
		t.Errorf("want both tokens substituted, got %q", got[0].Generate.Params)
	}
}

// TestResolveMesCasesDropsEnrolmentCasesWithNoToken: a rig running a signal-cli old enough not to
// mint one is still a good rig for every other mes case, so only the cases naming the token may be
// dropped -- and the message has to name *which* value was missing, since a batch that silently
// dropped nine cases for one reason and called it the other is how an operator ends up restarting
// the wrong process.
func TestResolveMesCasesDropsEnrolmentCasesWithNoToken(t *testing.T) {
	t.Setenv(mesBackendAddrEnvVar, "/ip4/192.0.2.7/tcp/4001/p2p/12D3KooWTest")
	t.Setenv(mesEnrolTokenEnvVar, "")
	// The file fallback is the operator's real home, where a rig session may well have left a
	// token -- point HOME somewhere empty so this test measures the absent case it means to.
	t.Setenv("HOME", t.TempDir())

	got := resolveMesCases([]e2edata.OpticalScanCase{
		mesCase("ordinary_mes", mesBackendAddrToken, "verify-alice"),
		mesCase("enrol", mesBackendAddrToken, mesEnrolTokenToken, ""),
		mesCase("no_backend_needed"),
	})
	if len(got) != 2 {
		t.Fatalf("want the two cases that need no token kept, got %d: %+v", len(got), got)
	}
	for _, c := range got {
		if c.CaseID == "enrol" {
			t.Fatal("a case naming the enrolment token should not survive with no token to substitute")
		}
	}
}

// TestMesEnrolTokenPrefersTheEnvironment pins the same precedence the address has, and for the
// same reason: an operator naming a token by hand must not be silently overridden by whatever a
// previous serve session left in the rendezvous file -- which for a *token* is worse than for an
// address, since a spent one fails the case rather than failing to connect.
func TestMesEnrolTokenPrefersTheEnvironment(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := os.MkdirAll(home+"/.libp2p-kv-raft", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(mesEnrolTokenPath(), []byte("  FROMTHEFILE1234  \n"), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv(mesEnrolTokenEnvVar, "FROMTHEENVIRONMEN")
	if got := mesEnrolToken(); got != "FROMTHEENVIRONMEN" {
		t.Errorf("want the environment to win, got %q", got)
	}

	t.Setenv(mesEnrolTokenEnvVar, "")
	if got := mesEnrolToken(); got != "FROMTHEFILE1234" {
		t.Errorf("want the file read and trimmed when the environment is silent, got %q", got)
	}
}

// TestResolveHumanCasesKeepsThemOutOfAnUnattendedRun: a case nobody is standing next to did not
// fail, it was never runnable, and a permanent red mark meaning "nobody was there" is worse than
// not measuring it.
func TestResolveHumanCasesKeepsThemOutOfAnUnattendedRun(t *testing.T) {
	cases := []e2edata.OpticalScanCase{
		{CaseID: "ordinary"},
		{CaseID: "needs_a_person", NeedsHuman: true},
	}

	got := resolveHumanCases(cases)
	if len(got) != 1 || got[0].CaseID != "ordinary" {
		t.Fatalf("resolved %v, want just the ordinary case", got)
	}

	t.Setenv(humanCasesEnvVar, "1")
	if got := resolveHumanCases(cases); len(got) != 2 {
		t.Fatalf("resolved %d cases with %s set, want both", len(got), humanCasesEnvVar)
	}
}
