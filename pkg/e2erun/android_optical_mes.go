package e2erun

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
)

// mesBackendAddrToken is the substitution token an optical case puts where the mes/signal-cli
// dispatch backend's dialable multiaddr belongs.
//
// It is resolved here, on the host, rather than on device A the way "{{selfAddr}}" and
// "{{journalLine}}" are (see e2edata.OpticalGenerateSpec.Params). Those two describe the
// *generating device* and can only be answered there; this one describes a third process that
// neither device runs, that is stood up fresh per rig session, and whose address is therefore
// known to the host and to nothing else. Baking it into the app instead was tried in spirit and
// is exactly what BuildConfig.MES_BACKEND_ADDR already is: a build-time constant that goes stale
// the moment the backend is restarted on a different port.
const mesBackendAddrToken = "{{mesBackendAddr}}"

// mesBackendAddrEnvVar names the backend directly, and wins over mesBackendAddrFile.
const mesBackendAddrEnvVar = "MES_OPTICAL_BACKEND_ADDR"

// mesBackendAddrFile is where signal-cli's own optical rig (that repo's
// test/e2e/serve_test.go's TestServeOpticalBackend) writes the address of the backend it just
// stood up, under the same home directory every node in this project already keeps its state in.
// A file rather than only an env var because the backend and this harness are two processes
// started by hand in two terminals, and copying a multiaddr between them by eye is exactly the
// kind of step that silently pins a run to a previous session's port.
//
// Resolved against the operator's real home rather than registry.Open()'s root, even though the
// two are normally the same directory: signal-cli's rig points registry.EnvHome at a throwaway
// temp dir so its node state never touches the operator's own, and a rendezvous file written
// there would be deleted with it before this harness ever looked.
const mesBackendAddrFile = "mes-optical-backend.addr"

// resolveMesCases substitutes mesBackendAddrToken into every case that names it, and drops the
// cases that name it when no backend address can be found.
//
// Dropping rather than failing, because the mes cases are the only ones in the plan that depend
// on a third process being up: a rig with no signal-cli backend running is still a perfectly
// good rig for the other 137 cases, and failing the whole batch over an absent optional
// dependency would make the common run report a problem it does not have. Dropping rather than
// failing them individually, too -- a case that never ran is not a case that failed, and 93
// automatic failures would drown the real result.
//
// It is loud about it either way. A dropped case is a hole in the coverage a run reports, and a
// run that quietly measured 137 of 230 while printing "137 of 137" is worse than one that
// measured nothing.
func resolveMesCases(cases []e2edata.OpticalScanCase) []e2edata.OpticalScanCase {
	addr := mesBackendAddr()

	out := make([]e2edata.OpticalScanCase, 0, len(cases))
	var dropped []string
	for _, c := range cases {
		if !caseNeedsMesBackend(c) {
			out = append(out, c)
			continue
		}
		if addr == "" {
			dropped = append(dropped, c.CaseID)
			continue
		}
		params := make([]string, len(c.Generate.Params))
		for i, p := range c.Generate.Params {
			params[i] = strings.ReplaceAll(p, mesBackendAddrToken, addr)
		}
		c.Generate.Params = params
		// The expectation side needs the same substitution, not just the generate side: a "form"
		// case asserts that the values device A typed arrived in device B's form, so it names the
		// very same address. Left unsubstituted it compares a real multiaddr against the literal
		// "{{mesBackendAddr}}" and fails with a mismatch that looks like a dropped param.
		if len(c.Expect.ExpectParams) > 0 {
			want := make([]string, len(c.Expect.ExpectParams))
			for i, p := range c.Expect.ExpectParams {
				want[i] = strings.ReplaceAll(p, mesBackendAddrToken, addr)
			}
			c.Expect.ExpectParams = want
		}
		out = append(out, c)
	}

	if len(dropped) > 0 {
		fmt.Fprintf(os.Stderr,
			"e2erun: optical: no mes backend address (set %s, or start signal-cli's own "+
				"TestServeOpticalBackend, which writes %s) -- skipping %d of %d case(s), so this "+
				"run measures the other %d only: %s\n",
			mesBackendAddrEnvVar, mesBackendAddrPath(), len(dropped), len(cases), len(out),
			strings.Join(dropped, ", "))
	} else if addr != "" {
		fmt.Fprintf(os.Stderr, "e2erun: optical: mes backend at %s\n", addr)
	}
	return out
}

// caseNeedsMesBackend reports whether c cannot run without a backend to dial. Decided by the
// token actually appearing in the case's params rather than by its category being "Mes", so a
// case that reaches the backend some other way (a Dispatch: DialSubmitCommand aimed at it, say)
// is covered by the same rule without needing to be listed anywhere.
func caseNeedsMesBackend(c e2edata.OpticalScanCase) bool {
	for _, p := range c.Generate.Params {
		if strings.Contains(p, mesBackendAddrToken) {
			return true
		}
	}
	return false
}

// mesBackendAddr reads the backend's address from the environment, then from the file the
// backend itself writes. Returns "" when neither names one.
func mesBackendAddr() string {
	if addr := strings.TrimSpace(os.Getenv(mesBackendAddrEnvVar)); addr != "" {
		return addr
	}
	body, err := os.ReadFile(mesBackendAddrPath())
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(body))
}

func mesBackendAddrPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return mesBackendAddrFile
	}
	return filepath.Join(home, ".libp2p-kv-raft", mesBackendAddrFile)
}

// humanCasesEnvVar opts a batch into the cases that need a person standing next to the rig.
//
// Off by default and never inferred: an unattended run that includes one does not fail because
// anything is broken, it fails because nobody was there -- and a permanent red mark that means
// "nobody was there" is worse than not measuring the case at all. It is the same bargain
// resolveMesCases makes for a rig with no signal-cli running.
const humanCasesEnvVar = "MES_OPTICAL_HUMAN"

// resolveHumanCases drops every case marked NeedsHuman unless humanCasesEnvVar asks for them,
// and says which it dropped.
//
// Loud on both paths, deliberately. Dropping silently would let a run report "all cases passed"
// for a plan whose most expensive case was skipped; including them silently would strand a runner
// who does not know a phone is about to be needed, in front of a case that waits minutes and then
// fails.
func resolveHumanCases(cases []e2edata.OpticalScanCase) []e2edata.OpticalScanCase {
	want := os.Getenv(humanCasesEnvVar) != ""

	out := make([]e2edata.OpticalScanCase, 0, len(cases))
	var dropped []string
	for _, c := range cases {
		if c.NeedsHuman && !want {
			dropped = append(dropped, c.CaseID)
			continue
		}
		out = append(out, c)
	}

	if len(dropped) > 0 {
		fmt.Fprintf(os.Stderr,
			"e2erun: optical: skipping %d case(s) that need somebody at the rig (set %s=1 to run them): %s\n",
			len(dropped), humanCasesEnvVar, strings.Join(dropped, ", "))
	} else if want {
		fmt.Fprintf(os.Stderr,
			"e2erun: optical: %s is set -- this batch includes cases that WAIT FOR A PERSON. Be at the rig.\n",
			humanCasesEnvVar)
	}
	return out
}
