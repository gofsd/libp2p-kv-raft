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

// mesEnrolTokenToken is where an optical case puts a live enrolment token: the one-time secret
// that turns the phone into a device the mes backend takes admin orders from.
//
// Host-resolved for the same reason mesBackendAddrToken is, and then one reason more. A token is
// minted per rig session, so no committed plan can name one -- but the deeper point is that
// enrolment's real door does not pass this way at all. The backend sends a code *into Signal*,
// and every Signal edge on the optical rig is a fake, so no code the product would send ever
// reaches a camera. The rig therefore mints one and says where it is (signal-cli's
// test/e2e/serve_test.go, publishOpticalEnrolmentToken), and the plan generates the same RunCode
// the backend would have sent. What that costs is honest to state: the enrolment cases exercise
// the redeeming half for real -- the arity of the code, the scan, the grant, what the grant then
// permits -- and not the delivery half, which only a real Signal account can show.
const mesEnrolTokenToken = "{{mesEnrolToken}}"

// mesEnrolTokenEnvVar names the token directly, and wins over mesEnrolTokenFile.
const mesEnrolTokenEnvVar = "MES_OPTICAL_ENROL_TOKEN"

// mesEnrolTokenFile is where signal-cli's optical rig writes the token it minted, beside the
// address file and read the same way.
const mesEnrolTokenFile = "mes-optical-enrol.token"

// mesSubstitution is one host-resolved token: what to look for, what to put there, and what to
// tell an operator whose rig cannot supply it.
//
// A list rather than two hand-written passes because the two behave identically -- substitute
// where present, drop the case where absent, say so once at the end -- and because a case may
// name both, in which case it needs *both* before it can run and the reason it was dropped has
// to name the one that was missing.
type mesSubstitution struct {
	token string
	value string
	hint  string
}

// resolveMesCases substitutes every host-resolved token into the cases that name it, and drops
// the cases naming one this rig cannot supply.
//
// Two tokens today, and they fail independently: a rig may have a backend and no enrolment token
// (an older signal-cli, or one killed and restarted without republishing), in which case the nine
// enrolment cases go and the other ninety-odd mes cases stay. Which is why the report below names
// the missing *value* rather than counting cases -- an operator told that cases were skipped for
// want of "a mes backend" restarts a process that is already running.
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
	subs := []mesSubstitution{{
		token: mesBackendAddrToken,
		value: mesBackendAddr(),
		hint: fmt.Sprintf("no mes backend address (set %s, or start signal-cli's own TestServeOpticalBackend, which writes %s)",
			mesBackendAddrEnvVar, mesBackendAddrPath()),
	}, {
		token: mesEnrolTokenToken,
		value: mesEnrolToken(),
		hint: fmt.Sprintf("no enrolment token (set %s, or run a signal-cli rig new enough to write %s)",
			mesEnrolTokenEnvVar, mesEnrolTokenPath()),
	}}

	out := make([]e2edata.OpticalScanCase, 0, len(cases))
	dropped := map[string][]string{}
	for _, c := range cases {
		missing := ""
		for _, sub := range subs {
			if sub.value == "" && caseNeedsToken(c, sub.token) {
				missing = sub.hint
				break
			}
		}
		if missing != "" {
			dropped[missing] = append(dropped[missing], c.CaseID)
			continue
		}
		for _, sub := range subs {
			if sub.value == "" {
				continue
			}
			c = substituteToken(c, sub.token, sub.value)
		}
		out = append(out, c)
	}

	if addr := subs[0].value; addr != "" {
		fmt.Fprintf(os.Stderr, "e2erun: optical: mes backend at %s\n", addr)
	}
	for _, sub := range subs {
		ids := dropped[sub.hint]
		if len(ids) == 0 {
			continue
		}
		fmt.Fprintf(os.Stderr,
			"e2erun: optical: %s -- skipping %d of %d case(s), so this run measures the other %d only: %s\n",
			sub.hint, len(ids), len(cases), len(out), strings.Join(ids, ", "))
	}
	return out
}

// substituteToken replaces one token everywhere a case can name it.
//
// The expectation side needs the same substitution, not just the generate side, for two separate
// reasons. A "form" case asserts that the values device A typed arrived in device B's form, so it
// names the very same address; left unsubstituted it compares a real multiaddr against the literal
// "{{mesBackendAddr}}" and fails with a mismatch that looks like a dropped param. And a
// "sheets_cell" case has to say which backend to ask for its cell -- that read happens after the
// command has already answered, so there is no generate spec on device B to take an address from.
// A token surviving there is worse than a mismatch, because it is silent: the device falls back to
// its build-time constant and reads a cell of whatever backend that names, which on a rig that
// mints a fresh address per session is not this run's backend at all.
func substituteToken(c e2edata.OpticalScanCase, token, value string) e2edata.OpticalScanCase {
	params := make([]string, len(c.Generate.Params))
	for i, p := range c.Generate.Params {
		params[i] = strings.ReplaceAll(p, token, value)
	}
	c.Generate.Params = params
	if len(c.Expect.ExpectParams) > 0 {
		want := make([]string, len(c.Expect.ExpectParams))
		for i, p := range c.Expect.ExpectParams {
			want[i] = strings.ReplaceAll(p, token, value)
		}
		c.Expect.ExpectParams = want
	}
	c.Expect.SheetsBackendAddr = strings.ReplaceAll(c.Expect.SheetsBackendAddr, token, value)
	return c
}

// caseNeedsToken reports whether c cannot run without a value for token. Decided by the token
// actually appearing in the case rather than by its category being "Mes", so a case that reaches
// the backend some other way (a Dispatch: DialSubmitCommand aimed at it, say) is covered by the
// same rule without needing to be listed anywhere.
func caseNeedsToken(c e2edata.OpticalScanCase, token string) bool {
	for _, p := range c.Generate.Params {
		if strings.Contains(p, token) {
			return true
		}
	}
	for _, p := range c.Expect.ExpectParams {
		if strings.Contains(p, token) {
			return true
		}
	}
	// Every place substituteToken rewrites has to be asked here too, or a case whose only
	// mention of the token is in one of them is run rather than skipped -- and then passes the
	// literal through to a device that quietly falls back to its build-time backend.
	return strings.Contains(c.Expect.SheetsBackendAddr, token)
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

// mesEnrolToken reads the enrolment token from the environment, then from the file the rig
// writes. Returns "" when neither names one.
func mesEnrolToken() string {
	if token := strings.TrimSpace(os.Getenv(mesEnrolTokenEnvVar)); token != "" {
		return token
	}
	body, err := os.ReadFile(mesEnrolTokenPath())
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(body))
}

func mesEnrolTokenPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return mesEnrolTokenFile
	}
	return filepath.Join(home, ".libp2p-kv-raft", mesEnrolTokenFile)
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
