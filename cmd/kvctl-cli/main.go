// Command kvctl-cli is a plain, no-Go-toolchain-required client for
// pkg/kvctl, meant to run alongside an already-built kvnode binary on a
// machine that doesn't have this repo's source or a Go toolchain -- e.g. a
// remote deployment target reached over SSH, where both binaries were
// cross-compiled elsewhere and copied over.
//
// Unlike the mage targets (which build kvnode from source via
// kvctl.AddNode), addnode here always takes a pre-built binary path via
// -bin and calls kvctl.AddNodeWithBinary.
package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"image/png"
	"os"
	"strconv"
	"time"

	"github.com/boombuler/barcode"
	"github.com/boombuler/barcode/datamatrix"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
	"github.com/gofsd/libp2p-kv-raft/pkg/ipc"
	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "addnode":
		cmdAddNode(os.Args[2:])
	case "resumenode":
		cmdResumeNode(os.Args[2:])
	case "use":
		cmdUse(os.Args[2:])
	case "set":
		cmdSet(os.Args[2:])
	case "get":
		cmdGet(os.Args[2:])
	case "listclusters":
		cmdListClusters(os.Args[2:])
	case "listnodes":
		cmdListNodes(os.Args[2:])
	case "rangescan":
		cmdRangeScan(os.Args[2:])
	case "requestpermit":
		cmdRequestPermit(os.Args[2:])
	case "confirmpermit":
		cmdConfirmPermit(os.Args[2:])
	case "revokepermit":
		cmdRevokePermit(os.Args[2:])
	case "createjoininvite":
		cmdCreateJoinInvite(os.Args[2:])
	case "revokejoininvite":
		cmdRevokeJoinInvite(os.Args[2:])
	case "printjoininvitedatamatrix":
		cmdPrintJoinInviteDataMatrix(os.Args[2:])
	case "createexecinvite":
		cmdCreateExecInvite(os.Args[2:])
	case "revokeexecinvite":
		cmdRevokeExecInvite(os.Args[2:])
	case "redeemexecinvite":
		cmdRedeemExecInvite(os.Args[2:])
	case "printexecinvitedatamatrix":
		cmdPrintExecInviteDataMatrix(os.Args[2:])
	case "execute":
		cmdExecute(os.Args[2:])
	case "pollexecute":
		cmdPollExecute(os.Args[2:])
	case "logappend":
		cmdLogAppend(os.Args[2:])
	case "logquery":
		cmdLogQuery(os.Args[2:])
	case "requestlogpermit":
		cmdRequestLogPermit(os.Args[2:])
	case "confirmlogpermit":
		cmdConfirmLogPermit(os.Args[2:])
	case "revokelogpermit":
		cmdRevokeLogPermit(os.Args[2:])
	case "sendevent":
		cmdSendEvent(os.Args[2:])
	case "sendrawevent":
		cmdSendRawEvent(os.Args[2:])
	case "printeventdatamatrix":
		cmdPrintEventDataMatrix(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  kvctl-cli addnode -bin <kvnode-binary-path> [-listen-port N] [-relay-service] [raft flags] [leaderPeerIDOrMultiaddr] [ownPeerID]
  kvctl-cli resumenode -bin <kvnode-binary-path> [raft flags] <ownPeerID>
  kvctl-cli use <peerID>
  kvctl-cli set <key> <value>
  kvctl-cli get <key>
  kvctl-cli listclusters
  kvctl-cli listnodes <peerID>
  kvctl-cli rangescan <start> <end> [-limit N]
  kvctl-cli requestpermit <kind: peer|bootstrap> <peerID> <metadata>
  kvctl-cli confirmpermit <kind: peer|bootstrap> <peerID>
  kvctl-cli revokepermit <kind: peer|bootstrap> <peerID>
  kvctl-cli createjoininvite <voter|learner>
  kvctl-cli revokejoininvite <tokenHex>
  kvctl-cli printjoininvitedatamatrix <leaderMultiaddr> <tokenHex> <outFile.png>
  kvctl-cli createexecinvite <commandID> <inputsJSON>
  kvctl-cli revokeexecinvite <tokenHex>
  kvctl-cli redeemexecinvite <sourceAddr#tokenHex>
  kvctl-cli printexecinvitedatamatrix <sourceMultiaddr> <tokenHex> <outFile.png>
  kvctl-cli execute <destPeerID> <value>
  kvctl-cli pollexecute
  kvctl-cli logappend <kind> <unitID> <fieldsJSON> <narrative>
  kvctl-cli logquery <kind> <unitID> [-since RFC3339] [-until RFC3339] [-limit N]
  kvctl-cli requestlogpermit <logKind> <peerID> <metadata>
  kvctl-cli confirmlogpermit <logKind> <peerID>
  kvctl-cli revokelogpermit <logKind> <peerID>
  kvctl-cli sendevent <peerID> <eventJSON>
  kvctl-cli sendrawevent <peerID> <base64Payload>
  kvctl-cli printeventdatamatrix <peerID> <eventJSON> <outFile.png>

sendevent sends one raw pkg/shmevent.Msg (JSON-encoded, human-readable, e.g.
'{"event":"get_field","value":"hello"}' -- see pkg/e2edata.Event for the
field names and how "value" handles binary data, and pkg/shmevent's
EventName for the "event" name strings) to peerID over the
local shmring transport, signing it with peerID's own key when the event
type requires one (fetched via an unsigned EventGetPrivateKey first). It
prints the JSON response event to stdout and exits non-zero if the response
is EventError (255) or the call itself failed. This is the low-level
primitive the e2e test pipeline drives -- both locally and, since this
binary is the one already cross-compiled and copied to remote deployment
targets, identically over SSH against a remote node.

sendrawevent sends base64Payload -- a complete shmevent.Encode output
(capnp framing + CRC + signature), produced ahead of time by
printeventdatamatrix or anything else that emits the same shape -- to
peerID verbatim, over pkg/ipc.CallRaw: unlike sendevent, it never re-signs
or otherwise touches the payload, so whatever signature was baked into it
(possibly by a different peerID's key, possibly long before this call)
survives unchanged. This is what actually redeems a one-time ticket: e.g. a
raft voter runs printeventdatamatrix once, in advance, to pre-sign an
EventPermitConfirm for a specific not-yet-arrived KindClusterJoin request;
later, replaying those same bytes here (locally, on that same voter's own
node, since shmring is same-machine-only) completes the confirm without
that operator needing to compose/sign anything at redemption time -- and
kvfsm's own OpConfirm already deletes the pending record it consumes, so a
second replay attempt fails on its own with no extra bookkeeping needed
here.

printeventdatamatrix builds and signs the exact same event sendevent would
(same eventJSON shape, same peerID-key-fetch-if-needed signing step), but
instead of transmitting it now, writes a Data Matrix barcode image of the
resulting bytes to outFile.png and prints the base64 payload to stdout --
the latter is what a script feeds straight into sendrawevent for testing,
without needing to decode the image at all.

createjoininvite/revokejoininvite/printjoininvitedatamatrix are a
different one-time mechanism, for admitting a brand-new device the
cluster has never seen before (sendrawevent's ticket always needs a
device's peer id already known in advance -- an invite doesn't).
createjoininvite generates a fresh random token and lodges it as a
shmevent.KindJoinInvite record (only a current raft voter may do this);
whichever device's join request presents that still-valid token gets
admitted immediately -- raft.AddVoter/AddNonvoter -- even with
-require-confirm-for-join on, with no live voter confirming anything at
that moment, and the token is consumed atomically so a second device
presenting the same one is rejected outright. printjoininvitedatamatrix
barcodes the plain string "<leaderMultiaddr>#<tokenHex>" (not a signed
event -- there's nothing to sign here, the token itself is the
credential); scanning it and passing the decoded string straight to mage
addfollower/addnode (or kvctl-cli addnode) is the entire redemption step.
revokejoininvite deletes a token outright before it's ever redeemed.

createexecinvite/revokeexecinvite/redeemexecinvite/printexecinvitedatamatrix
are join-invite's counterpart for triggering a specific command execution
instead of admitting a device: createexecinvite generates a fresh random
token and lodges it as a shmevent.KindExecInvite record binding
commandID+inputsJSON (only a current raft voter may do this).
printexecinvitedatamatrix barcodes the plain string
"<sourceMultiaddr>#<tokenHex>" (not a signed event, same reasoning as
printjoininvitedatamatrix -- the token itself is the credential).
redeemexecinvite splits that scanned string and has this node's own daemon
dial sourceAddr, sign a redemption message with this node's own key, and
send it -- the receiving cluster's raft leader atomically re-checks this
node's real Group/Command ACL standing *and* consumes the token in one
step, so an unauthorized or already-used redemption is rejected without
this node needing any prior relationship with that cluster beyond already
having a Group/Command ACL grant there. Prints the new instance id on
success; track it with getcommandrequest/querycommandlog/latestcommandlog
(mage) against the target's own node. revokeexecinvite deletes a token
outright before it's ever redeemed.

raft flags (all default to hashicorp/raft's own WAN-appropriate values):
  -raft-heartbeat-timeout, -raft-election-timeout, -raft-commit-timeout, -raft-leader-lease-timeout`)
}

// raftTimeoutFlags registers the four raft timing flags shared by addnode
// and resumenode, and returns a function that turns whichever were set
// into "-flag value" pairs for the spawned kvnode's command line.
func raftTimeoutFlags(fs *flag.FlagSet) func() []string {
	heartbeatTimeout := fs.Duration("raft-heartbeat-timeout", 0, "raft heartbeat timeout (0 = default, 1s)")
	electionTimeout := fs.Duration("raft-election-timeout", 0, "raft election timeout (0 = default, 1s)")
	commitTimeout := fs.Duration("raft-commit-timeout", 0, "raft commit timeout (0 = default, 50ms)")
	leaderLeaseTimeout := fs.Duration("raft-leader-lease-timeout", 0, "raft leader lease timeout (0 = default, 500ms)")

	return func() []string {
		var extra []string
		if *heartbeatTimeout != 0 {
			extra = append(extra, "-raft-heartbeat-timeout", heartbeatTimeout.String())
		}
		if *electionTimeout != 0 {
			extra = append(extra, "-raft-election-timeout", electionTimeout.String())
		}
		if *commitTimeout != 0 {
			extra = append(extra, "-raft-commit-timeout", commitTimeout.String())
		}
		if *leaderLeaseTimeout != 0 {
			extra = append(extra, "-raft-leader-lease-timeout", leaderLeaseTimeout.String())
		}
		return extra
	}
}

func cmdAddNode(args []string) {
	fs := flag.NewFlagSet("addnode", flag.ExitOnError)
	binPath := fs.String("bin", "", "path to a pre-built kvnode binary (required)")
	listenPort := fs.Int("listen-port", 0, "TCP/QUIC port for the new node to listen on (0 = ephemeral)")
	relayService := fs.Bool("relay-service", false, "make the new node act as a relay for others (only for nodes with a real public address)")
	raftArgs := raftTimeoutFlags(fs)
	fs.Parse(args)

	if *binPath == "" {
		fmt.Fprintln(os.Stderr, "addnode: -bin is required")
		os.Exit(2)
	}

	extra := raftArgs()
	if *listenPort != 0 {
		extra = append(extra, "-listen-port", strconv.Itoa(*listenPort))
	}
	if *relayService {
		extra = append(extra, "-relay-service")
	}

	peerID, err := kvctl.AddNodeWithBinary(*binPath, extra, fs.Args()...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "addnode: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(peerID)
}

func cmdResumeNode(args []string) {
	fs := flag.NewFlagSet("resumenode", flag.ExitOnError)
	binPath := fs.String("bin", "", "path to a pre-built kvnode binary (required)")
	raftArgs := raftTimeoutFlags(fs)
	fs.Parse(args)

	if *binPath == "" {
		fmt.Fprintln(os.Stderr, "resumenode: -bin is required")
		os.Exit(2)
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli resumenode -bin <path> [raft flags] <ownPeerID>")
		os.Exit(2)
	}

	peerID, err := kvctl.ResumeNodeWithBinary(*binPath, fs.Arg(0), raftArgs())
	if err != nil {
		fmt.Fprintf(os.Stderr, "resumenode: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(peerID)
}

func cmdUse(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli use <peerID>")
		os.Exit(2)
	}
	if err := kvctl.Use(args[0]); err != nil {
		fmt.Fprintf(os.Stderr, "use: %v\n", err)
		os.Exit(1)
	}
}

func cmdSet(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli set <key> <value>")
		os.Exit(2)
	}
	if err := kvctl.Set(args[0], args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "set: %v\n", err)
		os.Exit(1)
	}
}

func cmdGet(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli get <key>")
		os.Exit(2)
	}
	value, err := kvctl.Get(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "get: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(value)
}

// cmdRangeScan prints, one JSON object per line, every key/value pair
// between start and end (both inclusive, lexicographic byte order) on the
// current node -- kvctl.RangeScan, the generic counterpart to cmdSet/
// cmdGet for a whole range of keys at once.
func cmdRangeScan(args []string) {
	fs := flag.NewFlagSet("rangescan", flag.ExitOnError)
	limit := fs.Int("limit", 0, "maximum results to return (0 = unlimited)")
	fs.Parse(args)

	if fs.NArg() != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli rangescan <start> <end> [-limit N]")
		os.Exit(2)
	}

	results, err := kvctl.RangeScan(fs.Arg(0), fs.Arg(1), *limit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "rangescan: %v\n", err)
		os.Exit(1)
	}
	for _, kv := range results {
		out, err := json.Marshal(kv)
		if err != nil {
			fmt.Fprintf(os.Stderr, "rangescan: encode result: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(out))
	}
}

// cmdListClusters prints, one JSON object per line, every raft cluster
// known to this machine's registry (kvctl.ListClusters) -- a pure local
// registry read, no running daemon required.
func cmdListClusters(args []string) {
	if len(args) != 0 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli listclusters")
		os.Exit(2)
	}
	clusters, err := kvctl.ListClusters()
	if err != nil {
		fmt.Fprintf(os.Stderr, "listclusters: %v\n", err)
		os.Exit(1)
	}
	for _, c := range clusters {
		out, err := json.Marshal(c)
		if err != nil {
			fmt.Fprintf(os.Stderr, "listclusters: encode result: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(out))
	}
}

// cmdListNodes prints, one JSON object per line, every peer id currently
// in the raft cluster that the already-running node peerID belongs to
// (kvctl.ListClusterMembers) -- a live query, unlike cmdListClusters.
func cmdListNodes(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli listnodes <peerID>")
		os.Exit(2)
	}
	members, err := kvctl.ListClusterMembers(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "listnodes: %v\n", err)
		os.Exit(1)
	}
	for _, m := range members {
		out, err := json.Marshal(m)
		if err != nil {
			fmt.Fprintf(os.Stderr, "listnodes: encode result: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(out))
	}
}

func cmdRequestPermit(args []string) {
	if len(args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli requestpermit <kind: peer|bootstrap> <peerID> <metadata>")
		os.Exit(2)
	}
	kind, ok := shmevent.KindFromName(args[0])
	if !ok {
		fmt.Fprintf(os.Stderr, "requestpermit: unknown permit kind %q (want \"peer\" or \"bootstrap\")\n", args[0])
		os.Exit(2)
	}
	if err := kvctl.RequestPermit(kind, []byte(args[1]), []byte(args[2])); err != nil {
		fmt.Fprintf(os.Stderr, "requestpermit: %v\n", err)
		os.Exit(1)
	}
}

func cmdConfirmPermit(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli confirmpermit <kind: peer|bootstrap> <peerID>")
		os.Exit(2)
	}
	kind, ok := shmevent.KindFromName(args[0])
	if !ok {
		fmt.Fprintf(os.Stderr, "confirmpermit: unknown permit kind %q (want \"peer\" or \"bootstrap\")\n", args[0])
		os.Exit(2)
	}
	if err := kvctl.ConfirmPermit(kind, []byte(args[1])); err != nil {
		fmt.Fprintf(os.Stderr, "confirmpermit: %v\n", err)
		os.Exit(1)
	}
}

func cmdRevokePermit(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli revokepermit <kind: peer|bootstrap> <peerID>")
		os.Exit(2)
	}
	kind, ok := shmevent.KindFromName(args[0])
	if !ok {
		fmt.Fprintf(os.Stderr, "revokepermit: unknown permit kind %q (want \"peer\" or \"bootstrap\")\n", args[0])
		os.Exit(2)
	}
	if err := kvctl.RevokePermit(kind, []byte(args[1])); err != nil {
		fmt.Fprintf(os.Stderr, "revokepermit: %v\n", err)
		os.Exit(1)
	}
}

func cmdCreateJoinInvite(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli createjoininvite <voter|learner>")
		os.Exit(2)
	}
	var suffrage byte
	switch args[0] {
	case "voter":
		suffrage = shmevent.SuffrageVoter
	case "learner":
		suffrage = shmevent.SuffrageLearner
	default:
		fmt.Fprintf(os.Stderr, "createjoininvite: unknown suffrage %q (want \"voter\" or \"learner\")\n", args[0])
		os.Exit(2)
	}
	tokenHex, err := kvctl.CreateJoinInvite(suffrage)
	if err != nil {
		fmt.Fprintf(os.Stderr, "createjoininvite: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(tokenHex)
}

func cmdRevokeJoinInvite(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli revokejoininvite <tokenHex>")
		os.Exit(2)
	}
	if err := kvctl.RevokeJoinInvite(args[0]); err != nil {
		fmt.Fprintf(os.Stderr, "revokejoininvite: %v\n", err)
		os.Exit(1)
	}
}

// cmdPrintJoinInviteDataMatrix implements `kvctl-cli
// printjoininvitedatamatrix <leaderMultiaddr> <tokenHex> <outFile.png>` --
// unlike printeventdatamatrix, this barcodes a plain string, not a signed
// shmevent.Msg: "<leaderMultiaddr>#<tokenHex>" is exactly the format
// pkg/daemon's splitInviteToken (handleAdd) expects when passed to mage
// addfollower/addnode, so scanning this code and feeding the decoded
// string straight into that command is the entire redemption step -- no
// separate sendrawevent call needed, since createjoininvite's token isn't
// itself a signed event.
func cmdPrintJoinInviteDataMatrix(args []string) {
	if len(args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli printjoininvitedatamatrix <leaderMultiaddr> <tokenHex> <outFile.png>")
		os.Exit(2)
	}
	leaderAddr, tokenHex, outFile := args[0], args[1], args[2]

	joinString := leaderAddr + "#" + tokenHex

	code, err := datamatrix.Encode(joinString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "printjoininvitedatamatrix: encode data matrix: %v\n", err)
		os.Exit(1)
	}
	bounds := code.Bounds()
	scaled, err := barcode.Scale(code, bounds.Dx()*dataMatrixModuleSize, bounds.Dy()*dataMatrixModuleSize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "printjoininvitedatamatrix: scale data matrix: %v\n", err)
		os.Exit(1)
	}

	f, err := os.Create(outFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "printjoininvitedatamatrix: create %s: %v\n", outFile, err)
		os.Exit(1)
	}
	defer f.Close()
	if err := png.Encode(f, scaled); err != nil {
		fmt.Fprintf(os.Stderr, "printjoininvitedatamatrix: write %s: %v\n", outFile, err)
		os.Exit(1)
	}

	fmt.Printf("wrote %s\n", outFile)
	fmt.Println(joinString)
}

func cmdCreateExecInvite(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli createexecinvite <commandID> <inputsJSON>")
		os.Exit(2)
	}
	tokenHex, err := kvctl.CreateExecInvite(args[0], args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "createexecinvite: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(tokenHex)
}

func cmdRevokeExecInvite(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli revokeexecinvite <tokenHex>")
		os.Exit(2)
	}
	if err := kvctl.RevokeExecInvite(args[0]); err != nil {
		fmt.Fprintf(os.Stderr, "revokeexecinvite: %v\n", err)
		os.Exit(1)
	}
}

func cmdRedeemExecInvite(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli redeemexecinvite <sourceAddr#tokenHex>")
		os.Exit(2)
	}
	instanceID, err := kvctl.RedeemExecInvite(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "redeemexecinvite: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(instanceID)
}

// cmdPrintExecInviteDataMatrix implements `kvctl-cli
// printexecinvitedatamatrix <sourceMultiaddr> <tokenHex> <outFile.png>` --
// mirrors cmdPrintJoinInviteDataMatrix exactly: barcodes a plain string,
// not a signed shmevent.Msg, since the token itself is the credential (see
// createexecinvite's doc comment above).
func cmdPrintExecInviteDataMatrix(args []string) {
	if len(args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli printexecinvitedatamatrix <sourceMultiaddr> <tokenHex> <outFile.png>")
		os.Exit(2)
	}
	sourceAddr, tokenHex, outFile := args[0], args[1], args[2]

	redeemString := sourceAddr + "#" + tokenHex

	code, err := datamatrix.Encode(redeemString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "printexecinvitedatamatrix: encode data matrix: %v\n", err)
		os.Exit(1)
	}
	bounds := code.Bounds()
	scaled, err := barcode.Scale(code, bounds.Dx()*dataMatrixModuleSize, bounds.Dy()*dataMatrixModuleSize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "printexecinvitedatamatrix: scale data matrix: %v\n", err)
		os.Exit(1)
	}

	f, err := os.Create(outFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "printexecinvitedatamatrix: create %s: %v\n", outFile, err)
		os.Exit(1)
	}
	defer f.Close()
	if err := png.Encode(f, scaled); err != nil {
		fmt.Fprintf(os.Stderr, "printexecinvitedatamatrix: write %s: %v\n", outFile, err)
		os.Exit(1)
	}

	fmt.Printf("wrote %s\n", outFile)
	fmt.Println(redeemString)
}

func cmdRequestLogPermit(args []string) {
	if len(args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli requestlogpermit <logKind> <peerID> <metadata>")
		os.Exit(2)
	}
	if err := kvctl.RequestLogPermit(args[0], []byte(args[1]), []byte(args[2])); err != nil {
		fmt.Fprintf(os.Stderr, "requestlogpermit: %v\n", err)
		os.Exit(1)
	}
}

func cmdConfirmLogPermit(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli confirmlogpermit <logKind> <peerID>")
		os.Exit(2)
	}
	if err := kvctl.ConfirmLogPermit(args[0], []byte(args[1])); err != nil {
		fmt.Fprintf(os.Stderr, "confirmlogpermit: %v\n", err)
		os.Exit(1)
	}
}

func cmdRevokeLogPermit(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli revokelogpermit <logKind> <peerID>")
		os.Exit(2)
	}
	if err := kvctl.RevokeLogPermit(args[0], []byte(args[1])); err != nil {
		fmt.Fprintf(os.Stderr, "revokelogpermit: %v\n", err)
		os.Exit(1)
	}
}

func cmdExecute(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli execute <destPeerID> <value>")
		os.Exit(2)
	}
	if err := kvctl.Execute(args[0], args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "execute: %v\n", err)
		os.Exit(1)
	}
}

func cmdPollExecute(args []string) {
	if len(args) != 0 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli pollexecute")
		os.Exit(2)
	}
	senderPeerID, value, ok, err := kvctl.PollExecute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "pollexecute: %v\n", err)
		os.Exit(1)
	}
	if !ok {
		fmt.Println("(no execute notification pending)")
		return
	}
	fmt.Printf("%s: %s\n", senderPeerID, value)
}

func cmdLogAppend(args []string) {
	if len(args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli logappend <kind> <unitID> <fieldsJSON> <narrative>")
		os.Exit(2)
	}
	var fields map[string]string
	if args[2] != "" {
		if err := json.Unmarshal([]byte(args[2]), &fields); err != nil {
			fmt.Fprintf(os.Stderr, "logappend: decode fieldsJSON: %v\n", err)
			os.Exit(2)
		}
	}
	if err := kvctl.LogAppend(args[0], args[1], fields, args[3]); err != nil {
		fmt.Fprintf(os.Stderr, "logappend: %v\n", err)
		os.Exit(1)
	}
}

func cmdLogQuery(args []string) {
	fs := flag.NewFlagSet("logquery", flag.ExitOnError)
	since := fs.String("since", "", "RFC3339 lower time bound, inclusive (default: unbounded)")
	until := fs.String("until", "", "RFC3339 upper time bound, inclusive (default: now)")
	limit := fs.Int("limit", 0, "maximum records to return (0 = unlimited)")
	fs.Parse(args)

	if fs.NArg() != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli logquery <kind> <unitID> [-since RFC3339] [-until RFC3339] [-limit N]")
		os.Exit(2)
	}
	start := time.Unix(0, 0)
	if *since != "" {
		t, err := time.Parse(time.RFC3339, *since)
		if err != nil {
			fmt.Fprintf(os.Stderr, "logquery: -since: %v\n", err)
			os.Exit(2)
		}
		start = t
	}
	end := time.Now()
	if *until != "" {
		t, err := time.Parse(time.RFC3339, *until)
		if err != nil {
			fmt.Fprintf(os.Stderr, "logquery: -until: %v\n", err)
			os.Exit(2)
		}
		end = t
	}

	records, err := kvctl.LogQuery(fs.Arg(0), fs.Arg(1), start, end, *limit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "logquery: %v\n", err)
		os.Exit(1)
	}
	for _, rec := range records {
		out, err := json.Marshal(rec)
		if err != nil {
			fmt.Fprintf(os.Stderr, "logquery: encode result: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(out))
	}
}

// sendEventTimeout bounds both the optional GetPrivateKey signing-key
// fetch and the event call itself. Generous relative to how fast a local
// shmring round trip normally is: a node that's also a raft voter can get
// busy servicing real WAN-latency raft traffic (heartbeats/AppendEntries
// retries against a genuinely distant leader) and briefly fall behind on
// its local IPC responder -- observed directly running e2e against a real
// cross-continental deployment, where 10s wasn't always enough even for a
// local, non-network call like get_public_key.
const sendEventTimeout = 30 * time.Second

// signEventForCurrentKey fetches peerID's own private key (via an unsigned
// EventGetPrivateKey call) and signs m with it if m's event type requires a
// signature, returning the complete shmevent.Encode output. Shared by
// cmdSendEvent (transmits the result immediately) and
// cmdPrintEventDataMatrix (barcodes the result for a later
// cmdSendRawEvent replay) so both build a signed event through the
// identical path -- any divergence here would mean a barcoded "ticket"
// isn't actually signed the same way an ordinary sendevent call is.
func signEventForCurrentKey(ctx context.Context, peerID string, m shmevent.Msg) ([]byte, error) {
	var priv shmevent.PrivateKey
	if shmevent.RequiresSignature(m.EventType) {
		keyResp, err := ipc.Call(ctx, peerID, shmevent.Msg{EventType: shmevent.EventGetPrivateKey, ID: randomID()}, nil)
		if err != nil {
			return nil, fmt.Errorf("fetch signing key: %w", err)
		}
		if keyResp.EventType == shmevent.EventError {
			return nil, fmt.Errorf("fetch signing key: %s", keyResp.Value)
		}
		priv = shmevent.PrivateKey(keyResp.Value)
	}
	return shmevent.Encode(m, priv)
}

// parseEventArg parses eventJSON into a shmevent.Msg, defaulting ID to a
// fresh random value when the caller left it unset (0) -- shared by
// cmdSendEvent and cmdPrintEventDataMatrix.
func parseEventArg(eventJSON string) (shmevent.Msg, error) {
	var ev e2edata.Event
	if err := json.Unmarshal([]byte(eventJSON), &ev); err != nil {
		return shmevent.Msg{}, fmt.Errorf("parse event json: %w", err)
	}
	if ev.ID == 0 {
		ev.ID = randomID()
	}
	return ev.ToMsg(), nil
}

func cmdSendEvent(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli sendevent <peerID> <eventJSON>")
		os.Exit(2)
	}
	peerID := args[0]

	m, err := parseEventArg(args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "sendevent: %v\n", err)
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), sendEventTimeout)
	defer cancel()

	encoded, err := signEventForCurrentKey(ctx, peerID, m)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sendevent: %v\n", err)
		os.Exit(1)
	}

	resp, err := ipc.CallRaw(ctx, peerID, encoded)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sendevent: %v\n", err)
		os.Exit(1)
	}

	out, err := json.Marshal(e2edata.EventFromMsg(resp))
	if err != nil {
		fmt.Fprintf(os.Stderr, "sendevent: encode response: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(out))
	if resp.EventType == shmevent.EventError {
		os.Exit(1)
	}
}

// cmdSendRawEvent implements `kvctl-cli sendrawevent <peerID>
// <base64Payload>` -- see usage()'s doc comment on this being CallRaw's
// pass-through (no re-signing) counterpart to sendevent.
func cmdSendRawEvent(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli sendrawevent <peerID> <base64Payload>")
		os.Exit(2)
	}
	peerID := args[0]

	encoded, err := base64.StdEncoding.DecodeString(args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "sendrawevent: decode base64 payload: %v\n", err)
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), sendEventTimeout)
	defer cancel()

	resp, err := ipc.CallRaw(ctx, peerID, encoded)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sendrawevent: %v\n", err)
		os.Exit(1)
	}

	out, err := json.Marshal(e2edata.EventFromMsg(resp))
	if err != nil {
		fmt.Fprintf(os.Stderr, "sendrawevent: encode response: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(out))
	if resp.EventType == shmevent.EventError {
		os.Exit(1)
	}
}

// dataMatrixModuleSize scales each Data Matrix module up to this many
// pixels square -- boombuler/barcode's raw encoder output is one pixel per
// module, unreadable by any real scanner/decoder at that size.
const dataMatrixModuleSize = 8

// cmdPrintEventDataMatrix implements `kvctl-cli printeventdatamatrix
// <peerID> <eventJSON> <outFile.png>` -- see usage()'s doc comment.
func cmdPrintEventDataMatrix(args []string) {
	if len(args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli printeventdatamatrix <peerID> <eventJSON> <outFile.png>")
		os.Exit(2)
	}
	peerID, eventJSONArg, outFile := args[0], args[1], args[2]

	m, err := parseEventArg(eventJSONArg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "printeventdatamatrix: %v\n", err)
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), sendEventTimeout)
	defer cancel()

	encoded, err := signEventForCurrentKey(ctx, peerID, m)
	if err != nil {
		fmt.Fprintf(os.Stderr, "printeventdatamatrix: %v\n", err)
		os.Exit(1)
	}

	payload := base64.StdEncoding.EncodeToString(encoded)

	code, err := datamatrix.Encode(payload)
	if err != nil {
		fmt.Fprintf(os.Stderr, "printeventdatamatrix: encode data matrix: %v\n", err)
		os.Exit(1)
	}
	bounds := code.Bounds()
	scaled, err := barcode.Scale(code, bounds.Dx()*dataMatrixModuleSize, bounds.Dy()*dataMatrixModuleSize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "printeventdatamatrix: scale data matrix: %v\n", err)
		os.Exit(1)
	}

	f, err := os.Create(outFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "printeventdatamatrix: create %s: %v\n", outFile, err)
		os.Exit(1)
	}
	defer f.Close()
	if err := png.Encode(f, scaled); err != nil {
		fmt.Fprintf(os.Stderr, "printeventdatamatrix: write %s: %v\n", outFile, err)
		os.Exit(1)
	}

	fmt.Printf("wrote %s\n", outFile)
	fmt.Println(payload)
}

// randomID returns a random non-zero id -- 0 is reserved meaning
// "SourceID/DestinationID not used" (see api/shmevent.capnp), so a real
// message's own id avoids it too.
func randomID() uint16 {
	for {
		var b [2]byte
		if _, err := rand.Read(b[:]); err != nil {
			return 1
		}
		if id := binary.BigEndian.Uint16(b[:]); id != 0 {
			return id
		}
	}
}
