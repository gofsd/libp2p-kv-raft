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
	case "addpending":
		cmdAddPending(os.Args[2:])
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
	case "accesstoken":
		cmdAccessToken(os.Args[2:])
	case "join":
		cmdJoin(os.Args[2:])
	case "leave":
		cmdLeave(os.Args[2:])
	case "rm":
		cmdRm(os.Args[2:])
	case "kick":
		cmdKick(os.Args[2:])
	case "deletenode":
		cmdDeleteNode(os.Args[2:])
	case "backup":
		cmdBackup(os.Args[2:])
	case "restore":
		cmdRestore(os.Args[2:])
	case "rangescan":
		cmdRangeScan(os.Args[2:])
	case "txn":
		cmdTxn(os.Args[2:])
	case "cas":
		cmdCas(os.Args[2:])
	case "casabsent":
		cmdCasAbsent(os.Args[2:])
	case "creategroup", "updategroup":
		cmdCreateGroup(os.Args[2:])
	case "deletegroup":
		cmdDeleteGroup(os.Args[2:])
	case "getgroup":
		cmdGetGroup(os.Args[2:])
	case "listgroups":
		cmdListGroups(os.Args[2:])
	case "addpeertogroup":
		cmdAddPeerToGroup(os.Args[2:])
	case "removepeerfromgroup":
		cmdRemovePeerFromGroup(os.Args[2:])
	case "listgroupsforpeer":
		cmdListGroupsForPeer(os.Args[2:])
	case "createcommand", "updatecommand":
		cmdCreateCommand(os.Args[2:])
	case "deletecommand":
		cmdDeleteCommand(os.Args[2:])
	case "getcommand":
		cmdGetCommand(os.Args[2:])
	case "listcommands":
		cmdListCommands(os.Args[2:])
	case "createcommandspec", "updatecommandspec":
		cmdCreateCommandSpec(os.Args[2:])
	case "clearcommandspec":
		cmdClearCommandSpec(os.Args[2:])
	case "addcommandtogroup":
		cmdAddCommandToGroup(os.Args[2:])
	case "removecommandfromgroup":
		cmdRemoveCommandFromGroup(os.Args[2:])
	case "listgroupsforcommand":
		cmdListGroupsForCommand(os.Args[2:])
	case "createstation", "updatestation":
		cmdCreateStation(os.Args[2:])
	case "deletestation":
		cmdDeleteStation(os.Args[2:])
	case "getstation":
		cmdGetStation(os.Args[2:])
	case "liststations":
		cmdListStations(os.Args[2:])
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
	case "createjoininviteticket":
		cmdCreateJoinInviteTicket(os.Args[2:])
	case "verifyjoininviteticket":
		cmdVerifyJoinInviteTicket(os.Args[2:])
	case "printjoininviteticketdatamatrix":
		cmdPrintJoinInviteTicketDataMatrix(os.Args[2:])
	case "createjoinrequest":
		cmdCreateJoinRequest(os.Args[2:])
	case "canceljoinrequest":
		cmdCancelJoinRequest(os.Args[2:])
	case "printjoinrequestdatamatrix":
		cmdPrintJoinRequestDataMatrix(os.Args[2:])
	case "recruitpeer":
		cmdRecruitPeer(os.Args[2:])
	case "createjoinrequestticket":
		cmdCreateJoinRequestTicket(os.Args[2:])
	case "redeemjoinrequestticket":
		cmdRedeemJoinRequestTicket(os.Args[2:])
	case "printjoinrequestticketdatamatrix":
		cmdPrintJoinRequestTicketDataMatrix(os.Args[2:])
	case "getownaddr":
		cmdGetOwnAddr(os.Args[2:])
	case "version":
		cmdVersion(os.Args[2:])
	case "createexecinvite":
		cmdCreateExecInvite(os.Args[2:])
	case "revokeexecinvite":
		cmdRevokeExecInvite(os.Args[2:])
	case "redeemexecinvite":
		cmdRedeemExecInvite(os.Args[2:])
	case "requestpublicaccess":
		cmdRequestPublicAccess(os.Args[2:])
	case "enablepublicaccess":
		cmdEnablePublicAccess(os.Args[2:])
	case "dialsubmitcommand":
		cmdDialSubmitCommand(os.Args[2:])
	case "dialquerycommandlog":
		cmdDialQueryCommandLog(os.Args[2:])
	case "printexecinvitedatamatrix":
		cmdPrintExecInviteDataMatrix(os.Args[2:])
	case "createexecinviteticket":
		cmdCreateExecInviteTicket(os.Args[2:])
	case "redeemexecinviteticket":
		cmdRedeemExecInviteTicket(os.Args[2:])
	case "printexecinviteticketdatamatrix":
		cmdPrintExecInviteTicketDataMatrix(os.Args[2:])
	case "execute":
		cmdExecute(os.Args[2:])
	case "pollexecute":
		cmdPollExecute(os.Args[2:])
	case "submitcommand":
		cmdSubmitCommand(os.Args[2:])
	case "getcommandrequest":
		cmdGetCommandRequest(os.Args[2:])
	case "listcommandrequests":
		cmdListCommandRequests(os.Args[2:])
	case "listexecutions":
		cmdListExecutions(os.Args[2:])
	case "appendcommandlog":
		cmdAppendCommandLog(os.Args[2:])
	case "reportprogress":
		cmdReportProgress(os.Args[2:])
	case "querycommandlog":
		cmdQueryCommandLog(os.Args[2:])
	case "latestcommandlog":
		cmdLatestCommandLog(os.Args[2:])
	case "addrelaynode":
		cmdAddRelayNode(os.Args[2:])
	case "confirmrelaynode":
		cmdConfirmRelayNode(os.Args[2:])
	case "removerelaynode":
		cmdRemoveRelayNode(os.Args[2:])
	case "getrelaynode":
		cmdGetRelayNode(os.Args[2:])
	case "listrelaynodes":
		cmdListRelayNodes(os.Args[2:])
	case "logappend":
		cmdLogAppend(os.Args[2:])
	case "logquery":
		cmdLogQuery(os.Args[2:])
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
  kvctl-cli addpending -bin <kvnode-binary-path> [-listen-port N] [-relay-service] [raft flags]
  kvctl-cli use <peerID>
  kvctl-cli set <key> <value>
  kvctl-cli get <key>
  kvctl-cli listclusters
  kvctl-cli listnodes <peerID>
  kvctl-cli accesstoken <peerID>
  kvctl-cli join -bin <kvnode-binary-path> [raft flags] <targetPeerID>
  kvctl-cli leave -bin <kvnode-binary-path> [raft flags] <ownPeerID>
  kvctl-cli rm -bin <kvnode-binary-path> [raft flags] <ownPeerID>
  kvctl-cli kick <peerID> <targetPeerID>
  kvctl-cli deletenode <peerID>
  kvctl-cli backup <peerID> <destArchive>
  kvctl-cli restore <archive> <destDir>
  kvctl-cli rangescan <start> <end> [-limit N]
  kvctl-cli txn "<op> [op...]"                 (ops: k=v, del:k, if:k=v, ifabsent:k)
  kvctl-cli cas <key> <expected> <value>
  kvctl-cli casabsent <key> <value>
  kvctl-cli creategroup <id> <name> <public: true|false>
  kvctl-cli updategroup <id> <name> <public: true|false>
  kvctl-cli deletegroup <id>
  kvctl-cli getgroup <id>
  kvctl-cli listgroups
  kvctl-cli addpeertogroup <peerID> <groupID>
  kvctl-cli removepeerfromgroup <peerID> <groupID>
  kvctl-cli listgroupsforpeer <peerID>
  kvctl-cli createcommand <id> <name> <peerID>
  kvctl-cli updatecommand <id> <name> <peerID>
  kvctl-cli deletecommand <id>
  kvctl-cli getcommand <id>
  kvctl-cli listcommands
  kvctl-cli createcommandspec <id> <name> <peerID|""> <specJSON>
  kvctl-cli clearcommandspec <id> <name> <peerID|"">
  kvctl-cli addcommandtogroup <commandID> <groupID>
  kvctl-cli removecommandfromgroup <commandID> <groupID>
  kvctl-cli listgroupsforcommand <commandID>
  kvctl-cli createstation <peerID> <name> [attrsJSON]
  kvctl-cli deletestation <peerID>
  kvctl-cli getstation <peerID>
  kvctl-cli liststations
  kvctl-cli requestpermit <kind: bootstrap> <peerID> <metadata>
  kvctl-cli confirmpermit <kind: bootstrap|cluster-join> <peerID>
  kvctl-cli revokepermit <kind: bootstrap> <peerID>
  kvctl-cli createjoininvite <voter|learner>
  kvctl-cli revokejoininvite <tokenHex>
  kvctl-cli printjoininvitedatamatrix <leaderMultiaddr> <tokenHex> <outFile.png>
  kvctl-cli createjoininviteticket <voter|learner>
  kvctl-cli verifyjoininviteticket <ticketB64>
  kvctl-cli printjoininviteticketdatamatrix <ticketB64> <outFile.png>
  kvctl-cli createjoinrequest
  kvctl-cli canceljoinrequest <tokenHex>
  kvctl-cli printjoinrequestdatamatrix <ownMultiaddr> <tokenHex> <outFile.png>
  kvctl-cli recruitpeer <ticket> <voter|learner>
  kvctl-cli createjoinrequestticket
  kvctl-cli redeemjoinrequestticket <ticketB64> <voter|learner>
  kvctl-cli printjoinrequestticketdatamatrix <ticketB64> <outFile.png>
  kvctl-cli getownaddr
  kvctl-cli version
  kvctl-cli createexecinvite <commandID> <inputsJSON> [-ttl seconds]
  kvctl-cli revokeexecinvite <tokenHex>
  kvctl-cli redeemexecinvite <sourceAddr#tokenHex>
  kvctl-cli requestpublicaccess <targetAddr> [note]
  kvctl-cli enablepublicaccess
  kvctl-cli dialsubmitcommand <targetAddr> <commandID> [inputsJSON]
  kvctl-cli dialquerycommandlog <targetAddr> <instanceID>
  kvctl-cli printexecinvitedatamatrix <sourceMultiaddr> <tokenHex> <outFile.png>
  kvctl-cli createexecinviteticket <commandID> <inputsJSON> [-ttl seconds]
  kvctl-cli redeemexecinviteticket <ticketB64>
  kvctl-cli printexecinviteticketdatamatrix <ticketB64> <outFile.png>
  kvctl-cli execute <destPeerID> <value>
  kvctl-cli pollexecute
  kvctl-cli submitcommand <commandID> <inputsJSON>
  kvctl-cli getcommandrequest <commandID> <instanceID>
  kvctl-cli listcommandrequests <commandID>
  kvctl-cli listexecutions <peerID>
  kvctl-cli appendcommandlog <requesterPeerID> <instanceID> <fieldsJSON> <narrative>
  kvctl-cli reportprogress <requesterPeerID> <instanceID> <fieldsJSON> <narrative>
  kvctl-cli querycommandlog <instanceID> [-since RFC3339] [-until RFC3339] [-limit N]
  kvctl-cli latestcommandlog <instanceID>
  kvctl-cli addrelaynode <multiaddr> <priority>
  kvctl-cli confirmrelaynode <multiaddr>
  kvctl-cli removerelaynode <multiaddr>
  kvctl-cli getrelaynode <multiaddr>
  kvctl-cli listrelaynodes
  kvctl-cli logappend <kind> <unitID> <fieldsJSON> <narrative>
  kvctl-cli logquery <kind> <unitID> [-since RFC3339] [-until RFC3339] [-limit N]
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

createjoininviteticket/verifyjoininviteticket/printjoininviteticketdatamatrix
are createjoininvite's signed counterpart: createjoininviteticket mints the
same token but wraps this node's own address and the token into a single
self-contained ticket, signed with this node's own key (see
pkg/shmevent.EventJoinTicket's doc comment) -- a scanning device can then
verify the ticket really came from the peer id embedded in its own
address, before ever dialing it, which the plain printjoininvitedatamatrix
form has no way to do (its token is the sole credential; forging one just
fails harmlessly at redemption instead). verifyjoininviteticket checks
that signature and, on success, prints the same plain
"<leaderMultiaddr>#<tokenHex>" string createjoininvite's caller has always
hand-assembled -- pass it to addnode/addfollower exactly as before.
printjoininviteticketdatamatrix barcodes an already-minted ticketB64 as-is
(unlike printjoininvitedatamatrix, it takes one already-complete string,
not separate address/token arguments, since the ticket already carries
both).

createjoinrequest/canceljoinrequest/printjoinrequestdatamatrix/recruitpeer
are join-invite's reverse: instead of an existing cluster voter minting a
token that some outside device dials in to redeem, a device with no
cluster of its own yet (addpending -- never bootstrapped or joined
anything, unlike addnode) mints a ticket naming *itself*, for some other
cluster's voter to redeem -- ending with that device, not the voter,
admitted into the voter's cluster, with no further action on the device's
side at all. createjoinrequest generates a fresh random token on the
current (addpending) node. printjoinrequestdatamatrix barcodes the plain
string "<thisDevice'sOwnMultiaddr>#<tokenHex>" (not a signed event, same
reasoning as printjoininvitedatamatrix). recruitpeer, run on some other
cluster's own voter, splits that scanned string, mints a normal join
invite on its own cluster, and dials the device directly to hand it that
invite -- the device admits itself on receipt (pkg/daemon's
handleRecruitStream), the same way a normal addfollower would, just
triggered by this network push instead of a local operator command.
Prints the recruited device's own join result ("<peerID> ok"/"<peerID>
pending") on success. canceljoinrequest clears a device's own pending
ticket before it's ever redeemed.

createjoinrequestticket/redeemjoinrequestticket/printjoinrequestticketdatamatrix
are createjoinrequest's signed counterpart, same relationship
createjoininviteticket has to createjoininvite but in the reverse
direction: createjoinrequestticket wraps this (addpending) device's own
address and its fresh token into a ticket signed with *this device's own*
key -- the signature here proves the ticket really came from the device
asking to join, not from the cluster admitting it (see
pkg/shmevent.EventJoinRequestTicket's doc comment). redeemjoinrequestticket
is recruitpeer's counterpart: it verifies that signature against the peer
id embedded in the ticket's own address, then -- unlike
verifyjoininviteticket, which only hands back a string -- calls recruitpeer
itself, since recruiting (unlike addfollower) already dials and completes
the join in one step. printjoinrequestticketdatamatrix barcodes an
already-minted ticketB64 as-is, same reasoning as
printjoininviteticketdatamatrix.

getownaddr prints this node's own current best-advertised multiaddr
(public first, then a relay reservation, then anything else, loopback
last) -- the address to embed in a "<ownAddr>#<tokenHex>" ticket/invite
(printjoinrequestdatamatrix/printjoininvitedatamatrix/
printexecinvitedatamatrix). Queried live, never cached: a node started
with -relay-peer only gets its actual circuit-relay reservation
asynchronously in the background after startup, so re-run this a moment
later if an earlier call returned a private/loopback address instead of
the relayed one.

version prints this node's own build/version info as one JSON object --
git commit, dirty flag, build time, Go version, and the go-libp2p version
it was built against (see shmevent.EventGetVersion's doc comment). Queried
live against the running daemon, never cached.

createexecinvite/revokeexecinvite/redeemexecinvite/printexecinvitedatamatrix
are join-invite's counterpart for triggering a specific command execution
instead of admitting a device: createexecinvite generates a fresh random
token and lodges it as a shmevent.KindExecInvite record binding
commandID+inputsJSON (only a current raft voter may do this). -ttl seconds
(default 0) bounds how long the invite stays redeemable from the moment
it's created; 0 means it never expires.
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

createexecinviteticket/redeemexecinviteticket/printexecinviteticketdatamatrix
are createexecinvite's signed counterpart, the same relationship
createjoininviteticket has to createjoininvite: createexecinviteticket
takes the identical -ttl flag and mints the same token and KindExecInvite
record but wraps this node's own
address and the token into a ticket signed with this node's own key (see
pkg/shmevent.EventExecTicket's doc comment). redeemexecinviteticket
verifies that signature against the peer id embedded in the ticket's own
address before ever dialing anything, then redeems it exactly like
redeemexecinvite does. printexecinviteticketdatamatrix barcodes an
already-minted ticketB64 as-is, same reasoning as
printjoininviteticketdatamatrix.

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

// cmdAddPending implements `kvctl-cli addpending -bin <path>`: spawns a
// brand new node like addnode's bootstrap case, except it never bootstraps
// or joins anything -- see kvctl.AddPending's doc comment.
func cmdAddPending(args []string) {
	fs := flag.NewFlagSet("addpending", flag.ExitOnError)
	binPath := fs.String("bin", "", "path to a pre-built kvnode binary (required)")
	listenPort := fs.Int("listen-port", 0, "TCP/QUIC port for the new node to listen on (0 = ephemeral)")
	relayService := fs.Bool("relay-service", false, "make the new node act as a relay for others (only for nodes with a real public address)")
	raftArgs := raftTimeoutFlags(fs)
	fs.Parse(args)

	if *binPath == "" {
		fmt.Fprintln(os.Stderr, "addpending: -bin is required")
		os.Exit(2)
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli addpending -bin <path> [-listen-port N] [-relay-service] [raft flags]")
		os.Exit(2)
	}

	extra := raftArgs()
	if *listenPort != 0 {
		extra = append(extra, "-listen-port", strconv.Itoa(*listenPort))
	}
	if *relayService {
		extra = append(extra, "-relay-service")
	}

	peerID, err := kvctl.AddPendingWithBinary(*binPath, extra)
	if err != nil {
		fmt.Fprintf(os.Stderr, "addpending: %v\n", err)
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
	skip := fs.Int("skip", 0, "results to drop from the front of the requested order")
	order := fs.String("order", kvctl.RangeOrderAsc, "key order: asc or desc")
	fs.Parse(args)

	if fs.NArg() != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli rangescan <start> <end> [-limit N] [-skip N] [-order asc|desc]")
		os.Exit(2)
	}

	results, err := kvctl.RangeScan(fs.Arg(0), fs.Arg(1), *limit, *skip, *order)
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

// cmdAccessToken prints peerID's deterministic cmd/kvhttp bearer token --
// see kvctl.AccessToken's doc comment.
func cmdAccessToken(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli accesstoken <peerID>")
		os.Exit(2)
	}
	token, err := kvctl.AccessToken(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "accesstoken: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(token)
}

// cmdJoin implements `kvctl-cli join -bin <path> [raft flags] <targetPeerID>`
// -- asks the cluster reachable through targetPeerID to admit the current
// node's own identity (see kvctl.JoinWithBinary's doc comment), switching
// it onto a composite cluster data dir. Whether it's admitted immediately
// or only lodges a pending request depends on the target's own
// -require-confirm-for-join setting.
func cmdJoin(args []string) {
	fs := flag.NewFlagSet("join", flag.ExitOnError)
	binPath := fs.String("bin", "", "path to a pre-built kvnode binary (required)")
	raftArgs := raftTimeoutFlags(fs)
	fs.Parse(args)

	if *binPath == "" {
		fmt.Fprintln(os.Stderr, "join: -bin is required")
		os.Exit(2)
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli join -bin <path> [raft flags] <targetPeerID>")
		os.Exit(2)
	}

	peerID, err := kvctl.JoinWithBinary(*binPath, raftArgs(), fs.Arg(0))
	if err != nil {
		fmt.Fprintf(os.Stderr, "join: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(peerID)
}

// cmdLeave implements `kvctl-cli leave -bin <path> [raft flags] <ownPeerID>`
// -- see kvctl.LeaveWithBinary's doc comment: a graceful raft.RemoveServer
// shrink of the remote cluster, then ownPeerID resumes its own solo db.
func cmdLeave(args []string) {
	fs := flag.NewFlagSet("leave", flag.ExitOnError)
	binPath := fs.String("bin", "", "path to a pre-built kvnode binary (required)")
	raftArgs := raftTimeoutFlags(fs)
	fs.Parse(args)

	if *binPath == "" {
		fmt.Fprintln(os.Stderr, "leave: -bin is required")
		os.Exit(2)
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli leave -bin <path> [raft flags] <ownPeerID>")
		os.Exit(2)
	}

	if err := kvctl.LeaveWithBinary(*binPath, raftArgs(), fs.Arg(0)); err != nil {
		fmt.Fprintf(os.Stderr, "leave: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("%s left its cluster and resumed its solo db\n", fs.Arg(0))
}

// cmdRm implements `kvctl-cli rm -bin <path> [raft flags] <ownPeerID>` --
// see kvctl.RmWithBinary's doc comment: everything leave does, plus
// revokes cluster-join standing and deletes the composite cluster data
// dir outright.
func cmdRm(args []string) {
	fs := flag.NewFlagSet("rm", flag.ExitOnError)
	binPath := fs.String("bin", "", "path to a pre-built kvnode binary (required)")
	raftArgs := raftTimeoutFlags(fs)
	fs.Parse(args)

	if *binPath == "" {
		fmt.Fprintln(os.Stderr, "rm: -bin is required")
		os.Exit(2)
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli rm -bin <path> [raft flags] <ownPeerID>")
		os.Exit(2)
	}

	if err := kvctl.RmWithBinary(*binPath, raftArgs(), fs.Arg(0)); err != nil {
		fmt.Fprintf(os.Stderr, "rm: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("%s removed from its cluster and resumed its solo db\n", fs.Arg(0))
}

// cmdKick implements `kvctl-cli kick <peerID> <targetPeerID>` -- asks the
// raft cluster peerID belongs to (over its already-running daemon's local
// IPC) to force-remove targetPeerID, no cooperation from targetPeerID
// needed. See kvctl.Kick's doc comment.
func cmdKick(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli kick <peerID> <targetPeerID>")
		os.Exit(2)
	}
	if err := kvctl.Kick(args[0], args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "kick: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("%s removed from %s's cluster\n", args[1], args[0])
}

// cmdDeleteNode implements `kvctl-cli deletenode <peerID>` -- permanently
// deletes a stopped node's on-disk data and registry entry.
func cmdDeleteNode(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli deletenode <peerID>")
		os.Exit(2)
	}
	if err := kvctl.DeleteNode(args[0]); err != nil {
		fmt.Fprintf(os.Stderr, "deletenode: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("%s deleted\n", args[0])
}

// cmdBackup implements `kvctl-cli backup <peerID> <destArchive>` -- see
// kvctl.BackupNode's doc comment. Refuses while peerID's daemon still
// appears to be running.
func cmdBackup(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli backup <peerID> <destArchive>")
		os.Exit(2)
	}
	if err := kvctl.BackupNode(args[0], args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "backup: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("%s backed up to %s\n", args[0], args[1])
}

// cmdRestore implements `kvctl-cli restore <archive> <destDir>` -- see
// kvctl.RestoreNode's doc comment. A pure extraction; does not touch the
// registry or start anything.
func cmdRestore(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli restore <archive> <destDir>")
		os.Exit(2)
	}
	if err := kvctl.RestoreNode(args[0], args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "restore: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("%s extracted to %s\n", args[0], args[1])
}

// cmdTxn/cmdCas/cmdCasAbsent are the conditional-write family (see README's
// "Conditional writes"). A refused swap prints to stdout and exits 0: losing
// a race is these commands' ordinary second outcome, and a script looping on
// one wants to read that rather than trap a failure.
func cmdTxn(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, `usage: kvctl-cli txn "<op> [op...]"  (ops: k=v, del:k, if:k=v, ifabsent:k)`)
		os.Exit(2)
	}
	if err := kvctl.Txn(args[0]); err != nil {
		fmt.Fprintf(os.Stderr, "txn: %v\n", err)
		os.Exit(1)
	}
}

func cmdCas(args []string) {
	if len(args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli cas <key> <expected> <value>")
		os.Exit(2)
	}
	if err := kvctl.CompareAndSwap(args[0], args[1], args[2], false); err != nil {
		fmt.Fprintf(os.Stderr, "cas: %v\n", err)
		os.Exit(1)
	}
}

func cmdCasAbsent(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli casabsent <key> <value>")
		os.Exit(2)
	}
	if err := kvctl.CompareAndSwap(args[0], "", args[1], true); err != nil {
		fmt.Fprintf(os.Stderr, "casabsent: %v\n", err)
		os.Exit(1)
	}
}

// cmdCreateGroup implements `kvctl-cli creategroup/updategroup <id> <name>
// <public>` -- creates or updates the Group record id=name. public
// ("true"/"false") grants unconditional access to this group's linked
// commands to any peer if true. Only a current raft voter may do this.
func cmdCreateGroup(args []string) {
	if len(args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli creategroup <id> <name> <public: true|false>")
		os.Exit(2)
	}
	pub, err := strconv.ParseBool(args[2])
	if err != nil {
		fmt.Fprintf(os.Stderr, "creategroup: public: %v\n", err)
		os.Exit(2)
	}
	if err := kvctl.PutGroup(args[0], args[1], pub); err != nil {
		fmt.Fprintf(os.Stderr, "creategroup: %v\n", err)
		os.Exit(1)
	}
}

func cmdDeleteGroup(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli deletegroup <id>")
		os.Exit(2)
	}
	if err := kvctl.DeleteGroup(args[0]); err != nil {
		fmt.Fprintf(os.Stderr, "deletegroup: %v\n", err)
		os.Exit(1)
	}
}

func cmdGetGroup(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli getgroup <id>")
		os.Exit(2)
	}
	group, err := kvctl.GetGroup(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "getgroup: %v\n", err)
		os.Exit(1)
	}
	out, err := json.Marshal(group)
	if err != nil {
		fmt.Fprintf(os.Stderr, "getgroup: encode: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(out))
}

func cmdListGroups(args []string) {
	if len(args) != 0 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli listgroups")
		os.Exit(2)
	}
	groups, err := kvctl.ListGroups()
	if err != nil {
		fmt.Fprintf(os.Stderr, "listgroups: %v\n", err)
		os.Exit(1)
	}
	for _, g := range groups {
		out, err := json.Marshal(g)
		if err != nil {
			fmt.Fprintf(os.Stderr, "listgroups: encode: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(out))
	}
}

func cmdAddPeerToGroup(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli addpeertogroup <peerID> <groupID>")
		os.Exit(2)
	}
	if err := kvctl.AddPeerToGroup(args[0], args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "addpeertogroup: %v\n", err)
		os.Exit(1)
	}
}

func cmdRemovePeerFromGroup(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli removepeerfromgroup <peerID> <groupID>")
		os.Exit(2)
	}
	if err := kvctl.RemovePeerFromGroup(args[0], args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "removepeerfromgroup: %v\n", err)
		os.Exit(1)
	}
}

func cmdListGroupsForPeer(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli listgroupsforpeer <peerID>")
		os.Exit(2)
	}
	groupIDs, err := kvctl.ListGroupsForPeer(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "listgroupsforpeer: %v\n", err)
		os.Exit(1)
	}
	for _, id := range groupIDs {
		fmt.Println(id)
	}
}

// cmdCreateCommand implements `kvctl-cli createcommand/updatecommand <id>
// <name> <peerID>` -- creates or updates the Command record
// id={name, peerID}. Only a current raft voter may do this.
func cmdCreateCommand(args []string) {
	if len(args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli createcommand <id> <name> <peerID>")
		os.Exit(2)
	}
	if err := kvctl.PutCommand(args[0], args[1], args[2]); err != nil {
		fmt.Fprintf(os.Stderr, "createcommand: %v\n", err)
		os.Exit(1)
	}
}

func cmdDeleteCommand(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli deletecommand <id>")
		os.Exit(2)
	}
	if err := kvctl.DeleteCommand(args[0]); err != nil {
		fmt.Fprintf(os.Stderr, "deletecommand: %v\n", err)
		os.Exit(1)
	}
}

func cmdGetCommand(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli getcommand <id>")
		os.Exit(2)
	}
	cmd, err := kvctl.GetCommand(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "getcommand: %v\n", err)
		os.Exit(1)
	}
	out, err := json.Marshal(cmd)
	if err != nil {
		fmt.Fprintf(os.Stderr, "getcommand: encode: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(out))
}

func cmdListCommands(args []string) {
	if len(args) != 0 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli listcommands")
		os.Exit(2)
	}
	commands, err := kvctl.ListCommands()
	if err != nil {
		fmt.Fprintf(os.Stderr, "listcommands: %v\n", err)
		os.Exit(1)
	}
	for _, c := range commands {
		out, err := json.Marshal(c)
		if err != nil {
			fmt.Fprintf(os.Stderr, "listcommands: encode: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(out))
	}
}

func cmdAddCommandToGroup(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli addcommandtogroup <commandID> <groupID>")
		os.Exit(2)
	}
	if err := kvctl.CreateGroupCommand(args[0], args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "addcommandtogroup: %v\n", err)
		os.Exit(1)
	}
}

func cmdRemoveCommandFromGroup(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli removecommandfromgroup <commandID> <groupID>")
		os.Exit(2)
	}
	if err := kvctl.DeleteGroupCommand(args[0], args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "removecommandfromgroup: %v\n", err)
		os.Exit(1)
	}
}

func cmdListGroupsForCommand(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli listgroupsforcommand <commandID>")
		os.Exit(2)
	}
	groupIDs, err := kvctl.ListGroupsForCommand(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "listgroupsforcommand: %v\n", err)
		os.Exit(1)
	}
	for _, id := range groupIDs {
		fmt.Println(id)
	}
}

// cmdCreateCommandSpec writes a command carrying its own form definition.
// peerID may be empty for a command not yet bound to a station.
func cmdCreateCommandSpec(args []string) {
	if len(args) != 4 {
		fmt.Fprintln(os.Stderr, `usage: kvctl-cli createcommandspec <id> <name> <peerID|""> <specJSON>`)
		os.Exit(2)
	}
	if err := kvctl.PutCommandWithSpec(args[0], args[1], args[2], args[3]); err != nil {
		fmt.Fprintf(os.Stderr, "createcommandspec: %v\n", err)
		os.Exit(1)
	}
}

// cmdClearCommandSpec removes a command's form definition. Its own
// subcommand because a plain command put preserves the stored spec rather
// than replacing it (see kvfsm.preserveCommandSpec).
func cmdClearCommandSpec(args []string) {
	if len(args) != 3 {
		fmt.Fprintln(os.Stderr, `usage: kvctl-cli clearcommandspec <id> <name> <peerID|"">`)
		os.Exit(2)
	}
	if err := kvctl.ClearCommandSpec(args[0], args[1], args[2]); err != nil {
		fmt.Fprintf(os.Stderr, "clearcommandspec: %v\n", err)
		os.Exit(1)
	}
}

func cmdCreateStation(args []string) {
	if len(args) != 2 && len(args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli createstation <peerID> <name> [attrsJSON]")
		os.Exit(2)
	}
	attrs := ""
	if len(args) == 3 {
		attrs = args[2]
	}
	if err := kvctl.PutStation(args[0], args[1], attrs); err != nil {
		fmt.Fprintf(os.Stderr, "createstation: %v\n", err)
		os.Exit(1)
	}
}

func cmdDeleteStation(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli deletestation <peerID>")
		os.Exit(2)
	}
	if err := kvctl.DeleteStation(args[0]); err != nil {
		fmt.Fprintf(os.Stderr, "deletestation: %v\n", err)
		os.Exit(1)
	}
}

func cmdGetStation(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli getstation <peerID>")
		os.Exit(2)
	}
	station, err := kvctl.GetStation(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "getstation: %v\n", err)
		os.Exit(1)
	}
	out, err := json.Marshal(station)
	if err != nil {
		fmt.Fprintf(os.Stderr, "getstation: encode: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(out))
}

func cmdListStations(args []string) {
	if len(args) != 0 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli liststations")
		os.Exit(2)
	}
	stations, err := kvctl.ListStations()
	if err != nil {
		fmt.Fprintf(os.Stderr, "liststations: %v\n", err)
		os.Exit(1)
	}
	for _, station := range stations {
		out, err := json.Marshal(station)
		if err != nil {
			fmt.Fprintf(os.Stderr, "liststations: encode: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(out))
	}
}

func cmdRequestPermit(args []string) {
	if len(args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli requestpermit <kind: bootstrap> <peerID> <metadata>")
		os.Exit(2)
	}
	kind, ok := shmevent.KindFromName(args[0])
	if !ok {
		fmt.Fprintf(os.Stderr, "requestpermit: unknown permit kind %q (want \"bootstrap\")\n", args[0])
		os.Exit(2)
	}
	if err := kvctl.RequestPermit(kind, []byte(args[1]), args[2]); err != nil {
		fmt.Fprintf(os.Stderr, "requestpermit: %v\n", err)
		os.Exit(1)
	}
}

func cmdConfirmPermit(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli confirmpermit <kind: bootstrap|cluster-join> <peerID>")
		os.Exit(2)
	}
	kind, ok := shmevent.KindFromName(args[0])
	if !ok {
		fmt.Fprintf(os.Stderr, "confirmpermit: unknown permit kind %q (want \"bootstrap\" or \"cluster-join\")\n", args[0])
		os.Exit(2)
	}
	if err := kvctl.ConfirmPermit(kind, []byte(args[1])); err != nil {
		fmt.Fprintf(os.Stderr, "confirmpermit: %v\n", err)
		os.Exit(1)
	}
}

func cmdRevokePermit(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli revokepermit <kind: bootstrap> <peerID>")
		os.Exit(2)
	}
	kind, ok := shmevent.KindFromName(args[0])
	if !ok {
		fmt.Fprintf(os.Stderr, "revokepermit: unknown permit kind %q (want \"bootstrap\")\n", args[0])
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

func cmdCreateJoinInviteTicket(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli createjoininviteticket <voter|learner>")
		os.Exit(2)
	}
	var suffrage byte
	switch args[0] {
	case "voter":
		suffrage = shmevent.SuffrageVoter
	case "learner":
		suffrage = shmevent.SuffrageLearner
	default:
		fmt.Fprintf(os.Stderr, "createjoininviteticket: unknown suffrage %q (want \"voter\" or \"learner\")\n", args[0])
		os.Exit(2)
	}
	ticket, err := kvctl.CreateJoinInviteTicket(suffrage)
	if err != nil {
		fmt.Fprintf(os.Stderr, "createjoininviteticket: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(ticket)
}

func cmdVerifyJoinInviteTicket(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli verifyjoininviteticket <ticketB64>")
		os.Exit(2)
	}
	addrAndToken, err := kvctl.VerifyJoinInviteTicket(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "verifyjoininviteticket: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(addrAndToken)
}

// cmdPrintJoinInviteTicketDataMatrix implements `kvctl-cli
// printjoininviteticketdatamatrix <ticketB64> <outFile.png>` -- unlike
// cmdPrintJoinInviteDataMatrix, ticketB64 is already one complete,
// self-contained, signed string (see createjoininviteticket), so this
// only barcodes it as-is -- no address/token args to combine.
func cmdPrintJoinInviteTicketDataMatrix(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli printjoininviteticketdatamatrix <ticketB64> <outFile.png>")
		os.Exit(2)
	}
	ticket, outFile := args[0], args[1]

	code, err := datamatrix.Encode(ticket)
	if err != nil {
		fmt.Fprintf(os.Stderr, "printjoininviteticketdatamatrix: encode data matrix: %v\n", err)
		os.Exit(1)
	}
	bounds := code.Bounds()
	scaled, err := barcode.Scale(code, bounds.Dx()*dataMatrixModuleSize, bounds.Dy()*dataMatrixModuleSize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "printjoininviteticketdatamatrix: scale data matrix: %v\n", err)
		os.Exit(1)
	}

	f, err := os.Create(outFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "printjoininviteticketdatamatrix: create %s: %v\n", outFile, err)
		os.Exit(1)
	}
	defer f.Close()
	if err := png.Encode(f, scaled); err != nil {
		fmt.Fprintf(os.Stderr, "printjoininviteticketdatamatrix: write %s: %v\n", outFile, err)
		os.Exit(1)
	}

	fmt.Printf("wrote %s\n", outFile)
	fmt.Println(ticket)
}

func cmdCreateJoinRequest(args []string) {
	if len(args) != 0 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli createjoinrequest")
		os.Exit(2)
	}
	tokenHex, err := kvctl.CreateJoinRequest()
	if err != nil {
		fmt.Fprintf(os.Stderr, "createjoinrequest: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(tokenHex)
}

func cmdCancelJoinRequest(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli canceljoinrequest <tokenHex>")
		os.Exit(2)
	}
	if err := kvctl.CancelJoinRequest(args[0]); err != nil {
		fmt.Fprintf(os.Stderr, "canceljoinrequest: %v\n", err)
		os.Exit(1)
	}
}

// cmdPrintJoinRequestDataMatrix implements `kvctl-cli
// printjoinrequestdatamatrix <ownMultiaddr> <tokenHex> <outFile.png>` --
// the reverse-invite counterpart of cmdPrintJoinInviteDataMatrix: barcodes
// "<ownMultiaddr>#<tokenHex>", this device's own address rather than a
// leader's, for some other cluster's operator to scan and pass to `mage
// recruitpeer`/`kvctl-cli recruitpeer`.
func cmdPrintJoinRequestDataMatrix(args []string) {
	if len(args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli printjoinrequestdatamatrix <ownMultiaddr> <tokenHex> <outFile.png>")
		os.Exit(2)
	}
	ownAddr, tokenHex, outFile := args[0], args[1], args[2]

	ticket := ownAddr + "#" + tokenHex

	code, err := datamatrix.Encode(ticket)
	if err != nil {
		fmt.Fprintf(os.Stderr, "printjoinrequestdatamatrix: encode data matrix: %v\n", err)
		os.Exit(1)
	}
	bounds := code.Bounds()
	scaled, err := barcode.Scale(code, bounds.Dx()*dataMatrixModuleSize, bounds.Dy()*dataMatrixModuleSize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "printjoinrequestdatamatrix: scale data matrix: %v\n", err)
		os.Exit(1)
	}

	f, err := os.Create(outFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "printjoinrequestdatamatrix: create %s: %v\n", outFile, err)
		os.Exit(1)
	}
	defer f.Close()
	if err := png.Encode(f, scaled); err != nil {
		fmt.Fprintf(os.Stderr, "printjoinrequestdatamatrix: write %s: %v\n", outFile, err)
		os.Exit(1)
	}

	fmt.Printf("wrote %s\n", outFile)
	fmt.Println(ticket)
}

// cmdRecruitPeer implements `kvctl-cli recruitpeer <ticket> <voter|learner>`
// -- see kvctl.RecruitPeer's doc comment.
func cmdRecruitPeer(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli recruitpeer <ticket> <voter|learner>")
		os.Exit(2)
	}
	var suffrage byte
	switch args[1] {
	case "voter":
		suffrage = shmevent.SuffrageVoter
	case "learner":
		suffrage = shmevent.SuffrageLearner
	default:
		fmt.Fprintf(os.Stderr, "recruitpeer: unknown suffrage %q (want \"voter\" or \"learner\")\n", args[1])
		os.Exit(2)
	}
	result, err := kvctl.RecruitPeer(args[0], suffrage)
	if err != nil {
		fmt.Fprintf(os.Stderr, "recruitpeer: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(result)
}

func cmdCreateJoinRequestTicket(args []string) {
	if len(args) != 0 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli createjoinrequestticket")
		os.Exit(2)
	}
	ticket, err := kvctl.CreateJoinRequestTicket()
	if err != nil {
		fmt.Fprintf(os.Stderr, "createjoinrequestticket: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(ticket)
}

func cmdRedeemJoinRequestTicket(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli redeemjoinrequestticket <ticketB64> <voter|learner>")
		os.Exit(2)
	}
	var suffrage byte
	switch args[1] {
	case "voter":
		suffrage = shmevent.SuffrageVoter
	case "learner":
		suffrage = shmevent.SuffrageLearner
	default:
		fmt.Fprintf(os.Stderr, "redeemjoinrequestticket: unknown suffrage %q (want \"voter\" or \"learner\")\n", args[1])
		os.Exit(2)
	}
	result, err := kvctl.RedeemJoinRequestTicket(args[0], suffrage)
	if err != nil {
		fmt.Fprintf(os.Stderr, "redeemjoinrequestticket: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(result)
}

// cmdPrintJoinRequestTicketDataMatrix implements `kvctl-cli
// printjoinrequestticketdatamatrix <ticketB64> <outFile.png>` -- unlike
// cmdPrintJoinRequestDataMatrix, ticketB64 is already one complete,
// self-contained, signed string (see createjoinrequestticket), so this
// only barcodes it as-is -- no address/token args to combine.
func cmdPrintJoinRequestTicketDataMatrix(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli printjoinrequestticketdatamatrix <ticketB64> <outFile.png>")
		os.Exit(2)
	}
	ticket, outFile := args[0], args[1]

	code, err := datamatrix.Encode(ticket)
	if err != nil {
		fmt.Fprintf(os.Stderr, "printjoinrequestticketdatamatrix: encode data matrix: %v\n", err)
		os.Exit(1)
	}
	bounds := code.Bounds()
	scaled, err := barcode.Scale(code, bounds.Dx()*dataMatrixModuleSize, bounds.Dy()*dataMatrixModuleSize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "printjoinrequestticketdatamatrix: scale data matrix: %v\n", err)
		os.Exit(1)
	}

	f, err := os.Create(outFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "printjoinrequestticketdatamatrix: create %s: %v\n", outFile, err)
		os.Exit(1)
	}
	defer f.Close()
	if err := png.Encode(f, scaled); err != nil {
		fmt.Fprintf(os.Stderr, "printjoinrequestticketdatamatrix: write %s: %v\n", outFile, err)
		os.Exit(1)
	}

	fmt.Printf("wrote %s\n", outFile)
	fmt.Println(ticket)
}

// cmdGetOwnAddr implements `kvctl-cli getownaddr` -- see kvctl.GetOwnAddr's
// doc comment.
func cmdGetOwnAddr(args []string) {
	if len(args) != 0 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli getownaddr")
		os.Exit(2)
	}
	addr, err := kvctl.GetOwnAddr()
	if err != nil {
		fmt.Fprintf(os.Stderr, "getownaddr: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(addr)
}

// cmdVersion implements `kvctl-cli version` -- see kvctl.Version's doc
// comment.
func cmdVersion(args []string) {
	if len(args) != 0 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli version")
		os.Exit(2)
	}
	info, err := kvctl.Version()
	if err != nil {
		fmt.Fprintf(os.Stderr, "version: %v\n", err)
		os.Exit(1)
	}
	out, err := json.Marshal(info)
	if err != nil {
		fmt.Fprintf(os.Stderr, "version: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(out))
}

func cmdCreateExecInvite(args []string) {
	fs := flag.NewFlagSet("createexecinvite", flag.ExitOnError)
	ttl := fs.Uint64("ttl", 0, "seconds the invite stays redeemable (0 = no expiry)")
	fs.Parse(args)

	if fs.NArg() != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli createexecinvite <commandID> <inputsJSON> [-ttl seconds]")
		os.Exit(2)
	}
	tokenHex, err := kvctl.CreateExecInvite(fs.Arg(0), fs.Arg(1), *ttl)
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

func cmdRequestPublicAccess(args []string) {
	if len(args) < 1 || len(args) > 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli requestpublicaccess <targetAddr> [note]")
		os.Exit(2)
	}
	var note string
	if len(args) == 2 {
		note = args[1]
	}
	instanceID, err := kvctl.RequestPublicAccess(args[0], note)
	if err != nil {
		fmt.Fprintf(os.Stderr, "requestpublicaccess: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(instanceID)
}

func cmdEnablePublicAccess(args []string) {
	if len(args) != 0 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli enablepublicaccess")
		os.Exit(2)
	}
	if err := kvctl.EnablePublicAccess(); err != nil {
		fmt.Fprintf(os.Stderr, "enablepublicaccess: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("ok")
}

// cmdDialSubmitCommand implements `kvctl-cli dialsubmitcommand <targetAddr>
// <commandID> [inputsJSON]` -- asks the current node to dial targetAddr
// directly and submit commandID there as a write into *that* cluster's
// own log, no raft membership in targetAddr's cluster required, provided
// commandID is linked to a public group there. See
// shmevent.EventDialSubmitCommand's doc comment.
func cmdDialSubmitCommand(args []string) {
	if len(args) < 2 || len(args) > 3 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli dialsubmitcommand <targetAddr> <commandID> [inputsJSON]")
		os.Exit(2)
	}
	var inputsJSON string
	if len(args) == 3 {
		inputsJSON = args[2]
	}
	instanceID, err := kvctl.DialSubmitCommand(args[0], args[1], inputsJSON, "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "dialsubmitcommand: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(instanceID)
}

// cmdDialQueryCommandLog implements `kvctl-cli dialquerycommandlog
// <targetAddr> <instanceID>` -- dialsubmitcommand's read-back
// counterpart: dials targetAddr directly and prints back instanceID's own
// command-log entries. Needs no standing in targetAddr's cluster at all --
// possessing instanceID is the credential.
func cmdDialQueryCommandLog(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli dialquerycommandlog <targetAddr> <instanceID>")
		os.Exit(2)
	}
	records, err := kvctl.DialQueryCommandLog(args[0], args[1], time.Time{}, time.Now(), 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dialquerycommandlog: %v\n", err)
		os.Exit(1)
	}
	for _, rec := range records {
		out, err := json.Marshal(rec)
		if err != nil {
			fmt.Fprintf(os.Stderr, "dialquerycommandlog: encode result: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(out))
	}
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

func cmdCreateExecInviteTicket(args []string) {
	fs := flag.NewFlagSet("createexecinviteticket", flag.ExitOnError)
	ttl := fs.Uint64("ttl", 0, "seconds the invite stays redeemable (0 = no expiry)")
	fs.Parse(args)

	if fs.NArg() != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli createexecinviteticket <commandID> <inputsJSON> [-ttl seconds]")
		os.Exit(2)
	}
	ticket, err := kvctl.CreateExecInviteTicket(fs.Arg(0), fs.Arg(1), *ttl)
	if err != nil {
		fmt.Fprintf(os.Stderr, "createexecinviteticket: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(ticket)
}

func cmdRedeemExecInviteTicket(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli redeemexecinviteticket <ticketB64>")
		os.Exit(2)
	}
	instanceID, err := kvctl.RedeemExecInviteTicket(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "redeemexecinviteticket: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(instanceID)
}

// cmdPrintExecInviteTicketDataMatrix implements `kvctl-cli
// printexecinviteticketdatamatrix <ticketB64> <outFile.png>` -- unlike
// cmdPrintExecInviteDataMatrix, ticketB64 is already one complete,
// self-contained, signed string (see createexecinviteticket), so this
// only barcodes it as-is -- no address/token args to combine.
func cmdPrintExecInviteTicketDataMatrix(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli printexecinviteticketdatamatrix <ticketB64> <outFile.png>")
		os.Exit(2)
	}
	ticket, outFile := args[0], args[1]

	code, err := datamatrix.Encode(ticket)
	if err != nil {
		fmt.Fprintf(os.Stderr, "printexecinviteticketdatamatrix: encode data matrix: %v\n", err)
		os.Exit(1)
	}
	bounds := code.Bounds()
	scaled, err := barcode.Scale(code, bounds.Dx()*dataMatrixModuleSize, bounds.Dy()*dataMatrixModuleSize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "printexecinviteticketdatamatrix: scale data matrix: %v\n", err)
		os.Exit(1)
	}

	f, err := os.Create(outFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "printexecinviteticketdatamatrix: create %s: %v\n", outFile, err)
		os.Exit(1)
	}
	defer f.Close()
	if err := png.Encode(f, scaled); err != nil {
		fmt.Fprintf(os.Stderr, "printexecinviteticketdatamatrix: write %s: %v\n", outFile, err)
		os.Exit(1)
	}

	fmt.Printf("wrote %s\n", outFile)
	fmt.Println(ticket)
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

// cmdSubmitCommand implements `kvctl-cli submitcommand <commandID>
// <inputsJSON>` -- see kvctl.SubmitCommand's doc comment. Requires the
// current node's own peer id to be permitted for commandID.
func cmdSubmitCommand(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli submitcommand <commandID> <inputsJSON>")
		os.Exit(2)
	}
	instanceID, err := kvctl.SubmitCommand(args[0], args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "submitcommand: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(instanceID)
}

func cmdGetCommandRequest(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli getcommandrequest <commandID> <instanceID>")
		os.Exit(2)
	}
	req, err := kvctl.GetCommandRequest(args[0], args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "getcommandrequest: %v\n", err)
		os.Exit(1)
	}
	out, err := json.Marshal(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "getcommandrequest: encode: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(out))
}

func cmdListCommandRequests(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli listcommandrequests <commandID>")
		os.Exit(2)
	}
	requests, err := kvctl.ListCommandRequests(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "listcommandrequests: %v\n", err)
		os.Exit(1)
	}
	for _, r := range requests {
		out, err := json.Marshal(r)
		if err != nil {
			fmt.Fprintf(os.Stderr, "listcommandrequests: encode: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(out))
	}
}

// cmdListExecutions implements `kvctl-cli listexecutions <peerID>` -- see
// kvctl.ListExecutionsByPeer's doc comment.
func cmdListExecutions(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli listexecutions <peerID>")
		os.Exit(2)
	}
	executions, err := kvctl.ListExecutionsByPeer(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "listexecutions: %v\n", err)
		os.Exit(1)
	}
	for _, e := range executions {
		out, err := json.Marshal(e)
		if err != nil {
			fmt.Fprintf(os.Stderr, "listexecutions: encode: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(out))
	}
}

func cmdAppendCommandLog(args []string) {
	if len(args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli appendcommandlog <requesterPeerID> <instanceID> <fieldsJSON> <narrative>")
		os.Exit(2)
	}
	var fields map[string]string
	if args[2] != "" {
		if err := json.Unmarshal([]byte(args[2]), &fields); err != nil {
			fmt.Fprintf(os.Stderr, "appendcommandlog: decode fieldsJSON: %v\n", err)
			os.Exit(2)
		}
	}
	if err := kvctl.AppendCommandLog(args[0], args[1], fields, args[3]); err != nil {
		fmt.Fprintf(os.Stderr, "appendcommandlog: %v\n", err)
		os.Exit(1)
	}
}

// cmdReportProgress implements `kvctl-cli reportprogress <requesterPeerID>
// <instanceID> <fieldsJSON> <narrative>` -- like appendcommandlog, but
// stamps fields["status"] = "running" first (see kvctl.ReportProgress's
// doc comment).
func cmdReportProgress(args []string) {
	if len(args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli reportprogress <requesterPeerID> <instanceID> <fieldsJSON> <narrative>")
		os.Exit(2)
	}
	var fields map[string]string
	if args[2] != "" {
		if err := json.Unmarshal([]byte(args[2]), &fields); err != nil {
			fmt.Fprintf(os.Stderr, "reportprogress: decode fieldsJSON: %v\n", err)
			os.Exit(2)
		}
	}
	if err := kvctl.ReportProgress(args[0], args[1], fields, args[3]); err != nil {
		fmt.Fprintf(os.Stderr, "reportprogress: %v\n", err)
		os.Exit(1)
	}
}

func cmdQueryCommandLog(args []string) {
	fs := flag.NewFlagSet("querycommandlog", flag.ExitOnError)
	since := fs.String("since", "", "RFC3339 lower time bound, inclusive (default: unbounded)")
	until := fs.String("until", "", "RFC3339 upper time bound, inclusive (default: now)")
	limit := fs.Int("limit", 0, "maximum records to return (0 = unlimited)")
	fs.Parse(args)

	if fs.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli querycommandlog <instanceID> [-since RFC3339] [-until RFC3339] [-limit N]")
		os.Exit(2)
	}
	start := time.Unix(0, 0)
	if *since != "" {
		t, err := time.Parse(time.RFC3339, *since)
		if err != nil {
			fmt.Fprintf(os.Stderr, "querycommandlog: -since: %v\n", err)
			os.Exit(2)
		}
		start = t
	}
	end := time.Now()
	if *until != "" {
		t, err := time.Parse(time.RFC3339, *until)
		if err != nil {
			fmt.Fprintf(os.Stderr, "querycommandlog: -until: %v\n", err)
			os.Exit(2)
		}
		end = t
	}

	records, err := kvctl.QueryCommandLog(fs.Arg(0), start, end, *limit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "querycommandlog: %v\n", err)
		os.Exit(1)
	}
	for _, rec := range records {
		out, err := json.Marshal(rec)
		if err != nil {
			fmt.Fprintf(os.Stderr, "querycommandlog: encode result: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(out))
	}
}

func cmdLatestCommandLog(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli latestcommandlog <instanceID>")
		os.Exit(2)
	}
	rec, err := kvctl.LatestCommandLog(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "latestcommandlog: %v\n", err)
		os.Exit(1)
	}
	out, err := json.Marshal(rec)
	if err != nil {
		fmt.Fprintf(os.Stderr, "latestcommandlog: encode: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(out))
}

// cmdAddRelayNode implements `kvctl-cli addrelaynode <multiaddr>
// <priority>` -- see kvctl.AddRelayNode's doc comment. Needs
// confirmrelaynode from a current voter before it's actually picked up.
func cmdAddRelayNode(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli addrelaynode <multiaddr> <priority>")
		os.Exit(2)
	}
	p, err := strconv.ParseUint(args[1], 10, 8)
	if err != nil {
		fmt.Fprintf(os.Stderr, "addrelaynode: priority: %v\n", err)
		os.Exit(2)
	}
	if err := kvctl.AddRelayNode(args[0], uint8(p)); err != nil {
		fmt.Fprintf(os.Stderr, "addrelaynode: %v\n", err)
		os.Exit(1)
	}
}

func cmdConfirmRelayNode(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli confirmrelaynode <multiaddr>")
		os.Exit(2)
	}
	if err := kvctl.ConfirmRelayNode(args[0]); err != nil {
		fmt.Fprintf(os.Stderr, "confirmrelaynode: %v\n", err)
		os.Exit(1)
	}
}

func cmdRemoveRelayNode(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli removerelaynode <multiaddr>")
		os.Exit(2)
	}
	if err := kvctl.RemoveRelayNode(args[0]); err != nil {
		fmt.Fprintf(os.Stderr, "removerelaynode: %v\n", err)
		os.Exit(1)
	}
}

func cmdGetRelayNode(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli getrelaynode <multiaddr>")
		os.Exit(2)
	}
	node, err := kvctl.GetRelayNode(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "getrelaynode: %v\n", err)
		os.Exit(1)
	}
	out, err := json.Marshal(node)
	if err != nil {
		fmt.Fprintf(os.Stderr, "getrelaynode: encode: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(out))
}

func cmdListRelayNodes(args []string) {
	if len(args) != 0 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli listrelaynodes")
		os.Exit(2)
	}
	nodes, err := kvctl.ListRelayNodes()
	if err != nil {
		fmt.Fprintf(os.Stderr, "listrelaynodes: %v\n", err)
		os.Exit(1)
	}
	for _, n := range nodes {
		out, err := json.Marshal(n)
		if err != nil {
			fmt.Fprintf(os.Stderr, "listrelaynodes: encode: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(out))
	}
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
//
// Must comfortably exceed pkg/daemon.Node.join's own internal
// awaitRelayAddr(45 * time.Second) wait: an EventAdd on a node that hasn't
// joined a cluster yet runs that whole join() synchronously before the
// daemon ever writes an IPC response, so a cold node's first relay
// reservation alone can legitimately eat 45s before the raft AddVoter call
// even starts. 30s made that "add" call fail every single time (context
// deadline exceeded well before the daemon replied), not just occasionally
// -- confirmed directly against a real deploy target.
const sendEventTimeout = 60 * time.Second

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
	if shmevent.RequiresSignature(m.Which()) {
		keyMsg, err := shmevent.NewGetPrivateKey()
		if err != nil {
			return nil, fmt.Errorf("fetch signing key: %w", err)
		}
		keyMsg.SetId(randomID())
		keyResp, err := ipc.Call(ctx, peerID, keyMsg, nil)
		if err != nil {
			return nil, fmt.Errorf("fetch signing key: %w", err)
		}
		if keyResp.Which() == shmevent.Event_Which_error {
			errMsg, _ := keyResp.Error().Message_()
			return nil, fmt.Errorf("fetch signing key: %s", errMsg)
		}
		privKey, err := keyResp.GetPrivateKey().PrivKey()
		if err != nil {
			return nil, fmt.Errorf("fetch signing key: %w", err)
		}
		priv = shmevent.PrivateKey(privKey)
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
	m, err := ev.ToMsg()
	if err != nil {
		return shmevent.Msg{}, err
	}
	return m, nil
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

	respEv, err := e2edata.EventFromMsg(resp)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sendevent: decode response: %v\n", err)
		os.Exit(1)
	}
	out, err := json.Marshal(respEv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sendevent: encode response: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(out))
	if resp.Which() == shmevent.Event_Which_error {
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

	respEv, err := e2edata.EventFromMsg(resp)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sendrawevent: decode response: %v\n", err)
		os.Exit(1)
	}
	out, err := json.Marshal(respEv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sendrawevent: encode response: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(out))
	if resp.Which() == shmevent.Event_Which_error {
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
