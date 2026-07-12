// Command kvrecover force-recovers a hashicorp/raft node into an explicit
// voter configuration via raft.RecoverCluster, either to break a permanent
// quorum deadlock (every missing voter dropped, only survivors kept) or to
// drop a single bad voter (e.g. one that joined advertising an undialable
// address) from an otherwise healthy cluster. This is the manual recovery
// path pkg/daemon has no built-in equivalent for -- see
// raft.RecoverCluster's own doc comment for exactly what it does (replays
// the existing log/snapshot into the FSM, then writes a new snapshot
// carrying the given configuration and truncates the old log) and why it's
// a last resort, not routine.
//
// Run it with the target node's kvnode daemon stopped -- it opens the same
// raft.db/snapshots files the daemon does, and BoltDB only allows one
// writer at a time. When recovering more than one surviving node (the
// "drop a bad voter" case), stop all of them first and run kvrecover on
// each with the identical -voter set before restarting any -- see
// RecoverCluster's own doc comment on why every survivor needs the same
// configuration. Restart the daemon(s) normally afterward; the existing
// resume-on-existing-state path in daemon.Run picks the recovered
// configuration back up with no other coordination needed.
package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvfsm"
	"github.com/gofsd/libp2p-kv-raft/pkg/rafttransport"
	"github.com/gofsd/libp2p-kv-raft/pkg/store"
)

// voterList collects repeated -voter flags into an ordered list.
type voterList []string

func (v *voterList) String() string { return strings.Join(*v, ",") }
func (v *voterList) Set(s string) error {
	*v = append(*v, s)
	return nil
}

func main() {
	dataDir := flag.String("data-dir", "", "node data dir (same -data-dir the daemon was run with)")
	keyPath := flag.String("key-path", "", "node identity key file (same -key-path the daemon was run with)")
	var voters voterList
	flag.Var(&voters, "voter", "a surviving voter's dialable multiaddr, including /p2p/<peer-id> (repeat once per voter to keep; the recovering node itself must be included)")
	flag.Parse()
	if *dataDir == "" || *keyPath == "" || len(voters) == 0 {
		fmt.Fprintln(os.Stderr, "usage: kvrecover -data-dir <dir> -key-path <file> -voter <multiaddr> [-voter <multiaddr> ...]")
		os.Exit(2)
	}

	if err := run(*dataDir, *keyPath, voters); err != nil {
		fmt.Fprintln(os.Stderr, "kvrecover:", err)
		os.Exit(1)
	}
	fmt.Println("kvrecover: recovered to the given voter configuration; restart the daemon normally now.")
}

func run(dataDir, keyPath string, voters voterList) error {
	priv, err := loadKey(keyPath)
	if err != nil {
		return fmt.Errorf("load identity: %w", err)
	}
	peerID, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		return fmt.Errorf("derive peer id: %w", err)
	}

	servers := make([]raft.Server, 0, len(voters))
	for _, addr := range voters {
		maddr, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			return fmt.Errorf("invalid -voter %q: %w", addr, err)
		}
		info, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			return fmt.Errorf("-voter %q missing /p2p/<peer-id>: %w", addr, err)
		}
		servers = append(servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(info.ID.String()),
			Address:  raft.ServerAddress(addr),
		})
	}

	// A bare, non-listening host is enough here: RecoverCluster never
	// dials or accepts through the transport, it only calls
	// EncodePeer/LocalAddr (see raft.NetworkTransport) while writing the
	// new snapshot's legacy Peers blob.
	h, err := libp2p.New(libp2p.Identity(priv), libp2p.NoListenAddrs)
	if err != nil {
		return fmt.Errorf("create libp2p host: %w", err)
	}
	defer h.Close()
	transport := rafttransport.NewTransport(h, 10*time.Second)
	defer transport.Close()

	st, err := store.Open(filepath.Join(dataDir, "sqlite"))
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer st.Close()
	fsm := kvfsm.New(st)

	raftDir := filepath.Join(dataDir, "raft")
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("open raft log store: %w", err)
	}
	defer logStore.Close()

	snapStore, err := raft.NewFileSnapshotStore(filepath.Join(raftDir, "snapshots"), 2, io.Discard)
	if err != nil {
		return fmt.Errorf("open snapshot store: %w", err)
	}

	raftConf := raft.DefaultConfig()
	raftConf.LocalID = raft.ServerID(peerID.String())

	configuration := raft.Configuration{Servers: servers}

	return raft.RecoverCluster(raftConf, fsm, logStore, logStore, snapStore, transport, configuration)
}

// loadKey mirrors pkg/daemon.loadKey's hex-encoded marshaled-key format.
func loadKey(keyPath string) (crypto.PrivKey, error) {
	data, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("read key file %s: %w", keyPath, err)
	}
	raw, err := hex.DecodeString(string(data))
	if err != nil {
		return nil, fmt.Errorf("decode key file %s: %w", keyPath, err)
	}
	return crypto.UnmarshalPrivateKey(raw)
}
