package daemon

import (
	"sync"
	"time"

	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"golang.org/x/time/rate"
)

// Default quota values -- pkg/daemon.start substitutes these for whichever
// Config.Quota* fields are left at their zero value, mirroring
// relayLimits' identical substitution of shmevent.DefaultRelayLimits (see
// that function's own doc comment): a fresh node gets real quota
// enforcement out of the box, not silently-unlimited buckets, without an
// operator needing to discover and set eight flags by hand. Unlike
// RelayLimits, there is deliberately no way to configure "actually
// unlimited" once past this substitution -- consistent with every other
// Default* constant in this codebase.
//
// Relay defaults are tight: a reservation/connect event is inherently
// rare regardless of legitimate workload (a peer reserves a slot once on
// startup/reconnect, not per operation), so 1/sec sustained with a burst
// of 5 comfortably covers reconnect churn while still capping a runaway
// retry loop; the per-IP values are only slightly looser, covering a
// handful of devices behind one shared gateway.
//
// Channel defaults are deliberately generous: unlike relay, Channel is a
// first-class bulk-transfer feature (README's "Raw Channel" section
// benchmarks sustained ~300+ MiB/s two-way desktop-to-desktop throughput,
// pkg/e2erun's Android channel-transfer scenario pushes real files) --
// a tight per-peer byte-rate cap here would throttle exactly the
// legitimate use case this feature exists for. These values are set well
// above realistic single-peer sustained throughput, functioning as a
// backstop against pathological abuse (many simultaneous max-size
// transfers, a buggy client retry-flooding) rather than a meaningful
// ceiling on ordinary use -- see
// TestChannelDataplaneDesktopThroughputBenchmark, which this must never
// throttle.
const (
	DefaultQuotaChannelBytesPerPeerPerSec = 512 << 20  // 512 MiB/s
	DefaultQuotaChannelBurstPerPeer       = 64 << 20   // 64 MiB
	DefaultQuotaChannelBytesPerIPPerSec   = 1024 << 20 // 1 GiB/s
	DefaultQuotaChannelBurstPerIP         = 128 << 20  // 128 MiB

	DefaultQuotaRelayEventsPerPeerPerSec = 1
	DefaultQuotaRelayBurstPerPeer        = 5
	DefaultQuotaRelayEventsPerIPPerSec   = 5
	DefaultQuotaRelayBurstPerIP          = 10
)

// quotaTracker enforces one independent token-bucket rate.Limiter per key,
// in two parallel key-spaces (peer id, IP address) that never share
// state -- a peer rotating its IP, or many peers sharing one IP, still has
// to clear whichever bucket is actually unspoofable for that event.
// Mirrors cmd/kvhttp/main.go's rateLimiter (map[string]*rate.Limiter
// behind a mutex, one bucket per peer id), generalized to a second
// IP-keyed map and an AllowN-shaped cost so the same type metes out real
// channel bytes and one-off relay admission events alike -- see
// (*Node).channelQuota/relayQuota's doc comments for how each is
// configured.
//
// A zero rate for either key-space means genuinely "unlimited" for that
// key-space at this primitive's own level: allow never allocates a
// limiter for it and always reports true against it. Config.Quota*'s own
// zero value never actually reaches here, though -- pkg/daemon.start
// resolves it through relayQuotaLimits/channelQuotaLimits first, which
// substitute this package's Default* constants above (the same
// zero-means-default pattern relayLimits already uses for RelayLimits) --
// so a real daemon never constructs an actually-unlimited tracker. Only
// this type's own tests (and any other direct caller) see the raw
// zero-means-unlimited behavior described here.
type quotaTracker struct {
	mu   sync.Mutex
	peer map[string]*rate.Limiter
	ip   map[string]*rate.Limiter

	peerLimit rate.Limit
	peerBurst int
	ipLimit   rate.Limit
	ipBurst   int
}

// newQuotaTracker builds a tracker whose peer-bucket sustains
// peerPerSec/peerBurst and whose IP-bucket sustains ipPerSec/ipBurst,
// independently. Either pair may be left at 0/0 to leave that key-space
// unlimited.
func newQuotaTracker(peerPerSec float64, peerBurst int, ipPerSec float64, ipBurst int) *quotaTracker {
	return &quotaTracker{
		peer:      make(map[string]*rate.Limiter),
		ip:        make(map[string]*rate.Limiter),
		peerLimit: rate.Limit(peerPerSec),
		peerBurst: peerBurst,
		ipLimit:   rate.Limit(ipPerSec),
		ipBurst:   ipBurst,
	}
}

// allow reports whether cost units (bytes, for channel traffic; 1, for a
// single relay reservation/connect event) are currently permitted for
// both peerID and ip -- both configured buckets must have room. An empty
// peerID/ip, or a key-space left at its zero/unlimited rate, is skipped
// entirely rather than charged against a shared "" bucket.
func (q *quotaTracker) allow(peerID, ip string, cost int) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := time.Now()
	if q.peerLimit > 0 && peerID != "" {
		lim, ok := q.peer[peerID]
		if !ok {
			lim = rate.NewLimiter(q.peerLimit, q.peerBurst)
			q.peer[peerID] = lim
		}
		if !lim.AllowN(now, cost) {
			return false
		}
	}
	if q.ipLimit > 0 && ip != "" {
		lim, ok := q.ip[ip]
		if !ok {
			lim = rate.NewLimiter(q.ipLimit, q.ipBurst)
			q.ip[ip] = lim
		}
		if !lim.AllowN(now, cost) {
			return false
		}
	}
	return true
}

// extractRemoteIP returns addr's host IP as a string, or "" if addr is nil
// or carries no resolvable IP component (e.g. a bare /p2p-circuit relay
// address with no underlying IP leg) -- the shared helper relayACL,
// handleChannelStream and dispatchChannelOpen all use to turn a libp2p
// multiaddr into a quotaTracker IP-bucket key.
func extractRemoteIP(addr multiaddr.Multiaddr) string {
	if addr == nil {
		return ""
	}
	ip, err := manet.ToIP(addr)
	if err != nil {
		return ""
	}
	return ip.String()
}
