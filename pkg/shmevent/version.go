package shmevent

import "encoding/json"

// VersionInfo is EventGetVersion's response shape -- a snapshot of the
// answering node's own build, gathered live from runtime/debug.ReadBuildInfo
// (see pkg/daemon's currentBuildInfo, the only place that ever constructs
// one). Commit/BuildTime/Dirty come from the Go toolchain's automatic VCS
// build stamping (present whenever the binary was built from within a git
// checkout, which is how every one of this project's own build paths --
// `go build`, mage's Build/BuildLinux/etc. -- works); they're empty/false on
// a binary built with -buildvcs=false or from a source tree with no VCS at
// all, not an error.
type VersionInfo struct {
	Commit        string `json:"commit"`
	Dirty         bool   `json:"dirty"`
	BuildTime     string `json:"buildTime"`
	GoVersion     string `json:"goVersion"`
	Libp2pVersion string `json:"libp2pVersion"`
}

// EncodeVersionInfo packs v into EventGetVersion's response Value. JSON,
// unlike this package's other Encode* helpers (which use a fixed
// length-prefixed binary layout): VersionInfo is a small, diagnostic,
// read-only payload with no hot path and no reason to hand-roll framing for,
// and JSON is already this codebase's convention for exactly this kind of
// informational blob (see pkg/logrecord.Record). v's fields are all
// string/bool, so json.Marshal can never actually fail here.
func EncodeVersionInfo(v VersionInfo) []byte {
	b, _ := json.Marshal(v)
	return b
}

// DecodeVersionInfo is the inverse of EncodeVersionInfo.
func DecodeVersionInfo(payload []byte) (VersionInfo, error) {
	var v VersionInfo
	err := json.Unmarshal(payload, &v)
	return v, err
}
