package shmevent

import "fmt"

// VersionInfo is getVersion's response shape -- a snapshot of the
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

// NewGetVersionResponse builds a getVersion Msg carrying v -- the response
// side of NewGetVersion's request, since a fresh Msg is needed either way
// (see this package's doc comment on why request/response share one
// variant).
func NewGetVersionResponse(v VersionInfo) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetGetVersion()
	grp := m.GetVersion()
	if err := grp.SetCommit(v.Commit); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_get_version_response: %w", err)
	}
	grp.SetDirty(v.Dirty)
	if err := grp.SetBuildTime(v.BuildTime); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_get_version_response: %w", err)
	}
	if err := grp.SetGoVersion(v.GoVersion); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_get_version_response: %w", err)
	}
	if err := grp.SetLibp2pVersion(v.Libp2pVersion); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_get_version_response: %w", err)
	}
	return m, nil
}

// VersionInfoFrom reads a getVersion response Msg's fields back into a
// VersionInfo -- NewGetVersionResponse's inverse.
func VersionInfoFrom(grp Event_getVersion) (VersionInfo, error) {
	commit, err := grp.Commit()
	if err != nil {
		return VersionInfo{}, fmt.Errorf("shmevent: get_version commit: %w", err)
	}
	buildTime, err := grp.BuildTime()
	if err != nil {
		return VersionInfo{}, fmt.Errorf("shmevent: get_version build_time: %w", err)
	}
	goVersion, err := grp.GoVersion()
	if err != nil {
		return VersionInfo{}, fmt.Errorf("shmevent: get_version go_version: %w", err)
	}
	libp2pVersion, err := grp.Libp2pVersion()
	if err != nil {
		return VersionInfo{}, fmt.Errorf("shmevent: get_version libp2p_version: %w", err)
	}
	return VersionInfo{
		Commit:        commit,
		Dirty:         grp.Dirty(),
		BuildTime:     buildTime,
		GoVersion:     goVersion,
		Libp2pVersion: libp2pVersion,
	}, nil
}
