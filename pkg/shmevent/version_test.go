package shmevent

import "testing"

func TestVersionInfoRoundTrip(t *testing.T) {
	want := VersionInfo{
		Commit:        "613b303c8268b06f1f33f632475236d08dd3da62",
		Dirty:         true,
		BuildTime:     "2026-07-12T18:05:23Z",
		GoVersion:     "go1.25.7",
		Libp2pVersion: "v0.48.0",
	}
	m, err := NewGetVersionResponse(want)
	if err != nil {
		t.Fatalf("NewGetVersionResponse: %v", err)
	}
	if m.Which() != Event_Which_getVersion {
		t.Fatalf("Which() = %v, want getVersion", m.Which())
	}
	got, err := VersionInfoFrom(m.GetVersion())
	if err != nil {
		t.Fatalf("VersionInfoFrom: %v", err)
	}
	if got != want {
		t.Fatalf("round trip mismatch: got %+v, want %+v", got, want)
	}
}

func TestVersionInfoZeroValueRoundTrip(t *testing.T) {
	m, err := NewGetVersionResponse(VersionInfo{})
	if err != nil {
		t.Fatalf("NewGetVersionResponse: %v", err)
	}
	got, err := VersionInfoFrom(m.GetVersion())
	if err != nil {
		t.Fatalf("VersionInfoFrom: %v", err)
	}
	if got != (VersionInfo{}) {
		t.Fatalf("round trip of zero value mismatch: got %+v", got)
	}
}
