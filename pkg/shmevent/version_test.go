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
	got, err := DecodeVersionInfo(EncodeVersionInfo(want))
	if err != nil {
		t.Fatalf("DecodeVersionInfo: %v", err)
	}
	if got != want {
		t.Fatalf("round trip mismatch: got %+v, want %+v", got, want)
	}
}

func TestVersionInfoZeroValueRoundTrip(t *testing.T) {
	got, err := DecodeVersionInfo(EncodeVersionInfo(VersionInfo{}))
	if err != nil {
		t.Fatalf("DecodeVersionInfo: %v", err)
	}
	if got != (VersionInfo{}) {
		t.Fatalf("round trip of zero value mismatch: got %+v", got)
	}
}

func TestDecodeVersionInfoRejectsGarbage(t *testing.T) {
	if _, err := DecodeVersionInfo([]byte("not json")); err == nil {
		t.Fatal("DecodeVersionInfo unexpectedly accepted non-JSON input")
	}
}
