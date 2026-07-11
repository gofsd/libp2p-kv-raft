package shmevent

import "testing"

func TestRegistryRegisterLookup(t *testing.T) {
	r := NewRegistry()
	if _, ok := r.Lookup(1); ok {
		t.Fatal("Lookup on empty registry unexpectedly found an entry")
	}
	r.Register(1, []byte("hello"))
	got, ok := r.Lookup(1)
	if !ok || string(got) != "hello" {
		t.Fatalf("Lookup(1) = %q, %v; want %q, true", got, ok, "hello")
	}
	r.Register(1, []byte("overwritten"))
	got, ok = r.Lookup(1)
	if !ok || string(got) != "overwritten" {
		t.Fatalf("Lookup(1) after overwrite = %q, %v; want %q, true", got, ok, "overwritten")
	}

	// Mutating a value handed back must not corrupt the stored copy.
	got[0] = 'X'
	got2, _ := r.Lookup(1)
	if string(got2) != "overwritten" {
		t.Fatalf("Lookup returned an aliased slice: got %q after mutating a previous result", got2)
	}
}
