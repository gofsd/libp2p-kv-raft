package e2erun

import (
	"reflect"
	"testing"
)

func TestParseConnectedSerials(t *testing.T) {
	cases := []struct {
		name string
		out  string
		want []string
	}{
		{"no devices, just header", "List of devices attached\n\n", nil},
		{"one real device", "List of devices attached\nO7AQS4UO9LWWJZI7\tdevice\n\n", []string{"O7AQS4UO9LWWJZI7"}},
		{"two devices", "List of devices attached\nemulator-5554\tdevice\nemulator-5556\tdevice\n\n", []string{"emulator-5554", "emulator-5556"}},
		{"offline device excluded", "List of devices attached\nemulator-5554\toffline\nemulator-5556\tdevice\n\n", []string{"emulator-5556"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := parseConnectedSerials(c.out)
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("parseConnectedSerials(%q) = %v, want %v", c.out, got, c.want)
			}
		})
	}
}

func TestValidateClusterMembers(t *testing.T) {
	twoMembers := `[{"peer_id":"A","role":"leader"},{"peer_id":"B","role":"learner"}]`

	if err := validateClusterMembers(twoMembers, 2, "", ""); err != nil {
		t.Fatalf("validateClusterMembers count-only: %v", err)
	}
	if err := validateClusterMembers(twoMembers, 2, "B", "learner"); err != nil {
		t.Fatalf("validateClusterMembers with matching peer/role: %v", err)
	}
	if err := validateClusterMembers(twoMembers, 1, "", ""); err == nil {
		t.Fatal("validateClusterMembers accepted wrong count")
	}
	if err := validateClusterMembers(twoMembers, 2, "B", "voter"); err == nil {
		t.Fatal("validateClusterMembers accepted wrong role")
	}
	if err := validateClusterMembers(twoMembers, 2, "C", "learner"); err == nil {
		t.Fatal("validateClusterMembers accepted missing peer")
	}
	if err := validateClusterMembers("not json", 2, "", ""); err == nil {
		t.Fatal("validateClusterMembers accepted invalid JSON")
	}
}

func TestPairKeyHexProducesDistinctDecodableKeys(t *testing.T) {
	a, err := freshKeyHex()
	if err != nil {
		t.Fatalf("freshKeyHex A: %v", err)
	}
	b, err := freshKeyHex()
	if err != nil {
		t.Fatalf("freshKeyHex B: %v", err)
	}
	if a == b {
		t.Fatal("freshKeyHex produced identical keys for two separate calls")
	}
	if a == "" || b == "" {
		t.Fatal("freshKeyHex produced an empty key")
	}
}
