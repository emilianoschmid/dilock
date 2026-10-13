package dilock

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestDraftOneIdentityVectors(t *testing.T) {
	data, err := os.ReadFile("../../spec/vectors/identity.json")
	if err != nil {
		t.Fatal(err)
	}
	// Use explicit field tags because the published vector names are snake_case.
	var parsed struct {
		Valid []struct {
			Namespace string `json:"namespace"`
			Resource  string `json:"resource"`
			SHA256    string `json:"sha256"`
			RedisKey  string `json:"redis_key"`
		} `json:"valid"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatal(err)
	}
	if len(parsed.Valid) != 13 {
		t.Fatalf("valid vector count = %d, want 13", len(parsed.Valid))
	}
	for i, v := range parsed.Valid {
		got, err := Identity(v.Namespace, v.Resource)
		if err != nil {
			t.Fatalf("vector %d rejected: %v", i+1, err)
		}
		if got != v.SHA256 {
			t.Errorf("vector %d identity = %s, want %s", i+1, got, v.SHA256)
		}
		key, err := RedisKey(v.Namespace, v.Resource)
		if err != nil || key != v.RedisKey {
			t.Errorf("vector %d Redis key = %q, %v; want %q", i+1, key, err, v.RedisKey)
		}
	}
}

func TestIdentityRejectsInvalidInputs(t *testing.T) {
	for _, tc := range []struct{ namespace, resource string }{
		{"", "r"}, {"Prod", "r"}, {"a/b", "r"}, {"default", ""}, {"default", "a\x00b"},
		{"default", string([]byte{0xff})}, {"default", strings.Repeat("x", 1025)},
	} {
		if _, err := Identity(tc.namespace, tc.resource); err == nil {
			t.Errorf("Identity(%q, %q) succeeded", tc.namespace, tc.resource)
		}
	}
}

func TestIdentityAcceptsF1ToF3FourByteScalarsAndRejectsMalformedBytes(t *testing.T) {
	for _, resource := range []string{string([]byte{0xf1, 0x80, 0x80, 0x80}), string([]byte{0xf3, 0xbf, 0xbf, 0xbf})} {
		if _, err := Identity("default", resource); err != nil {
			t.Errorf("valid four-byte scalar %x rejected: %v", []byte(resource), err)
		}
	}
	for _, resource := range []string{string([]byte{0xf1, 0x7f, 0x80, 0x80}), string([]byte{0xf3, 0xbf, 0xbf, 0xc0})} {
		if _, err := Identity("default", resource); err == nil {
			t.Errorf("malformed UTF-8 %x accepted", []byte(resource))
		}
	}
}
