package redis

import (
	"os"
	"testing"
)

func TestEmbeddedScriptsMatchCanonicalProtocol(t *testing.T) {
	for _, pair := range []struct{ name, embedded string }{{"release", string(releaseScript)}, {"renew", string(renewScript)}} {
		canonical, err := os.ReadFile("../../../spec/backends/redis-" + pair.name + ".lua")
		if err != nil {
			t.Fatal(err)
		}
		if string(canonical) != pair.embedded {
			t.Errorf("embedded %s script differs from canonical protocol", pair.name)
		}
		if scriptHash(canonical) != scriptHash([]byte(pair.embedded)) {
			t.Errorf("embedded %s script hash differs from canonical protocol", pair.name)
		}
	}
}
