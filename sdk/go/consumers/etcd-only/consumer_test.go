package consumer

import (
	"testing"

	"example.invalid/dilock/etcd"
)

func TestEtcdConsumerSurface(t *testing.T) {
	name, err := etcd.LockName("orders", "invoice/42")
	if err != nil {
		t.Fatal(err)
	}
	if name == "" {
		t.Fatal("expected a canonical etcd lock name")
	}
}
