package consumer

import (
	"testing"

	"example.invalid/dilock/etcd"
	"example.invalid/dilock/redis"
)

func TestCombinedAdapters(t *testing.T) {
	if _, err := etcd.LockName("orders", "invoice/42"); err != nil {
		t.Fatal(err)
	}
	options := redis.ProviderOptions{Namespace: "orders", Epoch: "candidate"}
	if options.Namespace != "orders" {
		t.Fatal("unexpected Redis options")
	}
}
