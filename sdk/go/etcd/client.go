// Package etcd implements the Dilock etcd native Lock profile.
package etcd

import (
	"context"
	"errors"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb"
	"google.golang.org/grpc"
)

// Client is the borrowed etcd v3 KV and lease surface used by this adapter.
// Implementations must use linearizable reads unless explicitly requested
// otherwise; this package never requests serializable reads.
type Client interface {
	Get(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error)
	Grant(context.Context, int64) (*clientv3.LeaseGrantResponse, error)
	KeepAliveOnce(context.Context, clientv3.LeaseID) (*clientv3.LeaseKeepAliveResponse, error)
	Revoke(context.Context, clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error)
}

// LockRPC is the etcd native gRPC Lock service. The key returned by Lock is
// opaque and must be passed unchanged to Unlock and exact-key verification.
type LockRPC interface {
	Lock(context.Context, string, clientv3.LeaseID) ([]byte, error)
	Unlock(context.Context, []byte) error
}

// NewGRPCLockRPC binds the official etcd Lock service client to a caller-owned
// gRPC connection. The connection must reach the same etcd cluster as Client
// and carry the TLS and authentication configuration required by that cluster.
// The returned adapter does not close conn.
func NewGRPCLockRPC(conn grpc.ClientConnInterface) (LockRPC, error) {
	if conn == nil {
		return nil, errors.New("etcd gRPC connection is required")
	}
	return grpcLockRPC{client: v3lockpb.NewLockClient(conn)}, nil
}

type grpcLockRPC struct{ client v3lockpb.LockClient }

func (c grpcLockRPC) Lock(ctx context.Context, name string, id clientv3.LeaseID) ([]byte, error) {
	response, err := c.client.Lock(ctx, &v3lockpb.LockRequest{Name: []byte(name), Lease: int64(id)})
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), response.GetKey()...), nil
}

func (c grpcLockRPC) Unlock(ctx context.Context, key []byte) error {
	_, err := c.client.Unlock(ctx, &v3lockpb.UnlockRequest{Key: append([]byte(nil), key...)})
	return err
}
