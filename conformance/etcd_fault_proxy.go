// etcd_fault_proxy is a test-only generic gRPC proxy used by M6 fault tests.
// Build it from sdk/go so it uses the repository-pinned gRPC client version.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type rawCodec struct{}

func (rawCodec) Name() string { return "proto" }

func (rawCodec) Marshal(message any) ([]byte, error) {
	data, ok := message.([]byte)
	if !ok {
		return nil, fmt.Errorf("raw gRPC codec received %T", message)
	}
	return data, nil
}

func (rawCodec) Unmarshal(data []byte, message any) error {
	target, ok := message.(*[]byte)
	if !ok {
		return fmt.Errorf("raw gRPC codec cannot decode into %T", message)
	}
	*target = append((*target)[:0], data...)
	return nil
}

type proxyState struct {
	armed                   atomic.Bool
	lockRequests            atomic.Int64
	successfulLockResponses atomic.Int64
	droppedLockAcknowledges atomic.Int64
	injected                chan struct{}
}

type genericProxy struct {
	upstream *grpc.ClientConn
	state    *proxyState
}

func (p *genericProxy) forward(_ any, downstream grpc.ServerStream) error {
	transport := grpc.ServerTransportStreamFromContext(downstream.Context())
	if transport == nil {
		return status.Error(codes.Internal, "gRPC method is unavailable")
	}
	method := transport.Method()
	isLock := method == "/v3lockpb.Lock/Lock"
	if isLock {
		p.state.lockRequests.Add(1)
	}

	ctx, cancel := context.WithCancel(downstream.Context())
	defer cancel()
	if incoming, ok := metadata.FromIncomingContext(ctx); ok {
		ctx = metadata.NewOutgoingContext(ctx, incoming.Copy())
	}
	stream, err := p.upstream.NewStream(ctx,
		&grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, method,
		grpc.ForceCodec(rawCodec{}))
	if err != nil {
		return relayStatus(err)
	}

	requestDone := make(chan error, 1)
	go func() {
		for {
			var request []byte
			err := downstream.RecvMsg(&request)
			if errors.Is(err, io.EOF) {
				requestDone <- stream.CloseSend()
				return
			}
			if err != nil {
				requestDone <- err
				return
			}
			if err := stream.SendMsg(request); err != nil {
				requestDone <- err
				return
			}
		}
	}()

	var headersSent bool
	for {
		var response []byte
		err := stream.RecvMsg(&response)
		if errors.Is(err, io.EOF) {
			if !headersSent {
				headers, headerErr := stream.Header()
				if headerErr != nil {
					return relayStatus(headerErr)
				}
				if sendErr := downstream.SendHeader(headers); sendErr != nil {
					return sendErr
				}
			}
			downstream.SetTrailer(stream.Trailer())
			select {
			case requestErr := <-requestDone:
				if requestErr != nil {
					return relayStatus(requestErr)
				}
			default:
			}
			return nil
		}
		if err != nil {
			return relayStatus(err)
		}
		if !headersSent {
			headers, headerErr := stream.Header()
			if headerErr != nil {
				return relayStatus(headerErr)
			}
			if sendErr := downstream.SendHeader(headers); sendErr != nil {
				return sendErr
			}
			headersSent = true
		}
		if isLock {
			p.state.successfulLockResponses.Add(1)
			if p.state.armed.CompareAndSwap(true, false) {
				p.state.droppedLockAcknowledges.Add(1)
				close(p.state.injected)
				// The upstream Lock committed and returned its opaque key. Report a
				// transport failure before any response body reaches the caller.
				return status.Error(codes.Unavailable, "injected loss of etcd Lock acknowledgement after commit")
			}
		}
		if err := downstream.SendMsg(response); err != nil {
			return err
		}
	}
}

func relayStatus(err error) error {
	if value, ok := status.FromError(err); ok {
		return status.Error(value.Code(), value.Message())
	}
	return status.Error(codes.Unavailable, "etcd gRPC upstream transport failed")
}

type result struct {
	LockRequests            int64 `json:"lock_requests"`
	SuccessfulLockResponses int64 `json:"successful_lock_responses"`
	DroppedAcknowledgements int64 `json:"dropped_acknowledgements"`
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o600)
}

func main() {
	listenHost := flag.String("listen-host", "127.0.0.1", "local bind address")
	upstreamAddress := flag.String("upstream", "", "plain-text etcd gRPC host:port")
	readyFile := flag.String("ready-file", "", "write the selected listen port here")
	resultFile := flag.String("result-file", "", "write call counters here after injection")
	stopFile := flag.String("stop-file", "", "stop cleanly when this file appears")
	grace := flag.Duration("retry-observation", 400*time.Millisecond, "time to observe transparent Lock retries after injection")
	flag.Parse()
	if *upstreamAddress == "" || *readyFile == "" || *resultFile == "" || *stopFile == "" {
		fmt.Fprintln(os.Stderr, "upstream, ready-file, result-file and stop-file are required")
		os.Exit(2)
	}

	listener, err := net.Listen("tcp", net.JoinHostPort(*listenHost, "0"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	upstream, err := grpc.NewClient(*upstreamAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{})))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	state := &proxyState{injected: make(chan struct{})}
	state.armed.Store(true)
	server := grpc.NewServer(
		grpc.UnknownServiceHandler((&genericProxy{upstream: upstream, state: state}).forward),
		grpc.ForceServerCodec(rawCodec{}),
	)
	if err := writeJSON(*readyFile, map[string]int{"port": listener.Addr().(*net.TCPAddr).Port}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()

	stopTicker := time.NewTicker(25 * time.Millisecond)
	defer stopTicker.Stop()
	for {
		select {
		case <-state.injected:
			time.Sleep(*grace)
			server.Stop()
			<-serveDone
			_ = upstream.Close()
			if err := writeJSON(*resultFile, result{
				LockRequests:            state.lockRequests.Load(),
				SuccessfulLockResponses: state.successfulLockResponses.Load(),
				DroppedAcknowledgements: state.droppedLockAcknowledges.Load(),
			}); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			return
		case err := <-serveDone:
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			return
		case <-stopTicker.C:
			if _, err := os.Stat(*stopFile); err == nil {
				server.Stop()
				<-serveDone
				_ = upstream.Close()
				_ = writeJSON(*resultFile, result{
					LockRequests:            state.lockRequests.Load(),
					SuccessfulLockResponses: state.successfulLockResponses.Load(),
					DroppedAcknowledgements: state.droppedLockAcknowledges.Load(),
				})
				return
			}
		}
	}
}
