// tcp_partition_proxy is a local test-only TCP link fault injector.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type proxyState struct {
	mu                   sync.Mutex
	connections          map[net.Conn]struct{}
	partitioned          bool
	handlers             sync.WaitGroup
	accepted             atomic.Int64
	blocked              atomic.Int64
	partitionTransitions atomic.Int64
}

type snapshot struct {
	Partitioned          bool  `json:"partitioned"`
	AcceptedConnections  int64 `json:"accepted_connections"`
	BlockedConnections   int64 `json:"blocked_connections"`
	ActiveConnections    int   `json:"active_connections"`
	PartitionTransitions int64 `json:"partition_transitions"`
}

func (s *proxyState) setPartitioned(value bool) {
	s.mu.Lock()
	if s.partitioned == value {
		s.mu.Unlock()
		return
	}
	s.partitioned = value
	s.partitionTransitions.Add(1)
	connections := make([]net.Conn, 0, len(s.connections))
	if value {
		for conn := range s.connections {
			connections = append(connections, conn)
		}
	}
	s.mu.Unlock()
	for _, conn := range connections {
		_ = conn.Close()
	}
}

func (s *proxyState) view() snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return snapshot{
		Partitioned:          s.partitioned,
		AcceptedConnections:  s.accepted.Load(),
		BlockedConnections:   s.blocked.Load(),
		ActiveConnections:    len(s.connections) / 2,
		PartitionTransitions: s.partitionTransitions.Load(),
	}
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, append(data, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func main() {
	listenHost := flag.String("listen-host", "127.0.0.1", "local bind address")
	upstreamAddress := flag.String("upstream", "", "etcd TCP endpoint")
	controlFile := flag.String("partition-file", "", "presence closes flows and rejects new connections")
	readyFile := flag.String("ready-file", "", "write the selected local port")
	stateFile := flag.String("state-file", "", "write atomic proxy state snapshots")
	resultFile := flag.String("result-file", "", "write final proxy counters")
	stopFile := flag.String("stop-file", "", "stop when this file appears")
	flag.Parse()
	if *upstreamAddress == "" || *controlFile == "" || *readyFile == "" || *stateFile == "" || *resultFile == "" || *stopFile == "" {
		fmt.Fprintln(os.Stderr, "upstream, partition-file, ready-file, state-file, result-file and stop-file are required")
		os.Exit(2)
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(*listenHost, "0"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "listen:", err)
		os.Exit(1)
	}
	state := &proxyState{connections: make(map[net.Conn]struct{})}
	if err := writeJSON(*readyFile, map[string]int{"port": listener.Addr().(*net.TCPAddr).Port}); err != nil {
		fmt.Fprintln(os.Stderr, "write readiness:", err)
		os.Exit(1)
	}
	if err := writeJSON(*stateFile, state.view()); err != nil {
		fmt.Fprintln(os.Stderr, "write initial state:", err)
		os.Exit(1)
	}
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			client, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			state.accepted.Add(1)
			state.handlers.Add(1)
			go func() {
				defer state.handlers.Done()
				handle(client, *upstreamAddress, state)
			}()
		}
	}()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_, statErr := os.Stat(*controlFile)
			state.setPartitioned(statErr == nil)
			if err := writeJSON(*stateFile, state.view()); err != nil {
				os.Exit(1)
			}
			if _, stopErr := os.Stat(*stopFile); stopErr == nil {
				_ = listener.Close()
				state.setPartitioned(true)
				<-acceptDone
				state.handlers.Wait()
				result := state.view()
				_ = writeJSON(*resultFile, result)
				return
			}
		}
	}
}

func handle(client net.Conn, upstreamAddress string, state *proxyState) {
	state.mu.Lock()
	if state.partitioned {
		state.blocked.Add(1)
		state.mu.Unlock()
		_ = client.Close()
		return
	}
	state.mu.Unlock()

	upstream, err := net.DialTimeout("tcp", upstreamAddress, 2*time.Second)
	if err != nil {
		_ = client.Close()
		return
	}
	state.mu.Lock()
	if state.partitioned {
		state.blocked.Add(1)
		state.mu.Unlock()
		_ = client.Close()
		_ = upstream.Close()
		return
	}
	state.connections[client] = struct{}{}
	state.connections[upstream] = struct{}{}
	state.mu.Unlock()

	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(upstream, client); done <- struct{}{} }()
	go func() { _, _ = io.Copy(client, upstream); done <- struct{}{} }()
	<-done
	_ = client.Close()
	_ = upstream.Close()
	state.mu.Lock()
	delete(state.connections, client)
	delete(state.connections, upstream)
	state.mu.Unlock()
	<-done
}
