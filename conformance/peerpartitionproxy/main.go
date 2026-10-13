// peerpartitionproxy is a local-only TCP peer-link fault injector with a
// synchronous control channel. It is separate from the client-path proxy so
// existing client-partition behavior stays unchanged.
package main

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
	identityMember       string
	knownPeers           map[string]struct{}
	blockedPeers         map[string]struct{}
	peerConnections      map[string]map[net.Conn]struct{}
	blockedByPeer        map[string]int64
	allowedByPeer        map[string]int64
	peerEvents           []peerEvent
	peerEventSequence    uint64
}

type snapshot struct {
	Partitioned          bool             `json:"partitioned"`
	AcceptedConnections  int64            `json:"accepted_connections"`
	BlockedConnections   int64            `json:"blocked_connections"`
	ActiveConnections    int              `json:"active_connections"`
	PartitionTransitions int64            `json:"partition_transitions"`
	IdentityMember       string           `json:"identity_member,omitempty"`
	AllowedPeers         []string         `json:"allowed_peers"`
	BlockedByPeer        map[string]int64 `json:"blocked_by_peer,omitempty"`
	AllowedByPeer        map[string]int64 `json:"allowed_by_peer,omitempty"`
	ActiveByPeer         map[string]int   `json:"active_by_peer,omitempty"`
	PeerEvents           []peerEvent      `json:"peer_events,omitempty"`
}

type peerEvent struct {
	Sequence              uint64 `json:"sequence"`
	Destination           string `json:"destination"`
	Identity              string `json:"identity,omitempty"`
	CertificateCommonName string `json:"certificate_common_name,omitempty"`
	CertificateSHA256     string `json:"certificate_sha256,omitempty"`
	IdentityVerified      bool   `json:"identity_verified"`
	Decision              string `json:"decision"`
}

const maxControlPeerEvents = 64

type identityTLSConfig struct {
	memberID             string
	ca                   *x509.CertPool
	certDir              string
	peerCertFingerprints map[string][32]byte
}

type controlRequest struct {
	RequestID string `json:"request_id"`
	Command   string `json:"command"`
	PeerID    string `json:"peer_id,omitempty"`
}

type controlResponse struct {
	RequestID string   `json:"request_id"`
	Status    string   `json:"status"`
	Error     string   `json:"error,omitempty"`
	State     snapshot `json:"state"`
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

func (s *proxyState) setPeerBlocked(peer string, blocked bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.knownPeers[peer]; !ok {
		return fmt.Errorf("unknown peer identity %q", peer)
	}
	if blocked {
		s.blockedPeers[peer] = struct{}{}
		for conn := range s.peerConnections[peer] {
			_ = conn.Close()
		}
	} else {
		delete(s.blockedPeers, peer)
	}
	return nil
}

func (s *proxyState) view() snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	view := snapshot{
		Partitioned:          s.partitioned,
		AcceptedConnections:  s.accepted.Load(),
		BlockedConnections:   s.blocked.Load(),
		ActiveConnections:    len(s.connections) / 2,
		PartitionTransitions: s.partitionTransitions.Load(),
	}
	if s.identityMember != "" {
		view.IdentityMember = s.identityMember
		// The empty set is meaningful to the partition controller: it means
		// this destination currently accepts no peer identities. Keep it as an
		// explicit JSON array instead of omitting the field or encoding null.
		view.AllowedPeers = make([]string, 0, len(s.knownPeers))
		view.BlockedByPeer = make(map[string]int64, len(s.blockedByPeer))
		view.AllowedByPeer = make(map[string]int64, len(s.allowedByPeer))
		view.ActiveByPeer = make(map[string]int, len(s.peerConnections))
		for peer := range s.knownPeers {
			if _, blocked := s.blockedPeers[peer]; !blocked {
				view.AllowedPeers = append(view.AllowedPeers, peer)
			}
			view.BlockedByPeer[peer] = s.blockedByPeer[peer]
			view.AllowedByPeer[peer] = s.allowedByPeer[peer]
			view.ActiveByPeer[peer] = len(s.peerConnections[peer]) / 2
		}
		view.PeerEvents = append([]peerEvent(nil), s.peerEvents...)
		sort.Strings(view.AllowedPeers)
	}
	return view
}

func (s *proxyState) recordPeerEvent(identity, commonName, fingerprint string, verified bool, decision string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.peerEventSequence++
	s.peerEvents = append(s.peerEvents, peerEvent{
		Sequence:              s.peerEventSequence,
		Destination:           s.identityMember,
		Identity:              identity,
		CertificateCommonName: commonName,
		CertificateSHA256:     fingerprint,
		IdentityVerified:      verified,
		Decision:              decision,
	})
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+"-*.tmp")
	if err != nil {
		return err
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}

	// Windows readers can briefly hold the previous snapshot without delete
	// sharing. Keep replacement atomic and retry that transient sharing window
	// instead of terminating the peer proxy during a topology check.
	var replaceErr error
	for attempt := 0; attempt < 20; attempt++ {
		replaceErr = os.Rename(temporary, path)
		if replaceErr == nil {
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return replaceErr
}

func serveControl(listener net.Listener, state *proxyState) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go handleControlConn(conn, state)
	}
}

func handleControlConn(conn net.Conn, state *proxyState) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	decoder := json.NewDecoder(io.LimitReader(conn, 4096))
	encoder := json.NewEncoder(conn)
	var request controlRequest
	if err := decoder.Decode(&request); err != nil {
		_ = encoder.Encode(controlResponse{Status: "error", Error: "invalid request", State: state.view()})
		return
	}
	response := controlResponse{RequestID: request.RequestID, Status: "ok"}
	switch request.Command {
	case "status":
	case "partition":
		state.setPartitioned(true)
	case "heal":
		state.setPartitioned(false)
	case "block-peer", "allow-peer":
		if request.PeerID == "" {
			response.Status = "error"
			response.Error = "peer_id is required"
			break
		}
		if err := state.setPeerBlocked(request.PeerID, request.Command == "block-peer"); err != nil {
			response.Status = "error"
			response.Error = err.Error()
		}
	default:
		response.Status = "error"
		response.Error = "unsupported command"
	}
	response.State = state.view()
	// Return a bounded tail of authenticated peer events with the counters.
	// The controller needs these events to verify each directed identity edge;
	// reading the periodically replaced snapshot file races with Windows file
	// replacement and can fail while the control channel itself is healthy.
	if len(response.State.PeerEvents) > maxControlPeerEvents {
		response.State.PeerEvents = response.State.PeerEvents[len(response.State.PeerEvents)-maxControlPeerEvents:]
	}
	if err := encoder.Encode(response); err != nil {
		return
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

func validPeerID(id string) bool {
	if id == "" || len(id) > 64 {
		return false
	}
	for _, r := range id {
		if !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') &&
			!(r >= '0' && r <= '9') && r != '.' && r != '_' && r != '-' {
			return false
		}
	}
	return true
}

func loadIdentityCertificate(certDir, id string) (tls.Certificate, error) {
	if !validPeerID(id) {
		return tls.Certificate{}, fmt.Errorf("invalid peer identity %q", id)
	}
	return tls.LoadX509KeyPair(
		filepath.Join(certDir, id+".crt"),
		filepath.Join(certDir, id+".key"),
	)
}

func loadIdentityTLSConfig(memberID, certDir, caPath string, peers []string) (*identityTLSConfig, error) {
	if !validPeerID(memberID) {
		return nil, fmt.Errorf("invalid proxy member identity %q", memberID)
	}
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read peer CA: %w", err)
	}
	ca := x509.NewCertPool()
	if !ca.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("peer CA file contains no certificates")
	}
	destinationCert, err := loadIdentityCertificate(certDir, memberID)
	if err != nil {
		return nil, fmt.Errorf("load destination identity certificate: %w", err)
	}
	if len(destinationCert.Certificate) == 0 {
		return nil, fmt.Errorf("destination identity %q has an empty certificate", memberID)
	}
	destinationLeaf, err := x509.ParseCertificate(destinationCert.Certificate[0])
	if err != nil || destinationLeaf.Subject.CommonName != memberID {
		return nil, fmt.Errorf("destination certificate common name does not match %q", memberID)
	}
	if err := verifyPeerIdentityCertificate(destinationLeaf, ca, memberID); err != nil {
		return nil, fmt.Errorf("verify destination identity certificate: %w", err)
	}
	seen := make(map[string]struct{}, len(peers))
	fingerprints := make(map[string][32]byte, len(peers))
	for _, peer := range peers {
		if !validPeerID(peer) {
			return nil, fmt.Errorf("invalid peer identity %q", peer)
		}
		if peer == memberID {
			return nil, fmt.Errorf("source peer identity list must not contain destination %q", memberID)
		}
		if _, duplicate := seen[peer]; duplicate {
			return nil, fmt.Errorf("duplicate peer identity %q", peer)
		}
		peerCert, err := loadIdentityCertificate(certDir, peer)
		if err != nil {
			return nil, fmt.Errorf("load source identity certificate %q: %w", peer, err)
		}
		peerLeaf, err := x509.ParseCertificate(peerCert.Certificate[0])
		if err != nil || peerLeaf.Subject.CommonName != peer {
			return nil, fmt.Errorf("peer certificate common name does not match %q", peer)
		}
		if err := verifyPeerIdentityCertificate(peerLeaf, ca, peer); err != nil {
			return nil, fmt.Errorf("verify peer identity certificate %q: %w", peer, err)
		}
		fingerprints[peer] = sha256.Sum256(peerLeaf.Raw)
		seen[peer] = struct{}{}
	}
	return &identityTLSConfig{memberID: memberID, ca: ca, certDir: certDir, peerCertFingerprints: fingerprints}, nil
}

func verifyPeerIdentityCertificate(cert *x509.Certificate, ca *x509.CertPool, id string) error {
	if err := cert.VerifyHostname(id); err != nil {
		return fmt.Errorf("certificate SAN does not cover identity %q: %w", id, err)
	}
	if _, err := cert.Verify(x509.VerifyOptions{
		Roots:     ca,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}); err != nil {
		return fmt.Errorf("certificate is not trusted for peer TLS: %w", err)
	}
	return nil
}

func (c *identityTLSConfig) serverConfig() (*tls.Config, error) {
	certificate, err := loadIdentityCertificate(c.certDir, c.memberID)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    c.ca,
		NextProtos:   []string{"h2"},
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("peer certificate is required")
			}
			leaf, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return fmt.Errorf("parse peer identity certificate: %w", err)
			}
			fingerprint, known := c.peerCertFingerprints[leaf.Subject.CommonName]
			if !known || sha256.Sum256(leaf.Raw) != fingerprint {
				return fmt.Errorf("peer certificate is not pinned for identity %q", leaf.Subject.CommonName)
			}
			return nil
		},
	}, nil
}

func (c *identityTLSConfig) clientConfig(sourceID string) (*tls.Config, error) {
	certificate, err := loadIdentityCertificate(c.certDir, sourceID)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
		RootCAs:      c.ca,
		ServerName:   c.memberID,
		NextProtos:   []string{"h2"},
	}, nil
}

func (s *proxyState) reservePeerConnection(peer string, conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.partitioned {
		s.blocked.Add(1)
		s.blockedByPeer[peer]++
		return false
	}
	if _, blocked := s.blockedPeers[peer]; blocked {
		s.blocked.Add(1)
		s.blockedByPeer[peer]++
		return false
	}
	if s.peerConnections[peer] == nil {
		s.peerConnections[peer] = make(map[net.Conn]struct{})
	}
	s.peerConnections[peer][conn] = struct{}{}
	s.connections[conn] = struct{}{}
	return true
}

func (s *proxyState) addPeerUpstream(peer string, client, upstream net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.partitioned {
		s.blocked.Add(1)
		s.blockedByPeer[peer]++
		return false
	}
	if _, blocked := s.blockedPeers[peer]; blocked {
		s.blocked.Add(1)
		s.blockedByPeer[peer]++
		return false
	}
	if _, reserved := s.peerConnections[peer][client]; !reserved {
		return false
	}
	s.peerConnections[peer][upstream] = struct{}{}
	s.connections[upstream] = struct{}{}
	s.allowedByPeer[peer]++
	return true
}

func (s *proxyState) removePeerConnection(peer string, conns ...net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, conn := range conns {
		delete(s.connections, conn)
		delete(s.peerConnections[peer], conn)
	}
	if len(s.peerConnections[peer]) == 0 {
		delete(s.peerConnections, peer)
	}
}

func certificateFingerprint(cert *x509.Certificate) string {
	if cert == nil {
		return ""
	}
	fingerprint := sha256.Sum256(cert.Raw)
	return hex.EncodeToString(fingerprint[:])
}

func handleIdentityTLS(rawClient net.Conn, upstreamAddress string, state *proxyState, identity *identityTLSConfig) {
	serverConfig, err := identity.serverConfig()
	if err != nil {
		_ = rawClient.Close()
		return
	}
	client := tls.Server(rawClient, serverConfig)
	_ = client.SetDeadline(time.Now().Add(5 * time.Second))
	if err := client.Handshake(); err != nil {
		state.blocked.Add(1)
		peerCerts := client.ConnectionState().PeerCertificates
		if len(peerCerts) > 0 {
			state.recordPeerEvent("", peerCerts[0].Subject.CommonName, certificateFingerprint(peerCerts[0]), false, "tls_rejected")
		} else {
			state.recordPeerEvent("", "", "", false, "tls_rejected")
		}
		_ = client.Close()
		return
	}
	peerID := ""
	if peerCerts := client.ConnectionState().PeerCertificates; len(peerCerts) > 0 {
		peerID = peerCerts[0].Subject.CommonName
	}
	if !validPeerID(peerID) {
		state.blocked.Add(1)
		var leaf *x509.Certificate
		if peerCerts := client.ConnectionState().PeerCertificates; len(peerCerts) > 0 {
			leaf = peerCerts[0]
		}
		state.recordPeerEvent("", peerID, certificateFingerprint(leaf), false, "invalid_peer_identity")
		_ = client.Close()
		return
	}
	state.mu.Lock()
	_, known := state.knownPeers[peerID]
	state.mu.Unlock()
	if !known {
		state.blocked.Add(1)
		state.recordPeerEvent("", peerID, certificateFingerprint(client.ConnectionState().PeerCertificates[0]), false, "unknown_acl_identity")
		_ = client.Close()
		return
	}
	if !state.reservePeerConnection(peerID, client) {
		state.recordPeerEvent(peerID, peerID, certificateFingerprint(client.ConnectionState().PeerCertificates[0]), true, "blocked")
		_ = client.Close()
		return
	}
	defer func() {
		_ = client.Close()
		state.removePeerConnection(peerID, client)
	}()
	_ = client.SetDeadline(time.Time{})

	clientConfig, err := identity.clientConfig(peerID)
	if err != nil {
		state.recordPeerEvent(peerID, peerID, certificateFingerprint(client.ConnectionState().PeerCertificates[0]), true, "upstream_tls_failed")
		return
	}
	dialer := &net.Dialer{Timeout: 3 * time.Second}
	upstream, err := tls.DialWithDialer(dialer, "tcp", upstreamAddress, clientConfig)
	if err != nil {
		state.recordPeerEvent(peerID, peerID, certificateFingerprint(client.ConnectionState().PeerCertificates[0]), true, "upstream_tls_failed")
		return
	}
	defer upstream.Close()
	if !state.addPeerUpstream(peerID, client, upstream) {
		state.recordPeerEvent(peerID, peerID, certificateFingerprint(client.ConnectionState().PeerCertificates[0]), true, "blocked_during_connect")
		return
	}
	state.recordPeerEvent(peerID, peerID, certificateFingerprint(client.ConnectionState().PeerCertificates[0]), true, "forwarded")
	defer state.removePeerConnection(peerID, upstream)

	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(upstream, client); done <- struct{}{} }()
	go func() { _, _ = io.Copy(client, upstream); done <- struct{}{} }()
	<-done
	_ = client.Close()
	_ = upstream.Close()
	<-done
}

func run() error {
	listenHost := flag.String("listen-host", "127.0.0.1", "peer TCP bind address")
	controlHost := flag.String("control-host", "127.0.0.1", "local control bind address")
	upstreamAddress := flag.String("upstream", "", "etcd peer TCP endpoint")
	readyFile := flag.String("ready-file", "", "write selected peer and control ports")
	stateFile := flag.String("state-file", "", "write atomic proxy state snapshots")
	resultFile := flag.String("result-file", "", "write final proxy counters")
	stopFile := flag.String("stop-file", "", "stop when this file appears")
	identityMember := flag.String("identity-member-id", "", "enable mTLS peer filtering as this destination member")
	identityPeers := flag.String("identity-peer-ids", "", "comma-separated trusted peer identities")
	identityCA := flag.String("identity-ca-file", "", "CA certificate that signs peer identities")
	identityCertDir := flag.String("identity-cert-dir", "", "directory containing <peer-id>.crt and <peer-id>.key")
	generateIdentityCerts := flag.Bool("generate-identity-certificates", false, "generate one-run CA and peer certificates, then exit")
	flag.Parse()
	if *generateIdentityCerts {
		if *identityCertDir == "" || *identityPeers == "" || *identityMember != "" || *identityCA != "" {
			return errors.New("certificate generation requires identity-cert-dir and identity-peer-ids only")
		}
		return generateIdentityCertificates(*identityCertDir, strings.Split(*identityPeers, ","))
	}
	if *upstreamAddress == "" || *readyFile == "" || *stateFile == "" || *resultFile == "" || *stopFile == "" {
		return errors.New("upstream, ready-file, state-file, result-file and stop-file are required")
	}
	peerListener, err := net.Listen("tcp", net.JoinHostPort(*listenHost, "0"))
	if err != nil {
		return fmt.Errorf("listen peer: %w", err)
	}
	defer peerListener.Close()
	controlListener, err := net.Listen("tcp", net.JoinHostPort(*controlHost, "0"))
	if err != nil {
		return fmt.Errorf("listen control: %w", err)
	}
	defer controlListener.Close()
	identityRequested := *identityMember != "" || *identityPeers != "" || *identityCA != "" || *identityCertDir != ""
	var identity *identityTLSConfig
	state := &proxyState{connections: make(map[net.Conn]struct{})}
	if identityRequested {
		if *identityMember == "" || *identityPeers == "" || *identityCA == "" || *identityCertDir == "" {
			return errors.New("identity TLS requires identity-member-id, identity-peer-ids, identity-ca-file and identity-cert-dir")
		}
		peers := strings.Split(*identityPeers, ",")
		identity, err = loadIdentityTLSConfig(*identityMember, *identityCertDir, *identityCA, peers)
		if err != nil {
			return fmt.Errorf("configure identity TLS: %w", err)
		}
		state.identityMember = *identityMember
		state.knownPeers = make(map[string]struct{}, len(peers))
		state.blockedPeers = make(map[string]struct{})
		state.peerConnections = make(map[string]map[net.Conn]struct{})
		state.blockedByPeer = make(map[string]int64)
		state.allowedByPeer = make(map[string]int64)
		for _, peer := range peers {
			state.knownPeers[peer] = struct{}{}
		}
	}
	if err := writeJSON(*readyFile, map[string]int{
		"port":         peerListener.Addr().(*net.TCPAddr).Port,
		"control_port": controlListener.Addr().(*net.TCPAddr).Port,
	}); err != nil {
		return fmt.Errorf("write readiness: %w", err)
	}
	if err := writeJSON(*stateFile, state.view()); err != nil {
		return fmt.Errorf("write initial state: %w", err)
	}
	go serveControl(controlListener, state)
	var accept sync.WaitGroup
	accept.Add(1)
	go func() {
		defer accept.Done()
		for {
			client, acceptErr := peerListener.Accept()
			if acceptErr != nil {
				return
			}
			state.accepted.Add(1)
			state.handlers.Add(1)
			go func() {
				defer state.handlers.Done()
				if identity != nil {
					handleIdentityTLS(client, *upstreamAddress, state, identity)
				} else {
					handle(client, *upstreamAddress, state)
				}
			}()
		}
	}()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		if err := writeJSON(*stateFile, state.view()); err != nil {
			return fmt.Errorf("write state: %w", err)
		}
		if _, err := os.Stat(*stopFile); err == nil {
			_ = peerListener.Close()
			_ = controlListener.Close()
			accept.Wait()
			state.setPartitioned(true)
			state.handlers.Wait()
			return writeJSON(*resultFile, state.view())
		}
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
