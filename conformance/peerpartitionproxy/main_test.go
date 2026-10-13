package main

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func controlRequestForTest(t *testing.T, state *proxyState, command string) controlResponse {
	return controlRequestWithPeerForTest(t, state, command, "")
}

func controlRequestWithPeerForTest(t *testing.T, state *proxyState, command, peerID string) controlResponse {
	t.Helper()
	server, client := net.Pipe()
	done := make(chan struct{})
	go func() {
		handleControlConn(server, state)
		close(done)
	}()
	if err := json.NewEncoder(client).Encode(controlRequest{RequestID: "test-1", Command: command, PeerID: peerID}); err != nil {
		t.Fatal(err)
	}
	var response controlResponse
	if err := json.NewDecoder(bufio.NewReader(client)).Decode(&response); err != nil {
		t.Fatal(err)
	}
	_ = client.Close()
	<-done
	return response
}

func TestControlAppliesDirectedPeerACL(t *testing.T) {
	state := &proxyState{
		connections:     make(map[net.Conn]struct{}),
		identityMember:  "destination",
		knownPeers:      map[string]struct{}{"majority-a": {}, "minority": {}},
		blockedPeers:    make(map[string]struct{}),
		peerConnections: make(map[string]map[net.Conn]struct{}),
		blockedByPeer:   make(map[string]int64),
		allowedByPeer:   make(map[string]int64),
	}
	blocked := controlRequestWithPeerForTest(t, state, "block-peer", "minority")
	if blocked.Status != "ok" || strings.Join(blocked.State.AllowedPeers, ",") != "majority-a" {
		t.Fatalf("directed block was not acknowledged: %+v", blocked)
	}
	unknown := controlRequestWithPeerForTest(t, state, "block-peer", "unknown")
	if unknown.Status != "error" {
		t.Fatalf("unknown peer identity did not fail closed: %+v", unknown)
	}
	healed := controlRequestWithPeerForTest(t, state, "allow-peer", "minority")
	if healed.Status != "ok" || strings.Join(healed.State.AllowedPeers, ",") != "majority-a,minority" {
		t.Fatalf("directed heal was not acknowledged: %+v", healed)
	}
}

func TestWriteJSONReplacesRepeatedSnapshots(t *testing.T) {
	path := filepath.Join(t.TempDir(), "proxy-state.json")
	for sequence := 0; sequence < 25; sequence++ {
		want := map[string]int{"sequence": sequence}
		if err := writeJSON(path, want); err != nil {
			t.Fatalf("write snapshot %d: %v", sequence, err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read snapshot %d: %v", sequence, err)
		}
		var got map[string]int
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatalf("snapshot %d was not valid JSON: %v", sequence, err)
		}
		if got["sequence"] != sequence {
			t.Fatalf("snapshot sequence = %d, want %d", got["sequence"], sequence)
		}
	}
}

func TestIdentitySnapshotSerializesEmptyAllowedPeerSetAsArray(t *testing.T) {
	state := &proxyState{
		connections:     make(map[net.Conn]struct{}),
		identityMember:  "minority",
		knownPeers:      map[string]struct{}{"majority-a": {}, "majority-b": {}},
		blockedPeers:    map[string]struct{}{"majority-a": {}, "majority-b": {}},
		peerConnections: make(map[string]map[net.Conn]struct{}),
		blockedByPeer:   make(map[string]int64),
		allowedByPeer:   make(map[string]int64),
	}
	encoded, err := json.Marshal(state.view())
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	var allowed []string
	if raw, ok := fields["allowed_peers"]; !ok || string(raw) != "[]" {
		t.Fatalf("empty allowed peer set must be an explicit JSON array, got %s", raw)
	}
	if err := json.Unmarshal(fields["allowed_peers"], &allowed); err != nil || allowed == nil || len(allowed) != 0 {
		t.Fatalf("empty allowed peer set decoded unexpectedly: %#v, %v", allowed, err)
	}
}

func TestControlReplyIncludesBoundedPeerEventTail(t *testing.T) {
	events := make([]peerEvent, maxControlPeerEvents+5)
	for index := range events {
		events[index] = peerEvent{
			Sequence:    uint64(index + 1),
			Identity:    "source",
			Destination: "destination",
			Decision:    "forwarded",
		}
	}
	state := &proxyState{
		connections:    make(map[net.Conn]struct{}),
		identityMember: "destination",
		knownPeers:     map[string]struct{}{"source": {}},
		blockedPeers:   make(map[string]struct{}),
		peerEvents:     events,
	}
	response := controlRequestForTest(t, state, "status")
	if response.Status != "ok" || len(response.State.PeerEvents) != maxControlPeerEvents {
		t.Fatalf("control reply should include a bounded event tail: got %d events", len(response.State.PeerEvents))
	}
	if got, want := response.State.PeerEvents[0].Sequence, uint64(6); got != want {
		t.Fatalf("control event tail starts at sequence %d, want %d", got, want)
	}
	if got, want := response.State.PeerEvents[len(response.State.PeerEvents)-1].Sequence, uint64(len(events)); got != want {
		t.Fatalf("control event tail ends at sequence %d, want %d", got, want)
	}
	if len(state.view().PeerEvents) != len(events) {
		t.Fatal("bounded control reply mutated the persisted proxy state")
	}
}

func TestSynchronousControlAcknowledgesTransitions(t *testing.T) {
	state := &proxyState{connections: make(map[net.Conn]struct{})}
	partitioned := controlRequestForTest(t, state, "partition")
	if partitioned.Status != "ok" || partitioned.RequestID != "test-1" {
		t.Fatalf("partition was not acknowledged: %+v", partitioned)
	}
	if !partitioned.State.Partitioned || partitioned.State.PartitionTransitions != 1 {
		t.Fatalf("partition state was not applied: %+v", partitioned.State)
	}
	// Idempotent commands acknowledge the state without inventing transitions.
	partitioned = controlRequestForTest(t, state, "partition")
	if partitioned.State.PartitionTransitions != 1 {
		t.Fatalf("idempotent partition changed transition count: %+v", partitioned.State)
	}
	healed := controlRequestForTest(t, state, "heal")
	if healed.Status != "ok" || healed.State.Partitioned || healed.State.PartitionTransitions != 2 {
		t.Fatalf("heal state was not acknowledged: %+v", healed)
	}
}

func TestPartitionControlClosesExistingPeerStreams(t *testing.T) {
	state := &proxyState{connections: make(map[net.Conn]struct{})}
	left, right := net.Pipe()
	state.connections[left] = struct{}{}
	state.connections[right] = struct{}{}
	response := controlRequestForTest(t, state, "partition")
	if response.Status != "ok" || !response.State.Partitioned {
		t.Fatalf("partition was not acknowledged: %+v", response)
	}
	if _, err := right.Write([]byte("probe")); err == nil {
		t.Fatal("partition left the existing peer stream open")
	}
	_ = left.Close()
	_ = right.Close()
}

func TestControlRejectsUnknownCommand(t *testing.T) {
	state := &proxyState{connections: make(map[net.Conn]struct{})}
	response := controlRequestForTest(t, state, "partition-ish")
	if response.Status != "error" || response.Error == "" || response.State.Partitioned {
		t.Fatalf("unknown command did not fail closed: %+v", response)
	}
}

type testIdentityAuthority struct {
	ca   *x509.Certificate
	key  *ecdsa.PrivateKey
	cert []byte
}

func testCertificateSerial(t *testing.T) *big.Int {
	t.Helper()
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatal(err)
	}
	return serial.Add(serial, big.NewInt(1))
}

func newTestIdentityAuthority(t *testing.T) *testIdentityAuthority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          testCertificateSerial(t),
		Subject:               pkix.Name{CommonName: "Dilock test peer CA"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &testIdentityAuthority{ca: ca, key: key, cert: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func (a *testIdentityAuthority) writeIdentity(t *testing.T, dir, id string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: testCertificateSerial(t),
		Subject:      pkix.Name{CommonName: id},
		DNSNames:     []string{id},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, a.ca, &key.PublicKey, a.key)
	if err != nil {
		t.Fatal(err)
	}
	privateKey, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, id+".crt"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, id+".key"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateKey}), 0o600); err != nil {
		t.Fatal(err)
	}
}

func startIdentityProxyTest(t *testing.T) (string, *proxyState, *identityTLSConfig, *testIdentityAuthority, <-chan string, func()) {
	t.Helper()
	dir := t.TempDir()
	authority := newTestIdentityAuthority(t)
	if err := os.WriteFile(filepath.Join(dir, "ca.crt"), authority.cert, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"majority-a", "minority", "destination"} {
		authority.writeIdentity(t, dir, id)
	}
	ca := x509.NewCertPool()
	if !ca.AppendCertsFromPEM(authority.cert) {
		t.Fatal("could not load test CA")
	}
	destinationPair, err := loadIdentityCertificate(dir, "destination")
	if err != nil {
		t.Fatal(err)
	}
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	backendTLS := tls.NewListener(backend, &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{destinationPair},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    ca,
		NextProtos:   []string{"h2"},
	})
	seen := make(chan string, 8)
	go func() {
		for {
			conn, acceptErr := backendTLS.Accept()
			if acceptErr != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				tlsConn, ok := conn.(*tls.Conn)
				if !ok || tlsConn.Handshake() != nil {
					return
				}
				state := tlsConn.ConnectionState()
				if len(state.PeerCertificates) == 0 {
					return
				}
				seen <- state.PeerCertificates[0].Subject.CommonName
				buf := make([]byte, 32)
				n, readErr := tlsConn.Read(buf)
				if readErr == nil {
					_, _ = tlsConn.Write(buf[:n])
				}
			}(conn)
		}
	}()
	proxyListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = backendTLS.Close()
		t.Fatal(err)
	}
	identity, err := loadIdentityTLSConfig(
		"destination", dir, filepath.Join(dir, "ca.crt"),
		[]string{"majority-a", "minority"},
	)
	if err != nil {
		_ = proxyListener.Close()
		_ = backendTLS.Close()
		t.Fatal(err)
	}
	state := &proxyState{
		connections:     make(map[net.Conn]struct{}),
		identityMember:  "destination",
		knownPeers:      map[string]struct{}{"majority-a": {}, "minority": {}},
		blockedPeers:    make(map[string]struct{}),
		peerConnections: make(map[string]map[net.Conn]struct{}),
		blockedByPeer:   make(map[string]int64),
		allowedByPeer:   make(map[string]int64),
	}
	go func() {
		for {
			conn, acceptErr := proxyListener.Accept()
			if acceptErr != nil {
				return
			}
			state.accepted.Add(1)
			go handleIdentityTLS(conn, backend.Addr().String(), state, identity)
		}
	}()
	cleanup := func() {
		_ = proxyListener.Close()
		_ = backendTLS.Close()
	}
	return proxyListener.Addr().String(), state, identity, authority, seen, cleanup
}

func identityTLSExchange(proxyAddress, sourceID string, identity *identityTLSConfig) error {
	clientConfig, err := identity.clientConfig(sourceID)
	if err != nil {
		return err
	}
	rawConn, err := net.DialTimeout("tcp", proxyAddress, time.Second)
	if err != nil {
		return err
	}
	conn := tls.Client(rawConn, clientConfig)
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	if err := conn.Handshake(); err != nil {
		return fmt.Errorf("front proxy TLS handshake: %w", err)
	}
	if _, err := conn.Write([]byte("identity-probe")); err != nil {
		return err
	}
	response := make([]byte, len("identity-probe"))
	if _, err := conn.Read(response); err != nil {
		return err
	}
	if string(response) != "identity-probe" {
		return fmt.Errorf("unexpected echo %q", response)
	}
	return nil
}

func TestIdentityTLSMajorityMinorityDirectedMatrix(t *testing.T) {
	const majorityA, majorityB, minority = "majority-a", "majority-b", "minority"
	memberIDs := []string{majorityA, majorityB, minority}
	dir := t.TempDir()
	authority := newTestIdentityAuthority(t)
	caPath := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(caPath, authority.cert, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, member := range memberIDs {
		authority.writeIdentity(t, dir, member)
	}
	ca := x509.NewCertPool()
	if !ca.AppendCertsFromPEM(authority.cert) {
		t.Fatal("could not load test CA")
	}

	addresses := make(map[string]string, len(memberIDs))
	identities := make(map[string]*identityTLSConfig, len(memberIDs))
	states := make(map[string]*proxyState, len(memberIDs))
	listeners := make([]net.Listener, 0, 2*len(memberIDs))
	backendObservations := make(chan string, 16)
	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()

	for _, destination := range memberIDs {
		certificate, err := loadIdentityCertificate(dir, destination)
		if err != nil {
			t.Fatal(err)
		}
		backend, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		backendTLS := tls.NewListener(backend, &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{certificate},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    ca,
			NextProtos:   []string{"h2"},
		})
		listeners = append(listeners, backendTLS)
		go func(destination string, listener net.Listener) {
			for {
				conn, acceptErr := listener.Accept()
				if acceptErr != nil {
					return
				}
				go func(conn net.Conn) {
					defer conn.Close()
					tlsConn, ok := conn.(*tls.Conn)
					if !ok || tlsConn.Handshake() != nil {
						return
					}
					peerCerts := tlsConn.ConnectionState().PeerCertificates
					if len(peerCerts) == 0 {
						return
					}
					backendObservations <- destination + "<-" + peerCerts[0].Subject.CommonName
					request := make([]byte, len("identity-probe"))
					if _, readErr := io.ReadFull(tlsConn, request); readErr == nil {
						_, _ = tlsConn.Write(request)
					}
				}(conn)
			}
		}(destination, backendTLS)

		sources := make([]string, 0, len(memberIDs)-1)
		for _, source := range memberIDs {
			if source != destination {
				sources = append(sources, source)
			}
		}
		identity, err := loadIdentityTLSConfig(destination, dir, caPath, sources)
		if err != nil {
			t.Fatal(err)
		}
		identities[destination] = identity
		state := &proxyState{
			connections:     make(map[net.Conn]struct{}),
			identityMember:  destination,
			knownPeers:      make(map[string]struct{}, len(sources)),
			blockedPeers:    make(map[string]struct{}),
			peerConnections: make(map[string]map[net.Conn]struct{}),
			blockedByPeer:   make(map[string]int64),
			allowedByPeer:   make(map[string]int64),
		}
		for _, source := range sources {
			state.knownPeers[source] = struct{}{}
		}
		states[destination] = state
		proxy, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, proxy)
		addresses[destination] = proxy.Addr().String()
		go func(listener net.Listener, state *proxyState, upstream string, identity *identityTLSConfig) {
			for {
				conn, acceptErr := listener.Accept()
				if acceptErr != nil {
					return
				}
				go handleIdentityTLS(conn, upstream, state, identity)
			}
		}(proxy, state, backend.Addr().String(), identity)
	}

	// Retain exactly A<->B; cut A<->M and B<->M at both destination proxies.
	for _, destination := range memberIDs {
		for _, source := range memberIDs {
			if source == destination {
				continue
			}
			allowed := (destination == majorityA && source == majorityB) ||
				(destination == majorityB && source == majorityA)
			if !allowed {
				if err := states[destination].setPeerBlocked(source, true); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	expectedBackend := map[string]bool{majorityB + "<-" + majorityA: true, majorityA + "<-" + majorityB: true}
	for _, source := range memberIDs {
		for _, destination := range memberIDs {
			if source == destination {
				continue // Self-links are invalid and are never counted as connectivity.
			}
			allowed := expectedBackend[destination+"<-"+source]
			err := identityTLSExchange(addresses[destination], source, identities[destination])
			if allowed && err != nil {
				t.Errorf("expected directed edge %s->%s to pass: %v", source, destination, err)
			} else if !allowed && err == nil {
				t.Errorf("cross-partition edge %s->%s unexpectedly passed", source, destination)
			}
		}
	}

	observedBackend := make(map[string]bool)
	for len(observedBackend) < len(expectedBackend) {
		select {
		case edge := <-backendObservations:
			observedBackend[edge] = true
		case <-time.After(time.Second):
			t.Fatalf("backend observed edges %v, want %v", observedBackend, expectedBackend)
		}
	}
	if len(observedBackend) != len(expectedBackend) {
		t.Fatalf("backend observed unexpected edges: %v", observedBackend)
	}

	observedEdges := make(map[string]string, 6)
	for _, destination := range memberIDs {
		view := states[destination].view()
		for _, event := range view.PeerEvents {
			if !event.IdentityVerified || event.Identity == "" || event.CertificateSHA256 == "" {
				t.Errorf("edge event lacks verified certificate identity: %+v", event)
				continue
			}
			edge := event.Identity + "->" + event.Destination
			if _, duplicate := observedEdges[edge]; duplicate {
				t.Errorf("directed edge %s appeared more than once", edge)
			}
			observedEdges[edge] = event.Decision
			certificate, err := loadIdentityCertificate(dir, event.Identity)
			if err != nil {
				t.Fatal(err)
			}
			leaf, err := x509.ParseCertificate(certificate.Certificate[0])
			if err != nil {
				t.Fatal(err)
			}
			if event.CertificateSHA256 != certificateFingerprint(leaf) {
				t.Errorf("edge %s recorded unexpected certificate fingerprint %q", edge, event.CertificateSHA256)
			}
		}
	}
	if len(observedEdges) != 6 {
		t.Fatalf("observed %d valid directed edges, want 6 (self-links excluded): %v", len(observedEdges), observedEdges)
	}
	for edge, decision := range map[string]string{
		majorityA + "->" + majorityB: "forwarded",
		majorityB + "->" + majorityA: "forwarded",
		majorityA + "->" + minority:  "blocked",
		minority + "->" + majorityA:  "blocked",
		majorityB + "->" + minority:  "blocked",
		minority + "->" + majorityB:  "blocked",
	} {
		if observedEdges[edge] != decision {
			t.Errorf("directed edge %s decision = %q, want %q", edge, observedEdges[edge], decision)
		}
	}
}

func TestIdentityTLSDirectedPeerACLBlockAndHeal(t *testing.T) {
	proxyAddress, state, identity, _, seen, cleanup := startIdentityProxyTest(t)
	defer cleanup()
	if err := identityTLSExchange(proxyAddress, "majority-a", identity); err != nil {
		t.Fatalf("majority peer should be allowed: %v", err)
	}
	select {
	case peer := <-seen:
		if peer != "majority-a" {
			t.Fatalf("backend saw source identity %q", peer)
		}
	case <-time.After(time.Second):
		t.Fatal("backend did not observe allowed source identity")
	}

	if err := state.setPeerBlocked("minority", true); err != nil {
		t.Fatal(err)
	}
	if err := identityTLSExchange(proxyAddress, "minority", identity); err == nil {
		t.Fatal("blocked peer unexpectedly reached the destination backend")
	}
	if got := state.view().BlockedByPeer["minority"]; got != 1 {
		t.Fatalf("blocked peer counter = %d, want 1", got)
	}
	select {
	case peer := <-seen:
		t.Fatalf("blocked identity reached backend as %q", peer)
	case <-time.After(50 * time.Millisecond):
	}

	if err := state.setPeerBlocked("minority", false); err != nil {
		t.Fatal(err)
	}
	if err := identityTLSExchange(proxyAddress, "minority", identity); err != nil {
		t.Fatalf("healed peer should be allowed: %v", err)
	}
	select {
	case peer := <-seen:
		if peer != "minority" {
			t.Fatalf("backend saw healed source identity %q", peer)
		}
	case <-time.After(time.Second):
		t.Fatal("backend did not observe healed source identity")
	}
	if strings.Join(state.view().AllowedPeers, ",") != "majority-a,minority" {
		t.Fatalf("unexpected allowed peer set: %v", state.view().AllowedPeers)
	}
	if got := state.view().AllowedByPeer["minority"]; got != 1 {
		t.Fatalf("healed peer backend connection count = %d, want 1", got)
	}
}

func TestIdentityTLSRejectsUnpinnedCertificateWithKnownCommonName(t *testing.T) {
	proxyAddress, state, identity, authority, seen, cleanup := startIdentityProxyTest(t)
	defer cleanup()
	// A CA-signed certificate with a copied common name is not the configured
	// member identity. Pinning prevents the CN alone from authenticating a peer.
	authority.writeIdentity(t, identity.certDir, "minority")
	if err := identityTLSExchange(proxyAddress, "minority", identity); err == nil {
		t.Fatal("same-name replacement certificate unexpectedly passed peer identity pinning")
	}
	deadline := time.Now().Add(time.Second)
	var events []peerEvent
	for time.Now().Before(deadline) {
		events = state.view().PeerEvents
		if len(events) == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if len(events) != 1 || events[0].Decision != "tls_rejected" || events[0].IdentityVerified || events[0].Identity != "" {
		t.Fatalf("un-pinned certificate event was not marked unverified: %+v", events)
	}
	if events[0].CertificateCommonName != "minority" || events[0].CertificateSHA256 == "" || events[0].CertificateSHA256 == fmt.Sprintf("%x", identity.peerCertFingerprints["minority"]) {
		t.Fatalf("rejected event did not record the observed certificate and distinct fingerprint: %+v", events[0])
	}
	select {
	case peer := <-seen:
		t.Fatalf("un-pinned certificate reached backend as %q", peer)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestIdentityTLSConfigurationFailsClosedOnIdentityMismatch(t *testing.T) {
	dir := t.TempDir()
	authority := newTestIdentityAuthority(t)
	if err := os.WriteFile(filepath.Join(dir, "ca.crt"), authority.cert, 0o600); err != nil {
		t.Fatal(err)
	}
	authority.writeIdentity(t, dir, "member-a")
	authority.writeIdentity(t, dir, "member-b")
	if _, err := loadIdentityTLSConfig("member-a", dir, filepath.Join(dir, "ca.crt"), []string{"../member-b"}); err == nil {
		t.Fatal("path-like peer identity unexpectedly accepted")
	}
	if _, err := loadIdentityTLSConfig("member-a", dir, filepath.Join(dir, "ca.crt"), []string{"member-b"}); err != nil {
		t.Fatal(err)
	}
}

func TestGenerateOneRunPeerCertificatesAndFingerprintManifest(t *testing.T) {
	root := t.TempDir()
	certDir := filepath.Join(root, "certs")
	if err := generateIdentityCertificates(certDir, []string{"member-a", "member-b", "member-c"}); err != nil {
		t.Fatal(err)
	}
	manifestBytes, err := os.ReadFile(filepath.Join(certDir, "fingerprints.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		PeerCertSHA256 map[string]string `json:"peer_cert_sha256"`
	}
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest.PeerCertSHA256) != 3 {
		t.Fatalf("fingerprint manifest has %d peers, want 3", len(manifest.PeerCertSHA256))
	}
	for _, member := range []string{"member-a", "member-b", "member-c"} {
		fingerprint, ok := manifest.PeerCertSHA256[member]
		if !ok || len(fingerprint) != 64 {
			t.Fatalf("fingerprint for %s missing or malformed: %q", member, fingerprint)
		}
		peers := []string{}
		for _, peer := range []string{"member-a", "member-b", "member-c"} {
			if peer != member {
				peers = append(peers, peer)
			}
		}
		if _, err := loadIdentityTLSConfig(member, certDir, filepath.Join(certDir, "ca.crt"), peers); err != nil {
			t.Fatalf("generated certificates for %s did not verify: %v", member, err)
		}
	}
	if _, err := os.Stat(filepath.Join(certDir, "ca.key")); !os.IsNotExist(err) {
		t.Fatalf("CA private key should not be retained, stat error = %v", err)
	}
	if err := generateIdentityCertificates(certDir, []string{"other-a", "other-b"}); err == nil {
		t.Fatal("certificate generator unexpectedly reused a non-empty directory")
	}
}
