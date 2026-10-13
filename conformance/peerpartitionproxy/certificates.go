package main

import (
	"crypto/sha256"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// generateIdentityCertificates creates one-run peer identities without writing
// the CA private key. The caller must provide a new, empty output directory.
func generateIdentityCertificates(certDir string, peerIDs []string) error {
	if len(peerIDs) < 2 {
		return fmt.Errorf("at least two peer identities are required")
	}
	if err := os.Mkdir(certDir, 0o700); err != nil {
		return fmt.Errorf("create fresh certificate directory: %w", err)
	}
	seen := make(map[string]struct{}, len(peerIDs))
	for _, id := range peerIDs {
		if !validPeerID(id) {
			return fmt.Errorf("invalid peer identity %q", id)
		}
		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("duplicate peer identity %q", id)
		}
		seen[id] = struct{}{}
	}

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("generate test CA key: %w", err)
	}
	caSerial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("generate test CA serial: %w", err)
	}
	now := time.Now()
	caTemplate := &x509.Certificate{
		SerialNumber:          caSerial.Add(caSerial, big.NewInt(1)),
		Subject:               pkix.Name{CommonName: "Dilock local peer test CA"},
		NotBefore:             now.Add(-5 * time.Minute),
		NotAfter:              now.Add(2 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("create test CA certificate: %w", err)
	}
	if err := writePEMExclusive(filepath.Join(certDir, "ca.crt"), "CERTIFICATE", caDER); err != nil {
		return fmt.Errorf("write test CA certificate: %w", err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		return fmt.Errorf("parse test CA certificate: %w", err)
	}
	fingerprints := make(map[string]string, len(peerIDs))

	for _, id := range peerIDs {
		memberKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return fmt.Errorf("generate key for peer %q: %w", id, err)
		}
		serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
		if err != nil {
			return fmt.Errorf("generate serial for peer %q: %w", id, err)
		}
		memberTemplate := &x509.Certificate{
			SerialNumber: serial.Add(serial, big.NewInt(1)),
			Subject:      pkix.Name{CommonName: id},
			DNSNames:     []string{id},
			IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
			NotBefore:    now.Add(-5 * time.Minute),
			NotAfter:     now.Add(90 * time.Minute),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		}
		memberDER, err := x509.CreateCertificate(rand.Reader, memberTemplate, ca, &memberKey.PublicKey, caKey)
		if err != nil {
			return fmt.Errorf("create certificate for peer %q: %w", id, err)
		}
		leaf, err := x509.ParseCertificate(memberDER)
		if err != nil {
			return fmt.Errorf("parse certificate for peer %q: %w", id, err)
		}
		fingerprint := sha256.Sum256(leaf.Raw)
		fingerprints[id] = hex.EncodeToString(fingerprint[:])
		if err := writePEMExclusive(filepath.Join(certDir, id+".crt"), "CERTIFICATE", memberDER); err != nil {
			return fmt.Errorf("write certificate for peer %q: %w", id, err)
		}
		privateKey, err := x509.MarshalPKCS8PrivateKey(memberKey)
		if err != nil {
			return fmt.Errorf("marshal key for peer %q: %w", id, err)
		}
		if err := writePEMExclusive(filepath.Join(certDir, id+".key"), "PRIVATE KEY", privateKey); err != nil {
			return fmt.Errorf("write key for peer %q: %w", id, err)
		}
	}
	manifest, err := json.MarshalIndent(map[string]any{
		"schema_version": 1,
		"peer_cert_sha256": fingerprints,
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode peer certificate fingerprint manifest: %w", err)
	}
	return writeBytesExclusive(filepath.Join(certDir, "fingerprints.json"), append(manifest, '\n'))
}

func writePEMExclusive(path, blockType string, der []byte) error {
	return writeBytesExclusive(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}))
}

func writeBytesExclusive(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}
