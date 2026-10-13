// Package dilock provides distributed leases with explicit ownership and cleanup.
package dilock

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// ErrInvalidIdentity indicates a namespace or resource outside the shared wire contract.
var ErrInvalidIdentity = errors.New("dilock: invalid namespace or resource")

// Identity returns the draft 1 resource digest. It does not normalize Unicode.
func Identity(namespace, resource string) (string, error) {
	if len(namespace) < 1 || len(namespace) > 64 || !namespaceChar(namespace[0], true) {
		return "", ErrInvalidIdentity
	}
	for i := 1; i < len(namespace); i++ {
		if !namespaceChar(namespace[i], false) {
			return "", ErrInvalidIdentity
		}
	}
	if !utf8.ValidString(resource) || strings.IndexByte(resource, 0) >= 0 || len(resource) < 1 || len(resource) > 1024 {
		return "", ErrInvalidIdentity
	}
	ns := []byte(namespace)
	r := []byte(resource)
	buf := make([]byte, 0, len("dilock.lock.identity\x00")+8+len(ns)+len(r))
	buf = append(buf, "dilock.lock.identity\x00"...)
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(ns)))
	buf = append(buf, size[:]...)
	buf = append(buf, ns...)
	binary.BigEndian.PutUint32(size[:], uint32(len(r)))
	buf = append(buf, size[:]...)
	buf = append(buf, r...)
	sum := sha256.Sum256(buf)
	return hex.EncodeToString(sum[:]), nil
}

func namespaceChar(c byte, first bool) bool {
	return c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || !first && (c == '_' || c == '.' || c == '-')
}

// RedisKey constructs the shared Redis key for a validated identity.
func RedisKey(namespace, resource string) (string, error) {
	id, err := Identity(namespace, resource)
	if err != nil {
		return "", fmt.Errorf("redis key: %w", err)
	}
	return "dilock:{" + id + "}:lease", nil
}
