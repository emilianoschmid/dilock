# Resource identity and wire values — draft 1

## Canonical input

Namespace: ASCII `[a-z0-9][a-z0-9_.-]{0,63}`. Resource: non-empty Unicode scalar string encoded as strict UTF-8, 1–1024 bytes; U+0000 and unpaired surrogates are rejected. Case-sensitive; no trimming, Unicode normalization, culture-dependent casing or path normalization. Composed and decomposed Unicode forms are deliberately different resources.

Construct these bytes:

```text
ASCII("dilock.lock.identity") || 0x00
|| uint32_be(len(namespace_utf8)) || namespace_utf8
|| uint32_be(len(resource_utf8))  || resource_utf8
```

The resource ID is lowercase hexadecimal SHA-256 of those bytes. This is deterministic namespacing, not secrecy; resource names may still be guessable. Cryptographic collision resistance is an assumption, not mathematical uniqueness. Namespace, resource and endpoint identity must be documented consistently by deployers.

Golden vectors are in `vectors/identity.json`. `tools/reference_identity.py` is a small executable reference for this encoding only; it is not a lock runtime. SDKs must independently implement the encoding and consume these vectors.

## Backend names

Redis lock key: `dilock:{<resource_id>}:lease` (literal braces). Metadata key for a namespace: `dilock:namespace:<namespace>:meta`. Etcd native Lock name: UTF-8 `/dilock/locks/<resource_id>`. Etcd namespace metadata key: `/dilock/namespaces/<namespace>/meta`. NATS candidate KV key: `l.<resource_id>` within the configured bucket.

Do not add the language, owner, random process ID or SDK version to the lock key. Owner identity belongs in the ownership value. A deployment must provision/check compatible namespace metadata; it is not a magic split-brain detector and cannot protect against manually misconfigured independent coordinators.

## Redis ownership value

```text
ASCII("dilock/1/") || 64 lowercase hexadecimal characters
```

The 64 hex characters encode 32 fresh bytes from the platform CSPRNG for this acquisition generation. Values are compared byte-for-byte. They never appear in normal logs or metrics. Do not serialize language object metadata, timestamps, GUID endian-dependent binary layouts or JSON property order into ownership comparison.

## Versions

Protocol draft 1 and package versions are different. The key namespace intentionally has no SDK-version component. Incompatible protocol changes need coordinated draining/migration and visible refusal of incompatible metadata; silently changing the prefix would allow two independent owners of one logical resource. Additive diagnostics must not change identity or owner comparison.

Conformance JSON serializes backend 64-bit revision/fence values as decimal strings, avoiding precision loss in JavaScript numbers. UUID formatting is not used as the wire ownership format.
