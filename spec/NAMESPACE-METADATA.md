# Namespace metadata and migration gate — draft 1

Metadata makes protocol/profile misconfiguration visible; it does not create consensus between different coordinators. Provision it explicitly for the same namespace used by every SDK. The metadata key locations are in `IDENTITY.md`; NATS candidate storage will be finalized during the spike.

The JSON object contains `protocol_major` (1), `identity_scheme` (`sha256-framed-utf8-v1`), `profile` (`redis-single-authority-v1` or `etcd-native-lock-v1`), `coordination_epoch` (nonempty deployment identifier), and `fixed_lease_ms` (null for Redis/etcd; fixed positive integer for a future validated NATS profile). Unknown optional fields may be ignored; unknown major/identity/profile must be rejected. SDKs compare parsed values, not JSON whitespace/property order.

Ordinary runtime clients read/validate compatible metadata before acquiring and do not silently create, overwrite or repair it. Provide an explicit provisioning command for disposable development and a documented administrator workflow for deployment. The command uses create-if-absent; mismatched existing metadata is a conflict requiring inspection, never an overwrite. JSON parser limits and validation are required.

An epoch is stable for a coordinator incarnation and is shared by all SDKs. It is not generated on every client launch. For fenced use, the protected resource is provisioned to recognize the same approved epoch; an arbitrary unknown epoch must not reset its fence watermark.

Creating a new key prefix or bucket to avoid a mismatch is forbidden while old clients are active. A breaking migration drains/stops old acquisitions, confirms cleanup under the profile, migrates the protected resource's fence state where applicable, provisions the new agreed metadata and starts compatible clients. A manual backend reset can still destroy metadata/locks and lies outside Redis's stronger unsupported guarantees.

Metadata validation can detect configuration mistakes, not a coordinated attacker with backend admin rights. Reconnection/profile validation and restore procedures must be documented and tested. No “namespace missing” fallback to an in-memory lock.
