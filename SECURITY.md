# Security policy

## Current status

No stable runtime release exists in this blueprint, and no version is represented as production-supported. Supported version ranges and a response policy must be published when the first release is supported. There is no promised response SLA at this stage.

## Reporting

Before opening a public issue about an ownership bypass, stale-owner write, credential leak or release-chain compromise, use GitHub private vulnerability reporting for the published repository when configured. The maintainer must enable and verify that route before the first public release. No contact address is fabricated in this template. For the local unpublished blueprint, contact its owner directly through the channel in which it was shared.

## Threat boundaries

A distributed lock is not a security boundary between clients sharing backend credentials. Lease expiry does not stop an old process. Backend state deletion, unsupported failover or an incorrectly implemented protected resource can invalidate guarantees. See the backend profiles and semantic contract.

Do not run destructive/fault tests against production. Credentials and ownership proofs must be redacted from traces. Release credentials should be short-lived and restricted to reviewed workflows and protected environments.
