import 'contract.dart';

const _identityScheme = 'sha256-framed-utf8-v1';

/// Checks the shared draft 1 metadata fields used by native backend adapters.
///
/// JSON protocol versions must be integer values. Dart considers `1.0 == 1`,
/// so comparing only by numeric equality would accept a malformed wire value.
void validateNamespaceMetadata(
  Map<String, dynamic> metadata, {
  required String profile,
  required String coordinationEpoch,
}) {
  final protocolMajor = metadata['protocol_major'];
  if (protocolMajor is! int ||
      protocolMajor != 1 ||
      metadata['identity_scheme'] != _identityScheme ||
      metadata['profile'] != profile ||
      metadata['coordination_epoch'] != coordinationEpoch ||
      metadata['fixed_lease_ms'] != null) {
    throw const DilockException(
      FailureKind.protocolMismatch,
      'Namespace metadata does not match draft 1.',
    );
  }
}
