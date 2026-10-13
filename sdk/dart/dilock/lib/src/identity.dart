import 'dart:convert';
import 'dart:typed_data';

import 'package:crypto/crypto.dart';

/// Computes the draft 1 identity digest without normalizing resource text.
///
/// Namespace is ASCII `[a-z0-9][a-z0-9_.-]{0,63}`. Resource is strict UTF-8,
/// 1 through 1024 bytes, and cannot contain U+0000. Composed and decomposed
/// Unicode strings remain distinct identities.
String resourceId(String namespace, String resource) {
  final namespaceBytes = namespace.codeUnits;
  if (namespaceBytes.isEmpty ||
      namespaceBytes.length > 64 ||
      namespaceBytes.any((byte) => byte > 0x7f) ||
      !_namespaceByte(namespaceBytes.first, first: true) ||
      namespaceBytes.skip(1).any((byte) => !_namespaceByte(byte))) {
    throw const InvalidIdentityException('Invalid namespace.');
  }
  if (resource.contains('\u0000')) {
    throw const InvalidIdentityException('Resource contains U+0000.');
  }
  if (_containsUnpairedSurrogate(resource)) {
    throw const InvalidIdentityException('Resource is not valid Unicode.');
  }
  late final List<int> resourceBytes;
  try {
    resourceBytes = utf8.encode(resource);
  } on FormatException {
    throw const InvalidIdentityException('Resource is not valid Unicode.');
  }
  if (resourceBytes.isEmpty || resourceBytes.length > 1024) {
    throw const InvalidIdentityException(
      'Resource must use 1 through 1024 UTF-8 bytes.',
    );
  }

  final input = BytesBuilder(copy: false)
    ..add(ascii.encode('dilock.lock.identity'))
    ..addByte(0)
    ..add(_uint32be(namespaceBytes.length))
    ..add(namespaceBytes)
    ..add(_uint32be(resourceBytes.length))
    ..add(resourceBytes);
  return sha256.convert(input.takeBytes()).toString();
}

/// Dart strings store UTF-16 code units. Validate pairs before UTF-8 encoding,
/// whose default malformed-input behavior can replace an isolated surrogate.
bool _containsUnpairedSurrogate(String value) {
  for (var index = 0; index < value.length; index++) {
    final unit = value.codeUnitAt(index);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      if (index + 1 >= value.length) return true;
      final next = value.codeUnitAt(index + 1);
      if (next < 0xdc00 || next > 0xdfff) return true;
      index++;
    } else if (unit >= 0xdc00 && unit <= 0xdfff) {
      return true;
    }
  }
  return false;
}

/// Returns the canonical Redis lease key for an identity.
String redisKey(String namespace, String resource) =>
    'dilock:{${resourceId(namespace, resource)}}:lease';

/// Returns the etcd native Lock name for an identity.
String etcdLockName(String namespace, String resource) =>
    '/dilock/locks/${resourceId(namespace, resource)}';

bool _namespaceByte(int value, {bool first = false}) =>
    (value >= 0x61 && value <= 0x7a) ||
    (value >= 0x30 && value <= 0x39) ||
    (!first && (value == 0x5f || value == 0x2e || value == 0x2d));

List<int> _uint32be(int value) => <int>[
      (value >> 24) & 0xff,
      (value >> 16) & 0xff,
      (value >> 8) & 0xff,
      value & 0xff,
    ];

/// Invalid canonical namespace or resource input.
final class InvalidIdentityException implements Exception {
  /// Creates an identity validation error.
  const InvalidIdentityException(this.message);

  /// Human-readable validation detail.
  final String message;

  @override
  String toString() => 'InvalidIdentityException: $message';
}
