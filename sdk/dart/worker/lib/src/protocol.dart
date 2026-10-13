import 'dart:convert';

const maxLineBytes = 64 * 1024;
const maxRequestIdLength = 128;
const maxDurationMs = 86400000;

const supportedOperations = <String>{
  'hello',
  'acquire',
  'try_acquire',
  'renew',
  'release',
  'inspect',
  'cancel_request',
  'shutdown',
};

/// Validates a decoded JSONL request without including request contents in errors.
Map<String, Object?> parseRequest(String line) {
  final Object? decoded;
  try {
    decoded = jsonDecode(line);
  } on FormatException {
    throw const FormatException('malformed JSON request');
  }
  if (decoded is! Map<String, dynamic>) {
    throw const FormatException('request must be an object');
  }
  const allowed = <String>{
    'id',
    'op',
    'resource',
    'lease_id',
    'target_id',
    'wait_ms',
    'lease_ms',
    'auto_renew',
  };
  if (decoded.keys.any((key) => !allowed.contains(key))) {
    throw const FormatException('unknown request field');
  }
  final id = decoded['id'];
  final op = decoded['op'];
  if (id is! String || id.isEmpty || id.length > maxRequestIdLength) {
    throw const FormatException('invalid request id');
  }
  if (op is! String || !supportedOperations.contains(op)) {
    throw const FormatException('unknown operation');
  }
  switch (op) {
    case 'acquire':
    case 'try_acquire':
      _requiredText(decoded, 'resource');
    case 'renew':
    case 'release':
    case 'inspect':
      _requiredText(decoded, 'lease_id');
    case 'cancel_request':
      _requiredText(decoded, 'target_id');
  }
  final waitMs = decoded['wait_ms'];
  if (waitMs != null &&
      (waitMs is! int || waitMs < 0 || waitMs > maxDurationMs)) {
    throw const FormatException(
      'wait_ms must be a bounded non-negative integer',
    );
  }
  final leaseMs = decoded['lease_ms'];
  if (leaseMs != null &&
      (leaseMs is! int || leaseMs < 1 || leaseMs > maxDurationMs)) {
    throw const FormatException('lease_ms must be a bounded positive integer');
  }
  final autoRenew = decoded['auto_renew'];
  if (autoRenew != null && autoRenew is! bool) {
    throw const FormatException('auto_renew must be boolean');
  }
  return decoded.cast<String, Object?>();
}

String? requiredText(Map<String, Object?> request, String key) =>
    _requiredText(request, key);

int boundedInteger(
  Map<String, Object?> request,
  String key,
  int fallback, {
  required bool allowZero,
}) {
  final value = request[key];
  if (value == null) return fallback;
  if (value is! int || value < (allowZero ? 0 : 1) || value > maxDurationMs) {
    throw FormatException('invalid $key');
  }
  return value;
}

String _requiredText(Map<String, Object?> request, String key) {
  final value = request[key];
  if (value is! String || value.isEmpty) {
    throw FormatException('$key is required');
  }
  return value;
}
