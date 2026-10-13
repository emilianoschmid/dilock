// This is a generated file - do not edit.
//
// Generated from v3lock.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names, unused_import

import 'dart:convert' as $convert;
import 'dart:core' as $core;
import 'dart:typed_data' as $typed_data;

@$core.Deprecated('Use lockRequestDescriptor instead')
const LockRequest$json = {
  '1': 'LockRequest',
  '2': [
    {'1': 'name', '3': 1, '4': 1, '5': 12, '10': 'name'},
    {'1': 'lease', '3': 2, '4': 1, '5': 3, '10': 'lease'},
  ],
};

/// Descriptor for `LockRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List lockRequestDescriptor = $convert.base64Decode(
    'CgtMb2NrUmVxdWVzdBISCgRuYW1lGAEgASgMUgRuYW1lEhQKBWxlYXNlGAIgASgDUgVsZWFzZQ'
    '==');

@$core.Deprecated('Use lockResponseDescriptor instead')
const LockResponse$json = {
  '1': 'LockResponse',
  '2': [
    {
      '1': 'header',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.etcdserverpb.ResponseHeader',
      '10': 'header'
    },
    {'1': 'key', '3': 2, '4': 1, '5': 12, '10': 'key'},
  ],
};

/// Descriptor for `LockResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List lockResponseDescriptor = $convert.base64Decode(
    'CgxMb2NrUmVzcG9uc2USNAoGaGVhZGVyGAEgASgLMhwuZXRjZHNlcnZlcnBiLlJlc3BvbnNlSG'
    'VhZGVyUgZoZWFkZXISEAoDa2V5GAIgASgMUgNrZXk=');

@$core.Deprecated('Use unlockRequestDescriptor instead')
const UnlockRequest$json = {
  '1': 'UnlockRequest',
  '2': [
    {'1': 'key', '3': 1, '4': 1, '5': 12, '10': 'key'},
  ],
};

/// Descriptor for `UnlockRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List unlockRequestDescriptor =
    $convert.base64Decode('Cg1VbmxvY2tSZXF1ZXN0EhAKA2tleRgBIAEoDFIDa2V5');

@$core.Deprecated('Use unlockResponseDescriptor instead')
const UnlockResponse$json = {
  '1': 'UnlockResponse',
  '2': [
    {
      '1': 'header',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.etcdserverpb.ResponseHeader',
      '10': 'header'
    },
  ],
};

/// Descriptor for `UnlockResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List unlockResponseDescriptor = $convert.base64Decode(
    'Cg5VbmxvY2tSZXNwb25zZRI0CgZoZWFkZXIYASABKAsyHC5ldGNkc2VydmVycGIuUmVzcG9uc2'
    'VIZWFkZXJSBmhlYWRlcg==');
