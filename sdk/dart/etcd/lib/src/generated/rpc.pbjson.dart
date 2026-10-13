// This is a generated file - do not edit.
//
// Generated from rpc.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names, unused_import

import 'dart:convert' as $convert;
import 'dart:core' as $core;
import 'dart:typed_data' as $typed_data;

@$core.Deprecated('Use responseHeaderDescriptor instead')
const ResponseHeader$json = {
  '1': 'ResponseHeader',
  '2': [
    {'1': 'cluster_id', '3': 1, '4': 1, '5': 4, '10': 'clusterId'},
    {'1': 'member_id', '3': 2, '4': 1, '5': 4, '10': 'memberId'},
    {'1': 'revision', '3': 3, '4': 1, '5': 3, '10': 'revision'},
    {'1': 'raft_term', '3': 4, '4': 1, '5': 4, '10': 'raftTerm'},
  ],
};

/// Descriptor for `ResponseHeader`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List responseHeaderDescriptor = $convert.base64Decode(
    'Cg5SZXNwb25zZUhlYWRlchIdCgpjbHVzdGVyX2lkGAEgASgEUgljbHVzdGVySWQSGwoJbWVtYm'
    'VyX2lkGAIgASgEUghtZW1iZXJJZBIaCghyZXZpc2lvbhgDIAEoA1IIcmV2aXNpb24SGwoJcmFm'
    'dF90ZXJtGAQgASgEUghyYWZ0VGVybQ==');

@$core.Deprecated('Use rangeRequestDescriptor instead')
const RangeRequest$json = {
  '1': 'RangeRequest',
  '2': [
    {'1': 'key', '3': 1, '4': 1, '5': 12, '10': 'key'},
    {'1': 'range_end', '3': 2, '4': 1, '5': 12, '10': 'rangeEnd'},
    {'1': 'limit', '3': 3, '4': 1, '5': 3, '10': 'limit'},
    {'1': 'revision', '3': 4, '4': 1, '5': 3, '10': 'revision'},
    {
      '1': 'sort_order',
      '3': 5,
      '4': 1,
      '5': 14,
      '6': '.etcdserverpb.RangeRequest.SortOrder',
      '10': 'sortOrder'
    },
    {
      '1': 'sort_target',
      '3': 6,
      '4': 1,
      '5': 14,
      '6': '.etcdserverpb.RangeRequest.SortTarget',
      '10': 'sortTarget'
    },
    {'1': 'serializable', '3': 7, '4': 1, '5': 8, '10': 'serializable'},
    {'1': 'keys_only', '3': 8, '4': 1, '5': 8, '10': 'keysOnly'},
    {'1': 'count_only', '3': 9, '4': 1, '5': 8, '10': 'countOnly'},
    {'1': 'min_mod_revision', '3': 10, '4': 1, '5': 3, '10': 'minModRevision'},
    {'1': 'max_mod_revision', '3': 11, '4': 1, '5': 3, '10': 'maxModRevision'},
    {
      '1': 'min_create_revision',
      '3': 12,
      '4': 1,
      '5': 3,
      '10': 'minCreateRevision'
    },
    {
      '1': 'max_create_revision',
      '3': 13,
      '4': 1,
      '5': 3,
      '10': 'maxCreateRevision'
    },
  ],
  '4': [RangeRequest_SortOrder$json, RangeRequest_SortTarget$json],
};

@$core.Deprecated('Use rangeRequestDescriptor instead')
const RangeRequest_SortOrder$json = {
  '1': 'SortOrder',
  '2': [
    {'1': 'NONE', '2': 0},
    {'1': 'ASCEND', '2': 1},
    {'1': 'DESCEND', '2': 2},
  ],
};

@$core.Deprecated('Use rangeRequestDescriptor instead')
const RangeRequest_SortTarget$json = {
  '1': 'SortTarget',
  '2': [
    {'1': 'KEY', '2': 0},
    {'1': 'VERSION', '2': 1},
    {'1': 'CREATE', '2': 2},
    {'1': 'MOD', '2': 3},
    {'1': 'VALUE', '2': 4},
  ],
};

/// Descriptor for `RangeRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List rangeRequestDescriptor = $convert.base64Decode(
    'CgxSYW5nZVJlcXVlc3QSEAoDa2V5GAEgASgMUgNrZXkSGwoJcmFuZ2VfZW5kGAIgASgMUghyYW'
    '5nZUVuZBIUCgVsaW1pdBgDIAEoA1IFbGltaXQSGgoIcmV2aXNpb24YBCABKANSCHJldmlzaW9u'
    'EkMKCnNvcnRfb3JkZXIYBSABKA4yJC5ldGNkc2VydmVycGIuUmFuZ2VSZXF1ZXN0LlNvcnRPcm'
    'RlclIJc29ydE9yZGVyEkYKC3NvcnRfdGFyZ2V0GAYgASgOMiUuZXRjZHNlcnZlcnBiLlJhbmdl'
    'UmVxdWVzdC5Tb3J0VGFyZ2V0Ugpzb3J0VGFyZ2V0EiIKDHNlcmlhbGl6YWJsZRgHIAEoCFIMc2'
    'VyaWFsaXphYmxlEhsKCWtleXNfb25seRgIIAEoCFIIa2V5c09ubHkSHQoKY291bnRfb25seRgJ'
    'IAEoCFIJY291bnRPbmx5EigKEG1pbl9tb2RfcmV2aXNpb24YCiABKANSDm1pbk1vZFJldmlzaW'
    '9uEigKEG1heF9tb2RfcmV2aXNpb24YCyABKANSDm1heE1vZFJldmlzaW9uEi4KE21pbl9jcmVh'
    'dGVfcmV2aXNpb24YDCABKANSEW1pbkNyZWF0ZVJldmlzaW9uEi4KE21heF9jcmVhdGVfcmV2aX'
    'Npb24YDSABKANSEW1heENyZWF0ZVJldmlzaW9uIi4KCVNvcnRPcmRlchIICgROT05FEAASCgoG'
    'QVNDRU5EEAESCwoHREVTQ0VORBACIkIKClNvcnRUYXJnZXQSBwoDS0VZEAASCwoHVkVSU0lPTh'
    'ABEgoKBkNSRUFURRACEgcKA01PRBADEgkKBVZBTFVFEAQ=');

@$core.Deprecated('Use rangeResponseDescriptor instead')
const RangeResponse$json = {
  '1': 'RangeResponse',
  '2': [
    {
      '1': 'header',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.etcdserverpb.ResponseHeader',
      '10': 'header'
    },
    {'1': 'kvs', '3': 2, '4': 3, '5': 11, '6': '.mvccpb.KeyValue', '10': 'kvs'},
    {'1': 'more', '3': 3, '4': 1, '5': 8, '10': 'more'},
    {'1': 'count', '3': 4, '4': 1, '5': 3, '10': 'count'},
  ],
};

/// Descriptor for `RangeResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List rangeResponseDescriptor = $convert.base64Decode(
    'Cg1SYW5nZVJlc3BvbnNlEjQKBmhlYWRlchgBIAEoCzIcLmV0Y2RzZXJ2ZXJwYi5SZXNwb25zZU'
    'hlYWRlclIGaGVhZGVyEiIKA2t2cxgCIAMoCzIQLm12Y2NwYi5LZXlWYWx1ZVIDa3ZzEhIKBG1v'
    'cmUYAyABKAhSBG1vcmUSFAoFY291bnQYBCABKANSBWNvdW50');

@$core.Deprecated('Use leaseGrantRequestDescriptor instead')
const LeaseGrantRequest$json = {
  '1': 'LeaseGrantRequest',
  '2': [
    {'1': 'TTL', '3': 1, '4': 1, '5': 3, '10': 'TTL'},
    {'1': 'ID', '3': 2, '4': 1, '5': 3, '10': 'ID'},
  ],
};

/// Descriptor for `LeaseGrantRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List leaseGrantRequestDescriptor = $convert.base64Decode(
    'ChFMZWFzZUdyYW50UmVxdWVzdBIQCgNUVEwYASABKANSA1RUTBIOCgJJRBgCIAEoA1ICSUQ=');

@$core.Deprecated('Use leaseGrantResponseDescriptor instead')
const LeaseGrantResponse$json = {
  '1': 'LeaseGrantResponse',
  '2': [
    {
      '1': 'header',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.etcdserverpb.ResponseHeader',
      '10': 'header'
    },
    {'1': 'ID', '3': 2, '4': 1, '5': 3, '10': 'ID'},
    {'1': 'TTL', '3': 3, '4': 1, '5': 3, '10': 'TTL'},
    {'1': 'error', '3': 4, '4': 1, '5': 9, '10': 'error'},
  ],
};

/// Descriptor for `LeaseGrantResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List leaseGrantResponseDescriptor = $convert.base64Decode(
    'ChJMZWFzZUdyYW50UmVzcG9uc2USNAoGaGVhZGVyGAEgASgLMhwuZXRjZHNlcnZlcnBiLlJlc3'
    'BvbnNlSGVhZGVyUgZoZWFkZXISDgoCSUQYAiABKANSAklEEhAKA1RUTBgDIAEoA1IDVFRMEhQK'
    'BWVycm9yGAQgASgJUgVlcnJvcg==');

@$core.Deprecated('Use leaseRevokeRequestDescriptor instead')
const LeaseRevokeRequest$json = {
  '1': 'LeaseRevokeRequest',
  '2': [
    {'1': 'ID', '3': 1, '4': 1, '5': 3, '10': 'ID'},
  ],
};

/// Descriptor for `LeaseRevokeRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List leaseRevokeRequestDescriptor =
    $convert.base64Decode('ChJMZWFzZVJldm9rZVJlcXVlc3QSDgoCSUQYASABKANSAklE');

@$core.Deprecated('Use leaseRevokeResponseDescriptor instead')
const LeaseRevokeResponse$json = {
  '1': 'LeaseRevokeResponse',
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

/// Descriptor for `LeaseRevokeResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List leaseRevokeResponseDescriptor = $convert.base64Decode(
    'ChNMZWFzZVJldm9rZVJlc3BvbnNlEjQKBmhlYWRlchgBIAEoCzIcLmV0Y2RzZXJ2ZXJwYi5SZX'
    'Nwb25zZUhlYWRlclIGaGVhZGVy');

@$core.Deprecated('Use leaseKeepAliveRequestDescriptor instead')
const LeaseKeepAliveRequest$json = {
  '1': 'LeaseKeepAliveRequest',
  '2': [
    {'1': 'ID', '3': 1, '4': 1, '5': 3, '10': 'ID'},
  ],
};

/// Descriptor for `LeaseKeepAliveRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List leaseKeepAliveRequestDescriptor = $convert
    .base64Decode('ChVMZWFzZUtlZXBBbGl2ZVJlcXVlc3QSDgoCSUQYASABKANSAklE');

@$core.Deprecated('Use leaseKeepAliveResponseDescriptor instead')
const LeaseKeepAliveResponse$json = {
  '1': 'LeaseKeepAliveResponse',
  '2': [
    {
      '1': 'header',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.etcdserverpb.ResponseHeader',
      '10': 'header'
    },
    {'1': 'ID', '3': 2, '4': 1, '5': 3, '10': 'ID'},
    {'1': 'TTL', '3': 3, '4': 1, '5': 3, '10': 'TTL'},
  ],
};

/// Descriptor for `LeaseKeepAliveResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List leaseKeepAliveResponseDescriptor = $convert.base64Decode(
    'ChZMZWFzZUtlZXBBbGl2ZVJlc3BvbnNlEjQKBmhlYWRlchgBIAEoCzIcLmV0Y2RzZXJ2ZXJwYi'
    '5SZXNwb25zZUhlYWRlclIGaGVhZGVyEg4KAklEGAIgASgDUgJJRBIQCgNUVEwYAyABKANSA1RU'
    'TA==');
