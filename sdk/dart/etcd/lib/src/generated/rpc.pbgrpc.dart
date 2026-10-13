// This is a generated file - do not edit.
//
// Generated from rpc.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names

import 'dart:async' as $async;
import 'dart:core' as $core;

import 'package:grpc/service_api.dart' as $grpc;
import 'package:protobuf/protobuf.dart' as $pb;

import 'rpc.pb.dart' as $0;

export 'rpc.pb.dart';

/// Thin native gRPC bindings for the etcd v3.7.2 RPCs Dilock uses. Message
/// names, field names, field numbers and wire types are copied from the pinned
/// upstream API definitions; unused fields are intentionally omitted.
@$pb.GrpcServiceName('etcdserverpb.KV')
class KVClient extends $grpc.Client {
  /// The hostname for this service.
  static const $core.String defaultHost = '';

  /// OAuth scopes needed for the client.
  static const $core.List<$core.String> oauthScopes = [
    '',
  ];

  KVClient(super.channel, {super.options, super.interceptors});

  $grpc.ResponseFuture<$0.RangeResponse> range(
    $0.RangeRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$range, request, options: options);
  }

  // method descriptors

  static final _$range = $grpc.ClientMethod<$0.RangeRequest, $0.RangeResponse>(
      '/etcdserverpb.KV/Range',
      ($0.RangeRequest value) => value.writeToBuffer(),
      $0.RangeResponse.fromBuffer);
}

@$pb.GrpcServiceName('etcdserverpb.KV')
abstract class KVServiceBase extends $grpc.Service {
  $core.String get $name => 'etcdserverpb.KV';

  KVServiceBase() {
    $addMethod($grpc.ServiceMethod<$0.RangeRequest, $0.RangeResponse>(
        'Range',
        range_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.RangeRequest.fromBuffer(value),
        ($0.RangeResponse value) => value.writeToBuffer()));
  }

  $async.Future<$0.RangeResponse> range_Pre(
      $grpc.ServiceCall $call, $async.Future<$0.RangeRequest> $request) async {
    return range($call, await $request);
  }

  $async.Future<$0.RangeResponse> range(
      $grpc.ServiceCall call, $0.RangeRequest request);
}

@$pb.GrpcServiceName('etcdserverpb.Lease')
class LeaseClient extends $grpc.Client {
  /// The hostname for this service.
  static const $core.String defaultHost = '';

  /// OAuth scopes needed for the client.
  static const $core.List<$core.String> oauthScopes = [
    '',
  ];

  LeaseClient(super.channel, {super.options, super.interceptors});

  $grpc.ResponseFuture<$0.LeaseGrantResponse> leaseGrant(
    $0.LeaseGrantRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$leaseGrant, request, options: options);
  }

  $grpc.ResponseFuture<$0.LeaseRevokeResponse> leaseRevoke(
    $0.LeaseRevokeRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$leaseRevoke, request, options: options);
  }

  $grpc.ResponseStream<$0.LeaseKeepAliveResponse> leaseKeepAlive(
    $async.Stream<$0.LeaseKeepAliveRequest> request, {
    $grpc.CallOptions? options,
  }) {
    return $createStreamingCall(_$leaseKeepAlive, request, options: options);
  }

  // method descriptors

  static final _$leaseGrant =
      $grpc.ClientMethod<$0.LeaseGrantRequest, $0.LeaseGrantResponse>(
          '/etcdserverpb.Lease/LeaseGrant',
          ($0.LeaseGrantRequest value) => value.writeToBuffer(),
          $0.LeaseGrantResponse.fromBuffer);
  static final _$leaseRevoke =
      $grpc.ClientMethod<$0.LeaseRevokeRequest, $0.LeaseRevokeResponse>(
          '/etcdserverpb.Lease/LeaseRevoke',
          ($0.LeaseRevokeRequest value) => value.writeToBuffer(),
          $0.LeaseRevokeResponse.fromBuffer);
  static final _$leaseKeepAlive =
      $grpc.ClientMethod<$0.LeaseKeepAliveRequest, $0.LeaseKeepAliveResponse>(
          '/etcdserverpb.Lease/LeaseKeepAlive',
          ($0.LeaseKeepAliveRequest value) => value.writeToBuffer(),
          $0.LeaseKeepAliveResponse.fromBuffer);
}

@$pb.GrpcServiceName('etcdserverpb.Lease')
abstract class LeaseServiceBase extends $grpc.Service {
  $core.String get $name => 'etcdserverpb.Lease';

  LeaseServiceBase() {
    $addMethod($grpc.ServiceMethod<$0.LeaseGrantRequest, $0.LeaseGrantResponse>(
        'LeaseGrant',
        leaseGrant_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.LeaseGrantRequest.fromBuffer(value),
        ($0.LeaseGrantResponse value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.LeaseRevokeRequest, $0.LeaseRevokeResponse>(
            'LeaseRevoke',
            leaseRevoke_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.LeaseRevokeRequest.fromBuffer(value),
            ($0.LeaseRevokeResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.LeaseKeepAliveRequest,
            $0.LeaseKeepAliveResponse>(
        'LeaseKeepAlive',
        leaseKeepAlive,
        true,
        true,
        ($core.List<$core.int> value) =>
            $0.LeaseKeepAliveRequest.fromBuffer(value),
        ($0.LeaseKeepAliveResponse value) => value.writeToBuffer()));
  }

  $async.Future<$0.LeaseGrantResponse> leaseGrant_Pre($grpc.ServiceCall $call,
      $async.Future<$0.LeaseGrantRequest> $request) async {
    return leaseGrant($call, await $request);
  }

  $async.Future<$0.LeaseGrantResponse> leaseGrant(
      $grpc.ServiceCall call, $0.LeaseGrantRequest request);

  $async.Future<$0.LeaseRevokeResponse> leaseRevoke_Pre($grpc.ServiceCall $call,
      $async.Future<$0.LeaseRevokeRequest> $request) async {
    return leaseRevoke($call, await $request);
  }

  $async.Future<$0.LeaseRevokeResponse> leaseRevoke(
      $grpc.ServiceCall call, $0.LeaseRevokeRequest request);

  $async.Stream<$0.LeaseKeepAliveResponse> leaseKeepAlive(
      $grpc.ServiceCall call, $async.Stream<$0.LeaseKeepAliveRequest> request);
}
