// This is a generated file - do not edit.
//
// Generated from v3lock.proto.

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

import 'v3lock.pb.dart' as $0;

export 'v3lock.pb.dart';

/// Native lock service from etcd-io/etcd v3.7.2 server/etcdserver/api/v3lock/v3lockpb/v3lock.proto.
@$pb.GrpcServiceName('v3lockpb.Lock')
class LockClient extends $grpc.Client {
  /// The hostname for this service.
  static const $core.String defaultHost = '';

  /// OAuth scopes needed for the client.
  static const $core.List<$core.String> oauthScopes = [
    '',
  ];

  LockClient(super.channel, {super.options, super.interceptors});

  $grpc.ResponseFuture<$0.LockResponse> lock(
    $0.LockRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$lock, request, options: options);
  }

  $grpc.ResponseFuture<$0.UnlockResponse> unlock(
    $0.UnlockRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$unlock, request, options: options);
  }

  // method descriptors

  static final _$lock = $grpc.ClientMethod<$0.LockRequest, $0.LockResponse>(
      '/v3lockpb.Lock/Lock',
      ($0.LockRequest value) => value.writeToBuffer(),
      $0.LockResponse.fromBuffer);
  static final _$unlock =
      $grpc.ClientMethod<$0.UnlockRequest, $0.UnlockResponse>(
          '/v3lockpb.Lock/Unlock',
          ($0.UnlockRequest value) => value.writeToBuffer(),
          $0.UnlockResponse.fromBuffer);
}

@$pb.GrpcServiceName('v3lockpb.Lock')
abstract class LockServiceBase extends $grpc.Service {
  $core.String get $name => 'v3lockpb.Lock';

  LockServiceBase() {
    $addMethod($grpc.ServiceMethod<$0.LockRequest, $0.LockResponse>(
        'Lock',
        lock_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.LockRequest.fromBuffer(value),
        ($0.LockResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.UnlockRequest, $0.UnlockResponse>(
        'Unlock',
        unlock_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.UnlockRequest.fromBuffer(value),
        ($0.UnlockResponse value) => value.writeToBuffer()));
  }

  $async.Future<$0.LockResponse> lock_Pre(
      $grpc.ServiceCall $call, $async.Future<$0.LockRequest> $request) async {
    return lock($call, await $request);
  }

  $async.Future<$0.LockResponse> lock(
      $grpc.ServiceCall call, $0.LockRequest request);

  $async.Future<$0.UnlockResponse> unlock_Pre(
      $grpc.ServiceCall $call, $async.Future<$0.UnlockRequest> $request) async {
    return unlock($call, await $request);
  }

  $async.Future<$0.UnlockResponse> unlock(
      $grpc.ServiceCall call, $0.UnlockRequest request);
}
