// This is a generated file - do not edit.
//
// Generated from v3lock.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names

import 'dart:core' as $core;

import 'package:fixnum/fixnum.dart' as $fixnum;
import 'package:protobuf/protobuf.dart' as $pb;

import 'rpc.pb.dart' as $1;

export 'package:protobuf/protobuf.dart' show GeneratedMessageGenericExtensions;

class LockRequest extends $pb.GeneratedMessage {
  factory LockRequest({
    $core.List<$core.int>? name,
    $fixnum.Int64? lease,
  }) {
    final result = create();
    if (name != null) result.name = name;
    if (lease != null) result.lease = lease;
    return result;
  }

  LockRequest._();

  factory LockRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory LockRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'LockRequest',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'v3lockpb'),
      createEmptyInstance: create)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'name', $pb.PbFieldType.OY)
    ..aInt64(2, _omitFieldNames ? '' : 'lease')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  LockRequest clone() => LockRequest()..mergeFromMessage(this);
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  LockRequest copyWith(void Function(LockRequest) updates) =>
      super.copyWith((message) => updates(message as LockRequest))
          as LockRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static LockRequest create() => LockRequest._();
  @$core.override
  LockRequest createEmptyInstance() => create();
  static $pb.PbList<LockRequest> createRepeated() => $pb.PbList<LockRequest>();
  @$core.pragma('dart2js:noInline')
  static LockRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<LockRequest>(create);
  static LockRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get name => $_getN(0);
  @$pb.TagNumber(1)
  set name($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasName() => $_has(0);
  @$pb.TagNumber(1)
  void clearName() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get lease => $_getI64(1);
  @$pb.TagNumber(2)
  set lease($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasLease() => $_has(1);
  @$pb.TagNumber(2)
  void clearLease() => $_clearField(2);
}

class LockResponse extends $pb.GeneratedMessage {
  factory LockResponse({
    $1.ResponseHeader? header,
    $core.List<$core.int>? key,
  }) {
    final result = create();
    if (header != null) result.header = header;
    if (key != null) result.key = key;
    return result;
  }

  LockResponse._();

  factory LockResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory LockResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'LockResponse',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'v3lockpb'),
      createEmptyInstance: create)
    ..aOM<$1.ResponseHeader>(1, _omitFieldNames ? '' : 'header',
        subBuilder: $1.ResponseHeader.create)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'key', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  LockResponse clone() => LockResponse()..mergeFromMessage(this);
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  LockResponse copyWith(void Function(LockResponse) updates) =>
      super.copyWith((message) => updates(message as LockResponse))
          as LockResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static LockResponse create() => LockResponse._();
  @$core.override
  LockResponse createEmptyInstance() => create();
  static $pb.PbList<LockResponse> createRepeated() =>
      $pb.PbList<LockResponse>();
  @$core.pragma('dart2js:noInline')
  static LockResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<LockResponse>(create);
  static LockResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $1.ResponseHeader get header => $_getN(0);
  @$pb.TagNumber(1)
  set header($1.ResponseHeader value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasHeader() => $_has(0);
  @$pb.TagNumber(1)
  void clearHeader() => $_clearField(1);
  @$pb.TagNumber(1)
  $1.ResponseHeader ensureHeader() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.List<$core.int> get key => $_getN(1);
  @$pb.TagNumber(2)
  set key($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasKey() => $_has(1);
  @$pb.TagNumber(2)
  void clearKey() => $_clearField(2);
}

class UnlockRequest extends $pb.GeneratedMessage {
  factory UnlockRequest({
    $core.List<$core.int>? key,
  }) {
    final result = create();
    if (key != null) result.key = key;
    return result;
  }

  UnlockRequest._();

  factory UnlockRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory UnlockRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'UnlockRequest',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'v3lockpb'),
      createEmptyInstance: create)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'key', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  UnlockRequest clone() => UnlockRequest()..mergeFromMessage(this);
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  UnlockRequest copyWith(void Function(UnlockRequest) updates) =>
      super.copyWith((message) => updates(message as UnlockRequest))
          as UnlockRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static UnlockRequest create() => UnlockRequest._();
  @$core.override
  UnlockRequest createEmptyInstance() => create();
  static $pb.PbList<UnlockRequest> createRepeated() =>
      $pb.PbList<UnlockRequest>();
  @$core.pragma('dart2js:noInline')
  static UnlockRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<UnlockRequest>(create);
  static UnlockRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get key => $_getN(0);
  @$pb.TagNumber(1)
  set key($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasKey() => $_has(0);
  @$pb.TagNumber(1)
  void clearKey() => $_clearField(1);
}

class UnlockResponse extends $pb.GeneratedMessage {
  factory UnlockResponse({
    $1.ResponseHeader? header,
  }) {
    final result = create();
    if (header != null) result.header = header;
    return result;
  }

  UnlockResponse._();

  factory UnlockResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory UnlockResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'UnlockResponse',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'v3lockpb'),
      createEmptyInstance: create)
    ..aOM<$1.ResponseHeader>(1, _omitFieldNames ? '' : 'header',
        subBuilder: $1.ResponseHeader.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  UnlockResponse clone() => UnlockResponse()..mergeFromMessage(this);
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  UnlockResponse copyWith(void Function(UnlockResponse) updates) =>
      super.copyWith((message) => updates(message as UnlockResponse))
          as UnlockResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static UnlockResponse create() => UnlockResponse._();
  @$core.override
  UnlockResponse createEmptyInstance() => create();
  static $pb.PbList<UnlockResponse> createRepeated() =>
      $pb.PbList<UnlockResponse>();
  @$core.pragma('dart2js:noInline')
  static UnlockResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<UnlockResponse>(create);
  static UnlockResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $1.ResponseHeader get header => $_getN(0);
  @$pb.TagNumber(1)
  set header($1.ResponseHeader value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasHeader() => $_has(0);
  @$pb.TagNumber(1)
  void clearHeader() => $_clearField(1);
  @$pb.TagNumber(1)
  $1.ResponseHeader ensureHeader() => $_ensure(0);
}

const $core.bool _omitFieldNames =
    $core.bool.fromEnvironment('protobuf.omit_field_names');
const $core.bool _omitMessageNames =
    $core.bool.fromEnvironment('protobuf.omit_message_names');
