// This is a generated file - do not edit.
//
// Generated from rpc.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names

import 'dart:core' as $core;

import 'package:fixnum/fixnum.dart' as $fixnum;
import 'package:protobuf/protobuf.dart' as $pb;

import 'kv.pb.dart' as $1;
import 'rpc.pbenum.dart';

export 'package:protobuf/protobuf.dart' show GeneratedMessageGenericExtensions;

export 'rpc.pbenum.dart';

class ResponseHeader extends $pb.GeneratedMessage {
  factory ResponseHeader({
    $fixnum.Int64? clusterId,
    $fixnum.Int64? memberId,
    $fixnum.Int64? revision,
    $fixnum.Int64? raftTerm,
  }) {
    final result = create();
    if (clusterId != null) result.clusterId = clusterId;
    if (memberId != null) result.memberId = memberId;
    if (revision != null) result.revision = revision;
    if (raftTerm != null) result.raftTerm = raftTerm;
    return result;
  }

  ResponseHeader._();

  factory ResponseHeader.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory ResponseHeader.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ResponseHeader',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'etcdserverpb'),
      createEmptyInstance: create)
    ..a<$fixnum.Int64>(
        1, _omitFieldNames ? '' : 'clusterId', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'memberId', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aInt64(3, _omitFieldNames ? '' : 'revision')
    ..a<$fixnum.Int64>(
        4, _omitFieldNames ? '' : 'raftTerm', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ResponseHeader clone() => ResponseHeader()..mergeFromMessage(this);
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ResponseHeader copyWith(void Function(ResponseHeader) updates) =>
      super.copyWith((message) => updates(message as ResponseHeader))
          as ResponseHeader;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static ResponseHeader create() => ResponseHeader._();
  @$core.override
  ResponseHeader createEmptyInstance() => create();
  static $pb.PbList<ResponseHeader> createRepeated() =>
      $pb.PbList<ResponseHeader>();
  @$core.pragma('dart2js:noInline')
  static ResponseHeader getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ResponseHeader>(create);
  static ResponseHeader? _defaultInstance;

  @$pb.TagNumber(1)
  $fixnum.Int64 get clusterId => $_getI64(0);
  @$pb.TagNumber(1)
  set clusterId($fixnum.Int64 value) => $_setInt64(0, value);
  @$pb.TagNumber(1)
  $core.bool hasClusterId() => $_has(0);
  @$pb.TagNumber(1)
  void clearClusterId() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get memberId => $_getI64(1);
  @$pb.TagNumber(2)
  set memberId($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasMemberId() => $_has(1);
  @$pb.TagNumber(2)
  void clearMemberId() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get revision => $_getI64(2);
  @$pb.TagNumber(3)
  set revision($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasRevision() => $_has(2);
  @$pb.TagNumber(3)
  void clearRevision() => $_clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get raftTerm => $_getI64(3);
  @$pb.TagNumber(4)
  set raftTerm($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasRaftTerm() => $_has(3);
  @$pb.TagNumber(4)
  void clearRaftTerm() => $_clearField(4);
}

class RangeRequest extends $pb.GeneratedMessage {
  factory RangeRequest({
    $core.List<$core.int>? key,
    $core.List<$core.int>? rangeEnd,
    $fixnum.Int64? limit,
    $fixnum.Int64? revision,
    RangeRequest_SortOrder? sortOrder,
    RangeRequest_SortTarget? sortTarget,
    $core.bool? serializable,
    $core.bool? keysOnly,
    $core.bool? countOnly,
    $fixnum.Int64? minModRevision,
    $fixnum.Int64? maxModRevision,
    $fixnum.Int64? minCreateRevision,
    $fixnum.Int64? maxCreateRevision,
  }) {
    final result = create();
    if (key != null) result.key = key;
    if (rangeEnd != null) result.rangeEnd = rangeEnd;
    if (limit != null) result.limit = limit;
    if (revision != null) result.revision = revision;
    if (sortOrder != null) result.sortOrder = sortOrder;
    if (sortTarget != null) result.sortTarget = sortTarget;
    if (serializable != null) result.serializable = serializable;
    if (keysOnly != null) result.keysOnly = keysOnly;
    if (countOnly != null) result.countOnly = countOnly;
    if (minModRevision != null) result.minModRevision = minModRevision;
    if (maxModRevision != null) result.maxModRevision = maxModRevision;
    if (minCreateRevision != null) result.minCreateRevision = minCreateRevision;
    if (maxCreateRevision != null) result.maxCreateRevision = maxCreateRevision;
    return result;
  }

  RangeRequest._();

  factory RangeRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory RangeRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'RangeRequest',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'etcdserverpb'),
      createEmptyInstance: create)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'key', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'rangeEnd', $pb.PbFieldType.OY)
    ..aInt64(3, _omitFieldNames ? '' : 'limit')
    ..aInt64(4, _omitFieldNames ? '' : 'revision')
    ..e<RangeRequest_SortOrder>(
        5, _omitFieldNames ? '' : 'sortOrder', $pb.PbFieldType.OE,
        defaultOrMaker: RangeRequest_SortOrder.NONE,
        valueOf: RangeRequest_SortOrder.valueOf,
        enumValues: RangeRequest_SortOrder.values)
    ..e<RangeRequest_SortTarget>(
        6, _omitFieldNames ? '' : 'sortTarget', $pb.PbFieldType.OE,
        defaultOrMaker: RangeRequest_SortTarget.KEY,
        valueOf: RangeRequest_SortTarget.valueOf,
        enumValues: RangeRequest_SortTarget.values)
    ..aOB(7, _omitFieldNames ? '' : 'serializable')
    ..aOB(8, _omitFieldNames ? '' : 'keysOnly')
    ..aOB(9, _omitFieldNames ? '' : 'countOnly')
    ..aInt64(10, _omitFieldNames ? '' : 'minModRevision')
    ..aInt64(11, _omitFieldNames ? '' : 'maxModRevision')
    ..aInt64(12, _omitFieldNames ? '' : 'minCreateRevision')
    ..aInt64(13, _omitFieldNames ? '' : 'maxCreateRevision')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RangeRequest clone() => RangeRequest()..mergeFromMessage(this);
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RangeRequest copyWith(void Function(RangeRequest) updates) =>
      super.copyWith((message) => updates(message as RangeRequest))
          as RangeRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static RangeRequest create() => RangeRequest._();
  @$core.override
  RangeRequest createEmptyInstance() => create();
  static $pb.PbList<RangeRequest> createRepeated() =>
      $pb.PbList<RangeRequest>();
  @$core.pragma('dart2js:noInline')
  static RangeRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<RangeRequest>(create);
  static RangeRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get key => $_getN(0);
  @$pb.TagNumber(1)
  set key($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasKey() => $_has(0);
  @$pb.TagNumber(1)
  void clearKey() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get rangeEnd => $_getN(1);
  @$pb.TagNumber(2)
  set rangeEnd($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasRangeEnd() => $_has(1);
  @$pb.TagNumber(2)
  void clearRangeEnd() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get limit => $_getI64(2);
  @$pb.TagNumber(3)
  set limit($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasLimit() => $_has(2);
  @$pb.TagNumber(3)
  void clearLimit() => $_clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get revision => $_getI64(3);
  @$pb.TagNumber(4)
  set revision($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasRevision() => $_has(3);
  @$pb.TagNumber(4)
  void clearRevision() => $_clearField(4);

  @$pb.TagNumber(5)
  RangeRequest_SortOrder get sortOrder => $_getN(4);
  @$pb.TagNumber(5)
  set sortOrder(RangeRequest_SortOrder value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasSortOrder() => $_has(4);
  @$pb.TagNumber(5)
  void clearSortOrder() => $_clearField(5);

  @$pb.TagNumber(6)
  RangeRequest_SortTarget get sortTarget => $_getN(5);
  @$pb.TagNumber(6)
  set sortTarget(RangeRequest_SortTarget value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasSortTarget() => $_has(5);
  @$pb.TagNumber(6)
  void clearSortTarget() => $_clearField(6);

  @$pb.TagNumber(7)
  $core.bool get serializable => $_getBF(6);
  @$pb.TagNumber(7)
  set serializable($core.bool value) => $_setBool(6, value);
  @$pb.TagNumber(7)
  $core.bool hasSerializable() => $_has(6);
  @$pb.TagNumber(7)
  void clearSerializable() => $_clearField(7);

  @$pb.TagNumber(8)
  $core.bool get keysOnly => $_getBF(7);
  @$pb.TagNumber(8)
  set keysOnly($core.bool value) => $_setBool(7, value);
  @$pb.TagNumber(8)
  $core.bool hasKeysOnly() => $_has(7);
  @$pb.TagNumber(8)
  void clearKeysOnly() => $_clearField(8);

  @$pb.TagNumber(9)
  $core.bool get countOnly => $_getBF(8);
  @$pb.TagNumber(9)
  set countOnly($core.bool value) => $_setBool(8, value);
  @$pb.TagNumber(9)
  $core.bool hasCountOnly() => $_has(8);
  @$pb.TagNumber(9)
  void clearCountOnly() => $_clearField(9);

  @$pb.TagNumber(10)
  $fixnum.Int64 get minModRevision => $_getI64(9);
  @$pb.TagNumber(10)
  set minModRevision($fixnum.Int64 value) => $_setInt64(9, value);
  @$pb.TagNumber(10)
  $core.bool hasMinModRevision() => $_has(9);
  @$pb.TagNumber(10)
  void clearMinModRevision() => $_clearField(10);

  @$pb.TagNumber(11)
  $fixnum.Int64 get maxModRevision => $_getI64(10);
  @$pb.TagNumber(11)
  set maxModRevision($fixnum.Int64 value) => $_setInt64(10, value);
  @$pb.TagNumber(11)
  $core.bool hasMaxModRevision() => $_has(10);
  @$pb.TagNumber(11)
  void clearMaxModRevision() => $_clearField(11);

  @$pb.TagNumber(12)
  $fixnum.Int64 get minCreateRevision => $_getI64(11);
  @$pb.TagNumber(12)
  set minCreateRevision($fixnum.Int64 value) => $_setInt64(11, value);
  @$pb.TagNumber(12)
  $core.bool hasMinCreateRevision() => $_has(11);
  @$pb.TagNumber(12)
  void clearMinCreateRevision() => $_clearField(12);

  @$pb.TagNumber(13)
  $fixnum.Int64 get maxCreateRevision => $_getI64(12);
  @$pb.TagNumber(13)
  set maxCreateRevision($fixnum.Int64 value) => $_setInt64(12, value);
  @$pb.TagNumber(13)
  $core.bool hasMaxCreateRevision() => $_has(12);
  @$pb.TagNumber(13)
  void clearMaxCreateRevision() => $_clearField(13);
}

class RangeResponse extends $pb.GeneratedMessage {
  factory RangeResponse({
    ResponseHeader? header,
    $core.Iterable<$1.KeyValue>? kvs,
    $core.bool? more,
    $fixnum.Int64? count,
  }) {
    final result = create();
    if (header != null) result.header = header;
    if (kvs != null) result.kvs.addAll(kvs);
    if (more != null) result.more = more;
    if (count != null) result.count = count;
    return result;
  }

  RangeResponse._();

  factory RangeResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory RangeResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'RangeResponse',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'etcdserverpb'),
      createEmptyInstance: create)
    ..aOM<ResponseHeader>(1, _omitFieldNames ? '' : 'header',
        subBuilder: ResponseHeader.create)
    ..pc<$1.KeyValue>(2, _omitFieldNames ? '' : 'kvs', $pb.PbFieldType.PM,
        subBuilder: $1.KeyValue.create)
    ..aOB(3, _omitFieldNames ? '' : 'more')
    ..aInt64(4, _omitFieldNames ? '' : 'count')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RangeResponse clone() => RangeResponse()..mergeFromMessage(this);
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RangeResponse copyWith(void Function(RangeResponse) updates) =>
      super.copyWith((message) => updates(message as RangeResponse))
          as RangeResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static RangeResponse create() => RangeResponse._();
  @$core.override
  RangeResponse createEmptyInstance() => create();
  static $pb.PbList<RangeResponse> createRepeated() =>
      $pb.PbList<RangeResponse>();
  @$core.pragma('dart2js:noInline')
  static RangeResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<RangeResponse>(create);
  static RangeResponse? _defaultInstance;

  @$pb.TagNumber(1)
  ResponseHeader get header => $_getN(0);
  @$pb.TagNumber(1)
  set header(ResponseHeader value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasHeader() => $_has(0);
  @$pb.TagNumber(1)
  void clearHeader() => $_clearField(1);
  @$pb.TagNumber(1)
  ResponseHeader ensureHeader() => $_ensure(0);

  @$pb.TagNumber(2)
  $pb.PbList<$1.KeyValue> get kvs => $_getList(1);

  @$pb.TagNumber(3)
  $core.bool get more => $_getBF(2);
  @$pb.TagNumber(3)
  set more($core.bool value) => $_setBool(2, value);
  @$pb.TagNumber(3)
  $core.bool hasMore() => $_has(2);
  @$pb.TagNumber(3)
  void clearMore() => $_clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get count => $_getI64(3);
  @$pb.TagNumber(4)
  set count($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasCount() => $_has(3);
  @$pb.TagNumber(4)
  void clearCount() => $_clearField(4);
}

class LeaseGrantRequest extends $pb.GeneratedMessage {
  factory LeaseGrantRequest({
    $fixnum.Int64? tTL,
    $fixnum.Int64? iD,
  }) {
    final result = create();
    if (tTL != null) result.tTL = tTL;
    if (iD != null) result.iD = iD;
    return result;
  }

  LeaseGrantRequest._();

  factory LeaseGrantRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory LeaseGrantRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'LeaseGrantRequest',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'etcdserverpb'),
      createEmptyInstance: create)
    ..aInt64(1, _omitFieldNames ? '' : 'TTL', protoName: 'TTL')
    ..aInt64(2, _omitFieldNames ? '' : 'ID', protoName: 'ID')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  LeaseGrantRequest clone() => LeaseGrantRequest()..mergeFromMessage(this);
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  LeaseGrantRequest copyWith(void Function(LeaseGrantRequest) updates) =>
      super.copyWith((message) => updates(message as LeaseGrantRequest))
          as LeaseGrantRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static LeaseGrantRequest create() => LeaseGrantRequest._();
  @$core.override
  LeaseGrantRequest createEmptyInstance() => create();
  static $pb.PbList<LeaseGrantRequest> createRepeated() =>
      $pb.PbList<LeaseGrantRequest>();
  @$core.pragma('dart2js:noInline')
  static LeaseGrantRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<LeaseGrantRequest>(create);
  static LeaseGrantRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $fixnum.Int64 get tTL => $_getI64(0);
  @$pb.TagNumber(1)
  set tTL($fixnum.Int64 value) => $_setInt64(0, value);
  @$pb.TagNumber(1)
  $core.bool hasTTL() => $_has(0);
  @$pb.TagNumber(1)
  void clearTTL() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get iD => $_getI64(1);
  @$pb.TagNumber(2)
  set iD($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasID() => $_has(1);
  @$pb.TagNumber(2)
  void clearID() => $_clearField(2);
}

class LeaseGrantResponse extends $pb.GeneratedMessage {
  factory LeaseGrantResponse({
    ResponseHeader? header,
    $fixnum.Int64? iD,
    $fixnum.Int64? tTL,
    $core.String? error,
  }) {
    final result = create();
    if (header != null) result.header = header;
    if (iD != null) result.iD = iD;
    if (tTL != null) result.tTL = tTL;
    if (error != null) result.error = error;
    return result;
  }

  LeaseGrantResponse._();

  factory LeaseGrantResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory LeaseGrantResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'LeaseGrantResponse',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'etcdserverpb'),
      createEmptyInstance: create)
    ..aOM<ResponseHeader>(1, _omitFieldNames ? '' : 'header',
        subBuilder: ResponseHeader.create)
    ..aInt64(2, _omitFieldNames ? '' : 'ID', protoName: 'ID')
    ..aInt64(3, _omitFieldNames ? '' : 'TTL', protoName: 'TTL')
    ..aOS(4, _omitFieldNames ? '' : 'error')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  LeaseGrantResponse clone() => LeaseGrantResponse()..mergeFromMessage(this);
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  LeaseGrantResponse copyWith(void Function(LeaseGrantResponse) updates) =>
      super.copyWith((message) => updates(message as LeaseGrantResponse))
          as LeaseGrantResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static LeaseGrantResponse create() => LeaseGrantResponse._();
  @$core.override
  LeaseGrantResponse createEmptyInstance() => create();
  static $pb.PbList<LeaseGrantResponse> createRepeated() =>
      $pb.PbList<LeaseGrantResponse>();
  @$core.pragma('dart2js:noInline')
  static LeaseGrantResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<LeaseGrantResponse>(create);
  static LeaseGrantResponse? _defaultInstance;

  @$pb.TagNumber(1)
  ResponseHeader get header => $_getN(0);
  @$pb.TagNumber(1)
  set header(ResponseHeader value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasHeader() => $_has(0);
  @$pb.TagNumber(1)
  void clearHeader() => $_clearField(1);
  @$pb.TagNumber(1)
  ResponseHeader ensureHeader() => $_ensure(0);

  @$pb.TagNumber(2)
  $fixnum.Int64 get iD => $_getI64(1);
  @$pb.TagNumber(2)
  set iD($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasID() => $_has(1);
  @$pb.TagNumber(2)
  void clearID() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get tTL => $_getI64(2);
  @$pb.TagNumber(3)
  set tTL($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasTTL() => $_has(2);
  @$pb.TagNumber(3)
  void clearTTL() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.String get error => $_getSZ(3);
  @$pb.TagNumber(4)
  set error($core.String value) => $_setString(3, value);
  @$pb.TagNumber(4)
  $core.bool hasError() => $_has(3);
  @$pb.TagNumber(4)
  void clearError() => $_clearField(4);
}

class LeaseRevokeRequest extends $pb.GeneratedMessage {
  factory LeaseRevokeRequest({
    $fixnum.Int64? iD,
  }) {
    final result = create();
    if (iD != null) result.iD = iD;
    return result;
  }

  LeaseRevokeRequest._();

  factory LeaseRevokeRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory LeaseRevokeRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'LeaseRevokeRequest',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'etcdserverpb'),
      createEmptyInstance: create)
    ..aInt64(1, _omitFieldNames ? '' : 'ID', protoName: 'ID')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  LeaseRevokeRequest clone() => LeaseRevokeRequest()..mergeFromMessage(this);
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  LeaseRevokeRequest copyWith(void Function(LeaseRevokeRequest) updates) =>
      super.copyWith((message) => updates(message as LeaseRevokeRequest))
          as LeaseRevokeRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static LeaseRevokeRequest create() => LeaseRevokeRequest._();
  @$core.override
  LeaseRevokeRequest createEmptyInstance() => create();
  static $pb.PbList<LeaseRevokeRequest> createRepeated() =>
      $pb.PbList<LeaseRevokeRequest>();
  @$core.pragma('dart2js:noInline')
  static LeaseRevokeRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<LeaseRevokeRequest>(create);
  static LeaseRevokeRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $fixnum.Int64 get iD => $_getI64(0);
  @$pb.TagNumber(1)
  set iD($fixnum.Int64 value) => $_setInt64(0, value);
  @$pb.TagNumber(1)
  $core.bool hasID() => $_has(0);
  @$pb.TagNumber(1)
  void clearID() => $_clearField(1);
}

class LeaseRevokeResponse extends $pb.GeneratedMessage {
  factory LeaseRevokeResponse({
    ResponseHeader? header,
  }) {
    final result = create();
    if (header != null) result.header = header;
    return result;
  }

  LeaseRevokeResponse._();

  factory LeaseRevokeResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory LeaseRevokeResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'LeaseRevokeResponse',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'etcdserverpb'),
      createEmptyInstance: create)
    ..aOM<ResponseHeader>(1, _omitFieldNames ? '' : 'header',
        subBuilder: ResponseHeader.create)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  LeaseRevokeResponse clone() => LeaseRevokeResponse()..mergeFromMessage(this);
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  LeaseRevokeResponse copyWith(void Function(LeaseRevokeResponse) updates) =>
      super.copyWith((message) => updates(message as LeaseRevokeResponse))
          as LeaseRevokeResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static LeaseRevokeResponse create() => LeaseRevokeResponse._();
  @$core.override
  LeaseRevokeResponse createEmptyInstance() => create();
  static $pb.PbList<LeaseRevokeResponse> createRepeated() =>
      $pb.PbList<LeaseRevokeResponse>();
  @$core.pragma('dart2js:noInline')
  static LeaseRevokeResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<LeaseRevokeResponse>(create);
  static LeaseRevokeResponse? _defaultInstance;

  @$pb.TagNumber(1)
  ResponseHeader get header => $_getN(0);
  @$pb.TagNumber(1)
  set header(ResponseHeader value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasHeader() => $_has(0);
  @$pb.TagNumber(1)
  void clearHeader() => $_clearField(1);
  @$pb.TagNumber(1)
  ResponseHeader ensureHeader() => $_ensure(0);
}

class LeaseKeepAliveRequest extends $pb.GeneratedMessage {
  factory LeaseKeepAliveRequest({
    $fixnum.Int64? iD,
  }) {
    final result = create();
    if (iD != null) result.iD = iD;
    return result;
  }

  LeaseKeepAliveRequest._();

  factory LeaseKeepAliveRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory LeaseKeepAliveRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'LeaseKeepAliveRequest',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'etcdserverpb'),
      createEmptyInstance: create)
    ..aInt64(1, _omitFieldNames ? '' : 'ID', protoName: 'ID')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  LeaseKeepAliveRequest clone() =>
      LeaseKeepAliveRequest()..mergeFromMessage(this);
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  LeaseKeepAliveRequest copyWith(
          void Function(LeaseKeepAliveRequest) updates) =>
      super.copyWith((message) => updates(message as LeaseKeepAliveRequest))
          as LeaseKeepAliveRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static LeaseKeepAliveRequest create() => LeaseKeepAliveRequest._();
  @$core.override
  LeaseKeepAliveRequest createEmptyInstance() => create();
  static $pb.PbList<LeaseKeepAliveRequest> createRepeated() =>
      $pb.PbList<LeaseKeepAliveRequest>();
  @$core.pragma('dart2js:noInline')
  static LeaseKeepAliveRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<LeaseKeepAliveRequest>(create);
  static LeaseKeepAliveRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $fixnum.Int64 get iD => $_getI64(0);
  @$pb.TagNumber(1)
  set iD($fixnum.Int64 value) => $_setInt64(0, value);
  @$pb.TagNumber(1)
  $core.bool hasID() => $_has(0);
  @$pb.TagNumber(1)
  void clearID() => $_clearField(1);
}

class LeaseKeepAliveResponse extends $pb.GeneratedMessage {
  factory LeaseKeepAliveResponse({
    ResponseHeader? header,
    $fixnum.Int64? iD,
    $fixnum.Int64? tTL,
  }) {
    final result = create();
    if (header != null) result.header = header;
    if (iD != null) result.iD = iD;
    if (tTL != null) result.tTL = tTL;
    return result;
  }

  LeaseKeepAliveResponse._();

  factory LeaseKeepAliveResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory LeaseKeepAliveResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'LeaseKeepAliveResponse',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'etcdserverpb'),
      createEmptyInstance: create)
    ..aOM<ResponseHeader>(1, _omitFieldNames ? '' : 'header',
        subBuilder: ResponseHeader.create)
    ..aInt64(2, _omitFieldNames ? '' : 'ID', protoName: 'ID')
    ..aInt64(3, _omitFieldNames ? '' : 'TTL', protoName: 'TTL')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  LeaseKeepAliveResponse clone() =>
      LeaseKeepAliveResponse()..mergeFromMessage(this);
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  LeaseKeepAliveResponse copyWith(
          void Function(LeaseKeepAliveResponse) updates) =>
      super.copyWith((message) => updates(message as LeaseKeepAliveResponse))
          as LeaseKeepAliveResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static LeaseKeepAliveResponse create() => LeaseKeepAliveResponse._();
  @$core.override
  LeaseKeepAliveResponse createEmptyInstance() => create();
  static $pb.PbList<LeaseKeepAliveResponse> createRepeated() =>
      $pb.PbList<LeaseKeepAliveResponse>();
  @$core.pragma('dart2js:noInline')
  static LeaseKeepAliveResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<LeaseKeepAliveResponse>(create);
  static LeaseKeepAliveResponse? _defaultInstance;

  @$pb.TagNumber(1)
  ResponseHeader get header => $_getN(0);
  @$pb.TagNumber(1)
  set header(ResponseHeader value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasHeader() => $_has(0);
  @$pb.TagNumber(1)
  void clearHeader() => $_clearField(1);
  @$pb.TagNumber(1)
  ResponseHeader ensureHeader() => $_ensure(0);

  @$pb.TagNumber(2)
  $fixnum.Int64 get iD => $_getI64(1);
  @$pb.TagNumber(2)
  set iD($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasID() => $_has(1);
  @$pb.TagNumber(2)
  void clearID() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get tTL => $_getI64(2);
  @$pb.TagNumber(3)
  set tTL($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasTTL() => $_has(2);
  @$pb.TagNumber(3)
  void clearTTL() => $_clearField(3);
}

const $core.bool _omitFieldNames =
    $core.bool.fromEnvironment('protobuf.omit_field_names');
const $core.bool _omitMessageNames =
    $core.bool.fromEnvironment('protobuf.omit_message_names');
