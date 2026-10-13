// This is a generated file - do not edit.
//
// Generated from kv.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names

import 'dart:core' as $core;

import 'package:fixnum/fixnum.dart' as $fixnum;
import 'package:protobuf/protobuf.dart' as $pb;

export 'package:protobuf/protobuf.dart' show GeneratedMessageGenericExtensions;

/// Wire fields copied from etcd-io/etcd v3.7.2 api/mvccpb/kv.proto.
class KeyValue extends $pb.GeneratedMessage {
  factory KeyValue({
    $core.List<$core.int>? key,
    $fixnum.Int64? createRevision,
    $fixnum.Int64? modRevision,
    $fixnum.Int64? version,
    $core.List<$core.int>? value,
    $fixnum.Int64? lease,
  }) {
    final result = create();
    if (key != null) result.key = key;
    if (createRevision != null) result.createRevision = createRevision;
    if (modRevision != null) result.modRevision = modRevision;
    if (version != null) result.version = version;
    if (value != null) result.value = value;
    if (lease != null) result.lease = lease;
    return result;
  }

  KeyValue._();

  factory KeyValue.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromBuffer(data, registry);
  factory KeyValue.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      create()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'KeyValue',
      package: const $pb.PackageName(_omitMessageNames ? '' : 'mvccpb'),
      createEmptyInstance: create)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'key', $pb.PbFieldType.OY)
    ..aInt64(2, _omitFieldNames ? '' : 'createRevision')
    ..aInt64(3, _omitFieldNames ? '' : 'modRevision')
    ..aInt64(4, _omitFieldNames ? '' : 'version')
    ..a<$core.List<$core.int>>(
        5, _omitFieldNames ? '' : 'value', $pb.PbFieldType.OY)
    ..aInt64(6, _omitFieldNames ? '' : 'lease')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  KeyValue clone() => KeyValue()..mergeFromMessage(this);
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  KeyValue copyWith(void Function(KeyValue) updates) =>
      super.copyWith((message) => updates(message as KeyValue)) as KeyValue;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static KeyValue create() => KeyValue._();
  @$core.override
  KeyValue createEmptyInstance() => create();
  static $pb.PbList<KeyValue> createRepeated() => $pb.PbList<KeyValue>();
  @$core.pragma('dart2js:noInline')
  static KeyValue getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<KeyValue>(create);
  static KeyValue? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get key => $_getN(0);
  @$pb.TagNumber(1)
  set key($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasKey() => $_has(0);
  @$pb.TagNumber(1)
  void clearKey() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get createRevision => $_getI64(1);
  @$pb.TagNumber(2)
  set createRevision($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasCreateRevision() => $_has(1);
  @$pb.TagNumber(2)
  void clearCreateRevision() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get modRevision => $_getI64(2);
  @$pb.TagNumber(3)
  set modRevision($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasModRevision() => $_has(2);
  @$pb.TagNumber(3)
  void clearModRevision() => $_clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get version => $_getI64(3);
  @$pb.TagNumber(4)
  set version($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasVersion() => $_has(3);
  @$pb.TagNumber(4)
  void clearVersion() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.List<$core.int> get value => $_getN(4);
  @$pb.TagNumber(5)
  set value($core.List<$core.int> value) => $_setBytes(4, value);
  @$pb.TagNumber(5)
  $core.bool hasValue() => $_has(4);
  @$pb.TagNumber(5)
  void clearValue() => $_clearField(5);

  @$pb.TagNumber(6)
  $fixnum.Int64 get lease => $_getI64(5);
  @$pb.TagNumber(6)
  set lease($fixnum.Int64 value) => $_setInt64(5, value);
  @$pb.TagNumber(6)
  $core.bool hasLease() => $_has(5);
  @$pb.TagNumber(6)
  void clearLease() => $_clearField(6);
}

const $core.bool _omitFieldNames =
    $core.bool.fromEnvironment('protobuf.omit_field_names');
const $core.bool _omitMessageNames =
    $core.bool.fromEnvironment('protobuf.omit_message_names');
