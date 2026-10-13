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

import 'package:protobuf/protobuf.dart' as $pb;

class RangeRequest_SortOrder extends $pb.ProtobufEnum {
  static const RangeRequest_SortOrder NONE =
      RangeRequest_SortOrder._(0, _omitEnumNames ? '' : 'NONE');
  static const RangeRequest_SortOrder ASCEND =
      RangeRequest_SortOrder._(1, _omitEnumNames ? '' : 'ASCEND');
  static const RangeRequest_SortOrder DESCEND =
      RangeRequest_SortOrder._(2, _omitEnumNames ? '' : 'DESCEND');

  static const $core.List<RangeRequest_SortOrder> values =
      <RangeRequest_SortOrder>[
    NONE,
    ASCEND,
    DESCEND,
  ];

  static final $core.List<RangeRequest_SortOrder?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 2);
  static RangeRequest_SortOrder? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const RangeRequest_SortOrder._(super.value, super.name);
}

class RangeRequest_SortTarget extends $pb.ProtobufEnum {
  static const RangeRequest_SortTarget KEY =
      RangeRequest_SortTarget._(0, _omitEnumNames ? '' : 'KEY');
  static const RangeRequest_SortTarget VERSION =
      RangeRequest_SortTarget._(1, _omitEnumNames ? '' : 'VERSION');
  static const RangeRequest_SortTarget CREATE =
      RangeRequest_SortTarget._(2, _omitEnumNames ? '' : 'CREATE');
  static const RangeRequest_SortTarget MOD =
      RangeRequest_SortTarget._(3, _omitEnumNames ? '' : 'MOD');
  static const RangeRequest_SortTarget VALUE =
      RangeRequest_SortTarget._(4, _omitEnumNames ? '' : 'VALUE');

  static const $core.List<RangeRequest_SortTarget> values =
      <RangeRequest_SortTarget>[
    KEY,
    VERSION,
    CREATE,
    MOD,
    VALUE,
  ];

  static final $core.List<RangeRequest_SortTarget?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 4);
  static RangeRequest_SortTarget? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const RangeRequest_SortTarget._(super.value, super.name);
}

const $core.bool _omitEnumNames =
    $core.bool.fromEnvironment('protobuf.omit_enum_names');
