import kv_pb2 as _kv_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ResponseHeader(_message.Message):
    __slots__ = ("cluster_id", "member_id", "revision", "raft_term")
    CLUSTER_ID_FIELD_NUMBER: _ClassVar[int]
    MEMBER_ID_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    RAFT_TERM_FIELD_NUMBER: _ClassVar[int]
    cluster_id: int
    member_id: int
    revision: int
    raft_term: int
    def __init__(self, cluster_id: _Optional[int] = ..., member_id: _Optional[int] = ..., revision: _Optional[int] = ..., raft_term: _Optional[int] = ...) -> None: ...

class RangeRequest(_message.Message):
    __slots__ = ("key", "range_end", "limit", "revision", "sort_order", "sort_target", "serializable", "keys_only", "count_only", "min_mod_revision", "max_mod_revision", "min_create_revision", "max_create_revision")
    class SortOrder(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        NONE: _ClassVar[RangeRequest.SortOrder]
        ASCEND: _ClassVar[RangeRequest.SortOrder]
        DESCEND: _ClassVar[RangeRequest.SortOrder]
    NONE: RangeRequest.SortOrder
    ASCEND: RangeRequest.SortOrder
    DESCEND: RangeRequest.SortOrder
    class SortTarget(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        KEY: _ClassVar[RangeRequest.SortTarget]
        VERSION: _ClassVar[RangeRequest.SortTarget]
        CREATE: _ClassVar[RangeRequest.SortTarget]
        MOD: _ClassVar[RangeRequest.SortTarget]
        VALUE: _ClassVar[RangeRequest.SortTarget]
    KEY: RangeRequest.SortTarget
    VERSION: RangeRequest.SortTarget
    CREATE: RangeRequest.SortTarget
    MOD: RangeRequest.SortTarget
    VALUE: RangeRequest.SortTarget
    KEY_FIELD_NUMBER: _ClassVar[int]
    RANGE_END_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    SORT_ORDER_FIELD_NUMBER: _ClassVar[int]
    SORT_TARGET_FIELD_NUMBER: _ClassVar[int]
    SERIALIZABLE_FIELD_NUMBER: _ClassVar[int]
    KEYS_ONLY_FIELD_NUMBER: _ClassVar[int]
    COUNT_ONLY_FIELD_NUMBER: _ClassVar[int]
    MIN_MOD_REVISION_FIELD_NUMBER: _ClassVar[int]
    MAX_MOD_REVISION_FIELD_NUMBER: _ClassVar[int]
    MIN_CREATE_REVISION_FIELD_NUMBER: _ClassVar[int]
    MAX_CREATE_REVISION_FIELD_NUMBER: _ClassVar[int]
    key: bytes
    range_end: bytes
    limit: int
    revision: int
    sort_order: RangeRequest.SortOrder
    sort_target: RangeRequest.SortTarget
    serializable: bool
    keys_only: bool
    count_only: bool
    min_mod_revision: int
    max_mod_revision: int
    min_create_revision: int
    max_create_revision: int
    def __init__(self, key: _Optional[bytes] = ..., range_end: _Optional[bytes] = ..., limit: _Optional[int] = ..., revision: _Optional[int] = ..., sort_order: _Optional[_Union[RangeRequest.SortOrder, str]] = ..., sort_target: _Optional[_Union[RangeRequest.SortTarget, str]] = ..., serializable: _Optional[bool] = ..., keys_only: _Optional[bool] = ..., count_only: _Optional[bool] = ..., min_mod_revision: _Optional[int] = ..., max_mod_revision: _Optional[int] = ..., min_create_revision: _Optional[int] = ..., max_create_revision: _Optional[int] = ...) -> None: ...

class RangeResponse(_message.Message):
    __slots__ = ("header", "kvs", "more", "count")
    HEADER_FIELD_NUMBER: _ClassVar[int]
    KVS_FIELD_NUMBER: _ClassVar[int]
    MORE_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    header: ResponseHeader
    kvs: _containers.RepeatedCompositeFieldContainer[_kv_pb2.KeyValue]
    more: bool
    count: int
    def __init__(self, header: _Optional[_Union[ResponseHeader, _Mapping]] = ..., kvs: _Optional[_Iterable[_Union[_kv_pb2.KeyValue, _Mapping]]] = ..., more: _Optional[bool] = ..., count: _Optional[int] = ...) -> None: ...

class LeaseGrantRequest(_message.Message):
    __slots__ = ("TTL", "ID")
    TTL_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    TTL: int
    ID: int
    def __init__(self, TTL: _Optional[int] = ..., ID: _Optional[int] = ...) -> None: ...

class LeaseGrantResponse(_message.Message):
    __slots__ = ("header", "ID", "TTL", "error")
    HEADER_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    TTL_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    header: ResponseHeader
    ID: int
    TTL: int
    error: str
    def __init__(self, header: _Optional[_Union[ResponseHeader, _Mapping]] = ..., ID: _Optional[int] = ..., TTL: _Optional[int] = ..., error: _Optional[str] = ...) -> None: ...

class LeaseRevokeRequest(_message.Message):
    __slots__ = ("ID",)
    ID_FIELD_NUMBER: _ClassVar[int]
    ID: int
    def __init__(self, ID: _Optional[int] = ...) -> None: ...

class LeaseRevokeResponse(_message.Message):
    __slots__ = ("header",)
    HEADER_FIELD_NUMBER: _ClassVar[int]
    header: ResponseHeader
    def __init__(self, header: _Optional[_Union[ResponseHeader, _Mapping]] = ...) -> None: ...

class LeaseKeepAliveRequest(_message.Message):
    __slots__ = ("ID",)
    ID_FIELD_NUMBER: _ClassVar[int]
    ID: int
    def __init__(self, ID: _Optional[int] = ...) -> None: ...

class LeaseKeepAliveResponse(_message.Message):
    __slots__ = ("header", "ID", "TTL")
    HEADER_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    TTL_FIELD_NUMBER: _ClassVar[int]
    header: ResponseHeader
    ID: int
    TTL: int
    def __init__(self, header: _Optional[_Union[ResponseHeader, _Mapping]] = ..., ID: _Optional[int] = ..., TTL: _Optional[int] = ...) -> None: ...
