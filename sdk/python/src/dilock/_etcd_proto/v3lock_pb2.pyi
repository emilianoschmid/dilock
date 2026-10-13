import rpc_pb2 as _rpc_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class LockRequest(_message.Message):
    __slots__ = ("name", "lease")
    NAME_FIELD_NUMBER: _ClassVar[int]
    LEASE_FIELD_NUMBER: _ClassVar[int]
    name: bytes
    lease: int
    def __init__(self, name: _Optional[bytes] = ..., lease: _Optional[int] = ...) -> None: ...

class LockResponse(_message.Message):
    __slots__ = ("header", "key")
    HEADER_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    header: _rpc_pb2.ResponseHeader
    key: bytes
    def __init__(self, header: _Optional[_Union[_rpc_pb2.ResponseHeader, _Mapping]] = ..., key: _Optional[bytes] = ...) -> None: ...

class UnlockRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: bytes
    def __init__(self, key: _Optional[bytes] = ...) -> None: ...

class UnlockResponse(_message.Message):
    __slots__ = ("header",)
    HEADER_FIELD_NUMBER: _ClassVar[int]
    header: _rpc_pb2.ResponseHeader
    def __init__(self, header: _Optional[_Union[_rpc_pb2.ResponseHeader, _Mapping]] = ...) -> None: ...
