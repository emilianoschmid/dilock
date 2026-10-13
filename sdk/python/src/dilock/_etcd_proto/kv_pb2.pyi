from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class KeyValue(_message.Message):
    __slots__ = ("key", "create_revision", "mod_revision", "version", "value", "lease")
    KEY_FIELD_NUMBER: _ClassVar[int]
    CREATE_REVISION_FIELD_NUMBER: _ClassVar[int]
    MOD_REVISION_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    LEASE_FIELD_NUMBER: _ClassVar[int]
    key: bytes
    create_revision: int
    mod_revision: int
    version: int
    value: bytes
    lease: int
    def __init__(self, key: _Optional[bytes] = ..., create_revision: _Optional[int] = ..., mod_revision: _Optional[int] = ..., version: _Optional[int] = ..., value: _Optional[bytes] = ..., lease: _Optional[int] = ...) -> None: ...
