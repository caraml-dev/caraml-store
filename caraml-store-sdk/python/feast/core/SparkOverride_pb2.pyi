from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class SparkOverride(_message.Message):
    __slots__ = ["driver_cpu", "driver_memory", "executor_cpu", "executor_memory"]
    DRIVER_CPU_FIELD_NUMBER: _ClassVar[int]
    DRIVER_MEMORY_FIELD_NUMBER: _ClassVar[int]
    EXECUTOR_CPU_FIELD_NUMBER: _ClassVar[int]
    EXECUTOR_MEMORY_FIELD_NUMBER: _ClassVar[int]
    driver_cpu: int
    driver_memory: str
    executor_cpu: int
    executor_memory: str
    def __init__(self, driver_cpu: _Optional[int] = ..., driver_memory: _Optional[str] = ..., executor_cpu: _Optional[int] = ..., executor_memory: _Optional[str] = ...) -> None: ...
