# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: feast/core/OnlineStore.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1c\x66\x65\x61st/core/OnlineStore.proto\x12\nfeast.core\"U\n\x0bOnlineStore\x12\x0c\n\x04name\x18\x01 \x01(\t\x12#\n\x04type\x18\x02 \x01(\x0e\x32\x15.feast.core.StoreType\x12\x13\n\x0b\x64\x65scription\x18\x03 \x01(\t*/\n\tStoreType\x12\t\n\x05UNSET\x10\x00\x12\x0c\n\x08\x42IGTABLE\x10\x01\x12\t\n\x05REDIS\x10\x02\x42\x32\n\x1e\x64\x65v.caraml.store.protobuf.coreB\x10OnlineStoreProtob\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'feast.core.OnlineStore_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'\n\036dev.caraml.store.protobuf.coreB\020OnlineStoreProto'
  _STORETYPE._serialized_start=131
  _STORETYPE._serialized_end=178
  _ONLINESTORE._serialized_start=44
  _ONLINESTORE._serialized_end=129
# @@protoc_insertion_point(module_scope)
