# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: feast/types/Value.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x17\x66\x65\x61st/types/Value.proto\x12\x0b\x66\x65\x61st.types\"\xe0\x01\n\tValueType\"\xd2\x01\n\x04\x45num\x12\x0b\n\x07INVALID\x10\x00\x12\t\n\x05\x42YTES\x10\x01\x12\n\n\x06STRING\x10\x02\x12\t\n\x05INT32\x10\x03\x12\t\n\x05INT64\x10\x04\x12\n\n\x06\x44OUBLE\x10\x05\x12\t\n\x05\x46LOAT\x10\x06\x12\x08\n\x04\x42OOL\x10\x07\x12\x0e\n\nBYTES_LIST\x10\x0b\x12\x0f\n\x0bSTRING_LIST\x10\x0c\x12\x0e\n\nINT32_LIST\x10\r\x12\x0e\n\nINT64_LIST\x10\x0e\x12\x0f\n\x0b\x44OUBLE_LIST\x10\x0f\x12\x0e\n\nFLOAT_LIST\x10\x10\x12\r\n\tBOOL_LIST\x10\x11\"\x82\x04\n\x05Value\x12\x13\n\tbytes_val\x18\x01 \x01(\x0cH\x00\x12\x14\n\nstring_val\x18\x02 \x01(\tH\x00\x12\x13\n\tint32_val\x18\x03 \x01(\x05H\x00\x12\x13\n\tint64_val\x18\x04 \x01(\x03H\x00\x12\x14\n\ndouble_val\x18\x05 \x01(\x01H\x00\x12\x13\n\tfloat_val\x18\x06 \x01(\x02H\x00\x12\x12\n\x08\x62ool_val\x18\x07 \x01(\x08H\x00\x12\x30\n\x0e\x62ytes_list_val\x18\x0b \x01(\x0b\x32\x16.feast.types.BytesListH\x00\x12\x32\n\x0fstring_list_val\x18\x0c \x01(\x0b\x32\x17.feast.types.StringListH\x00\x12\x30\n\x0eint32_list_val\x18\r \x01(\x0b\x32\x16.feast.types.Int32ListH\x00\x12\x30\n\x0eint64_list_val\x18\x0e \x01(\x0b\x32\x16.feast.types.Int64ListH\x00\x12\x32\n\x0f\x64ouble_list_val\x18\x0f \x01(\x0b\x32\x17.feast.types.DoubleListH\x00\x12\x30\n\x0e\x66loat_list_val\x18\x10 \x01(\x0b\x32\x16.feast.types.FloatListH\x00\x12.\n\rbool_list_val\x18\x11 \x01(\x0b\x32\x15.feast.types.BoolListH\x00\x42\x05\n\x03val\"\x18\n\tBytesList\x12\x0b\n\x03val\x18\x01 \x03(\x0c\"\x19\n\nStringList\x12\x0b\n\x03val\x18\x01 \x03(\t\"\x18\n\tInt32List\x12\x0b\n\x03val\x18\x01 \x03(\x05\"\x18\n\tInt64List\x12\x0b\n\x03val\x18\x01 \x03(\x03\"\x19\n\nDoubleList\x12\x0b\n\x03val\x18\x01 \x03(\x01\"\x18\n\tFloatList\x12\x0b\n\x03val\x18\x01 \x03(\x02\"\x17\n\x08\x42oolList\x12\x0b\n\x03val\x18\x01 \x03(\x08\x42-\n\x1f\x64\x65v.caraml.store.protobuf.typesB\nValueProtob\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'feast.types.Value_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'\n\037dev.caraml.store.protobuf.typesB\nValueProto'
  _VALUETYPE._serialized_start=41
  _VALUETYPE._serialized_end=265
  _VALUETYPE_ENUM._serialized_start=55
  _VALUETYPE_ENUM._serialized_end=265
  _VALUE._serialized_start=268
  _VALUE._serialized_end=782
  _BYTESLIST._serialized_start=784
  _BYTESLIST._serialized_end=808
  _STRINGLIST._serialized_start=810
  _STRINGLIST._serialized_end=835
  _INT32LIST._serialized_start=837
  _INT32LIST._serialized_end=861
  _INT64LIST._serialized_start=863
  _INT64LIST._serialized_end=887
  _DOUBLELIST._serialized_start=889
  _DOUBLELIST._serialized_end=914
  _FLOATLIST._serialized_start=916
  _FLOATLIST._serialized_end=940
  _BOOLLIST._serialized_start=942
  _BOOLLIST._serialized_end=965
# @@protoc_insertion_point(module_scope)