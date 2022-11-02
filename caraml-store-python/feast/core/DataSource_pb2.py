# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: feast/core/DataSource.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from feast.core import DataFormat_pb2 as feast_dot_core_dot_DataFormat__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1b\x66\x65\x61st/core/DataSource.proto\x12\nfeast.core\x1a\x1b\x66\x65\x61st/core/DataFormat.proto\"\xcc\x07\n\nDataSource\x12/\n\x04type\x18\x01 \x01(\x0e\x32!.feast.core.DataSource.SourceType\x12?\n\rfield_mapping\x18\x02 \x03(\x0b\x32(.feast.core.DataSource.FieldMappingEntry\x12\x1e\n\x16\x65vent_timestamp_column\x18\x03 \x01(\t\x12\x1d\n\x15\x64\x61te_partition_column\x18\x04 \x01(\t\x12 \n\x18\x63reated_timestamp_column\x18\x05 \x01(\t\x12:\n\x0c\x66ile_options\x18\x0b \x01(\x0b\x32\".feast.core.DataSource.FileOptionsH\x00\x12\x42\n\x10\x62igquery_options\x18\x0c \x01(\x0b\x32&.feast.core.DataSource.BigQueryOptionsH\x00\x12<\n\rkafka_options\x18\r \x01(\x0b\x32#.feast.core.DataSource.KafkaOptionsH\x00\x12@\n\x0fkinesis_options\x18\x0e \x01(\x0b\x32%.feast.core.DataSource.KinesisOptionsH\x00\x1a\x33\n\x11\x46ieldMappingEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x1aL\n\x0b\x46ileOptions\x12+\n\x0b\x66ile_format\x18\x01 \x01(\x0b\x32\x16.feast.core.FileFormat\x12\x10\n\x08\x66ile_url\x18\x02 \x01(\t\x1a$\n\x0f\x42igQueryOptions\x12\x11\n\ttable_ref\x18\x01 \x01(\t\x1aj\n\x0cKafkaOptions\x12\x19\n\x11\x62ootstrap_servers\x18\x01 \x01(\t\x12\r\n\x05topic\x18\x02 \x01(\t\x12\x30\n\x0emessage_format\x18\x03 \x01(\x0b\x32\x18.feast.core.StreamFormat\x1a\x66\n\x0eKinesisOptions\x12\x0e\n\x06region\x18\x01 \x01(\t\x12\x13\n\x0bstream_name\x18\x02 \x01(\t\x12/\n\rrecord_format\x18\x03 \x01(\x0b\x32\x18.feast.core.StreamFormat\"c\n\nSourceType\x12\x0b\n\x07INVALID\x10\x00\x12\x0e\n\nBATCH_FILE\x10\x01\x12\x12\n\x0e\x42\x41TCH_BIGQUERY\x10\x02\x12\x10\n\x0cSTREAM_KAFKA\x10\x03\x12\x12\n\x0eSTREAM_KINESIS\x10\x04\x42\t\n\x07optionsB1\n\x1e\x64\x65v.caraml.store.protobuf.coreB\x0f\x44\x61taSourceProtob\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'feast.core.DataSource_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'\n\036dev.caraml.store.protobuf.coreB\017DataSourceProto'
  _DATASOURCE_FIELDMAPPINGENTRY._options = None
  _DATASOURCE_FIELDMAPPINGENTRY._serialized_options = b'8\001'
  _DATASOURCE._serialized_start=73
  _DATASOURCE._serialized_end=1045
  _DATASOURCE_FIELDMAPPINGENTRY._serialized_start=554
  _DATASOURCE_FIELDMAPPINGENTRY._serialized_end=605
  _DATASOURCE_FILEOPTIONS._serialized_start=607
  _DATASOURCE_FILEOPTIONS._serialized_end=683
  _DATASOURCE_BIGQUERYOPTIONS._serialized_start=685
  _DATASOURCE_BIGQUERYOPTIONS._serialized_end=721
  _DATASOURCE_KAFKAOPTIONS._serialized_start=723
  _DATASOURCE_KAFKAOPTIONS._serialized_end=829
  _DATASOURCE_KINESISOPTIONS._serialized_start=831
  _DATASOURCE_KINESISOPTIONS._serialized_end=933
  _DATASOURCE_SOURCETYPE._serialized_start=935
  _DATASOURCE_SOURCETYPE._serialized_end=1034
# @@protoc_insertion_point(module_scope)