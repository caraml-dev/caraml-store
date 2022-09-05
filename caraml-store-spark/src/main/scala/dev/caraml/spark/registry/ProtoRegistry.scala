package dev.caraml.spark.registry

import com.google.protobuf.Descriptors.Descriptor

trait ProtoRegistry extends Serializable {
  def getProtoDescriptor(className: String): Descriptor
}
