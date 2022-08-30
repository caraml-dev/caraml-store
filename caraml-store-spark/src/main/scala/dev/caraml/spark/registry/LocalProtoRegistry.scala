package dev.caraml.spark.registry

import java.io.{IOException, ObjectInputStream}

import com.google.protobuf.Descriptors.Descriptor

import collection.mutable
import scala.util.control.NonFatal

class LocalProtoRegistry extends ProtoRegistry {
  @transient
  private var cache: mutable.Map[String, Descriptor] = mutable.Map.empty

  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = {
    try {
      ois.defaultReadObject()
      cache = mutable.Map.empty
    } catch {
      case NonFatal(e) =>
        throw new IOException(e)
    }
  }

  override def getProtoDescriptor(className: String): Descriptor = {
    if (!cache.contains(className)) {
      cache(className) = Class
        .forName(className, true, getClass.getClassLoader)
        .getMethod("getDescriptor")
        .invoke(null)
        .asInstanceOf[Descriptor]
    }

    cache(className)
  }
}
