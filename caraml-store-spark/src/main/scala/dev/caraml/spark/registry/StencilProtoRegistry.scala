package dev.caraml.spark.registry

import com.google.protobuf.Descriptors
import io.odpf.stencil.StencilClientFactory
import io.odpf.stencil.client.StencilClient
import io.odpf.stencil.config.StencilConfig
import org.apache.http.{Header, HttpHeaders}
import org.apache.http.message.BasicHeader

import scala.collection.JavaConverters._

class StencilProtoRegistry(url: String, token: Option[String]) extends ProtoRegistry {
  import StencilProtoRegistry.stencilClient

  override def getProtoDescriptor(className: String): Descriptors.Descriptor = {
    stencilClient(url, token).get(className)
  }
}

object StencilProtoRegistry {
  @transient
  private var _stencilClient: StencilClient = _

  def stencilClient(url: String, token: Option[String]): StencilClient = {
    if (_stencilClient == null) {
      val stencilConfigBuilder = StencilConfig.builder
      for (t <- token) {
        val authHeader = new BasicHeader(HttpHeaders.AUTHORIZATION, "Bearer " + t)
        val headers    = List[Header](authHeader)
        stencilConfigBuilder.fetchHeaders(headers.asJava)
      }
      val stencilConfig = stencilConfigBuilder.build()
      _stencilClient = StencilClientFactory.getClient(url, stencilConfig)
    }
    _stencilClient
  }
}
