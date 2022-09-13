package dev.caraml.serving.monitoring;

import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.Status;
import lombok.Getter;

@Getter
public class ServerCallWithMetricCollection<ReqT, RespT>
    extends SimpleForwardingServerCall<ReqT, RespT> {

  private Status.Code responseCode = Status.Code.UNKNOWN;
  private RespT responseMessage;

  public ServerCallWithMetricCollection(ServerCall<ReqT, RespT> delegate) {
    super(delegate);
  }

  @Override
  public void close(final Status status, final Metadata responseHeaders) {
    responseCode = status.getCode();
    super.close(status, responseHeaders);
  }

  @Override
  public void sendMessage(final RespT responseMessage) {
    this.responseMessage = responseMessage;
    super.sendMessage(responseMessage);
  }
}
