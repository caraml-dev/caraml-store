package dev.caraml.serving.monitoring;

import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import io.grpc.ServerCall;
import io.grpc.Status;
import io.micrometer.core.instrument.Timer;
import java.util.function.Supplier;
import lombok.Getter;

@Getter
public class ServerCallWithMetricCollectionListener<ReqT, RespT>
    extends SimpleForwardingServerCallListener<ReqT> {
  private ReqT requestMessage;
  private final Metrics<ReqT, RespT> metrics;
  private final Supplier<Status.Code> responseCodeSupplier;
  private final Supplier<RespT> responseMessageSupplier;
  private final Timer.Sample timerSample;

  public ServerCallWithMetricCollectionListener(
      ServerCall.Listener<ReqT> delegate,
      Metrics<ReqT, RespT> metrics,
      Supplier<Status.Code> responseCodeSupplier,
      Supplier<RespT> responseMessageSupplier,
      Timer.Sample timerSample) {
    super(delegate);
    this.metrics = metrics;
    this.responseMessageSupplier = responseMessageSupplier;
    this.responseCodeSupplier = responseCodeSupplier;
    this.timerSample = timerSample;
  }

  @Override
  public void onMessage(ReqT requestMessage) {
    this.requestMessage = requestMessage;
    metrics.onRequestReceived(requestMessage);
    super.onMessage(requestMessage);
  }

  @Override
  public void onComplete() {
    report();
    super.onComplete();
  }

  @Override
  public void onCancel() {
    report();
    super.onCancel();
  }

  private void report() {
    metrics.onResponseSent(
        requestMessage, responseMessageSupplier.get(), responseCodeSupplier.get(), timerSample);
  }
}
