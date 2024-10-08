# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from feast_spark.api import JobService_pb2 as feast__spark_dot_api_dot_JobService__pb2


class JobServiceStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.StartOfflineToOnlineIngestionJob = channel.unary_unary(
                '/feast.core.JobService/StartOfflineToOnlineIngestionJob',
                request_serializer=feast__spark_dot_api_dot_JobService__pb2.StartOfflineToOnlineIngestionJobRequest.SerializeToString,
                response_deserializer=feast__spark_dot_api_dot_JobService__pb2.StartOfflineToOnlineIngestionJobResponse.FromString,
                )
        self.GetHistoricalFeatures = channel.unary_unary(
                '/feast.core.JobService/GetHistoricalFeatures',
                request_serializer=feast__spark_dot_api_dot_JobService__pb2.GetHistoricalFeaturesRequest.SerializeToString,
                response_deserializer=feast__spark_dot_api_dot_JobService__pb2.GetHistoricalFeaturesResponse.FromString,
                )
        self.GetJob = channel.unary_unary(
                '/feast.core.JobService/GetJob',
                request_serializer=feast__spark_dot_api_dot_JobService__pb2.GetJobRequest.SerializeToString,
                response_deserializer=feast__spark_dot_api_dot_JobService__pb2.GetJobResponse.FromString,
                )


class JobServiceServicer(object):
    """Missing associated documentation comment in .proto file."""

    def StartOfflineToOnlineIngestionJob(self, request, context):
        """Start job to ingest data from offline store into online store
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetHistoricalFeatures(self, request, context):
        """Produce a training dataset, return a job id that will provide a file reference
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetJob(self, request, context):
        """Get details of a single job
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_JobServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'StartOfflineToOnlineIngestionJob': grpc.unary_unary_rpc_method_handler(
                    servicer.StartOfflineToOnlineIngestionJob,
                    request_deserializer=feast__spark_dot_api_dot_JobService__pb2.StartOfflineToOnlineIngestionJobRequest.FromString,
                    response_serializer=feast__spark_dot_api_dot_JobService__pb2.StartOfflineToOnlineIngestionJobResponse.SerializeToString,
            ),
            'GetHistoricalFeatures': grpc.unary_unary_rpc_method_handler(
                    servicer.GetHistoricalFeatures,
                    request_deserializer=feast__spark_dot_api_dot_JobService__pb2.GetHistoricalFeaturesRequest.FromString,
                    response_serializer=feast__spark_dot_api_dot_JobService__pb2.GetHistoricalFeaturesResponse.SerializeToString,
            ),
            'GetJob': grpc.unary_unary_rpc_method_handler(
                    servicer.GetJob,
                    request_deserializer=feast__spark_dot_api_dot_JobService__pb2.GetJobRequest.FromString,
                    response_serializer=feast__spark_dot_api_dot_JobService__pb2.GetJobResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'feast.core.JobService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class JobService(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def StartOfflineToOnlineIngestionJob(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/feast.core.JobService/StartOfflineToOnlineIngestionJob',
            feast__spark_dot_api_dot_JobService__pb2.StartOfflineToOnlineIngestionJobRequest.SerializeToString,
            feast__spark_dot_api_dot_JobService__pb2.StartOfflineToOnlineIngestionJobResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetHistoricalFeatures(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/feast.core.JobService/GetHistoricalFeatures',
            feast__spark_dot_api_dot_JobService__pb2.GetHistoricalFeaturesRequest.SerializeToString,
            feast__spark_dot_api_dot_JobService__pb2.GetHistoricalFeaturesResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetJob(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/feast.core.JobService/GetJob',
            feast__spark_dot_api_dot_JobService__pb2.GetJobRequest.SerializeToString,
            feast__spark_dot_api_dot_JobService__pb2.GetJobResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
