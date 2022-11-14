import grpc
from datetime import datetime
from dataclasses import dataclass
from feast.core.CoreService_pb2_grpc import CoreServiceStub
from feast.core.CoreService_pb2 import (
    ListOnlineStoresRequest,
    ListOnlineStoresResponse,
    GetFeatureTableRequest,
)
from feast.core.FeatureTable_pb2 import FeatureTableSpec
from feast_spark.api.JobService_pb2 import (
    StartOfflineToOnlineIngestionJobRequest,
    StartOfflineToOnlineIngestionJobResponse,
    GetJobRequest,
    Job
)
from feast_spark.api.JobService_pb2_grpc import JobServiceStub

@dataclass
class Client:
    registry_url: str
    project: str = None
    _core_service_stub: CoreServiceStub = None
    _job_service_stub: JobServiceStub = None

    @property
    def _core_service(self):
        """
        Creates or returns the gRPC Feast Core Service Stub

        Returns: CoreServiceStub
        """
        if not self._core_service_stub:
            channel = grpc.insecure_channel(self.registry_url)
            self._core_service_stub = CoreServiceStub(channel)
        return self._core_service_stub

    @property
    def _job_service(self):
        """
        Creates or returns the gRPC Feast Job Service Stub

        Returns: JobServiceStub
        """
        if not self._job_service_stub:
            channel = grpc.insecure_channel(self.registry_url)
            self._job_service_stub = JobServiceStub(channel)
        return self._job_service_stub

#  Wrapper methods over rpc services

# Registry/core services"""

    def get_feature_table(self, name: str, project: str = None) -> FeatureTableSpec:
        """
        Retrieves a feature table.

        Args:
            project: Feast project that this feature table belongs to
            name: Name of feature table

        Returns:
            Returns either the requested feature table spec or raises an exception if
            none is found
        """
        if project is None:
            project = self.project

        try:
            response = self._core_service.GetFeatureTable(
                GetFeatureTableRequest(project=project, name=name.strip()),
            )
        except grpc.RpcError:
            raise
        return response.table.spec

    def list_online_stores(self) -> ListOnlineStoresResponse:
        """
        List online stores
        Returns: ListOnlineStoresResponse
        """
        try:
            response = self._core_service.ListOnlineStores(ListOnlineStoresRequest())
        except grpc.RpcError:
            raise
        return response

# Job services

    def start_offline_to_online_ingestion(
        self, feature_table: str, start: datetime, end: datetime, project: str = None, delta_ingestion: bool = False
    ) -> StartOfflineToOnlineIngestionJobResponse:
        """
        Start offline to online ingestion job
        Args:
            feature_table: feature table name
            start: start datetime to filter recoreds to be ingested
            end: end datetime to filter records to be ingested
            project: caraml store project name
            delta_ingestion: boolean setting for delta ingestion

        Returns: StartOfflineToOnlineIngestionJobResponse
        """
        if project is None:
            project = self.project

        request = StartOfflineToOnlineIngestionJobRequest(
            project=project, table_name=feature_table, delta_ingestion=delta_ingestion
        )
        request.start_date.FromDatetime(start)
        request.end_date.FromDatetime(end)
        try:
            response = self._job_service.StartOfflineToOnlineIngestionJob(request)
        except grpc.RpcError:
            raise
        return response

    def get_job(self, job_id: str) -> Job:
        """
        Get job details
        Args:
            job_id: spark job id

        Returns: Job protobuf object
        """
        request = GetJobRequest(job_id=job_id)
        try:
            response = self._job_service.GetJob(request)
        except grpc.RpcError:
            raise
        return response.job
