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
    StartOfflineToOnlineIngestionJobResponse
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

    def list_online_stores(self) -> ListOnlineStoresResponse:
        try:
            response = self._core_service.ListOnlineStores(ListOnlineStoresRequest())
        except grpc.RpcError:
            raise
        return response

    def start_offline_to_online_ingestion(
        self, feature_table: str, start: datetime, end: datetime, project: str = None, delta_ingestion: bool = False
    ) -> StartOfflineToOnlineIngestionJobResponse:

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
