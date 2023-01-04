from typing import Any, Dict, List, Optional
from datetime import datetime
from dataclasses import dataclass

import grpc

from feast.core.CoreService_pb2_grpc import CoreServiceStub
from feast.core.CoreService_pb2 import (
    ListOnlineStoresRequest,
    ListOnlineStoresResponse,
    GetFeatureTableRequest,
)
from feast.core.FeatureTable_pb2 import FeatureTableSpec
from feast.feature import build_feature_references
from feast.online_response import infer_online_entity_rows, OnlineResponse
from feast.serving.ServingService_pb2_grpc import ServingServiceStub
from feast.serving.ServingService_pb2 import GetOnlineFeaturesRequest, GetOnlineFeaturesResponse
from feast_spark.api.JobService_pb2 import (
    StartOfflineToOnlineIngestionJobRequest,
    StartOfflineToOnlineIngestionJobResponse,
    GetJobRequest,
    Job
)
from feast_spark.api.JobService_pb2_grpc import JobServiceStub


@dataclass
class Client:
    registry_url: str = None
    serving_url: str = None
    project: str = None
    _core_service_stub: CoreServiceStub = None
    _serving_service_stub: ServingServiceStub = None
    _job_service_stub: JobServiceStub = None

    @property
    def _core_service(self):
        """
        Creates or returns the gRPC Feast Core Service Stub

        Returns: CoreServiceStub
        """
        if not self.registry_url:
            raise ValueError("registry_url has not been set")

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
        if not self.registry_url:
            raise ValueError("registry_url has not been set")

        if not self._job_service_stub:
            channel = grpc.insecure_channel(self.registry_url)
            self._job_service_stub = JobServiceStub(channel)
        return self._job_service_stub

    @property
    def _serving_service(self):
        """
        Creates or returns the gRPC Feast Serving Service Stub

        Returns: ServingServiceStub
        """
        if not self.serving_url:
            raise ValueError("serving_url has not been set")

        if not self._serving_service_stub:
            channel = grpc.insecure_channel(self.serving_url)
            self._serving_service_stub = ServingServiceStub(channel)
        return self._serving_service_stub

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

        response = self._core_service.GetFeatureTable(
            GetFeatureTableRequest(project=project, name=name.strip()),
        )
        return response.table.spec

    def list_online_stores(self) -> ListOnlineStoresResponse:
        """
        List online stores
        Returns: ListOnlineStoresResponse
        """
        return self._core_service.ListOnlineStores(ListOnlineStoresRequest())

    def get_online_features(
        self,
        feature_refs: List[str],
        entity_rows: List[Dict[str, Any]],
        project: Optional[str] = None,
    ) -> OnlineResponse:
        """
        Retrieves the latest online feature data from Feast Serving.
        Args:
            feature_refs: List of feature references that will be returned for each entity.
                Each feature reference should have the following format:
                "feature_table:feature" where "feature_table" & "feature" refer to
                the feature and feature table names respectively.
                Only the feature name is required.
            entity_rows: A list of dictionaries where each key-value is an entity-name, entity-value pair.
            project: Optionally specify the the project override. If specified, uses given project for retrieval.
                Overrides the projects specified in Feature References if also are specified.
        Returns:
            OnlineResponse containing the feature data in records.
            Each EntityRow provided will yield one record, which contains
            data fields with data value and field status metadata (if included)
        """
        return OnlineResponse(self._serving_service.GetOnlineFeatures(
            GetOnlineFeaturesRequest(
                features=build_feature_references(feature_ref_strs=feature_refs),
                entity_rows=infer_online_entity_rows(entity_rows),
                project=project if project is not None else self.project,
            )
        ))

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
        return self._job_service.StartOfflineToOnlineIngestionJob(request)

    def get_job(self, job_id: str) -> Job:
        """
        Get job details
        Args:
            job_id: spark job id

        Returns: Job protobuf object
        """
        request = GetJobRequest(job_id=job_id)
        response = self._job_service.GetJob(request)
        return response.job
