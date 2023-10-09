from typing import Any, Dict, List, Union, Optional
from datetime import datetime
from dataclasses import dataclass

import grpc
from croniter import croniter

from feast.core.CoreService_pb2_grpc import CoreServiceStub
from feast.core.CoreService_pb2 import (
    ListOnlineStoresRequest,
    ListOnlineStoresResponse,
    GetFeatureTableRequest,
    ApplyFeatureTableRequest,
    ListProjectsRequest,
    ListFeatureTablesRequest,
    DeleteFeatureTableRequest,
    GetEntityRequest,
    ListEntitiesRequest,
    ApplyEntityRequest,
)
from feast.data_source import FileSource, BigQuerySource
from feast.entity import Entity
from feast.feature import build_feature_references
from feast.feature_table import FeatureTable
from feast.online_response import infer_online_entity_rows, OnlineResponse
from feast.serving.ServingService_pb2_grpc import ServingServiceStub
from feast.serving.ServingService_pb2 import GetOnlineFeaturesRequest
from feast_spark.api.JobService_pb2 import (
    StartOfflineToOnlineIngestionJobRequest,
    StartOfflineToOnlineIngestionJobResponse,
    GetHistoricalFeaturesRequest,
    GetHistoricalFeaturesResponse,
    GetJobRequest,
    ListJobsRequest,
    Job,
    ScheduleOfflineToOnlineIngestionJobRequest,
)
from feast_spark.api.JobService_pb2_grpc import JobServiceStub


@dataclass
class Client:
    registry_url: Optional[str] = None
    serving_url: Optional[str] = None
    _core_service_stub: Optional[CoreServiceStub] = None
    _serving_service_stub: Optional[ServingServiceStub] = None
    _job_service_stub: Optional[JobServiceStub] = None

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

    def list_projects(self) -> List[str]:
        """
        List all active Feast projects
        Returns:
            List of project names
        """

        response = self._core_service.ListProjects(
            ListProjectsRequest(),
        )
        return list(response.projects)

    def apply_entity(self, entity: Entity, project: str):
        """
        Registers a single entity with Feast
        Args:
            entity: Entity that will be registered
            project: Project name
        """

        entity.is_valid()
        entity_proto = entity.to_spec_proto()

        # Convert the entity to a request and send to Feast Core
        apply_entity_response = self._core_service.ApplyEntity(
            ApplyEntityRequest(project=project, spec=entity_proto),
        )

        # Extract the returned entity
        applied_entity = Entity.from_proto(apply_entity_response.entity)

        # Deep copy from the returned entity to the local entity
        entity._update_from_entity(applied_entity)

    def list_entities(
        self, project: str = "", labels: Optional[Dict[str, str]] = None
    ) -> List[Entity]:
        """
        Retrieve a list of entities from Feast Core
        Args:
            project: Filter entities based on project name
            labels: User-defined labels that these entities are associated with
        Returns:
            List of entities
        """

        filter = ListEntitiesRequest.Filter(project=project, labels=labels or dict())

        # Get latest entities from Feast Core
        entity_protos = self._core_service.ListEntities(
            ListEntitiesRequest(filter=filter),
        )

        # Extract entities and return
        entities = []
        for entity_proto in entity_protos.entities:
            entity = Entity.from_proto(entity_proto)
            entity._client = self
            entities.append(entity)
        return entities

    def get_entity(self, name: str, project: str) -> Entity:
        """
        Retrieves an entity.
        Args:
            project: Feast project that this entity belongs to
            name: Name of entity
        Returns:
            Returns either the specified entity, or raises an exception if
            none is found
        """

        get_entity_response = self._core_service.GetEntity(
            GetEntityRequest(project=project, name=name.strip()),
        )
        entity = Entity.from_proto(get_entity_response.entity)

        return entity

    def list_feature_tables(
        self, project: str, labels: Optional[Dict[str, str]] = None
    ) -> List[FeatureTable]:
        """
        Retrieve a list of feature tables from Feast Core
        Args:
            project: Filter feature tables based on project name
            labels: Filter by feature table labels
        Returns:
            List of feature tables
        """

        # Get latest feature tables from Feast Core
        feature_table_protos = self._core_service.ListFeatureTables(
            ListFeatureTablesRequest(
                filter=ListFeatureTablesRequest.Filter(
                    project=project, labels=labels or dict()
                )
            )
        )

        # Extract feature tables and return
        feature_tables = []
        for feature_table_proto in feature_table_protos.tables:
            feature_table = FeatureTable.from_proto(feature_table_proto)
            feature_table._client = self
            feature_tables.append(feature_table)
        return feature_tables

    def get_feature_table(self, name: str, project: str) -> FeatureTable:
        """
        Retrieves a feature table.

        Args:
            name: Name of feature table
            project: Feast project that this feature table belongs to

        Returns:
            Returns either the requested feature table spec or raises an exception if
            none is found
        """
        response = self._core_service.GetFeatureTable(
            GetFeatureTableRequest(project=project, name=name.strip()),
        )
        return FeatureTable.from_proto(response.table)

    def apply_feature_table(self, table: FeatureTable, project: str):
        """
        Apply a feature table.

        Args:
            table: Feature table spec
            project: Feast project that this feature table belongs to
        """
        self._core_service.ApplyFeatureTable(
            ApplyFeatureTableRequest(project=project, table_spec=table.to_spec_proto())
        )

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
        project,
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
            project: Feast project which the features belong to
        Returns:
            OnlineResponse containing the feature data in records.
            Each EntityRow provided will yield one record, which contains
            data fields with data value and field status metadata (if included)
        """
        return OnlineResponse(
            self._serving_service.GetOnlineFeatures(
                GetOnlineFeaturesRequest(
                    features=build_feature_references(feature_ref_strs=feature_refs),
                    entity_rows=infer_online_entity_rows(entity_rows),
                    project=project,
                )
            )
        )

    def start_offline_to_online_ingestion(
        self,
        feature_table: str,
        start: datetime,
        end: datetime,
        project: str,
        delta_ingestion: bool = False,
    ) -> StartOfflineToOnlineIngestionJobResponse:
        """
        Start offline to online ingestion job
        Args:
            feature_table: feature table name
            start: start datetime to filter records to be ingested
            end: end datetime to filter records to be ingested
            project: Feast project name
            delta_ingestion: boolean setting for delta ingestion

        Returns: StartOfflineToOnlineIngestionJobResponse
        """
        request = StartOfflineToOnlineIngestionJobRequest(
            project=project, table_name=feature_table, delta_ingestion=delta_ingestion
        )
        request.start_date.FromDatetime(start)
        request.end_date.FromDatetime(end)
        return self._job_service.StartOfflineToOnlineIngestionJob(request)

    def schedule_offline_to_online_ingestion(
        self,
        feature_table: str,
        project: str,
        ingestion_timespan: int,
        cron_schedule: str,
    ):
        """
        Launch Scheduled Ingestion Job from Batch Source to Online Store for given feature table

        Args:
            feature_table:  FeatureTable that will be ingested into the online store
            project: Project name
            ingestion_timespan: Days of data which will be ingestion per job. The boundaries
                on which to filter the source are [end of day of execution date - ingestion_timespan (days) ,
                end of day of execution date)
            cron_schedule: Cron schedule expression

        """
        if not croniter.is_valid(cron_schedule):
            raise RuntimeError(f"{cron_schedule} is not a valid cron expression")
        request = ScheduleOfflineToOnlineIngestionJobRequest(
            project=project,
            table_name=feature_table,
            ingestion_timespan=ingestion_timespan,
            cron_schedule=cron_schedule,
        )
        self._job_service.ScheduleOfflineToOnlineIngestionJob(request)

    def get_historical_features(
        self,
        feature_refs: List[str],
        entity_source: Union[FileSource, BigQuerySource],
        output_location: str,
        project: str,
        output_format: str = "parquet",
    ) -> GetHistoricalFeaturesResponse:
        """
        Launch a historical feature retrieval job.

        Args:
            feature_refs: List of feature references that will be returned for each entity.
                Each feature reference should have the following format:
                "feature_table:feature" where "feature_table" & "feature" refer to
                the feature and feature table names respectively.
            entity_source (Union[FileSource, BigQuerySource]): Source for the entity rows.
                It is assumed that the column event_timestamp is present
                in the dataframe, and is of type datetime without timezone information.
            output_location: Specifies the directory in a GCS bucket to write the exported feature data files
            project: Feast project name
            output_format: Spark output format

        Returns:
                Returns a retrieval job object that can be used to monitor retrieval
                progress asynchronously, and can be used to materialize the
                results.
        """

        return self._job_service.GetHistoricalFeatures(
            GetHistoricalFeaturesRequest(
                feature_refs=feature_refs,
                entity_source=entity_source.to_proto(),
                project=project,
                output_format=output_format,
                output_location=output_location,
            ),
        )

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

    def list_job(self, table: str, project: str, include_terminated=True) -> List[Job]:
        """
        List job details
        Args:
            table: feature table name
            project: feast project name
            include_terminated: include terminated jobs

        Returns: List of Job protobuf object
        """
        request = ListJobsRequest(table_name=table, project=project, include_terminated=include_terminated)
        response = self._job_service.ListJobs(request)
        return response.jobs

    def delete_feature_table(self, feature_table: str, project: str):
        """
        Delete Feature Table
        Args:
            feature_table: feature table name
            project: project name
        """
        request = DeleteFeatureTableRequest(name=feature_table, project=project)
        self._core_service.DeleteFeatureTable(request)
