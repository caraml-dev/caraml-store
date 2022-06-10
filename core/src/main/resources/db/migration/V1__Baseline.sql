CREATE SEQUENCE IF NOT EXISTS hibernate_sequence START WITH 1 INCREMENT BY 1;

CREATE TABLE data_sources
(
    id                       BIGINT       NOT NULL,
    type                     VARCHAR(255) NOT NULL,
    config                   VARCHAR(255),
    field_mapping            TEXT,
    timestamp_column         VARCHAR(255),
    created_timestamp_column VARCHAR(255),
    date_partition_column    VARCHAR(255),
    CONSTRAINT pk_data_sources PRIMARY KEY (id)
);

CREATE TABLE entities
(
    id           BIGINT       NOT NULL,
    created      TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    last_updated TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    name         VARCHAR(255) NOT NULL,
    project_name VARCHAR(255),
    description  TEXT,
    type         VARCHAR(255),
    labels       TEXT,
    CONSTRAINT pk_entities PRIMARY KEY (id)
);

CREATE TABLE feature_tables
(
    id               BIGINT       NOT NULL,
    created          TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    last_updated     TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    name             VARCHAR(255) NOT NULL,
    project_name     VARCHAR(255),
    labels           TEXT,
    max_age          BIGINT       NOT NULL,
    stream_source_id BIGINT,
    batch_source_id  BIGINT       NOT NULL,
    revision         INTEGER      NOT NULL,
    is_deleted       BOOLEAN      NOT NULL,
    CONSTRAINT pk_feature_tables PRIMARY KEY (id)
);

CREATE TABLE feature_tables_entities
(
    entity_id        BIGINT NOT NULL,
    feature_table_id BIGINT NOT NULL,
    CONSTRAINT pk_feature_tables_entities PRIMARY KEY (entity_id, feature_table_id)
);

CREATE TABLE features
(
    id               BIGINT NOT NULL,
    name             VARCHAR(255),
    feature_table_id BIGINT NOT NULL,
    type             VARCHAR(255),
    labels           TEXT,
    CONSTRAINT pk_features PRIMARY KEY (id)
);

CREATE TABLE projects
(
    name     VARCHAR(255) NOT NULL,
    archived BOOLEAN      NOT NULL,
    CONSTRAINT pk_projects PRIMARY KEY (name)
);

ALTER TABLE features
    ADD CONSTRAINT uc_76c77c61b299932ef422bf0c2 UNIQUE (name, feature_table_id);

ALTER TABLE feature_tables
    ADD CONSTRAINT uc_856cf6567d74972d47470b440 UNIQUE (name, project_name);

ALTER TABLE entities
    ADD CONSTRAINT uc_c8d069841bc147a0959561ad4 UNIQUE (name, project_name);

ALTER TABLE projects
    ADD CONSTRAINT uc_projects_name UNIQUE (name);

ALTER TABLE entities
    ADD CONSTRAINT FK_ENTITIES_ON_PROJECT_NAME FOREIGN KEY (project_name) REFERENCES projects (name);

ALTER TABLE features
    ADD CONSTRAINT FK_FEATURES_ON_FEATURE_TABLE FOREIGN KEY (feature_table_id) REFERENCES feature_tables (id);

ALTER TABLE feature_tables
    ADD CONSTRAINT FK_FEATURE_TABLES_ON_BATCH_SOURCE FOREIGN KEY (batch_source_id) REFERENCES data_sources (id);

ALTER TABLE feature_tables
    ADD CONSTRAINT FK_FEATURE_TABLES_ON_PROJECT_NAME FOREIGN KEY (project_name) REFERENCES projects (name);

ALTER TABLE feature_tables
    ADD CONSTRAINT FK_FEATURE_TABLES_ON_STREAM_SOURCE FOREIGN KEY (stream_source_id) REFERENCES data_sources (id);

ALTER TABLE feature_tables_entities
    ADD CONSTRAINT fk_featabent_on_entity FOREIGN KEY (entity_id) REFERENCES entities (id);

ALTER TABLE feature_tables_entities
    ADD CONSTRAINT fk_featabent_on_feature_table FOREIGN KEY (feature_table_id) REFERENCES feature_tables (id);