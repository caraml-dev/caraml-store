CREATE TABLE online_stores
(
    name        VARCHAR(255) NOT NULL,
    type        VARCHAR(255) NOT NULL,
    description VARCHAR(255),
    archived    BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT pk_online_stores PRIMARY KEY (name)
);

ALTER TABLE feature_tables
    ADD online_store_name VARCHAR(255);

ALTER TABLE online_stores
    ADD CONSTRAINT uc_online_stores_name UNIQUE (name);

ALTER TABLE feature_tables
    ADD CONSTRAINT FK_FEATURE_TABLES_ON_ONLINE_STORE_NAME FOREIGN KEY (online_store_name) REFERENCES online_stores (name);

INSERT INTO online_stores (name, type, description)
VALUES ('unset', 'UNSET', 'store info not captured');

UPDATE feature_tables
SET online_store_name='unset'
WHERE online_store_name IS NULL;