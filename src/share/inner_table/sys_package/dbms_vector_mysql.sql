CREATE OR REPLACE PACKAGE dbms_vector AUTHID CURRENT_USER

  PROCEDURE refresh_index(
    IN     idx_name               VARCHAR(65535),
    IN     table_name             VARCHAR(65535),
    IN     idx_vector_col         VARCHAR(65535) DEFAULT NULL,
    IN     refresh_threshold      INT DEFAULT 10000,
    IN     refresh_type           VARCHAR(65535) DEFAULT NULL);

  PROCEDURE rebuild_index(
    IN     idx_name                VARCHAR(65535),
    IN     table_name              VARCHAR(65535),
    IN     idx_vector_col          VARCHAR(65535) DEFAULT NULL,
    IN     delta_rate_threshold    FLOAT DEFAULT 0.2,
    IN     idx_organization        VARCHAR(65535) DEFAULT NULL,
    IN     idx_distance_metrics    VARCHAR(65535) DEFAULT 'EUCLIDEAN',
    IN     idx_parameters          LONGTEXT DEFAULT NULL,
    IN     idx_parallel_creation   INT DEFAULT 1);

  PROCEDURE refresh_index_inner(
    IN     idx_table_id           BIGINT,
    IN     refresh_threshold      INT DEFAULT 10000,
    IN     refresh_type           VARCHAR(65535) DEFAULT NULL);

  PROCEDURE rebuild_index_inner(
    IN     idx_table_id            BIGINT,
    IN     delta_rate_threshold    FLOAT DEFAULT 0.2,
    IN     idx_organization        VARCHAR(65535) DEFAULT NULL,
    IN     idx_distance_metrics    VARCHAR(65535) DEFAULT 'EUCLIDEAN',
    IN     idx_parameters          LONGTEXT DEFAULT NULL,
    IN     idx_parallel_creation   INT DEFAULT 1);

END dbms_vector;
