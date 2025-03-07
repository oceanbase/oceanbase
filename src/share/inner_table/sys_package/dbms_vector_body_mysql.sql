CREATE OR REPLACE PACKAGE BODY dbms_vector

  PROCEDURE do_refresh_index(
    IN     idx_name               VARCHAR(65535),
    IN     table_name             VARCHAR(65535),
    IN     idx_vector_col         VARCHAR(65535) DEFAULT NULL,
    IN     refresh_threshold      INT DEFAULT 10000,
    IN     refresh_type           VARCHAR(65535) DEFAULT NULL
  );
  PRAGMA INTERFACE(C, DBMS_VECTOR_MYSQL_REFRESH_INDEX);

  PROCEDURE refresh_index(
    IN     idx_name               VARCHAR(65535),
    IN     table_name             VARCHAR(65535),
    IN     idx_vector_col         VARCHAR(65535) DEFAULT NULL,
    IN     refresh_threshold      INT DEFAULT 10000,
    IN     refresh_type           VARCHAR(65535) DEFAULT NULL)
  BEGIN
    COMMIT;
    CALL do_refresh_index(idx_name, table_name, idx_vector_col, refresh_threshold, refresh_type);
  END;

  PROCEDURE do_rebuild_index (
    IN     idx_name                VARCHAR(65535),
    IN     table_name              VARCHAR(65535),
    IN     idx_vector_col          VARCHAR(65535) DEFAULT NULL,
    IN     delta_rate_threshold    FLOAT DEFAULT 0.2,
    IN     idx_organization        VARCHAR(65535) DEFAULT NULL,
    IN     idx_distance_metrics    VARCHAR(65535) DEFAULT 'EUCLIDEAN',
    IN     idx_parameters          LONGTEXT DEFAULT NULL,
    IN     idx_parallel_creation   INT DEFAULT 1
  );
  PRAGMA INTERFACE(C, DBMS_VECTOR_MYSQL_REBUILD_INDEX);

  PROCEDURE rebuild_index(
    IN     idx_name                VARCHAR(65535),
    IN     table_name              VARCHAR(65535),
    IN     idx_vector_col          VARCHAR(65535) DEFAULT NULL,
    IN     delta_rate_threshold    FLOAT DEFAULT 0.2,
    IN     idx_organization        VARCHAR(65535) DEFAULT NULL,
    IN     idx_distance_metrics    VARCHAR(65535) DEFAULT 'EUCLIDEAN',
    IN     idx_parameters          LONGTEXT DEFAULT NULL,
    IN     idx_parallel_creation   INT DEFAULT 1)
  BEGIN
    COMMIT;
    CALL do_rebuild_index(idx_name, table_name, idx_vector_col, delta_rate_threshold, idx_organization, idx_distance_metrics, idx_parameters, idx_parallel_creation);
  END;

  PROCEDURE do_refresh_index_inner(
    IN     idx_table_id           BIGINT,
    IN     refresh_threshold      INT DEFAULT 10000,
    IN     refresh_type           VARCHAR(65535) DEFAULT NULL
  );
  PRAGMA INTERFACE(C, DBMS_VECTOR_MYSQL_REFRESH_INDEX_INNER);

  PROCEDURE refresh_index_inner(
    IN     idx_table_id           BIGINT,
    IN     refresh_threshold      INT DEFAULT 10000,
    IN     refresh_type           VARCHAR(65535) DEFAULT NULL)
  BEGIN
    COMMIT;
    CALL do_refresh_index_inner(idx_table_id, refresh_threshold, refresh_type);
  END;

  PROCEDURE do_rebuild_index_inner (
    IN     idx_table_id            BIGINT,
    IN     delta_rate_threshold    FLOAT DEFAULT 0.2,
    IN     idx_organization        VARCHAR(65535) DEFAULT NULL,
    IN     idx_distance_metrics    VARCHAR(65535) DEFAULT 'EUCLIDEAN',
    IN     idx_parameters          LONGTEXT DEFAULT NULL,
    IN     idx_parallel_creation   INT DEFAULT 1
  );
  PRAGMA INTERFACE(C, DBMS_VECTOR_MYSQL_REBUILD_INDEX_INNER);

  PROCEDURE rebuild_index_inner(
    IN     idx_table_id            BIGINT,
    IN     delta_rate_threshold    FLOAT DEFAULT 0.2,
    IN     idx_organization        VARCHAR(65535) DEFAULT NULL,
    IN     idx_distance_metrics    VARCHAR(65535) DEFAULT 'EUCLIDEAN',
    IN     idx_parameters          LONGTEXT DEFAULT NULL,
    IN     idx_parallel_creation   INT DEFAULT 1)
  BEGIN
    COMMIT;
    CALL do_rebuild_index_inner(idx_table_id, delta_rate_threshold, idx_organization, idx_distance_metrics, idx_parameters, idx_parallel_creation);
  END;

END dbms_vector;
