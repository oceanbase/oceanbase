CREATE OR REPLACE PACKAGE dbms_vector AUTHID CURRENT_USER

  ------------
  --  OVERVIEW
  --
  --  These routines allow the user to refresh and rebuild vector index.

  ------------------------------------------------
  --  SUMMARY OF SERVICES PROVIDED BY THIS PACKAGE
  --
  --  refresh_index            - refresh delta modification to vector index.
  --  rebuild_index            - rebuild vector index.

  ----------------------------
  --  PROCEDURES AND FUNCTIONS
  --

  --  ----------------------------------------------------------------------------
  --  Refresh delta modification to vector index for vector ann searching performance.
  --
  --
  --   IDX_NAME
  --     Name of the vector index.
  --   TABLE_NAME
  --     Name of the master(base) table.
  --   IDX_VECTOR_COL
  --     Name of the vector column which vector index is built on.
  --   REFRESH_THRESHOLD
  --     If the row count of delta_buff_table is greater than REFRESH_THRESHOLD, refreshing is triggered.
  --     If not, nothing will happen.
  --   REFRESH_TYPE
  --     Only FAST is supported now.
  --
  --  EXCEPTIONS
  --

  PROCEDURE refresh_index(
    IN     idx_name               VARCHAR(65535),
    IN     table_name             VARCHAR(65535),
    IN     idx_vector_col         VARCHAR(65535) DEFAULT NULL,
    IN     refresh_threshold      INT DEFAULT 10000,
    IN     refresh_type           VARCHAR(65535) DEFAULT NULL);

  --  -----------------------------------------------------------------------
  --  Rebuild vector index.
  --
  --   IDX_NAME
  --     Name of the vector index.
  --   TABLE_NAME
  --     Name of the master(base) table.
  --   IDX_VECTOR_COL
  --     Name of the vector column which vector index is built on.
  --   DELTA_RATE_THRESHOLD
  --     If (the row count of delta_buff_table + the row count of index_id_table)
  --        / the row count of master(base) table is greater than REFRESH_THRESHOLD, rebuilding is triggered.
  --     If not, nothing will happen.
  --   IDX_ORGANIZATION
  --     Vector ann searching algorithm
  --       NEIGHBOR PARTITON: ivfflat/ivfpq/...
  --       IN MEMORY NEIGHBOR GRAPH: hnsw...
  --   IDX_PARAMETERS
  --     The parameters of vector ann searching algorithm.
  --   IDX_PARALLEL_CREATION
  --     The degree of parallization for building vector index.
  --  EXCEPTIONS
  --

  PROCEDURE rebuild_index(
    IN     idx_name                VARCHAR(65535),
    IN     table_name              VARCHAR(65535),
    IN     idx_vector_col          VARCHAR(65535) DEFAULT NULL,
    IN     delta_rate_threshold    FLOAT DEFAULT 0.2,
    IN     idx_organization        VARCHAR(65535) DEFAULT NULL,
    IN     idx_distance_metrics    VARCHAR(65535) DEFAULT 'EUCLIDEAN',
    IN     idx_parameters          LONGTEXT DEFAULT NULL,
    IN     idx_parallel_creation   INT DEFAULT 1);

END dbms_vector;
