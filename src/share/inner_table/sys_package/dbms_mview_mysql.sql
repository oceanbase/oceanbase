CREATE OR REPLACE PACKAGE dbms_mview AUTHID CURRENT_USER

  PROCEDURE purge_log(
    IN     master_name            VARCHAR(65535),
    IN     purge_log_parallel     INT            DEFAULT 0);

  PROCEDURE refresh(
    IN     mv_name                VARCHAR(65535),
    IN     method                 VARCHAR(65535) DEFAULT NULL,
    IN     refresh_parallel       INT            DEFAULT 0,
    IN     nested                 BOOLEAN        DEFAULT FALSE,
    IN     nested_refresh_mode    VARCHAR(65535) DEFAULT NULL,
    -- TEMPORARY v2 (task 0036): async=FALSE blocks until the batch completes.
    -- TRUE returns immediately after enqueue (prior 4.6.0 async behaviour).
    IN     async                  BOOLEAN        DEFAULT FALSE,
    -- force=TRUE kills any in-flight refresh targeting the same mview before
    -- scheduling the new one, so callers can pre-empt stuck refreshes.
    IN     force                  BOOLEAN        DEFAULT FALSE);

  FUNCTION refresh_report(
    IN     refresh_id             INT            DEFAULT NULL,
    IN     mv_name                VARCHAR(65535) DEFAULT NULL,
    IN     tenant_id              INT            DEFAULT NULL,
    IN     format                 VARCHAR(65535) DEFAULT 'TEXT')
  RETURN TEXT;

  PROCEDURE kill_refresh(
    IN     refresh_id              BIGINT);

  PROCEDURE set_refresh_params(
    IN     mv_name                VARCHAR(65535),
    IN     parameter_name         VARCHAR(65535),
    IN     parameter_value        VARCHAR(65535)
  );

END dbms_mview;
