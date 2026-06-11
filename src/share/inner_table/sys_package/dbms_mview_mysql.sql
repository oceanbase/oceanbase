CREATE OR REPLACE PACKAGE dbms_mview AUTHID CURRENT_USER

  PROCEDURE purge_log(
    IN     master_name            VARCHAR(65535),
    IN     purge_log_parallel     INT            DEFAULT 0);

  PROCEDURE refresh(
    IN     mv_name                VARCHAR(65535),
    IN     method                 VARCHAR(65535) DEFAULT NULL,
    IN     refresh_parallel       INT            DEFAULT 0,
    IN     nested                 BOOLEAN        DEFAULT FALSE,
    IN     nested_refresh_mode    VARCHAR(65535) DEFAULT NULL);

  PROCEDURE set_refresh_params(
    IN     mv_name                VARCHAR(65535),
    IN     parameter_name         VARCHAR(65535),
    IN     parameter_value        VARCHAR(65535)
  );

  FUNCTION explain_refresh(
    IN     mv_name                VARCHAR(65535),
    IN     method                 VARCHAR(65535) DEFAULT NULL,
    IN     nested                 BOOLEAN        DEFAULT FALSE,
    IN     tenant_id              INT            DEFAULT 0
  ) RETURN LONGTEXT;

END dbms_mview;
