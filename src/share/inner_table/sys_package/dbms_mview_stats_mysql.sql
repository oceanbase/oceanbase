CREATE OR REPLACE PACKAGE dbms_mview_stats AUTHID CURRENT_USER

  PROCEDURE purge_refresh_stats(
    IN     mv_name                VARCHAR(65535),
    IN     retention_period       INT);

  PROCEDURE set_mvref_stats_params(
    IN     mv_name                VARCHAR(65535),
    IN     collection_level       VARCHAR(65535)     DEFAULT NULL,
    IN     retention_period       INT                DEFAULT NULL);

  PROCEDURE set_system_default(
    IN     parameter_name         VARCHAR(65535),
    IN     value                  VARCHAR(65535));

END dbms_mview_stats;
