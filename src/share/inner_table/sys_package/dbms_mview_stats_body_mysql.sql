CREATE OR REPLACE PACKAGE BODY dbms_mview_stats

  -- ------------------------------------------------------------------------
  -- purge_refresh_stats

  PROCEDURE purge_refresh_stats(
    IN     mv_name                VARCHAR(65535),
    IN     retention_period       INT);
  PRAGMA INTERFACE(C, DBMS_MVIEW_STATS_MYSQL_PURGE_REFRESH_STATS);

  -- ------------------------------------------------------------------------
  -- set_mvref_stats_params

  PROCEDURE set_mvref_stats_params(
    IN     mv_name                VARCHAR(65535),
    IN     collection_level       VARCHAR(65535)     DEFAULT NULL,
    IN     retention_period       INT                DEFAULT NULL);
  PRAGMA INTERFACE(C, DBMS_MVIEW_STATS_MYSQL_SET_MVREF_STATS_PARAMS);

  -- ------------------------------------------------------------------------
  -- set_system_default

  PROCEDURE do_set_system_default(
    IN     parameter_name         VARCHAR(65535),
    IN     collection_level       VARCHAR(65535)     DEFAULT NULL,
    IN     retention_period       INT                DEFAULT NULL);
  PRAGMA INTERFACE(C, DBMS_MVIEW_STATS_MYSQL_SET_SYS_DEFAULT);

  PROCEDURE set_system_default(
    IN     parameter_name         VARCHAR(65535),
    IN     value                  VARCHAR(65535))
  BEGIN
    DECLARE collection_level  VARCHAR(65535) DEFAULT NULL;
    DECLARE retention_period  INT            DEFAULT NULL;

    IF parameter_name = 'COLLECTION_LEVEL' THEN
      SET collection_level = value;
    ELSEIF parameter_name = 'RETENTION_PERIOD' THEN
      SET retention_period = value;
    ELSE
      SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT = 'Invalid value specified for parameter name';
    END IF;

    CALL do_set_system_default(parameter_name, collection_level, retention_period);
  END;

END dbms_mview_stats;
