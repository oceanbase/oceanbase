CREATE OR REPLACE PACKAGE BODY dbms_mview_stats IS

  -- ------------------------------------------------------------------------
  -- purge_refresh_stats

  PROCEDURE purge_refresh_stats(
    mv_list                   IN     VARCHAR2,
    retention_period          IN     NUMBER);
  PRAGMA INTERFACE(C, DBMS_MVIEW_STATS_PURGE_REFRESH_STATS);

  -- ------------------------------------------------------------------------
  -- set_mvref_stats_params

  PROCEDURE set_mvref_stats_params(
    mv_list                   IN     VARCHAR2,
    collection_level          IN     VARCHAR2        := NULL,
    retention_period          IN     NUMBER          := NULL);
  PRAGMA INTERFACE(C, DBMS_MVIEW_STATS_SET_MVREF_STATS_PARAMS);

  -- ------------------------------------------------------------------------
  -- set_system_default

  PROCEDURE do_set_system_default(
    parameter_name            IN     VARCHAR2,
    collection_level          IN     VARCHAR2     := NULL,
    retention_period          IN     NUMBER       := NULL);
  PRAGMA INTERFACE(C, DBMS_MVIEW_STATS_SET_SYS_DEFAULT);

  PROCEDURE set_system_default(
    parameter_name            IN     VARCHAR2,
    value                     IN     VARCHAR2)
  IS
  parameter         VARCHAR2(128) := 'COLLECTION_LEVEL';
  BEGIN
    IF parameter_name IS NOT NULL AND NLS_UPPER(parameter_name) <> parameter THEN
      RAISE_APPLICATION_ERROR(-20000, 'ORA-13916: Invalid value "' || parameter_name || '" specified for parameter "' || parameter ||'"');
    END IF;
    do_set_system_default(parameter, value, NULL);
  END;

  PROCEDURE set_system_default(
    parameter_name            IN     VARCHAR2,
    value                     IN     NUMBER)
  IS
  parameter         VARCHAR2(128) := 'RETENTION_PERIOD';
  BEGIN
    IF parameter_name IS NOT NULL AND NLS_UPPER(parameter_name) <> parameter THEN
      RAISE_APPLICATION_ERROR(-20000, 'ORA-13916: Invalid value "' || parameter_name || '" specified for parameter "' || parameter ||'"');
    END IF;
    do_set_system_default(parameter, NULL, value);
  END;

END dbms_mview_stats;
//
