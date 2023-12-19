CREATE OR REPLACE PACKAGE dbms_mview_stats AUTHID CURRENT_USER

  ------------
  --  OVERVIEW
  --
  --  These routines allow the user to manage the collection and retention of
  --  statistics for materialized view refresh operations.

  ------------------------------------------------
  --  SUMMARY OF SERVICES PROVIDED BY THIS PACKAGE
  --
  --  purge_refresh_stats            - purges the statistics of materialized view refresh operations
  --  set_mvref_stats_params         - sets the values of parameters for materialized view refresh statistics
  --  set_system_default             - sets the system default value of a refresh statistics parameter

  ----------------------------
  --  PROCEDURES AND FUNCTIONS
  --

  --  ----------------------------------------------------------------------------
  --  Purges refresh statistics that are older than the specified retention period
  --  for the specified materialized views.
  --
  --
  --   MV_NAME
  --     The fully-qualified name of an existing materialized view in the form of
  --     database_name.mv_name.
  --     Specify NULL to purge materialized view refresh statistics for all
  --     materialized views in the database.
  --   RETENTION_PERIOD
  --     The number of days for which refresh statistics must be preserved in
  --     the data dictionary. Statistics for materialized view refresh
  --     operations that are older than the retention period are purged from
  --     the data dictionary.
  --     The retention period specified in this procedure overrides the
  --     retention period that may have been set previously either at the
  --     database level or for specified materialized views.
  --     Specify NULL to use the purging policy defined by the automatic
  --     statistics purge. Specify –1 to purge all refresh statistics.
  --
  --  EXCEPTIONS
  --
  PROCEDURE purge_refresh_stats(
    IN     mv_name                VARCHAR(65535),
    IN     retention_period       INT);

  --  -----------------------------------------------------------------------
  --  Sets the collection level and retention period for materialized view
  --  refresh statistics. You can set these properties either for individual
  --  materialized view or for all materialized views in the database.
  --
  --   MV_NAME
  --     The fully-qualified name of an existing materialized view in the form
  --     of database_name.mv_name.
  --     Specify NULL to set properties for all existing materialized views in
  --     the database.
  --   COLLECTION_LEVEL
  --     Specifies the level of detail used when collecting refresh statistics
  --     for the materialized view specified in mv_name.
  --     Set one of the following values for collection_level:
  --       * NONE: No materialized view refresh statistics are collected.
  --       * TYPICAL: Only basic refresh statistics are collected and stored
  --         for the materialized view specified in mv_name.
  --       * ADVANCED: Detailed refresh statistics are collected and stored for
  --         materialized view specified in mv_name.
  --     If this parameter is set to NULL, then the system default value for
  --     collection_level (set using SET_SYSTEM_DEFAULT) is used.
  --   RETENTION_PERIOD
  --     Specifies the retention period, in days, for the refresh statistics of
  --     the materialized view specified in mv_name. Statistics that are older
  --     than the retention period are automatically purged from the data
  --     dictionary.
  --     Valid values are between 1 and 365000.
  --     If this parameter is set to NULL, then the system default value for
  --     retention_period (set using SET_SYSTEM_DEAFULT) is used.
  --     Set retention_period to -1 to specify that refresh statistics for
  --     the materialized views in mv_list must never be purged.
  --
  --  EXCEPTIONS
  --
  PROCEDURE set_mvref_stats_params(
    IN     mv_name                VARCHAR(65535),
    IN     collection_level       VARCHAR(65535)     DEFAULT NULL,
    IN     retention_period       INT                DEFAULT NULL);

  --  -----------------------------------------------------------------------
  --  Sets system-wide defaults that manage the collection and retention of
  --  materialized view refresh statistics. All newly-created materialized
  --  views use these defaults until the parameters are reset explicitly using
  --  the SET_MVREF_STATS_PARAMS procedure.
  --
  --   PARAMETER_NAME
  --     The name of the materialized view refresh statistics parameter whose
  --     system default value is being set.
  --     The parameters that can be set are:
  --       * COLLECTION_LEVEL: Specifies the level of detail for collecting
  --         materialized view refresh statistics.
  --       * RETENTION_PERIOD: Specifies the duration, in days, for which
  --         refresh statistics are retained in the data dictionary
  --   VALUE
  --     The value of the materialized view refresh statistics parameter.
  --     The valid values for COLLECTION_LEVEL are:
  --       * NONE: No refresh statistics are collected for the refresh
  --         operation.
  --       * TYPICAL: Only basic refresh statistics are collected for the
  --         refresh operation. This is the default setting.
  --       * ADVANCED: Detailed refresh statistics are collected for the
  --         refresh operation.
  --     The valid values for RETENTION_PERIOD are:
  --       * -1
  --       * Numbers between 1 and 165000
  --     The default value for retention_period is 31.
  --     If you specify NULL for any of the parameters, then the system default
  --     setting for that parameter is used.
  --
  --  EXCEPTIONS
  --
  PROCEDURE set_system_default(
    IN     parameter_name         VARCHAR(65535),
    IN     value                  VARCHAR(65535));

END dbms_mview_stats;
