CREATE OR REPLACE PACKAGE dbms_mview_stats AUTHID CURRENT_USER IS

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
  --   MV_LIST
  --     The fully-qualified name of an existing materialized view in the form of
  --     schema_name.mv_name. Use a comma-separated list to specify multiple
  --     materialized views.
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
    mv_list                   IN     VARCHAR2,
    retention_period          IN     NUMBER);


  --  -----------------------------------------------------------------------
  --  Sets the collection level and retention period for materialized view
  --  refresh statistics. You can set these properties either for individual
  --  materialized views or for all materialized views in the database.
  --
  --   MV_LIST
  --     The fully-qualified name of an existing materialized view in the form
  --     of schema_name.mv_name. Use a comma-separated list to specify multiple
  --     materialized views.
  --     Specify NULL to set properties for all existing materialized views in
  --     the database.
  --   COLLECTION_LEVEL
  --     Specifies the level of detail used when collecting refresh statistics
  --     for the materialized views specified in mv_list.
  --     Set one of the following values for collection_level:
  --       * NONE: No materialized view refresh statistics are collected.
  --       * TYPICAL: Only basic refresh statistics are collected and stored
  --         for the materialized views specified in mv_list.
  --       * ADVANCED: Detailed refresh statistics are collected and stored for
  --         materialized view specified in mv_list.
  --     If this parameter is set to NULL, then the system default value for
  --     collection_level (set using SET_SYSTEM_DEFAULT) is used.
  --   RETENTION_PERIOD
  --     Specifies the retention period, in days, for the refresh statistics of
  --     the materialized views specified in mv_list. Statistics that are older
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
    mv_list                   IN     VARCHAR2,
    collection_level          IN     VARCHAR2        := NULL,
    retention_period          IN     NUMBER          := NULL);

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
  --       * Numbers between 1 and 365000
  --     The default value for retention_period is 31.
  --     If you specify NULL for any of the parameters, then the system default
  --     setting for that parameter is used.
  --
  --  EXCEPTIONS
  --
  PROCEDURE set_system_default(
    parameter_name            IN     VARCHAR2,
    value                     IN     VARCHAR2);

  PROCEDURE set_system_default(
    parameter_name            IN     VARCHAR2,
    value                     IN     NUMBER);

END dbms_mview_stats;
//
