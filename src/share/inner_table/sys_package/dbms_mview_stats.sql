CREATE OR REPLACE PACKAGE dbms_mview_stats AUTHID CURRENT_USER IS

  PROCEDURE purge_refresh_stats(
    mv_list                   IN     VARCHAR2,
    retention_period          IN     NUMBER);

  PROCEDURE set_mvref_stats_params(
    mv_list                   IN     VARCHAR2,
    collection_level          IN     VARCHAR2        := NULL,
    retention_period          IN     NUMBER          := NULL);

  PROCEDURE set_system_default(
    parameter_name            IN     VARCHAR2,
    value                     IN     VARCHAR2);

  PROCEDURE set_system_default(
    parameter_name            IN     VARCHAR2,
    value                     IN     NUMBER);

END dbms_mview_stats;
//
