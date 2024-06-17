CREATE OR REPLACE PACKAGE dbms_mview AUTHID CURRENT_USER

  ------------
  --  OVERVIEW
  --
  --  These routines allow the user to refresh materialized views and purge logs.

  ------------------------------------------------
  --  SUMMARY OF SERVICES PROVIDED BY THIS PACKAGE
  --
  --  purge_log            - purge log of unnecessary rows
  --  refresh              - refresh the given materialized views

  ----------------------------
  --  PROCEDURES AND FUNCTIONS
  --

  --  ----------------------------------------------------------------------------
  --  Purge the materialized view log for the specified master of unecessary rows.
  --
  --
  --   MASTER_NAME
  --     Name of the master table.
  --
  --  EXCEPTIONS
  --

  PROCEDURE purge_log(
    IN     master_name            VARCHAR(65535),
    IN     purge_log_parallel     INT            DEFAULT 1);

  --  -----------------------------------------------------------------------
  --  Refresh the given materialized view.
  --
  --   MV_NAME
  --     The materialized view that you want to refresh.
  --   METHOD
  --     A string of refresh methods indicating how to refresh the
  --     materialized view. An f indicates fast refresh, ? indicates force
  --     refresh, C or c indicates complete refresh, and A or a indicates
  --     always refresh. A and C are equivalent.
  --     If a materialized view does not have a corresponding refresh method
  --     (that is, if more materialized views are specified than refresh
  --     methods), then that materialized view is refreshed according to its
  --     default refresh method.
  --   REFRESH_PARALLEL
  --     Max degree of parallelism for executing refresh. Now only works on
  --     complete refresh.
  --     n <= 1 specifies serial executing.
  --     n > 1  specifies parallel executing with n parallel processes.
  --
  --  EXCEPTIONS
  --

  PROCEDURE refresh(
    IN     mv_name                VARCHAR(65535),
    IN     method                 VARCHAR(65535) DEFAULT NULL,
    IN     refresh_parallel       INT            DEFAULT 1);

END dbms_mview;
