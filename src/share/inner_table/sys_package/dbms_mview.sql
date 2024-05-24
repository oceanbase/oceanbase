CREATE OR REPLACE PACKAGE dbms_mview AUTHID CURRENT_USER IS

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
  --   MASTER
  --     Name of the master table or master materialized view.
  --   NUM
  --     Number of least recently refreshed materialized views whose rows you
  --     want to remove from materialized view log.
  --     For example, the following statement deletes rows needed to refresh
  --     the two least recently refreshed materialized views:
  --       DBMS_MVIEW.PURGE_LOG('master_table', 2);
  --     To delete all rows in the materialized view log, indicate a high
  --     number of materialized views to disregard, as in this example:
  --       DBMS_MVIEW.PURGE_LOG('master_table',9999);
  --     This statement completely purges the materialized view log that
  --     corresponds to master_table if fewer than 9999 materialized views are
  --     based on master_table.
  --     A simple materialized view whose rows have been purged from the
  --     materialized view log must be completely refreshed the next time it is
  --     refreshed.
  --   FLAG
  --     Specify delete to guarantee that rows are deleted from the
  --     materialized view log for at least one materialized view. This
  --     parameter can override the setting for the parameter num.
  --     For example, the following statement deletes rows from the
  --     materialized view log that has dependency rows in the least recently
  --     refreshed materialized view:
  --       DBMS_MVIEW.PURGE_LOG('master_table', 1, 'delete');
  --
  --  EXCEPTIONS
  --

  PROCEDURE purge_log(
    master              IN   VARCHAR2,
    num                 IN   BINARY_INTEGER := 1,
    flag                IN   VARCHAR2       := 'NOP',
    purge_log_parallel  IN   BINARY_INTEGER := 1);

  --  -----------------------------------------------------------------------
  --  Transaction consistent refresh of an array of materialized views.
  --  The materialized views are refreshed atomically and consistently.
  --  Atomically: all materialized views are refreshed or none are.
  --  Consistently: all integrity constraints that hold among master tables
  --                will hold among the materialized view tables.
  --
  --   LIST
  --     Comma-delimited list of materialized views that you want to refresh.
  --     (Synonyms are supported.) These materialized views can be located
  --     in different schemas and have different master tables or master
  --     materialized views. However, all of the listed materialized views must
  --     be in your local database.
  --   TAB
  --     A PL/SQL index-by table of type DBMS_UTILITY.UNCL_ARRAY, where each
  --     element is the name of a materialized view.
  --   METHOD
  --     A string of refresh methods indicating how to refresh the listed
  --     materialized views. An f indicates fast refresh, ? indicates force
  --     refresh, C or c indicates complete refresh, and A or a indicates
  --     always refresh. A and C are equivalent. P or p refreshes by
  --     recomputing the rows in the materialized view affected by changed
  --     partitions in the detail tables.
  --     If a materialized view does not have a corresponding refresh method
  --     (that is, if more materialized views are specified than refresh
  --     methods), then that materialized view is refreshed according to its
  --     default refresh method. For example, consider the following EXECUTE
  --     statement within SQL*Plus:
  --       DBMS_MVIEW.REFRESH('countries_mv,regions_mv,hr.employees_mv','cf');
  --     This statement performs a complete refresh of the countries_mv
  --     materialized view, a fast refresh of the regions_mv materialized view,
  --     and a default refresh of the hr.employees materialized view.
  --   ROLLBACK_SEG
  --     Name of the materialized view site rollback segment to use while
  --     refreshing materialized views
  --   PUSH_DEFERRED_RPC
  --     Used by updatable materialized views only. Set this parameter to true
  --     if you want to push changes from the materialized view to its
  --     associated master tables or master materialized views before
  --     refreshing the materialized view. Otherwise, these changes may appear
  --     to be temporarily lost.
  --   REFRESH_AFTER_ERRORS
  --     If this parameter is true, an updatable materialized view continues to
  --     refresh even if there are outstanding conflicts logged in the DEFERROR
  --     view for the materialized view's master table or master materialized
  --     view. If this parameter is true and atomic_refresh is false, this
  --     procedure continues to refresh other materialized views if it fails
  --     while refreshing a materialized view.
  --   PURGE_OPTION
  --     How to purge the transaction queue if PUSH_DEFERRED_RPC is true.
  --     0 = don't
  --     1 = cheap but imprecise (optimize for time)
  --     2 = expensive but precise (optimize for space)
  --     If you are using the parallel propagation mechanism (in other words,
  --     parallelism is set to 1 or greater), 0 means do not purge, 1 means
  --     lazy purge, and 2 means aggressive purge. In most cases, lazy purge is
  --     the optimal setting. Set purge to aggressive to trim the queue if
  --     multiple master replication groups are pushed to different target
  --     sites, and updates to one or more replication groups are infrequent
  --     and infrequently pushed. If all replication groups are infrequently
  --     updated and pushed, then set this parameter to 0 and occasionally
  --     execute PUSH with this parameter set to 2 to reduce the queue.
  --   PARALLELISM
  --     Max degree of parallelism for pushing deferred RPCs. This value is
  --     considered only if PUSH_DEFERRED_RPC is true.
  --     0 (old algorithm) specifies serial propagation.
  --     1 (new algorithm) specifies parallel propagation using only one
  --     parallel process.
  --     n > 1 (new algorithm) specifies parallel propagation with n parallel
  --     processes.
  --   HEAP_SIZE
  --     Maximum number of transactions to be examined simultaneously for
  --     parallel propagation scheduling. Automatically calculates the default
  --     setting for optimal performance.
  --     Note: Do not set this parameter unless directed to do so by Support
  --     Services.
  --   ATOMIC_REFRESH
  --     If this parameter is set to true, then the list of materialized views
  --     is refreshed in a single transaction. All of the refreshed
  --     materialized views are updated to a single point in time. If the
  --     refresh fails for any of the materialized views, none of the
  --     materialized views are updated.
  --     If this parameter is set to false, then each of the materialized views
  --     is refreshed non-atomically in separate transactions.
  --     As part of complete refresh, if truncate is used (non-atomic refresh),
  --     unique index rebuild is executed. INDEX REBUILD automatically computes
  --     statistics. Thus, statistics are updated for truncated tables.
  --   NESTED
  --     If true, then perform nested refresh operations for the specified set
  --     of materialized views. Nested refresh operations refresh all the
  --     depending materialized views and the specified set of materialized
  --     views based on a dependency order to ensure the nested materialized
  --     views are truly fresh with respect to the underlying base tables.
  --   OUT_OF_PLACE
  --     If true, then it performs an out-of-place refresh. The default is
  --     false.
  --     This parameter uses the four methods of refresh (F, P, C, ?). So, for
  --     example, if you specify F and out_of_place = true, then an
  --     out-of-place fast refresh will be attempted. Similarly, if you specify
  --     P and out_of_place = true, then out-of-place PCT refresh will be
  --     attempted.
  --   SKIP_EXT_DATA
  --     Provides you an option to skip the MV data refresh corresponding to
  --     the external partitions.
  --   REFRESH_PARALLEL
  --     Max degree of parallelism for executing refresh. Now only works on
  --     complete refresh.
  --     n <= 1 specifies serial executing.
  --     n > 1  specifies parallel executing with n parallel processes.
  --
  --  EXCEPTIONS
  --

  PROCEDURE refresh(
    list                   IN     VARCHAR2,
    method                 IN     VARCHAR2       := NULL,
    rollback_seg           IN     VARCHAR2       := NULL,
    push_deferred_rpc      IN     BOOLEAN        := true,
    refresh_after_errors   IN     BOOLEAN        := false,
    purge_option           IN     BINARY_INTEGER := 1,
    parallelism            IN     BINARY_INTEGER := 0,
    heap_size              IN     BINARY_INTEGER := 0,
    atomic_refresh         IN     BOOLEAN        := true,
    nested                 IN     BOOLEAN        := false,
    out_of_place           IN     BOOLEAN        := false,
    skip_ext_data          IN     BOOLEAN        := false,
    refresh_parallel       IN     BINARY_INTEGER := 1);

  PROCEDURE refresh(
    tab                    IN     DBMS_UTILITY.UNCL_ARRAY,
    method                 IN     VARCHAR2       := NULL,
    rollback_seg           IN     VARCHAR2       := NULL,
    push_deferred_rpc      IN     BOOLEAN        := true,
    refresh_after_errors   IN     BOOLEAN        := false,
    purge_option           IN     BINARY_INTEGER := 1,
    parallelism            IN     BINARY_INTEGER := 0,
    heap_size              IN     BINARY_INTEGER := 0,
    atomic_refresh         IN     BOOLEAN        := true,
    nested                 IN     BOOLEAN        := false,
    out_of_place           IN     BOOLEAN        := false,
    skip_ext_data          IN     BOOLEAN        := false,
    refresh_parallel       IN     BINARY_INTEGER := 1);

END dbms_mview;
//
