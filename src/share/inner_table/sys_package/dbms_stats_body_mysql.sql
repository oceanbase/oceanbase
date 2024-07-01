# package_name : dbms_stats_mysql
# author : jiangxiu.wt

CREATE OR REPLACE PACKAGE BODY dbms_stats
    PROCEDURE gather_table_stats (
      ownname            VARCHAR(65535),
      tabname            VARCHAR(65535),
      partname           VARCHAR(65535) DEFAULT NULL,
      estimate_percent   DECIMAL(20, 10) DEFAULT AUTO_SAMPLE_SIZE,
      block_sample       BOOLEAN DEFAULT NULL,
      method_opt         VARCHAR(65535) DEFAULT DEFAULT_METHOD_OPT,
      degree             DECIMAL(20, 10) DEFAULT NULL,
      granularity        VARCHAR(65535) DEFAULT DEFAULT_GRANULARITY,
      cascade            BOOLEAN DEFAULT NULL,
      stattab            VARCHAR(65535) DEFAULT NULL,
      statid             VARCHAR(65535) DEFAULT NULL,
      statown            VARCHAR(65535) DEFAULT NULL,
      no_invalidate      BOOLEAN DEFAULT FALSE,
      stattype           VARCHAR(65535) DEFAULT 'DATA',
      force              BOOLEAN DEFAULT FALSE
    );
    PRAGMA INTERFACE(C, GATHER_TABLE_STATS);

    PROCEDURE gather_schema_stats (
      ownname            VARCHAR(65535),
      estimate_percent   DECIMAL(20, 10) DEFAULT AUTO_SAMPLE_SIZE,
      block_sample       BOOLEAN DEFAULT NULL,
      method_opt         VARCHAR(65535) DEFAULT DEFAULT_METHOD_OPT,
      degree             DECIMAL(20, 10) DEFAULT NULL,
      granularity        VARCHAR(65535) DEFAULT DEFAULT_GRANULARITY,
      cascade            BOOLEAN DEFAULT NULL,
      stattab            VARCHAR(65535) DEFAULT NULL,
      statid             VARCHAR(65535) DEFAULT NULL,
      statown            VARCHAR(65535) DEFAULT NULL,
      no_invalidate      BOOLEAN DEFAULT FALSE,
      stattype           VARCHAR(65535) DEFAULT 'DATA',
      force              BOOLEAN DEFAULT FALSE
    );
    PRAGMA INTERFACE(C, GATHER_SCHEMA_STATS);

    PROCEDURE gather_index_stats (
      ownname            VARCHAR(65535),
      indname            VARCHAR(65535),
      partname           VARCHAR(65535) DEFAULT NULL,
      estimate_percent   DECIMAL(20, 10) DEFAULT AUTO_SAMPLE_SIZE,
      stattab            VARCHAR(65535) DEFAULT NULL,
      statid             VARCHAR(65535) DEFAULT NULL,
      statown            VARCHAR(65535) DEFAULT NULL,
      degree             DECIMAL(20, 10) DEFAULT NULL,
      granularity        VARCHAR(65535) DEFAULT DEFAULT_GRANULARITY,
      no_invalidate      BOOLEAN DEFAULT FALSE,
      force              BOOLEAN DEFAULT FALSE,
      tabname            VARCHAR(65535) DEFAULT NULL
    );
    PRAGMA INTERFACE(C, GATHER_INDEX_STATS);

    PROCEDURE set_table_stats (
      ownname            VARCHAR(65535),
      tabname            VARCHAR(65535),
      partname           VARCHAR(65535) DEFAULT NULL,
      stattab            VARCHAR(65535) DEFAULT NULL,
      statid             VARCHAR(65535) DEFAULT NULL,
      numrows            DECIMAL(20, 10) DEFAULT NULL,
      numblks            DECIMAL(20, 10) DEFAULT NULL,
      avgrlen            DECIMAL(20, 10) DEFAULT NULL,
      flags              DECIMAL(20, 10) DEFAULT NULL,
      statown            VARCHAR(65535) DEFAULT NULL,
      no_invalidate      BOOLEAN DEFAULT FALSE,
      cachedblk          DECIMAL(20, 10) DEFAULT NULL,
      cachehit           DECIMAL(20, 10) DEFAULT NULL,
      force              BOOLEAN DEFAULT FALSE,
      nummacroblks       DECIMAL(20, 10) DEFAULT NULL,
      nummicroblks       DECIMAL(20, 10) DEFAULT NULL
    );
    PRAGMA INTERFACE(C, SET_TABLE_STATS);

    PROCEDURE set_column_stats (
      ownname            VARCHAR(65535),
      tabname            VARCHAR(65535),
      colname            VARCHAR(65535),
      partname           VARCHAR(65535) DEFAULT NULL,
      stattab            VARCHAR(65535) DEFAULT NULL,
      statid             VARCHAR(65535) DEFAULT NULL,
      distcnt            DECIMAL(20, 10) DEFAULT NULL,
      density            DECIMAL(20, 10) DEFAULT NULL,
      nullcnt            DECIMAL(20, 10) DEFAULT NULL,
      epc                DECIMAL(20, 10) DEFAULT NULL,
      minval             TEXT DEFAULT NULL,
      maxval             TEXT DEFAULT NULL,
      bkvals             TEXT DEFAULT NULL,
      novals             TEXT DEFAULT NULL,
      chvals             TEXT DEFAULT NULL,
      eavals             TEXT DEFAULT NULL,
      rpcnts             TEXT DEFAULT NULL,
      eavs               DECIMAL(20, 10) DEFAULT NULL,
      avgclen            DECIMAL(20, 10) DEFAULT NULL,
      flags              DECIMAL(20, 10) DEFAULT NULL,
      statown            VARCHAR(65535) DEFAULT NULL,
      no_invalidate      BOOLEAN DEFAULT FALSE,
      force              BOOLEAN DEFAULT FALSE
    );
    PRAGMA INTERFACE(C, SET_COLUMN_STATS);

    PROCEDURE set_index_stats (
      ownname            VARCHAR(65535),
      indname            VARCHAR(65535),
      partname           VARCHAR(65535) DEFAULT NULL,
      stattab            VARCHAR(65535) DEFAULT NULL,
      statid             VARCHAR(65535) DEFAULT NULL,
      numrows            DECIMAL(20, 10)    DEFAULT NULL,
      numlblks           DECIMAL(20, 10)    DEFAULT NULL,
      numdist            DECIMAL(20, 10)    DEFAULT NULL,
      avglblk            DECIMAL(20, 10)    DEFAULT NULL,
      avgdblk            DECIMAL(20, 10)    DEFAULT NULL,
      clstfct            DECIMAL(20, 10)    DEFAULT NULL,
      indlevel           DECIMAL(20, 10)    DEFAULT NULL,
      flags              DECIMAL(20, 10)    DEFAULT NULL,
      statown            VARCHAR(65535) DEFAULT NULL,
      no_invalidate      BOOLEAN    DEFAULT FALSE,
      guessq             DECIMAL(20, 10)    DEFAULT NULL,
      cachedblk          DECIMAL(20, 10)    DEFAULT NULL,
      cachehit           DECIMAL(20, 10)    DEFAULT NULL,
      force              BOOLEAN    DEFAULT FALSE,
      avgrlen            DECIMAL(20, 10)    DEFAULT NULL,
      nummacroblks       DECIMAL(20, 10)    DEFAULT NULL,
      nummicroblks       DECIMAL(20, 10)    DEFAULT NULL,
      tabname            VARCHAR(65535) DEFAULT NULL
    );
    PRAGMA INTERFACE(C, SET_INDEX_STATS);

    PROCEDURE delete_table_stats (
      ownname           VARCHAR(65535),
      tabname           VARCHAR(65535),
      partname          VARCHAR(65535) DEFAULT NULL,
      stattab           VARCHAR(65535) DEFAULT NULL,
      statid            VARCHAR(65535) DEFAULT NULL,
      cascade_parts     BOOLEAN DEFAULT TRUE,
      cascade_columns   BOOLEAN DEFAULT TRUE,
      cascade_indexes   BOOLEAN DEFAULT TRUE,
      statown           VARCHAR(65535) DEFAULT NULL,
      no_invalidate     BOOLEAN DEFAULT FALSE,
      force             BOOLEAN DEFAULT FALSE,
      degree            DECIMAL(20, 10) DEFAULT 1
    );
    PRAGMA INTERFACE(C, DELETE_TABLE_STATS);

    PROCEDURE delete_column_stats (
      ownname          VARCHAR(65535),
      tabname          VARCHAR(65535),
      colname          VARCHAR(65535),
      partname         VARCHAR(65535) DEFAULT NULL,
      stattab          VARCHAR(65535) DEFAULT NULL,
      statid           VARCHAR(65535) DEFAULT NULL,
      cascade_parts    BOOLEAN DEFAULT TRUE,
      statown          VARCHAR(65535) DEFAULT NULL,
      no_invalidate    BOOLEAN DEFAULT FALSE,
      force            BOOLEAN DEFAULT FALSE,
      col_stat_type    VARCHAR(65535) DEFAULT 'ALL',
      degree           DECIMAL(20, 10) DEFAULT 1
    );
    PRAGMA INTERFACE(C, DELETE_COLUMN_STATS);

    procedure delete_index_stats(
      ownname          VARCHAR(65535),
      indname          VARCHAR(65535),
      partname         VARCHAR(65535) DEFAULT NULL,
      stattab          VARCHAR(65535) DEFAULT NULL,
      statid           VARCHAR(65535) DEFAULT NULL,
      cascade_parts    BOOLEAN        DEFAULT TRUE,
      statown          VARCHAR(65535) DEFAULT NULL,
      no_invalidate    BOOLEAN        DEFAULT FALSE,
      stattype         VARCHAR(65535) DEFAULT 'ALL',
      force            BOOLEAN        DEFAULT FALSE,
      tabname          VARCHAR(65535) DEFAULT NULL,
      degree           DECIMAL(20, 10) DEFAULT 1
    );
    PRAGMA INTERFACE(C, DELETE_INDEX_STATS);

    PROCEDURE delete_schema_stats (
      ownname           VARCHAR(65535),
      stattab           VARCHAR(65535) DEFAULT NULL,
      statid            VARCHAR(65535) DEFAULT NULL,
      statown           VARCHAR(65535) DEFAULT NULL,
      no_invalidate     BOOLEAN DEFAULT FALSE,
      force             BOOLEAN DEFAULT FALSE,
      degree            DECIMAL(20, 10) DEFAULT 1
    );
    PRAGMA INTERFACE(C, DELETE_SCHEMA_STATS);

    PROCEDURE FLUSH_DATABASE_MONITORING_INFO();
    PRAGMA INTERFACE(C, FLUSH_DATABASE_MONITORING_INFO);

    PROCEDURE GATHER_DATABASE_STATS_JOB_PROC(duration BIGINT DEFAULT NULL);
    PRAGMA INTERFACE(C, GATHER_DATABASE_STATS_JOB_PROC);

    PROCEDURE create_stat_table(
      ownname          VARCHAR(65535),
      stattab          VARCHAR(65535),
      tblspace         VARCHAR(65535) DEFAULT NULL,
      global_temporary BOOLEAN DEFAULT FALSE
    );
    PRAGMA INTERFACE(C, CREATE_STAT_TABLE);

    PROCEDURE drop_stat_table(
      ownname VARCHAR(65535),
      stattab VARCHAR(65535)
    );
    PRAGMA INTERFACE(C, DROP_STAT_TABLE);

     PROCEDURE export_table_stats (
      ownname          VARCHAR(65535),
      tabname          VARCHAR(65535),
      partname         VARCHAR(65535) DEFAULT NULL,
      stattab          VARCHAR(65535),
      statid           VARCHAR(65535) DEFAULT NULL,
      cascade          BOOLEAN DEFAULT TRUE,
      statown          VARCHAR(65535) DEFAULT NULL,
      stat_category    VARCHAR(65535) DEFAULT DEFAULT_STAT_CATEGORY
    );
    PRAGMA INTERFACE(C, EXPORT_TABLE_STATS);

    PROCEDURE export_column_stats (
      ownname          VARCHAR(65535),
      tabname          VARCHAR(65535),
      colname          VARCHAR(65535),
      partname         VARCHAR(65535) DEFAULT NULL,
      stattab          VARCHAR(65535),
      statid           VARCHAR(65535) DEFAULT NULL,
      statown          VARCHAR(65535) DEFAULT NULL
    );
    PRAGMA INTERFACE(C, EXPORT_COLUMN_STATS);

    PROCEDURE export_schema_stats (
      ownname          VARCHAR(65535),
      stattab          VARCHAR(65535),
      statid           VARCHAR(65535) DEFAULT NULL,
      statown          VARCHAR(65535) DEFAULT NULL
    );
    PRAGMA INTERFACE(C, EXPORT_SCHEMA_STATS);


    PROCEDURE export_index_stats (
      ownname           VARCHAR(65535),
      indname           VARCHAR(65535),
      partname          VARCHAR(65535) DEFAULT NULL,
      stattab           VARCHAR(65535),
      statid            VARCHAR(65535) DEFAULT NULL,
      statown           VARCHAR(65535) DEFAULT NULL,
      tabname           VARCHAR(65535) DEFAULT NULL
    );
    PRAGMA INTERFACE(C, EXPORT_INDEX_STATS);

    PROCEDURE import_table_stats (
      ownname          VARCHAR(65535),
      tabname          VARCHAR(65535),
      partname         VARCHAR(65535) DEFAULT NULL,
      stattab          VARCHAR(65535),
      statid           VARCHAR(65535) DEFAULT NULL,
      cascade          BOOLEAN DEFAULT TRUE,
      statown          VARCHAR(65535) DEFAULT NULL,
      no_invalidate    BOOLEAN DEFAULT FALSE,
      force            BOOLEAN DEFAULT FALSE,
      stat_category    VARCHAR(65535) DEFAULT DEFAULT_STAT_CATEGORY
    );
    PRAGMA INTERFACE(C, IMPORT_TABLE_STATS);

    PROCEDURE import_column_stats (
      ownname          VARCHAR(65535),
      tabname          VARCHAR(65535),
      colname          VARCHAR(65535),
      partname         VARCHAR(65535) DEFAULT NULL,
      stattab          VARCHAR(65535),
      statid           VARCHAR(65535) DEFAULT NULL,
      statown          VARCHAR(65535) DEFAULT NULL,
      no_invalidate    BOOLEAN DEFAULT FALSE,
      force            BOOLEAN DEFAULT FALSE
    );
    PRAGMA INTERFACE(C, IMPORT_COLUMN_STATS);

    PROCEDURE import_schema_stats (
      ownname          VARCHAR(65535),
      stattab          VARCHAR(65535),
      statid           VARCHAR(65535) DEFAULT NULL,
      statown          VARCHAR(65535) DEFAULT NULL,
      no_invalidate    BOOLEAN DEFAULT FALSE,
      force            BOOLEAN DEFAULT FALSE
    );
    PRAGMA INTERFACE(C, IMPORT_SCHEMA_STATS);

    PROCEDURE import_index_stats (
      ownname          VARCHAR(65535),
      indname          VARCHAR(65535),
      partname         VARCHAR(65535) DEFAULT NULL,
      stattab          VARCHAR(65535),
      statid           VARCHAR(65535) DEFAULT NULL,
      statown          VARCHAR(65535) DEFAULT NULL,
      no_invalidate    BOOLEAN DEFAULT FALSE,
      force            BOOLEAN DEFAULT FALSE,
      tabname          VARCHAR(65535) DEFAULT NULL
    );
    PRAGMA INTERFACE(C, IMPORT_INDEX_STATS);

    PROCEDURE lock_table_stats (
      ownname          VARCHAR(65535),
      tabname          VARCHAR(65535),
      stattype         VARCHAR(65535) DEFAULT 'ALL'
    );
    PRAGMA INTERFACE(C, LOCK_TABLE_STATS);

    PROCEDURE lock_partition_stats (
      ownname          VARCHAR(65535),
      tabname          VARCHAR(65535),
      partname         VARCHAR(65535)
    );
    PRAGMA INTERFACE(C, LOCK_PARTITION_STATS);

    PROCEDURE lock_schema_stats(
      ownname          VARCHAR(65535),
      STATTYPE         VARCHAR(65535) DEFAULT 'ALL'
    );
    PRAGMA INTERFACE(C, LOCK_SCHEMA_STATS);

    PROCEDURE unlock_table_stats (
      ownname          VARCHAR(65535),
      tabname          VARCHAR(65535),
      stattype         VARCHAR(65535) DEFAULT 'ALL'
    );
    PRAGMA INTERFACE(C, UNLOCK_TABLE_STATS);

    PROCEDURE unlock_partition_stats (
      ownname          VARCHAR(65535),
      tabname          VARCHAR(65535),
      partname         VARCHAR(65535)
    );
    PRAGMA INTERFACE(C, UNLOCK_PARTITION_STATS);

    PROCEDURE unlock_schema_stats(
      ownname          VARCHAR(65535),
      STATTYPE         VARCHAR(65535) DEFAULT 'ALL'
    );
    PRAGMA INTERFACE(C, UNLOCK_SCHEMA_STATS);

    PROCEDURE restore_table_stats (
      ownname               VARCHAR(65535),
      tabname               VARCHAR(65535),
      as_of_timestamp       DATETIME(6),
      restore_cluster_index BOOLEAN DEFAULT FALSE,
      force                 BOOLEAN DEFAULT FALSE,
      no_invalidate         BOOLEAN DEFAULT FALSE
    );
    PRAGMA INTERFACE(C, RESTORE_TABLE_STATS);

    PROCEDURE restore_schema_stats (
      ownname               VARCHAR(65535),
      as_of_timestamp       DATETIME(6),
      force                 BOOLEAN DEFAULT FALSE,
      no_invalidate         BOOLEAN DEFAULT FALSE
    );
    PRAGMA INTERFACE(C, RESTORE_SCHEMA_STATS);

    PROCEDURE purge_stats(
      before_timestamp      DATETIME(6)
    );
    PRAGMA INTERFACE(C, PURGE_STATS);

    PROCEDURE alter_stats_history_retention(
      retention             DECIMAL(20, 10)
    );
    PRAGMA INTERFACE(C, ALTER_STATS_HISTORY_RETENTION);

    FUNCTION get_stats_history_availability() RETURN DATETIME(6);
    PRAGMA INTERFACE(C, GET_STATS_HISTORY_AVAILABILITY);

    FUNCTION get_stats_history_retention() RETURN DECIMAL;
    PRAGMA INTERFACE(C, GET_STATS_HISTORY_RETENTION);

    PROCEDURE reset_global_pref_defaults();
    PRAGMA INTERFACE(C, RESET_GLOBAL_PREF_DEFAULTS);

    PROCEDURE reset_param_defaults()
    BEGIN
      call reset_global_pref_defaults();
    END;

    PROCEDURE set_global_prefs(
      pname         VARCHAR(65535),
      pvalue        VARCHAR(65535)
    );
    PRAGMA INTERFACE(C, SET_GLOBAL_PREFS);

    PROCEDURE set_param(pname VARCHAR(65535), pval VARCHAR(65535))
    BEGIN
      call set_global_prefs(pname, pval);
    END;

    PROCEDURE set_schema_prefs(
      ownname        VARCHAR(65535),
      pname          VARCHAR(65535),
      pvalue         VARCHAR(65535)
    );
    PRAGMA INTERFACE(C, SET_SCHEMA_PREFS);

    PROCEDURE set_table_prefs(
      ownname        VARCHAR(65535),
      tabname        VARCHAR(65535),
      pname          VARCHAR(65535),
      pvalue         VARCHAR(65535)
    );
    PRAGMA INTERFACE(C, SET_TABLE_PREFS);

    FUNCTION get_prefs (
      pname           VARCHAR(65535),
      ownname         VARCHAR(65535) DEFAULT NULL,
      tabname         VARCHAR(65535) DEFAULT NULL
    ) RETURN VARCHAR(65535);
    PRAGMA INTERFACE(C, GET_PREFS);

    FUNCTION get_param (
      pname           VARCHAR(65535)
    )RETURN VARCHAR(65535)
    BEGIN
      RETURN dbms_stats.get_prefs(pname);
    END;

    PROCEDURE delete_schema_prefs(
      ownname        VARCHAR(65535),
      pname          VARCHAR(65535)
    );
    PRAGMA INTERFACE(C, DELETE_SCHEMA_PREFS);

    PROCEDURE delete_table_prefs (
      ownname        VARCHAR(65535),
      tabname        VARCHAR(65535),
      pname          VARCHAR(65535)
    );
    PRAGMA INTERFACE(C, DELETE_TABLE_PREFS);

    PROCEDURE copy_table_stats (
      ownname        VARCHAR(65535),
      tabname        VARCHAR(65535),
      srcpartname    VARCHAR(65535),
      dstpartname		 VARCHAR(65535),
      scale_factor	 DECIMAL(20, 10) DEFAULT 1,
      flags					 DECIMAL(20, 10) DEFAULT NULL,
      force          BOOLEAN DEFAULT FALSE
    );
    PRAGMA INTERFACE(C, COPY_TABLE_STATS);

    PROCEDURE cancel_gather_stats (
      taskid          VARCHAR(65535)
    );
    PRAGMA INTERFACE(C, CANCEL_GATHER_STATS);
    PROCEDURE GATHER_SYSTEM_STATS();
    PRAGMA INTERFACE(C, GATHER_SYSTEM_STATS);

    PROCEDURE DELETE_SYSTEM_STATS();
    PRAGMA INTERFACE(C, DELETE_SYSTEM_STATS);

    PROCEDURE SET_SYSTEM_STATS (
      pname          VARCHAR(65535),
      pvalue         DECIMAL(20, 10)
    );
    PRAGMA INTERFACE(C, SET_SYSTEM_STATS);

END dbms_stats;
