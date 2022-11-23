# package_name : dbms_stats_mysql
# author : jiangxiu.wt

CREATE OR REPLACE PACKAGE BODY dbms_stats
    PROCEDURE gather_table_stats (
      ownname            VARCHAR(65535),
      tabname            VARCHAR(65535),
      partname           VARCHAR(65535) DEFAULT NULL,
      estimate_percent   DECIMAL DEFAULT AUTO_SAMPLE_SIZE,
      block_sample       BOOLEAN DEFAULT FALSE,
      method_opt         VARCHAR(65535) DEFAULT DEFAULT_METHOD_OPT,
      degree             DECIMAL DEFAULT NULL,
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
      estimate_percent   DECIMAL DEFAULT AUTO_SAMPLE_SIZE,
      block_sample       BOOLEAN DEFAULT FALSE,
      method_opt         VARCHAR(65535) DEFAULT DEFAULT_METHOD_OPT,
      degree             DECIMAL DEFAULT NULL,
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
      estimate_percent   DECIMAL DEFAULT AUTO_SAMPLE_SIZE,
      stattab            VARCHAR(65535) DEFAULT NULL,
      statid             VARCHAR(65535) DEFAULT NULL,
      statown            VARCHAR(65535) DEFAULT NULL,
      degree             DECIMAL DEFAULT NULL,
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
      numrows            DECIMAL DEFAULT NULL,
      numblks            DECIMAL DEFAULT NULL,
      avgrlen            DECIMAL DEFAULT NULL,
      flags              DECIMAL DEFAULT NULL,
      statown            VARCHAR(65535) DEFAULT NULL,
      no_invalidate      BOOLEAN DEFAULT FALSE,
      cachedblk          DECIMAL DEFAULT NULL,
      cachehit           DECIMAL DEFAULT NULL,
      force              BOOLEAN DEFAULT FALSE,
      nummacroblks       DECIMAL DEFAULT NULL,
      nummicroblks       DECIMAL DEFAULT NULL
    );
    PRAGMA INTERFACE(C, SET_TABLE_STATS);

    PROCEDURE set_column_stats (
      ownname            VARCHAR(65535),
      tabname            VARCHAR(65535),
      colname            VARCHAR(65535),
      partname           VARCHAR(65535) DEFAULT NULL,
      stattab            VARCHAR(65535) DEFAULT NULL,
      statid             VARCHAR(65535) DEFAULT NULL,
      distcnt            DECIMAL DEFAULT NULL,
      density            DECIMAL DEFAULT NULL,
      nullcnt            DECIMAL DEFAULT NULL,
      epc                DECIMAL DEFAULT NULL,
      minval             TEXT DEFAULT NULL,
      maxval             TEXT DEFAULT NULL,
      bkvals             TEXT DEFAULT NULL,
      novals             TEXT DEFAULT NULL,
      chvals             TEXT DEFAULT NULL,
      eavals             TEXT DEFAULT NULL,
      rpcnts             TEXT DEFAULT NULL,
      eavs               DECIMAL DEFAULT NULL,
      avgclen            DECIMAL DEFAULT NULL,
      flags              DECIMAL DEFAULT NULL,
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
      numrows            DECIMAL    DEFAULT NULL,
      numlblks           DECIMAL    DEFAULT NULL,
      numdist            DECIMAL    DEFAULT NULL,
      avglblk            DECIMAL    DEFAULT NULL,
      avgdblk            DECIMAL    DEFAULT NULL,
      clstfct            DECIMAL    DEFAULT NULL,
      indlevel           DECIMAL    DEFAULT NULL,
      flags              DECIMAL    DEFAULT NULL,
      statown            VARCHAR(65535) DEFAULT NULL,
      no_invalidate      BOOLEAN    DEFAULT FALSE,
      guessq             DECIMAL    DEFAULT NULL,
      cachedblk          DECIMAL    DEFAULT NULL,
      cachehit           DECIMAL    DEFAULT NULL,
      force              BOOLEAN    DEFAULT FALSE,
      avgrlen            DECIMAL    DEFAULT NULL,
      nummacroblks       DECIMAL    DEFAULT NULL,
      nummicroblks       DECIMAL    DEFAULT NULL,
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
      force             BOOLEAN DEFAULT FALSE
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
      col_stat_type    VARCHAR(65535) DEFAULT 'ALL'
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
      tabname          VARCHAR(65535) DEFAULT NULL
    );
    PRAGMA INTERFACE(C, DELETE_INDEX_STATS);

    PROCEDURE delete_schema_stats (
      ownname           VARCHAR(65535),
      stattab           VARCHAR(65535) DEFAULT NULL,
      statid            VARCHAR(65535) DEFAULT NULL,
      statown           VARCHAR(65535) DEFAULT NULL,
      no_invalidate     BOOLEAN DEFAULT FALSE,
      force             BOOLEAN DEFAULT FALSE
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
      retention             DECIMAL
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

END dbms_stats;
