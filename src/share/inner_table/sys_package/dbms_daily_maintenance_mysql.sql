#package_name: DBMS_DAILY_MAINTENANCE
#author: ouyanghongrong.oyhr

CREATE OR REPLACE PACKAGE dbms_daily_maintenance AUTHID CURRENT_USER

  PROCEDURE TRIGGER_WINDOW_COMPACTION_PROC (
    is_daily_maintenance BOOLEAN DEFAULT FALSE
  );

  PROCEDURE SET_THREAD_COUNT (
    thread_count INT
  );

  PROCEDURE ALTER_REPEAT_INTERVAL (
    repeat_interval VARCHAR(1024)
  );

END dbms_daily_maintenance;
//