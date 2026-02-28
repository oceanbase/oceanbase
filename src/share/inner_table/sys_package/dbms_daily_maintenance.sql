#package_name: DBMS_DAILY_MAINTENANCE
#author: ouyanghongrong.oyhr

CREATE OR REPLACE PACKAGE dbms_daily_maintenance AUTHID CURRENT_USER IS

  PROCEDURE TRIGGER_WINDOW_COMPACTION_PROC (
    is_daily_maintenance IN BOOLEAN DEFAULT FALSE
  );

  PROCEDURE SET_THREAD_COUNT (
    thread_count IN INT
  );

END dbms_daily_maintenance;
//