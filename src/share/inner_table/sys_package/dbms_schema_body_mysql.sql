#package_name: dbms_schema
#author: zhaoziqian.zzq

CREATE OR REPLACE PACKAGE BODY dbms_schema
  PROCEDURE recycle_schema_history();
  PRAGMA INTERFACE(C, DBMS_SCHEMA_RECYCLE_SCHEMA_HISTORY);

  PROCEDURE run_inspection();
  PRAGMA INTERFACE(C, DBMS_SCHEMA_RUN_INSPECTION);
END dbms_schema;
