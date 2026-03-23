#package_name: dbms_schema
#author: zhaoziqian.zzq

CREATE OR REPLACE PACKAGE dbms_schema
  PROCEDURE recycle_schema_history();
  PROCEDURE run_inspection();
END dbms_schema;
