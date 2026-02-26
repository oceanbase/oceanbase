#package_name:dbms_space
#author: liuqifan.lqf

CREATE OR REPLACE PACKAGE BODY DBMS_SPACE AS
  PROCEDURE CREATE_INDEX_COST (
    ddl           IN VARCHAR2,
    used_bytes    OUT NUMBER,
    alloc_bytes   OUT NUMBER,
    plan_table    IN VARCHAR2 DEFAULT NULL
  );
  PRAGMA INTERFACE(C, CREATE_INDEX_COST);

END DBMS_SPACE;
//