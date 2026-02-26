#package_name:dbms_space
#author: liuqifan.lqf

CREATE OR REPLACE PACKAGE DBMS_SPACE AUTHID CURRENT_USER AS
  PROCEDURE CREATE_INDEX_COST (
    ddl           IN VARCHAR2,
    used_bytes    OUT NUMBER,
    alloc_bytes   OUT NUMBER,
    plan_table    IN VARCHAR2 := NULL
  );

END DBMS_SPACE;
//