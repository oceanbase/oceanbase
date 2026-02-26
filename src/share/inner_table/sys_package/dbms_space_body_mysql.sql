#package_name:dbms_space
#author: liuqifan.lqf

CREATE OR REPLACE PACKAGE BODY DBMS_SPACE
  PROCEDURE CREATE_INDEX_COST (
    ddl             VARCHAR(65535),
    OUT used_bytes      DECIMAL,
    OUT alloc_bytes     DECIMAL,
    plan_table      VARCHAR(65535) DEFAULT NULL
  );
  PRAGMA INTERFACE(C, CREATE_INDEX_COST);

END DBMS_SPACE;