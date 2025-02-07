#package_name:dbms_space
#author: liuqifan.lqf

CREATE OR REPLACE PACKAGE DBMS_SPACE AUTHID CURRENT_USER
  PROCEDURE CREATE_INDEX_COST (
    ddl             VARCHAR(65535),
    OUT used_bytes      DECIMAL,
    OUT alloc_bytes     DECIMAL,
    plan_table      VARCHAR(65535) DEFAULT NULL
  );

END DBMS_SPACE;