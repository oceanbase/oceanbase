#package_name:dbms_routine_load
#author:wuyuefei.wyf

CREATE OR REPLACE PACKAGE dbms_routine_load AUTHID CURRENT_USER IS

  PROCEDURE consume_kafka(
    job_id            IN  NUMBER,
    partition_pos     IN  PLS_INTEGER,
    partition_len     IN  PLS_INTEGER,
    offsets_pos       IN  PLS_INTEGER,
    offsets_len       IN  PLS_INTEGER,
    job_name          IN  VARCHAR2,
    exec_sql          IN  VARCHAR2);

END;
//
