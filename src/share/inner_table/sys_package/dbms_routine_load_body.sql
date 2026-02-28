CREATE OR REPLACE PACKAGE BODY dbms_routine_load IS

  PROCEDURE do_consume_kafka(
    job_id            IN  NUMBER,
    partition_pos     IN  PLS_INTEGER,
    partition_len     IN  PLS_INTEGER,
    offsets_pos       IN  PLS_INTEGER,
    offsets_len       IN  PLS_INTEGER,
    job_name          IN  VARCHAR2,
    exec_sql          IN  VARCHAR2);
  PRAGMA INTERFACE(C, DBMS_ROUTINE_LOAD_ORACLE_CONSUME_KAFKA);

  PROCEDURE consume_kafka(
    job_id            IN  NUMBER,
    partition_pos     IN  PLS_INTEGER,
    partition_len     IN  PLS_INTEGER,
    offsets_pos       IN  PLS_INTEGER,
    offsets_len       IN  PLS_INTEGER,
    job_name          IN  VARCHAR2,
    exec_sql          IN  VARCHAR2)
  IS
  BEGIN
    COMMIT;
    do_consume_kafka(job_id, partition_pos, partition_len, offsets_pos, offsets_len, job_name, exec_sql);
  END;

END;
//
