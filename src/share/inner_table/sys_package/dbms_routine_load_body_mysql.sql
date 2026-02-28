CREATE OR REPLACE PACKAGE BODY dbms_routine_load
  PROCEDURE do_consume_kafka(
    IN     job_id                  BIGINT,
    IN     partition_pos           INT,
    IN     partition_len           INT,
    IN     offsets_pos             INT,
    IN     offsets_len             INT,
    IN     job_name                VARCHAR(128),
    IN     exec_sql                VARCHAR(65535));
    PRAGMA INTERFACE(C, DBMS_ROUTINE_LOAD_MYSQL_CONSUME_KAFKA);

  PROCEDURE consume_kafka(
    IN     job_id                  BIGINT,
    IN     partition_pos           INT,
    IN     partition_len           INT,
    IN     offsets_pos             INT,
    IN     offsets_len             INT,
    IN     job_name                VARCHAR(128),
    IN     exec_sql                VARCHAR(65535))
  BEGIN
    COMMIT;
    CALL do_consume_kafka(job_id, partition_pos, partition_len, offsets_pos, offsets_len, job_name, exec_sql);
  END;
END dbms_routine_load;
