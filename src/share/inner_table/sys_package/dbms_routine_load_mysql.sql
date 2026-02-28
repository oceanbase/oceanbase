#package_name:dbms_routine_load
#author:wuyuefei.wyf

CREATE OR REPLACE PACKAGE dbms_routine_load AUTHID CURRENT_USER
  PROCEDURE consume_kafka(
    IN     job_id                  BIGINT,
    IN     partition_pos           INT,
    IN     partition_len           INT,
    IN     offsets_pos             INT,
    IN     offsets_len             INT,
    IN     job_name                VARCHAR(128),
    IN     exec_sql                VARCHAR(65535));
END dbms_routine_load;