CREATE OR REPLACE PACKAGE BODY dbms_scheduler

  PROCEDURE create_job    ( job_name            VARCHAR(65535),
                            enabled             BOOLEAN DEFAULT FALSE);
  PRAGMA INTERFACE(C, DBMS_SCHEDULER_MYSQL_CREATE_JOB);

  PROCEDURE enable ( job_name  VARCHAR(65535));
  PRAGMA INTERFACE(C, DBMS_SCHEDULER_MYSQL_ENABLE);

  PROCEDURE disable ( job_name          VARCHAR(65535),
                      force             BOOLEAN DEFAULT FALSE,
                      commit_semantics  VARCHAR(65535) DEFAULT  'STOP_ON_FIRST_ERROR');
  PRAGMA INTERFACE(C, DBMS_SCHEDULER_MYSQL_DISABLE);

  PROCEDURE set_attribute ( job_name  VARCHAR(65535),
                            name      VARCHAR(65535),
                            value     VARCHAR(65535));
  PRAGMA INTERFACE(C, DBMS_SCHEDULER_MYSQL_SET_ATTRIBUTE);

END dbms_scheduler;
