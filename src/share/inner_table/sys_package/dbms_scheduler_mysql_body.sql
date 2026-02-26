CREATE OR REPLACE PACKAGE BODY dbms_scheduler

  PROCEDURE enable ( job_name  VARCHAR(65535));
  PRAGMA INTERFACE(C, DBMS_SCHEDULER_ENABLE);

  PROCEDURE disable ( job_name          VARCHAR(65535),
                      force             BOOLEAN DEFAULT FALSE,
                      commit_semantics  VARCHAR(65535) DEFAULT  'STOP_ON_FIRST_ERROR');
  PRAGMA INTERFACE(C, DBMS_SCHEDULER_DISABLE);

  PROCEDURE set_attribute ( job_name  VARCHAR(65535),
                            name      VARCHAR(65535),
                            value     VARCHAR(65535));
  PRAGMA INTERFACE(C, DBMS_SCHEDULER_SET_JOB_ATTRIBUTE);

END dbms_scheduler;
