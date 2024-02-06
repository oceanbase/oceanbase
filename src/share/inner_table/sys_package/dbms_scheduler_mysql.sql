CREATE OR REPLACE PACKAGE dbms_scheduler

  PROCEDURE create_job    ( job_name            VARCHAR(65535),
                            enabled             BOOLEAN DEFAULT FALSE);

  PROCEDURE enable ( job_name  VARCHAR(65535));

  PROCEDURE disable ( job_name          VARCHAR(65535),
                      force             BOOLEAN DEFAULT FALSE,
                      commit_semantics  VARCHAR(65535) DEFAULT  'STOP_ON_FIRST_ERROR');

  PROCEDURE set_attribute ( job_name  VARCHAR(65535),
                            name      VARCHAR(65535),
                            value     VARCHAR(65535));

END dbms_scheduler;
