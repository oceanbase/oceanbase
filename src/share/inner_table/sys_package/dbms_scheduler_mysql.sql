CREATE OR REPLACE PACKAGE dbms_scheduler

  FUNCTION GENERATE_JOB_NAME(IN in_type_name VARCHAR(255) DEFAULT 'JOB$_') RETURN VARCHAR(255);
  FUNCTION CALC_DELAY_TS(IN repeat_interval VARCHAR(255)) RETURN BIGINT;
  PROCEDURE create_job    (IN job_name            VARCHAR(255),
                           IN job_type            VARCHAR(255),
                           IN job_action          VARCHAR(255),
                           IN number_of_argument  INT DEFAULT 0,
                           IN start_date          TIMESTAMP DEFAULT NULL,
                           IN repeat_interval     VARCHAR(255) DEFAULT 'null',
                           IN end_date            TIMESTAMP DEFAULT NULL,
                           IN job_class           VARCHAR(255) DEFAULT 'DEFAULT_JOB_CLASS',
                           IN enabled             BOOLEAN DEFAULT FALSE,
                           IN auto_drop           BOOLEAN DEFAULT TRUE,
                           IN comments            VARCHAR(255) DEFAULT 'null',
                           IN credential_name     VARCHAR(255) DEFAULT 'null',
                           IN destination_name    VARCHAR(255) DEFAULT 'null',
                           IN max_run_duration    INT DEFAULT 0);

  -- PROCEDURE create_job    ( job_name            VARCHAR(65535),
  --                           enabled             BOOLEAN DEFAULT FALSE);

  PROCEDURE enable ( job_name  VARCHAR(65535));

  PROCEDURE disable ( job_name          VARCHAR(65535),
                      force             BOOLEAN DEFAULT FALSE,
                      commit_semantics  VARCHAR(65535) DEFAULT  'STOP_ON_FIRST_ERROR');

  PROCEDURE set_attribute ( job_name  VARCHAR(65535),
                            name      VARCHAR(65535),
                            value     VARCHAR(65535));

END dbms_scheduler;
