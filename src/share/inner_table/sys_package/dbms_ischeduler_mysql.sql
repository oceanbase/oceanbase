CREATE OR REPLACE PACKAGE dbms_ischeduler
  FUNCTION  PUSER() RETURN VARCHAR(128);
  FUNCTION  LUSER() RETURN VARCHAR(128);
  FUNCTION  GET_AND_INCREASE_JOB_ID() RETURN BIGINT;

  FUNCTION JOB_ID(IN my_job_name VARCHAR(255),
                  IN puser VARCHAR(255)) RETURN BIGINT;
  PROCEDURE CHECK_PRIVS ( IN my_job_name VARCHAR(255) );

  PROCEDURE do_create_job( IN job                BIGINT,
                           IN luser              VARCHAR(128),
                           IN puser              VARCHAR(128),
                           IN cuser              VARCHAR(128),
                           IN final_next_date    TIMESTAMP,
                           IN my_interval        VARCHAR(128),
                           IN my_flag            INT,
                           IN what               VARCHAR(128),
                           IN nlsenv             VARCHAR(128),
                           IN my_env             VARCHAR(128),
                           IN job_name           VARCHAR(255),
                           IN job_type           VARCHAR(255),
                           IN job_action         VARCHAR(255),
                           IN number_of_argument INT,
                           IN start_date         TIMESTAMP,
                           IN repeat_interval    VARCHAR(128),
                           IN end_date           TIMESTAMP,
                           IN job_class          VARCHAR(255),
                           IN enabled            BOOLEAN,
                           IN auto_drop          BOOLEAN,
                           IN comments           VARCHAR(255),
                           IN credential_name    VARCHAR(255),
                           IN destination_name   VARCHAR(255),
                           IN my_zone            VARCHAR(128),
                           IN program_name       VARCHAR(255),
                           IN job_style          VARCHAR(255),
                           IN interval_ts        BIGINT,
                           IN max_run_duration   BIGINT);

  PROCEDURE create_job( IN JOB                BIGINT,
                        IN LUSER              VARCHAR(128),
                        IN PUSER              VARCHAR(128),
                        IN CUSER              VARCHAR(128),
                        IN job_name           VARCHAR(255),
                        IN program_name       VARCHAR(255),
                        IN job_type           VARCHAR(255),
                        IN job_action         VARCHAR(128),
                        IN number_of_argument INT,
                        IN start_date         TIMESTAMP,
                        IN repeat_interval    VARCHAR(128),
                        IN end_date           TIMESTAMP,
                        IN job_class          VARCHAR(255),
                        IN enabled            BOOLEAN,
                        IN auto_drop          BOOLEAN,
                        IN comments           VARCHAR(255),
                        IN job_style          VARCHAR(255),
                        IN credential_name    VARCHAR(255),
                        IN destination_name   VARCHAR(255),
                        IN interval_ts        BIGINT,
                        IN max_run_duration   BIGINT);

END dbms_ischeduler;
