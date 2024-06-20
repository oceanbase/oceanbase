CREATE OR REPLACE PACKAGE BODY dbms_ischeduler

  FUNCTION  PUSER() RETURN VARCHAR(128)
  BEGIN
    DECLARE current_user_name VARCHAR(128);
    SELECT current_user() INTO current_user_name;
    RETURN current_user_name;
  END;

  FUNCTION  LUSER() RETURN VARCHAR(128)
  BEGIN
    DECLARE current_schema_name VARCHAR(128);
    SELECT database() INTO current_schema_name;
    RETURN current_schema_name;
  END;

  FUNCTION GET_AND_INCREASE_JOB_ID() RETURN BIGINT;
  PRAGMA INTERFACE(C, DBMS_SCHEDULER_MYSQL_GET_AND_INCREASE_JOB_ID);

  FUNCTION JOB_ID(IN my_job_name VARCHAR(255),
                  IN puser VARCHAR(255)) RETURN BIGINT
  BEGIN
    DECLARE jid BIGINT DEFAULT 0;
    IF my_job_name IS NOT NULL THEN
      SELECT MAX(job) INTO jid FROM OCEANBASE.__ALL_TENANT_SCHEDULER_JOB
        WHERE job_name = my_job_name AND powner = puser;
    END IF;
    RETURN ifnull(jid, 1);
  END;

  PROCEDURE CHECK_PRIVS (IN my_job_name VARCHAR(255))
  BEGIN
    DECLARE DUMMY BIGINT;
    DECLARE PUSER VARCHAR(128);
    SET PUSER = DBMS_ISCHEDULER.PUSER();
    SELECT 1 INTO DUMMY FROM OCEANBASE.__ALL_TENANT_SCHEDULER_JOB
      WHERE job_name = my_job_name AND POWNER = PUSER
            AND job != (SELECT MAX(job) FROM OCEANBASE.__ALL_TENANT_SCHEDULER_JOB);
  -- EXCEPTION
  --   WHEN NO_DATA_FOUND THEN
  --     RAISE_APPLICATION_ERROR(-20000, 'ORA-23421: job ' || job_name || ' is not a job in the job queue');
  END;

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
    PRAGMA INTERFACE(C, DBMS_SCHEDULER_MYSQL_CREATE_JOB);

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
                        IN max_run_duration   BIGINT)
  BEGIN
    DECLARE final_next_date TIMESTAMP;
    DECLARE final_start_date TIMESTAMP;
    DECLARE final_end_date TIMESTAMP;
    DECLARE my_flag INT;
    DECLARE my_interval VARCHAR(128);
    DECLARE what VARCHAR(128);
    DECLARE nlsenv VARCHAR(128);
    DECLARE env VARCHAR(128);
    DECLARE my_zone VARCHAR(128);

    DECLARE invalid_job_id CONDITION FOR SQLSTATE 'HY000';

    IF JOB < 1 THEN
      BEGIN
        DECLARE error_msg VARCHAR(255);
        SET error_msg = CONCAT('invalid job id of ', JOB);
        SIGNAL invalid_job_id
          SET MESSAGE_TEXT = error_msg;
      END;
    END IF;

    SET final_start_date = ifnull(start_date, sysdate());
    SET final_next_date = final_start_date;
    SET final_end_date = ifnull(end_date, STR_TO_DATE('4000-01-01', '%Y-%m-%d %H:%i:%s'));


    SET my_interval = repeat_interval;
    SET my_flag = 0;
    SET what = job_action;
    SET nlsenv = NULL;
    SET env = NULL;
    SET my_zone = NULL;
    CALL do_create_job(JOB, LUSER, PUSER, CUSER, final_next_date, my_interval, my_flag, what,
                       nlsenv, env, job_name, job_type, job_action, number_of_argument, final_start_date,
                       repeat_interval, final_end_date, job_class, enabled, auto_drop, comments,
                       credential_name, destination_name, my_zone, program_name, job_style, interval_ts, max_run_duration);
  END;

END dbms_ischeduler;
