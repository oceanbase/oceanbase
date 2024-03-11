CREATE OR REPLACE PACKAGE BODY dbms_scheduler

  FUNCTION GENERATE_JOB_NAME(IN in_type_name VARCHAR(255) DEFAULT 'JOB$_') RETURN VARCHAR(255)
  BEGIN
    DECLARE JID BIGINT;
    DECLARE JNAME VARCHAR(255);
    SET JID = DBMS_ISCHEDULER.GET_AND_INCREASE_JOB_ID();
    SET JNAME = CONCAT(in_type_name, JID);
    RETURN JNAME;
  END;

  FUNCTION CALC_DELAY_TS(IN repeat_interval VARCHAR(255)) RETURN BIGINT
  BEGIN
    DECLARE freq VARCHAR(128);
    DECLARE intv VARCHAR(128);
    DECLARE pos INT DEFAULT 1;
    DECLARE old_pos INT DEFAULT 1;
    DECLARE freq_ts BIGINT;
    DECLARE intv_cnt INT;
    DECLARE interval_ts BIGINT;
    DECLARE my_error_code INT DEFAULT 0;

    IF repeat_interval = 'null' THEN
      SET interval_ts = 0;
    ELSE
      SET pos = instr(repeat_interval, ';');
      SET freq = substr(repeat_interval, old_pos, pos - old_pos);
      SET intv = substr(repeat_interval, pos);
      SET pos = instr(freq, '=');
      SET freq = substr(freq, pos+1);
      SET pos = instr(intv, '=');
      SET intv = substr(intv, pos+1);
      SET intv_cnt = intv;
      IF freq = 'SECONDLY' THEN
        SET freq_ts = 1 * 1000000;
      ELSEIF freq = 'MINUTELY' THEN
        SET freq_ts = 60 * 1000000;
      ELSEIF freq = 'HOURLY' THEN
        SET freq_ts = 60 * 60 * 1000000;
      ELSEIF freq = 'DAYLY' THEN
        SET freq_ts = 24 * 60 * 60 * 1000000;
      ELSEIF freq = 'WEEKLY' THEN
        SET freq_ts = 7 * 24 * 60 * 60 * 1000000;
      ELSEIF freq = 'MONTHLY' THEN
        SET freq_ts = 30 * 7 * 24 * 60 * 60 * 1000000;
      ELSEIF freq = 'YEARLY' THEN
        SET freq_ts = 12 * 30 * 7 * 24 * 60 * 60 * 1000000;
      ELSE
        -- RAISE_APPLICATION_ERROR(-20000, 'interval expression not valid: ' || repeat_interval);
        SET my_error_code = -1;
      END IF;
      SET interval_ts = freq_ts * intv_cnt;
    END IF;
    RETURN interval_ts;
  END;

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
                           IN max_run_duration    INT DEFAULT 0)
  BEGIN
    DECLARE JOB     BIGINT;
    DECLARE LUSER   VARCHAR(128);
    DECLARE PUSER   VARCHAR(128);
    DECLARE CUSER   VARCHAR(128);
    DECLARE program_name VARCHAR(255) DEFAULT NULL;
    DECLARE job_style VARCHAR(255) DEFAULT 'REGULER';
    DECLARE interval_ts BIGINT;

    IF job_class != 'DATE_EXPRESSION_JOB_CLASS' THEN
      SET interval_ts = CALC_DELAY_TS(repeat_interval);
    END IF;

    SET PUSER = DBMS_ISCHEDULER.PUSER();
    SET LUSER = DBMS_ISCHEDULER.LUSER();
    SET CUSER = LUSER;
    SET JOB = 1;

    CALL DBMS_ISCHEDULER.create_job(JOB, LUSER, PUSER, CUSER, job_name, program_name, job_type, job_action,
                              number_of_argument, start_date, repeat_interval, end_date, job_class,
                              enabled, auto_drop, comments, job_style, credential_name, destination_name,
                              interval_ts, max_run_duration);
  END;

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
