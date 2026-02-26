CREATE OR REPLACE PACKAGE BODY dbms_workload_repository AS

-- main function
FUNCTION ASH_REPORT_TEXT(L_BTIME       IN TIMESTAMP,
                         L_ETIME       IN TIMESTAMP,
                         SQL_ID        IN VARCHAR2  DEFAULT NULL,
                         TRACE_ID      IN VARCHAR2  DEFAULT NULL,
                         WAIT_CLASS    IN VARCHAR2  DEFAULT NULL,
                         SVR_IP        IN VARCHAR2  DEFAULT NULL,
                         SVR_PORT      IN NUMBER    DEFAULT NULL,
                         TENANT_ID     IN NUMBER    DEFAULT NULL,
                         REPORT_TYPE   IN VARCHAR2  DEFAULT 'text'
                        )
  RETURN CLOB;
  PRAGMA INTERFACE(C, GENERATE_ASH_REPORT_TEXT);

PROCEDURE ASH_REPORT(BTIME         IN TIMESTAMP,
                     ETIME         IN TIMESTAMP,
                     SQL_ID        IN VARCHAR2  DEFAULT NULL,
                     TRACE_ID      IN VARCHAR2  DEFAULT NULL,
                     WAIT_CLASS    IN VARCHAR2  DEFAULT NULL,
                     REPORT_TYPE   IN VARCHAR2  DEFAULT 'text',
                     SVR_IP        IN VARCHAR2  DEFAULT NULL,
                     SVR_PORT      IN NUMBER       DEFAULT NULL,
                     TENANT_ID     IN NUMBER       DEFAULT NULL
                   )
IS
  res CLOB;
  clob_length INTEGER;
  begin_offset INTEGER := 1;
  end_offset INTEGER := 1;
  line CLOB;
  newline_char CHAR(1) := CHR(10); -- Newline character (LF)
BEGIN
  DBMS_OUTPUT.ENABLE(NULL);
  IF (LOWER(REPORT_TYPE) = 'text' OR LOWER(REPORT_TYPE) = 'html') THEN
    res := DBMS_WORKLOAD_REPOSITORY.ASH_REPORT_TEXT(BTIME, ETIME, SQL_ID, TRACE_ID, WAIT_CLASS, SVR_IP, SVR_PORT, TENANT_ID, LOWER(REPORT_TYPE));
    clob_length := DBMS_LOB.GETLENGTH(res);
    WHILE end_offset <= clob_length LOOP
      end_offset := DBMS_LOB.INSTR(res, newline_char, begin_offset, 1);
      IF end_offset = 0 THEN
        DBMS_OUTPUT.PUT_LINE(DBMS_LOB.SUBSTR(res, 32767, begin_offset));
        EXIT;
      END IF;
      line := DBMS_LOB.SUBSTR(res, end_offset - begin_offset, begin_offset);
      DBMS_OUTPUT.PUT_LINE(line);
      begin_offset := end_offset + 1;
    END LOOP;
  ELSE
    DBMS_OUTPUT.PUT_LINE('Other formats are not currently supported besides text and html');
  END IF;
  -- DBMS_OUTPUT.NEW_LINE();
END ASH_REPORT;

END dbms_workload_repository;
