#package_name:dbms_monitor
#author: guoyun.lgy
CREATE OR REPLACE PACKAGE DBMS_MONITOR
  PROCEDURE OB_SESSION_TRACE_ENABLE(SESSION_ID   DECIMAL,
                                    LEVEL        DECIMAL,
                                    SAMPLE_PCT   DECIMAL,
                                    RECORD_POLICY VARCHAR(65535));
  PROCEDURE OB_SESSION_TRACE_DISABLE(session_id   DECIMAL);

  PROCEDURE OB_CLIENT_ID_TRACE_ENABLE(CLIENT_ID    VARCHAR(65535),
                                      LEVEL        DECIMAL,
                                      SAMPLE_PCT   DECIMAL,
                                      RECORD_POLICY VARCHAR(65535));
  PROCEDURE OB_CLIENT_ID_TRACE_DISABLE(CLIENT_ID VARCHAR(65535));

  PROCEDURE OB_MOD_ACT_TRACE_ENABLE(MODULE_NAME     VARCHAR(65535),
                                    ACTION_NAME     VARCHAR(65535),
                                    LEVEL        DECIMAL,
                                    SAMPLE_PCT   DECIMAL,
                                    RECORD_POLICY VARCHAR(65535));
  PROCEDURE OB_MOD_ACT_TRACE_DISABLE(MODULE_NAME     VARCHAR(65535),
                                    ACTION_NAME     VARCHAR(65535));

  PROCEDURE OB_TENANT_TRACE_ENABLE(LEVEL        DECIMAL,
                            SAMPLE_PCT   DECIMAL,
                            RECORD_POLICY VARCHAR(65535));
  PROCEDURE OB_TENANT_TRACE_DISABLE(TENANT_NAME  VARCHAR(65535) DEFAULT NULL);
END;


//

