#package_name:dbms_session
#author: peihan.dph

CREATE OR REPLACE PACKAGE DBMS_SESSION
PROCEDURE SET_IDENTIFIER(CLIENT_ID VARCHAR(65535));
  --    Input parameters: 
  --    client_id
  --      client identifier being set for this session .
PROCEDURE CLEAR_IDENTIFIER();
  -- Input parameters:
  --   none
END;
//
