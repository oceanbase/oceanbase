#package_name:dbms_udr
#author: luofan.zp

CREATE OR REPLACE PACKAGE DBMS_UDR AUTHID CURRENT_USER AS

  PROCEDURE CREATE_RULE (
    rule_name          IN VARCHAR,
    pattern            IN CLOB,
    replacement        IN CLOB,
    enabled            IN VARCHAR  := 'YES'
  );

  PROCEDURE REMOVE_RULE (
    rule_name          IN VARCHAR
  );

  PROCEDURE ENABLE_RULE (
    rule_name          IN VARCHAR
  );

  PROCEDURE DISABLE_RULE (
    rule_name          IN VARCHAR
  );

END DBMS_UDR;
//
