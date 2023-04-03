#package_name:dbms_udr
#author: luofan.zp

CREATE OR REPLACE PACKAGE BODY DBMS_UDR AS

  PROCEDURE CREATE_RULE (
    rule_name          IN VARCHAR,
    pattern            IN CLOB,
    replacement        IN CLOB,
    enabled            IN VARCHAR := 'YES'
  );
  PRAGMA INTERFACE(c, CREATE_RULE);

  PROCEDURE REMOVE_RULE (
    rule_name          IN VARCHAR
  );
  PRAGMA INTERFACE(c, REMOVE_RULE);

  PROCEDURE ENABLE_RULE (
    rule_name          IN VARCHAR
  );
  PRAGMA INTERFACE(c, ENABLE_RULE);

  PROCEDURE DISABLE_RULE (
    rule_name          IN VARCHAR
  );
  PRAGMA INTERFACE(c, DISABLE_RULE);

END DBMS_UDR;
//
