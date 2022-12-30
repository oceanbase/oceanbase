#package_name: dbms_udr
#author: luofan.zp

CREATE OR REPLACE PACKAGE BODY DBMS_UDR

  PROCEDURE CREATE_RULE (
    rule_name          VARCHAR(256),
    rule_owner_name    VARCHAR(128),
    pattern            LONGTEXT,
    replacement        LONGTEXT,
    enabled            VARCHAR(64) DEFAULT 'YES'
  );
  PRAGMA INTERFACE(c, CREATE_RULE);

  PROCEDURE REMOVE_RULE (
    rule_name          VARCHAR(256)
  );
  PRAGMA INTERFACE(c, REMOVE_RULE);

  PROCEDURE ENABLE_RULE (
    rule_name          VARCHAR(256)
  );
  PRAGMA INTERFACE(c, ENABLE_RULE);

  PROCEDURE DISABLE_RULE (
    rule_name          VARCHAR(256)
  );
  PRAGMA INTERFACE(c, DISABLE_RULE);

END dbms_udr;
