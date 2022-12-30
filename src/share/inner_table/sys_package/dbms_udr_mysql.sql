#package_name:dbms_udr
#author: luofan.zp

CREATE OR REPLACE PACKAGE DBMS_UDR AUTHID CURRENT_USER

  PROCEDURE CREATE_RULE (
    rule_name          VARCHAR(256),
    rule_owner_name    VARCHAR(128),
    pattern            LONGTEXT,
    replacement        LONGTEXT,
    enabled            VARCHAR(64) DEFAULT 'YES'
  );

  PROCEDURE REMOVE_RULE (
    rule_name          VARCHAR(256)
  );

  PROCEDURE ENABLE_RULE (
    rule_name          VARCHAR(256)
  );

  PROCEDURE DISABLE_RULE (
    rule_name          VARCHAR(256)
  );

END DBMS_UDR;
