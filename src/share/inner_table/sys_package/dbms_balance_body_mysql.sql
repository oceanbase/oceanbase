#package_name: DBMS_BALANCE
#author: wangzhennan.wzn

CREATE OR REPLACE PACKAGE BODY dbms_balance

  PROCEDURE trigger_partition_balance(balance_timeout INT DEFAULT NULL);
  PRAGMA INTERFACE(C, DBMS_BALANCE_TRIGGER_PARTITION_BALANCE);

END dbms_balance;
//