#package_name: DBMS_BALANCE
#author: wangzhennan.wzn

CREATE OR REPLACE PACKAGE BODY dbms_balance AS

  PROCEDURE trigger_partition_balance(balance_timeout BINARY_INTEGER DEFAULT NULL);
  PRAGMA INTERFACE(C, DBMS_BALANCE_TRIGGER_PARTITION_BALANCE);

END dbms_balance;
//