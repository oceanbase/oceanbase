#package_name: DBMS_BALANCE
#author: wangzhennan.wzn

CREATE OR REPLACE PACKAGE dbms_balance AUTHID CURRENT_USER AS

  PROCEDURE trigger_partition_balance(balance_timeout BINARY_INTEGER DEFAULT NULL);

END dbms_balance;
//