#package_name: DBMS_BALANCE
#author: wangzhennan.wzn

CREATE OR REPLACE PACKAGE dbms_balance AUTHID CURRENT_USER

  PROCEDURE trigger_partition_balance(balance_timeout INT DEFAULT NULL);

END dbms_balance;
//