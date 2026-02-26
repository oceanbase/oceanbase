#package_name: DBMS_BALANCE
#author: wangzhennan.wzn

CREATE OR REPLACE PACKAGE dbms_balance AUTHID CURRENT_USER IS

  PROCEDURE trigger_partition_balance(balance_timeout BINARY_INTEGER DEFAULT NULL);

  PROCEDURE set_balance_weight(
    weight              IN BINARY_INTEGER,
    schema_name         IN VARCHAR2,
    table_name          IN VARCHAR2,
    partition_name      IN VARCHAR2 DEFAULT NULL,
    subpartition_name   IN VARCHAR2 DEFAULT NULL);

  PROCEDURE clear_balance_weight(
    schema_name         IN VARCHAR2,
    table_name          IN VARCHAR2,
    partition_name      IN VARCHAR2 DEFAULT NULL,
    subpartition_name   IN VARCHAR2 DEFAULT NULL);

END dbms_balance;
//