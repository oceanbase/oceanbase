#package_name: DBMS_BALANCE
#author: wangzhennan.wzn

CREATE OR REPLACE PACKAGE BODY dbms_balance IS

  PROCEDURE trigger_partition_balance(balance_timeout BINARY_INTEGER DEFAULT NULL);
  PRAGMA INTERFACE(C, DBMS_BALANCE_TRIGGER_PARTITION_BALANCE);

  PROCEDURE set_balance_weight(
    weight              IN BINARY_INTEGER,
    schema_name         IN VARCHAR2,
    table_name          IN VARCHAR2,
    partition_name      IN VARCHAR2 DEFAULT NULL,
    subpartition_name   IN VARCHAR2 DEFAULT NULL);
  PRAGMA INTERFACE(C, DBMS_BALANCE_SET_BALANCE_WEIGHT);

  PROCEDURE clear_balance_weight(
    schema_name         IN VARCHAR2,
    table_name          IN VARCHAR2,
    partition_name      IN VARCHAR2 DEFAULT NULL,
    subpartition_name   IN VARCHAR2 DEFAULT NULL);
  PRAGMA INTERFACE(C, DBMS_BALANCE_CLEAR_BALANCE_WEIGHT);

END dbms_balance;
//