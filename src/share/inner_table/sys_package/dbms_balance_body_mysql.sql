#package_name: DBMS_BALANCE
#author: wangzhennan.wzn

CREATE OR REPLACE PACKAGE BODY dbms_balance

  PROCEDURE trigger_partition_balance(balance_timeout INT DEFAULT NULL);
  PRAGMA INTERFACE(C, DBMS_BALANCE_TRIGGER_PARTITION_BALANCE);

  PROCEDURE set_balance_weight(
    weight              INT,
    database_name       VARCHAR(65535),
    table_name          VARCHAR(65535),
    partition_name      VARCHAR(65535) DEFAULT NULL,
    subpartition_name   VARCHAR(65535) DEFAULT NULL);
  PRAGMA INTERFACE(C, DBMS_BALANCE_SET_BALANCE_WEIGHT);

  PROCEDURE clear_balance_weight(
    database_name       VARCHAR(65535),
    table_name          VARCHAR(65535),
    partition_name      VARCHAR(65535) DEFAULT NULL,
    subpartition_name   VARCHAR(65535) DEFAULT NULL);
  PRAGMA INTERFACE(C, DBMS_BALANCE_CLEAR_BALANCE_WEIGHT);

  PROCEDURE set_tablegroup_balance_weight(
    weight              INT,
    tablegroup_name     VARCHAR(65535));
  PRAGMA INTERFACE(C, DBMS_BALANCE_SET_TABLEGROUP_BALANCE_WEIGHT);

  PROCEDURE clear_tablegroup_balance_weight(
    tablegroup_name     VARCHAR(65535));
  PRAGMA INTERFACE(C, DBMS_BALANCE_CLEAR_TABLEGROUP_BALANCE_WEIGHT);

END dbms_balance;
//