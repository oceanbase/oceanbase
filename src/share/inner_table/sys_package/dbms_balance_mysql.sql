#package_name: DBMS_BALANCE
#author: wangzhennan.wzn

CREATE OR REPLACE PACKAGE dbms_balance AUTHID CURRENT_USER

  PROCEDURE trigger_partition_balance(balance_timeout INT DEFAULT NULL);

  PROCEDURE set_balance_weight(
    weight              INT,
    database_name       VARCHAR(65535),
    table_name          VARCHAR(65535),
    partition_name      VARCHAR(65535) DEFAULT NULL,
    subpartition_name   VARCHAR(65535) DEFAULT NULL);

  PROCEDURE clear_balance_weight(
    database_name       VARCHAR(65535),
    table_name          VARCHAR(65535),
    partition_name      VARCHAR(65535) DEFAULT NULL,
    subpartition_name   VARCHAR(65535) DEFAULT NULL);

  PROCEDURE set_tablegroup_balance_weight(
    weight              INT,
    tablegroup_name     VARCHAR(65535));

  PROCEDURE clear_tablegroup_balance_weight(
    tablegroup_name     VARCHAR(65535));

END dbms_balance;
//