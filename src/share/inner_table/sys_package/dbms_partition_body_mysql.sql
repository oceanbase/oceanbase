#package_name: DBMS_PARTITION
#author: zhaoziqian.zzq

CREATE OR REPLACE PACKAGE BODY dbms_partition

  PROCEDURE manage_dynamic_partition (precreate_time VARCHAR(64) DEFAULT NULL,
                                      time_unit VARCHAR(64) DEFAULT NULL);
  PRAGMA INTERFACE(C, DBMS_PARTITION_MANAGE_DYNAMIC_PARTITION);

END dbms_partition;
