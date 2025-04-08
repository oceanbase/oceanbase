#package_name: DBMS_PARTITION
#author: zhaoziqian.zzq

CREATE OR REPLACE PACKAGE BODY dbms_partition IS

  PROCEDURE manage_dynamic_partition (precreate_time IN VARCHAR2 DEFAULT NULL,
                                      time_unit IN VARCHAR2 DEFAULT NULL);
  PRAGMA INTERFACE(C, DBMS_PARTITION_MANAGE_DYNAMIC_PARTITION);

END dbms_partition;
//
