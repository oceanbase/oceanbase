#package_name: DBMS_PARTITION
#author: zhaoziqian.zzq

CREATE OR REPLACE PACKAGE dbms_partition AUTHID CURRENT_USER IS

  PROCEDURE manage_dynamic_partition (precreate_time IN VARCHAR2 DEFAULT NULL,
                                      time_unit IN VARCHAR2 DEFAULT NULL);

END dbms_partition;
//
