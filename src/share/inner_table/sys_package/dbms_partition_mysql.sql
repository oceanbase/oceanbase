#package_name: DBMS_PARTITION
#author: zhaoziqian.zzq

CREATE OR REPLACE PACKAGE dbms_partition AUTHID CURRENT_USER

  PROCEDURE manage_dynamic_partition (precreate_time VARCHAR(64) DEFAULT NULL,
                                      time_unit VARCHAR(64) DEFAULT NULL);

END dbms_partition;
