#package_name:dbms_external_table
#author:mingye.swj

CREATE OR REPLACE PACKAGE dbms_external_table AUTHID CURRENT_USER

  PROCEDURE AUTO_REFRESH_EXTERNAL_TABLE(
    IN     interval               INT            DEFAULT 0);

END dbms_external_table;
