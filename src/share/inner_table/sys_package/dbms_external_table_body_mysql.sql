#package_name:dbms_external_table
#author:mingye.swj

CREATE OR REPLACE PACKAGE BODY dbms_external_table
  PROCEDURE AUTO_REFRESH_EXTERNAL_TABLE(
    IN     interval               INT            DEFAULT 0);
  PRAGMA INTERFACE(c, AUTO_REFRESH_EXTERNAL_TABLE);

END dbms_external_table;
