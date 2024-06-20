#package_name:dbms_external_table
#author:mingye.swj

CREATE OR REPLACE PACKAGE dbms_external_table IS

  PROCEDURE AUTO_REFRESH_EXTERNAL_TABLE(
        interval IN BINARY_INTEGER := 0);

END dbms_external_table;
//
