#package_name: __dbms_upgrade
#author: linlin.xll

CREATE OR REPLACE PACKAGE "__DBMS_UPGRADE" IS
  PROCEDURE UPGRADE(package_name VARCHAR2);
  PROCEDURE UPGRADE_ALL;
END;
//
