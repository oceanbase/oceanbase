#package_name:dbms_upgrade
#author: hr351303


CREATE OR REPLACE PACKAGE __DBMS_UPGRADE
  PROCEDURE UPGRADE(package_name VARCHAR(1024),
                    load_from_file BOOLEAN DEFAULT TRUE);
  PROCEDURE UPGRADE_ALL(load_from_file BOOLEAN DEFAULT TRUE);
END;
//
