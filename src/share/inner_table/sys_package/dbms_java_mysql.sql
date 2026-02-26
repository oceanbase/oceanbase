#package_name:dbms_java
#author:heyongyi.hyy

CREATE OR REPLACE PACKAGE dbms_java AUTHID CURRENT_USER
  PROCEDURE loadjava(IN jar_name VARCHAR(65535), IN content LONGBLOB, IN comment VARCHAR(65535) DEFAULT '');

  PROCEDURE dropjava(IN jar_name VARCHAR(65535));
END dbms_java;
