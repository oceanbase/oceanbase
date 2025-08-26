#package_name:dbms_java
#author:heyongyi.hyy

CREATE OR REPLACE PACKAGE BODY dbms_java
  PROCEDURE loadjava(IN jar_name VARCHAR(65535), IN content LONGBLOB, IN comment VARCHAR(65535) DEFAULT '');
    PRAGMA INTERFACE(C, DBMS_JAVA_LOADJAVA_MYSQL);

  PROCEDURE dropjava(IN jar_name VARCHAR(65535));
    PRAGMA INTERFACE(C, DBMS_JAVA_DROPJAVA_MYSQL);
END dbms_java;
