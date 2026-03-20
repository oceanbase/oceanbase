#package_name:dbms_java
#author:heyongyi.hyy

CREATE OR REPLACE PACKAGE BODY dbms_java
  PROCEDURE loadjava(IN jar_name VARCHAR(65535), IN content LONGBLOB, IN comment VARCHAR(65535) DEFAULT '');
    PRAGMA INTERFACE(C, DBMS_JAVA_LOADJAVA_MYSQL);

  PROCEDURE dropjava(IN jar_name VARCHAR(65535));
    PRAGMA INTERFACE(C, DBMS_JAVA_DROPJAVA_MYSQL);



  -- permission interfaces
  PROCEDURE grant_permission(IN grantee VARCHAR(65535),
                             IN permission_type VARCHAR(65535),
                             IN permission_name VARCHAR(65535),
                             IN permission_action VARCHAR(65535));
    PRAGMA INTERFACE(C, DBMS_JAVA_GRANT_PERMISSION);

  PROCEDURE grant_permission(IN grantee VARCHAR(65535),
                             IN permission_type VARCHAR(65535),
                             IN permission_name VARCHAR(65535),
                             IN permission_action VARCHAR(65535),
                             OUT key BIGINT);
    PRAGMA INTERFACE(C, DBMS_JAVA_GRANT_PERMISSION_WITH_KEY);


  PROCEDURE restrict_permission(IN grantee VARCHAR(65535),
                                IN permission_type VARCHAR(65535),
                                IN permission_name VARCHAR(65535),
                                IN permission_action VARCHAR(65535));
    PRAGMA INTERFACE(C, DBMS_JAVA_RESTRICT_PERMISSION);

  PROCEDURE restrict_permission(IN grantee VARCHAR(65535),
                                IN permission_type VARCHAR(65535),
                                IN permission_name VARCHAR(65535),
                                IN permission_action VARCHAR(65535),
                                OUT key BIGINT);
    PRAGMA INTERFACE(C, DBMS_JAVA_RESTRICT_PERMISSION_WITH_KEY);


  PROCEDURE revoke_permission(IN permission_schema VARCHAR(65535),
                              IN permission_type VARCHAR(65535),
                              IN permission_name VARCHAR(65535),
                              IN permission_action VARCHAR(65535));
    PRAGMA INTERFACE(C, DBMS_JAVA_REVOKE_PERMISSION);


  PROCEDURE disable_permission(IN key BIGINT);
    PRAGMA INTERFACE(C, DBMS_JAVA_DISABLE_PERMISSION);


  PROCEDURE enable_permission(IN key BIGINT);
    PRAGMA INTERFACE(C, DBMS_JAVA_ENABLE_PERMISSION);


  PROCEDURE delete_permission(IN key BIGINT);
    PRAGMA INTERFACE(C, DBMS_JAVA_DELETE_PERMISSION);
END dbms_java;
