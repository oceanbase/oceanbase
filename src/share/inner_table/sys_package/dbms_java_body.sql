#package_name:dbms_java
#author:heyongyi.hyy

CREATE OR REPLACE PACKAGE BODY dbms_java
AS
  PROCEDURE ob_loadjar(jar_binary blob, flag varchar2 := '');
    PRAGMA INTERFACE(C, DBMS_JAVA_OB_LOADJAR);

  PROCEDURE ob_dropjar(jar_binary blob);
    PRAGMA INTERFACE(C, DBMS_JAVA_OB_DROPJAR);



  -- permission interfaces
  PROCEDURE grant_permission(grantee VARCHAR2,
                             permission_type VARCHAR2,
                             permission_name VARCHAR2,
                             permission_action VARCHAR2);
    PRAGMA INTERFACE(C, DBMS_JAVA_GRANT_PERMISSION);

  PROCEDURE grant_permission(grantee VARCHAR2,
                             permission_type VARCHAR2,
                             permission_name VARCHAR2,
                             permission_action VARCHAR2,
                             key OUT NUMBER);
    PRAGMA INTERFACE(C, DBMS_JAVA_GRANT_PERMISSION_WITH_KEY);


  PROCEDURE restrict_permission(grantee VARCHAR2,
                                permission_type VARCHAR2,
                                permission_name VARCHAR2,
                                permission_action VARCHAR2);
    PRAGMA INTERFACE(C, DBMS_JAVA_RESTRICT_PERMISSION);

  PROCEDURE restrict_permission(grantee VARCHAR2,
                                permission_type VARCHAR2,
                                permission_name VARCHAR2,
                                permission_action VARCHAR2,
                                key OUT NUMBER);
    PRAGMA INTERFACE(C, DBMS_JAVA_RESTRICT_PERMISSION_WITH_KEY);


  PROCEDURE revoke_permission(permission_schema VARCHAR2,
                              permission_type VARCHAR2,
                              permission_name VARCHAR2,
                              permission_action VARCHAR2);
    PRAGMA INTERFACE(C, DBMS_JAVA_REVOKE_PERMISSION);


  PROCEDURE disable_permission(key NUMBER);
    PRAGMA INTERFACE(C, DBMS_JAVA_DISABLE_PERMISSION);


  PROCEDURE enable_permission(key NUMBER);
    PRAGMA INTERFACE(C, DBMS_JAVA_ENABLE_PERMISSION);


  PROCEDURE delete_permission(key NUMBER);
    PRAGMA INTERFACE(C, DBMS_JAVA_DELETE_PERMISSION);
END dbms_java;
