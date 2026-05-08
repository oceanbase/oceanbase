#package_name:dbms_java
#author:heyongyi.hyy

CREATE OR REPLACE PACKAGE dbms_java AUTHID CURRENT_USER
AS
  PROCEDURE ob_loadjar(jar_binary blob, flag varchar2 := '');

  PROCEDURE ob_dropjar(jar_binary blob);



  -- permission interfaces
  PROCEDURE grant_permission(grantee VARCHAR2,
                             permission_type VARCHAR2,
                             permission_name VARCHAR2,
                             permission_action VARCHAR2);

  PROCEDURE grant_permission(grantee VARCHAR2,
                             permission_type VARCHAR2,
                             permission_name VARCHAR2,
                             permission_action VARCHAR2,
                             key OUT NUMBER);


  PROCEDURE restrict_permission(grantee VARCHAR2,
                                permission_type VARCHAR2,
                                permission_name VARCHAR2,
                                permission_action VARCHAR2);

  PROCEDURE restrict_permission(grantee VARCHAR2,
                                permission_type VARCHAR2,
                                permission_name VARCHAR2,
                                permission_action VARCHAR2,
                                key OUT NUMBER);


  PROCEDURE revoke_permission(permission_schema VARCHAR2,
                              permission_type VARCHAR2,
                              permission_name VARCHAR2,
                              permission_action VARCHAR2);


  PROCEDURE disable_permission(key NUMBER);


  PROCEDURE enable_permission(key NUMBER);


  PROCEDURE delete_permission(key NUMBER);
END dbms_java;
//
