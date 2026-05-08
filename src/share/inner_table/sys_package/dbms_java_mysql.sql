#package_name:dbms_java
#author:heyongyi.hyy

CREATE OR REPLACE PACKAGE dbms_java AUTHID CURRENT_USER
  PROCEDURE loadjava(IN jar_name VARCHAR(65535), IN content LONGBLOB, IN comment VARCHAR(65535) DEFAULT '');

  PROCEDURE dropjava(IN jar_name VARCHAR(65535));



  -- permission interfaces
  PROCEDURE grant_permission(IN grantee VARCHAR(65535),
                             IN permission_type VARCHAR(65535),
                             IN permission_name VARCHAR(65535),
                             IN permission_action VARCHAR(65535));

  PROCEDURE grant_permission(IN grantee VARCHAR(65535),
                             IN permission_type VARCHAR(65535),
                             IN permission_name VARCHAR(65535),
                             IN permission_action VARCHAR(65535),
                             OUT key BIGINT);


  PROCEDURE restrict_permission(IN grantee VARCHAR(65535),
                                IN permission_type VARCHAR(65535),
                                IN permission_name VARCHAR(65535),
                                IN permission_action VARCHAR(65535));

  PROCEDURE restrict_permission(IN grantee VARCHAR(65535),
                                IN permission_type VARCHAR(65535),
                                IN permission_name VARCHAR(65535),
                                IN permission_action VARCHAR(65535),
                                OUT key BIGINT);


  PROCEDURE revoke_permission(IN permission_schema VARCHAR(65535),
                              IN permission_type VARCHAR(65535),
                              IN permission_name VARCHAR(65535),
                              IN permission_action VARCHAR(65535));


  PROCEDURE disable_permission(IN key BIGINT);


  PROCEDURE enable_permission(IN key BIGINT);


  PROCEDURE delete_permission(IN key BIGINT);
END dbms_java;
