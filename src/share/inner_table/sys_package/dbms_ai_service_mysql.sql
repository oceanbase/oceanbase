#package_name:dbms_ai_service
#author:shenyunlong.syl

CREATE OR REPLACE PACKAGE dbms_ai_service AUTHID CURRENT_USER
  PROCEDURE create_ai_model(IN name VARCHAR(128), IN params JSON);
  PROCEDURE drop_ai_model(IN name VARCHAR(128));
  PROCEDURE create_ai_model_endpoint(IN name VARCHAR(128), IN params JSON);
  PROCEDURE alter_ai_model_endpoint(IN name VARCHAR(128), IN params JSON);
  PROCEDURE drop_ai_model_endpoint(IN name VARCHAR(128));

END dbms_ai_service;
