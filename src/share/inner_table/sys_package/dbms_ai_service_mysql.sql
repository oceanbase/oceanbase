#package_name:dbms_ai_service
#author:shenyunlong.syl

CREATE OR REPLACE PACKAGE dbms_ai_service AUTHID CURRENT_USER
  PROCEDURE create_ai_model(IN name VARCHAR(128), IN params JSON);
  PROCEDURE drop_ai_model(IN name VARCHAR(128));
  PROCEDURE create_ai_model_endpoint(IN name VARCHAR(128), IN params JSON);
  PROCEDURE alter_ai_model_endpoint(IN name VARCHAR(128), IN params JSON);
  PROCEDURE drop_ai_model_endpoint(IN name VARCHAR(128));
  PROCEDURE register_provider(IN name VARCHAR(256), IN params JSON);
  PROCEDURE unregister_provider(IN name VARCHAR(256));
  PROCEDURE alter_provider(IN name VARCHAR(256), IN params JSON);
  PROCEDURE create_ai_gateway(IN name VARCHAR(256), IN params JSON);
  PROCEDURE alter_ai_gateway(IN name VARCHAR(256), IN params JSON);
  PROCEDURE drop_ai_gateway(IN name VARCHAR(256));
  PROCEDURE alter_model_profile(IN model VARCHAR(256), IN params JSON);
  PROCEDURE drop_model_profile(IN model VARCHAR(256));

END dbms_ai_service;
