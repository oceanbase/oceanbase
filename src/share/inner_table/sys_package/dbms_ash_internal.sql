-- package_name:dbms_ash_internal
-- author:xiaochu.yh


CREATE OR REPLACE PACKAGE dbms_ash_internal AUTHID CURRENT_USER AS

  FUNCTION  IN_MEMORY_ASH_VIEW_SQL
    RETURN  VARCHAR2;

  FUNCTION  ASH_VIEW_SQL
    RETURN  VARCHAR2;

END dbms_ash_internal;
