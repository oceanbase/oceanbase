#package_name:dbms_ob_limit_calculator
#author:cxf262476, yangyifei.yyf

CREATE OR replace PACKAGE BODY dbms_ob_limit_calculator
  PROCEDURE phy_res_calculate_by_logic_res_inner(
    IN  args                                VARCHAR(1024) DEFAULT '',
    OUT res                                 VARCHAR(2048));
  PRAGMA INTERFACE(C, PHY_RES_CALCULATE_BY_LOGIC_RES);

  PROCEDURE phy_res_calculate_by_unit_inner(
    IN  tenant_id                           INTEGER,
    IN  server                              VARCHAR(64),
    OUT res                                 VARCHAR(2048));
  PRAGMA INTERFACE(C, PHY_RES_CALCULATE_BY_UNIT);

  PROCEDURE phy_res_calculate_by_standby_tenant_inner(
    IN  primary_tenant_id                   INTEGER,
    IN  standby_tenant_unit_num             INTEGER,
    OUT res                                 VARCHAR(2048));
  PRAGMA INTERFACE(C, PHY_RES_CALCULATE_BY_STANDBY_TENANT);

  PROCEDURE calculate_min_phy_res_needed_by_unit(
    IN tenant_id                            INTEGER,
    IN server                               VARCHAR(64))
  BEGIN
    DECLARE res VARCHAR(2048);
    CALL phy_res_calculate_by_unit_inner(tenant_id, server, res);
    SELECT * FROM JSON_TABLE(res, '$[*]' COLUMNS (SVR_IP VARCHAR(64) PATH '$.svr_ip',
                                                  SVR_PORT INTEGER PATH '$.svr_port',
                                                  TENANT_ID INTEGER PATH '$.tenant_id',
                                                  PHYSICAL_RESOURCE_NAME  VARCHAR(64) PATH '$.physical_resource_name',
                                                  MIN_VALUE BIGINT PATH '$.min_value')) t;
  END;

  PROCEDURE calculate_min_phy_res_needed_by_logic_res(
    IN args                                 VARCHAR(1024) DEFAULT '')
  BEGIN
    DECLARE res VARCHAR(2048);
    CALL phy_res_calculate_by_logic_res_inner(args, res);
    SELECT * FROM JSON_TABLE(res, '$[*]' COLUMNS (PHYSICAL_RESOURCE_NAME VARCHAR(64) PATH '$.physical_resource_name',
                                                  MIN_VALUE BIGINT PATH '$.min_value')) t;
  END;

  PROCEDURE calculate_min_phy_res_needed_by_standby_tenant(
    IN primary_tenant_id                    INTEGER,
    IN standby_tenant_unit_num              INTEGER)
  BEGIN
    DECLARE res VARCHAR(2048);
    CALL phy_res_calculate_by_standby_tenant_inner(primary_tenant_id, standby_tenant_unit_num, res);
    SELECT * FROM JSON_TABLE(res, '$[*]' COLUMNS (PHYSICAL_RESOURCE_NAME VARCHAR(64) PATH '$.physical_resource_name',
                                                  MIN_VALUE BIGINT PATH '$.min_value')) t;
  END;


END;
//
