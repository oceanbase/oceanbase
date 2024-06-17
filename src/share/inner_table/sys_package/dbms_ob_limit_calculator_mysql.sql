#package_name:dbms_ob_limit_calculator
#author:cxf262476, yangyifei.yyf

CREATE OR REPLACE PACKAGE dbms_ob_limit_calculator
  PROCEDURE calculate_min_phy_res_needed_by_unit(
    IN tenant_id                                  INTEGER,
    IN server                                     VARCHAR(64));
  PROCEDURE calculate_min_phy_res_needed_by_logic_res(
    IN args                                       VARCHAR(1024) DEFAULT '');
  PROCEDURE calculate_min_phy_res_needed_by_standby_tenant(
    IN primary_tenant_id                          INTEGER,
    IN standby_tenant_unit_num                    INTEGER);
END;
//
