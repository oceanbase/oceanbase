#package_name:dbms_resource_manager
#author:dachuan.sdc

--
-- 完整 PL 包参考文档：https://docs.oracle.com/database/121/ARPLS/d_resmgr.htm
--
CREATE OR REPLACE PACKAGE dbms_resource_manager AUTHID CURRENT_USER

--
-- create a resource plan
--
  PROCEDURE create_plan(
    plan    VARCHAR(65535),
    comment VARCHAR(65535) DEFAULT ''
  );

--
-- delete resource plan
--
  PROCEDURE delete_plan(
    plan    VARCHAR(65535)
  );

--
-- create consumer group
--
  PROCEDURE create_consumer_group (
    consumer_group  VARCHAR(65535),
    comment         VARCHAR(65535) DEFAULT NULL
  );

--
-- delete consumer group
--
  PROCEDURE delete_consumer_group (
    consumer_group VARCHAR(65535)
  );

--
-- create plan directive
--
  PROCEDURE create_plan_directive(
    plan              VARCHAR(65535),
    group_or_subplan  VARCHAR(65535),
    comment           VARCHAR(65535) DEFAULT '',
    mgmt_p1           INT DEFAULT 100,
    utilization_limit INT DEFAULT 100,
    min_iops          INT DEFAULT 0,
    max_iops          INT DEFAULT 100,
    weight_iops       INT DEFAULT 0
  );

--
-- update plan directive
--
  PROCEDURE update_plan_directive(
    plan                  VARCHAR(65535),
    group_or_subplan      VARCHAR(65535),
    new_comment           VARCHAR(65535) DEFAULT NULL,
    new_mgmt_p1           INT DEFAULT NULL,
    new_utilization_limit INT DEFAULT NULL,
    new_min_iops          INT DEFAULT NULL,
    new_max_iops          INT DEFAULT NULL,
    new_weight_iops       INT DEFAULT NULL
  );

--
-- delete plan directive
--
  PROCEDURE delete_plan_directive (
    plan              VARCHAR(65535),
    group_or_subplan  VARCHAR(65535)
  );

--
-- set consumer group mapping rule
--
  PROCEDURE set_consumer_group_mapping (
    attribute VARCHAR(65535),
    value VARCHAR(65535),
    consumer_group VARCHAR(65535) DEFAULT NULL
  );



END dbms_resource_manager;
