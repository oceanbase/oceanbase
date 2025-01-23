#package_name:dbms_resource_manager
#author:dachuan.sdc

CREATE OR REPLACE PACKAGE dbms_resource_manager AUTHID CURRENT_USER

  PROCEDURE create_plan(
    plan    VARCHAR(65535),
    comment VARCHAR(65535) DEFAULT ''
  );


  PROCEDURE delete_plan(
    plan    VARCHAR(65535)
  );

  PROCEDURE create_consumer_group (
    consumer_group  VARCHAR(65535),
    comment         VARCHAR(65535) DEFAULT NULL
  );

  PROCEDURE delete_consumer_group (
    consumer_group VARCHAR(65535)
  );

  PROCEDURE create_plan_directive(
    plan                 VARCHAR(65535),
    group_or_subplan     VARCHAR(65535),
    comment              VARCHAR(65535) DEFAULT '',
    mgmt_p1              INT DEFAULT 100,
    utilization_limit    INT DEFAULT 100,
    min_iops             INT DEFAULT 0,
    max_iops             INT DEFAULT 100,
    weight_iops          INT DEFAULT 0,
    max_net_bandwidth    INT DEFAULT 100,
    net_bandwidth_weight INT DEFAULT 0
  );

  PROCEDURE update_plan_directive(
    plan                      VARCHAR(65535),
    group_or_subplan          VARCHAR(65535),
    new_comment               VARCHAR(65535) DEFAULT NULL,
    new_mgmt_p1               INT DEFAULT NULL,
    new_utilization_limit     INT DEFAULT NULL,
    new_min_iops              INT DEFAULT NULL,
    new_max_iops              INT DEFAULT NULL,
    new_weight_iops           INT DEFAULT NULL,
    new_max_net_bandwidth     INT DEFAULT NULL,
    new_net_bandwidth_weight  INT DEFAULT NULL
  );

  PROCEDURE delete_plan_directive (
    plan              VARCHAR(65535),
    group_or_subplan  VARCHAR(65535)
  );

  PROCEDURE set_consumer_group_mapping (
    attribute VARCHAR(65535),
    value VARCHAR(65535),
    consumer_group VARCHAR(65535) DEFAULT NULL
  );

END dbms_resource_manager;
