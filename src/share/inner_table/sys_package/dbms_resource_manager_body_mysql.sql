#package_name:dbms_resource_manager
#author:dachuan.sdc

CREATE OR REPLACE PACKAGE BODY dbms_resource_manager

PROCEDURE create_plan (
  plan VARCHAR(65535),
  comment VARCHAR(65535) DEFAULT ''
);
pragma interface (C, CREATE_PLAN_INNER);

PROCEDURE delete_plan (
  plan    VARCHAR(65535)
);
pragma interface (C, DELETE_PLAN_INNER);

PROCEDURE create_consumer_group (
  consumer_group  VARCHAR(65535),
  comment         VARCHAR(65535)  DEFAULT NULL
);
pragma interface (C, CREATE_CONSUMER_GROUP_INNER);

PROCEDURE delete_consumer_group (
  consumer_group VARCHAR(65535)
);
pragma interface (C, DELETE_CONSUMER_GROUP_INNER);

PROCEDURE create_plan_directive (
  plan              VARCHAR(65535),
  group_or_subplan  VARCHAR(65535),
  comment           VARCHAR(65535) DEFAULT '',
  mgmt_p1           INT DEFAULT 100,
  utilization_limit INT DEFAULT 100,
  min_iops          INT DEFAULT 0,
  max_iops          INT DEFAULT 100,
  weight_iops       INT DEFAULT 0
);
pragma interface (C, CREATE_PLAN_DIRECTIVE_INNER);

PROCEDURE update_plan_directive (
  plan                  VARCHAR(65535),
  group_or_subplan      VARCHAR(65535),
  new_comment           VARCHAR(65535) DEFAULT NULL,
  new_mgmt_p1           INT DEFAULT NULL,
  new_utilization_limit INT DEFAULT NULL,
  new_min_iops          INT DEFAULT NULL,
  new_max_iops          INT DEFAULT NULL,
  new_weight_iops       INT DEFAULT NULL
);
pragma interface (C, UPDATE_PLAN_DIRECTIVE_INNER);

PROCEDURE delete_plan_directive (
  plan              VARCHAR(65535),
  group_or_subplan  VARCHAR(65535)
);
pragma interface (C, DELETE_PLAN_DIRECTIVE_INNER);

PROCEDURE set_consumer_group_mapping (
  attribute VARCHAR(65535),
  value VARCHAR(65535),
  consumer_group VARCHAR(65535) DEFAULT NULL
);
pragma interface (C, SET_CONSUMER_GROUP_MAPPING_INNER);


END dbms_resource_manager;
