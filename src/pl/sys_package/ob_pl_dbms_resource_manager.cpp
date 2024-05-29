/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX PL
#include "ob_pl_dbms_resource_manager.h"
#include "share/resource_manager/ob_resource_manager_proxy.h"
#include "sql/privilege_check/ob_ora_priv_check.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::pl;

int ObPlDBMSResourceManager::create_plan(
    sql::ObExecContext &ctx,
    sql::ParamStore &params,
    common::ObObj &result)
{
  enum {
    PLAN = 0,
    COMMENT = 1,
    MAX_PARAM
  };

  int ret = OB_SUCCESS;
  uint64_t tenant_id;
  UNUSED(result);
  ObResourceManagerProxy proxy;
  ObString plan;
  sql::ObSQLSessionInfo *sess = GET_MY_SESSION(ctx);
  if (OB_ISNULL(sess) || params.count() < MAX_PARAM) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("err unexpected", K(params.count()), K(MAX_PARAM), K(ret));
  } else {
    tenant_id = sess->get_effective_tenant_id();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count() && i < COMMENT; ++i) {
    ObObj &obj = params.at(i);
    if (PLAN == i) {
      ret = obj.get_string(plan);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(0 == plan.length())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("name of plan cannot be null or empty", K(ret));
    // plan 肯定存在，所以可以 get_string 读取，
    // COMMENT 是可选的可能为 null，所以传入 ObObj
    } else if (OB_FAIL(proxy.create_plan(tenant_id, plan, params.at(COMMENT)))) {
      LOG_WARN("fail create plan", K(tenant_id), K(plan), K(ret));
    }
  }
  return ret;
}


int ObPlDBMSResourceManager::delete_plan(
    sql::ObExecContext &ctx,
    sql::ParamStore &params,
    common::ObObj &result)
{
  enum {
    PLAN = 0,
    MAX_PARAM
  };

  int ret = OB_SUCCESS;
  uint64_t tenant_id;
  UNUSED(result);
  ObResourceManagerProxy proxy;
  ObString plan;
  sql::ObSQLSessionInfo *sess = GET_MY_SESSION(ctx);
  if (OB_ISNULL(sess) || params.count() < MAX_PARAM) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("err unexpected", K(params.count()), K(MAX_PARAM), K(ret));
  } else {
    tenant_id = sess->get_effective_tenant_id();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count() && i < MAX_PARAM; ++i) {
    ObObj &obj = params.at(i);
    if (PLAN == i) {
      ret = obj.get_string(plan);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(proxy.delete_plan(tenant_id, plan))) {
      LOG_WARN("fail create plan", K(tenant_id), K(plan), K(ret));
    }
  }
  return ret;
}

int ObPlDBMSResourceManager::create_consumer_group(
    sql::ObExecContext &ctx,
    sql::ParamStore &params,
    common::ObObj &result)
{
  enum {
    CONSUMER_GROUP = 0,
    COMMENT = 1,
    MAX_PARAM
  };

  int ret = OB_SUCCESS;
  uint64_t tenant_id;
  UNUSED(result);
  ObResourceManagerProxy proxy;
  ObString consumer_group;
  sql::ObSQLSessionInfo *sess = GET_MY_SESSION(ctx);
  if (OB_ISNULL(sess) || params.count() < MAX_PARAM) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("err unexpected", K(params.count()), K(MAX_PARAM), K(ret));
  } else {
    tenant_id = sess->get_effective_tenant_id();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count() && i < COMMENT; ++i) {
    ObObj &obj = params.at(i);
    if (CONSUMER_GROUP == i) {
      ret = obj.get_string(consumer_group);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(0 == consumer_group.length())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("name of consumer group cannot be null or empty", K(ret));
    // consumer_group 肯定存在，所以可以 get_string 读取，
    // COMMENT 是可选的可能为 null，所以传入 ObObj
    } else if (!GCTX.cgroup_ctrl_->is_valid_group_name(consumer_group)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid consumer group name");
      LOG_WARN("invalid consumer group name", K(ret), K(consumer_group));
    } else if (OB_FAIL(proxy.create_consumer_group(tenant_id, consumer_group, params.at(COMMENT)))) {
      LOG_WARN("fail create consumer_group", K(tenant_id), K(consumer_group), K(ret));
    }
  }
  return ret;
}


int ObPlDBMSResourceManager::delete_consumer_group(
    sql::ObExecContext &ctx,
    sql::ParamStore &params,
    common::ObObj &result)
{
  enum {
    CONSUMER_GROUP = 0,
    MAX_PARAM
  };

  int ret = OB_SUCCESS;
  uint64_t tenant_id;
  UNUSED(result);
  ObResourceManagerProxy proxy;
  ObString consumer_group;
  sql::ObSQLSessionInfo *sess = GET_MY_SESSION(ctx);
  if (OB_ISNULL(sess) || params.count() < MAX_PARAM) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("err unexpected", K(params.count()), K(MAX_PARAM), K(ret));
  } else {
    tenant_id = sess->get_effective_tenant_id();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count() && i < MAX_PARAM; ++i) {
    ObObj &obj = params.at(i);
    if (CONSUMER_GROUP == i) {
      ret = obj.get_string(consumer_group);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(proxy.delete_consumer_group(tenant_id, consumer_group))) {
      LOG_WARN("fail delete consumer_group", K(tenant_id), K(consumer_group), K(ret));
    }
  }
  return ret;
}

int ObPlDBMSResourceManager::create_plan_directive(
    sql::ObExecContext &ctx,
    sql::ParamStore &params,
    common::ObObj &result)
{
  enum {
    PLAN = 0,
    GROUP = 1,
    COMMENT = 2,
    MGMT_P1 = 3,
    UTILIZATION_LIMIT = 4,
    MIN_IOPS = 5,
    MAX_IOPS = 6,
    WEIGHT_IOPS = 7,
    // MAX_NET_BADNWIDTH = 8,    placeholder
    // NET_BANDWIDTH_WEIGHT = 9, placeholder
    MAX_PARAM
  };

  int ret = OB_SUCCESS;
  uint64_t tenant_id;
  UNUSED(result);
  ObResourceManagerProxy proxy;
  ObString plan;
  ObString group;
  sql::ObSQLSessionInfo *sess = GET_MY_SESSION(ctx);
  if (OB_ISNULL(sess) || params.count() < MAX_PARAM) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("err unexpected", K(params.count()), K(MAX_PARAM), K(ret));
  } else {
    tenant_id = sess->get_effective_tenant_id();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count() && i < COMMENT; ++i) {
    LOG_INFO("pl params", K(params.at(i)));
    ObObj &obj = params.at(i);
    if (PLAN == i) {
      ret = obj.get_string(plan);
    } else if (GROUP == i) {
      ret = obj.get_string(group);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(proxy.create_plan_directive(tenant_id,
                                            plan,
                                            group,
                                            params.at(COMMENT),
                                            params.at(MGMT_P1),
                                            params.at(UTILIZATION_LIMIT),
                                            params.at(MIN_IOPS),
                                            params.at(MAX_IOPS),
                                            params.at(WEIGHT_IOPS)))) {
      LOG_WARN("fail create plan directive", K(tenant_id), K(plan), K(group), K(ret));
    }
  }
  return ret;
}


int ObPlDBMSResourceManager::delete_plan_directive(
    sql::ObExecContext &ctx,
    sql::ParamStore &params,
    common::ObObj &result)
{
  enum {
    PLAN = 0,
    GROUP = 1,
    MAX_PARAM
  };

  int ret = OB_SUCCESS;
  uint64_t tenant_id;
  UNUSED(result);
  ObResourceManagerProxy proxy;
  ObString plan;
  ObString group;
  sql::ObSQLSessionInfo *sess = GET_MY_SESSION(ctx);
  if (OB_ISNULL(sess) || params.count() < MAX_PARAM) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("err unexpected", K(params.count()), K(MAX_PARAM), K(ret));
  } else {
    tenant_id = sess->get_effective_tenant_id();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count() && i < MAX_PARAM; ++i) {
    ObObj &obj = params.at(i);
    if (PLAN == i) {
      ret = obj.get_string(plan);
    } else if (GROUP == i) {
      ret = obj.get_string(group);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(proxy.delete_plan_directive(tenant_id, plan, group))) {
      LOG_WARN("fail create plan", K(tenant_id), K(plan), K(ret));
    }
  }
  return ret;
}

int ObPlDBMSResourceManager::update_plan_directive(
    sql::ObExecContext &ctx,
    sql::ParamStore &params,
    common::ObObj &result)
{
  enum {
    PLAN = 0,
    GROUP = 1,
    COMMENT = 2,
    MGMT_P1 = 3,
    UTILIZATION_LIMIT = 4,
    MIN_IOPS = 5,
    MAX_IOPS = 6,
    WEIGHT_IOPS = 7,
    // MAX_NET_BADNWIDTH = 8,    placeholder
    // NET_BANDWIDTH_WEIGHT = 9, placeholder
    MAX_PARAM
  };
  int ret = OB_SUCCESS;
  uint64_t tenant_id;
  UNUSED(result);
  ObResourceManagerProxy proxy;
  ObString plan;
  ObString group;
  sql::ObSQLSessionInfo *sess = GET_MY_SESSION(ctx);
  if (OB_ISNULL(sess) || params.count() < MAX_PARAM) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("err unexpected", K(params.count()), K(MAX_PARAM), K(ret));
  } else {
    tenant_id = sess->get_effective_tenant_id();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count() && i < 2; ++i) {
    ObObj &obj = params.at(i);
    if (PLAN == i) {
      ret = obj.get_string(plan);
    } else if (GROUP == i) {
      ret = obj.get_string(group);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(proxy.update_plan_directive(tenant_id,
                                            plan,
                                            group,
                                            params.at(COMMENT),
                                            params.at(MGMT_P1),
                                            params.at(UTILIZATION_LIMIT),
                                            params.at(MIN_IOPS),
                                            params.at(MAX_IOPS),
                                            params.at(WEIGHT_IOPS)))) {
      LOG_WARN("fail update plan directive", K(tenant_id), K(plan), K(group), K(ret));
    }
  }
  return ret;
}

int ObPlDBMSResourceManager::set_consumer_group_mapping(
    sql::ObExecContext &ctx,
    sql::ParamStore &params,
    common::ObObj &result)
{
  enum {
    ATTR = 0,
    VALUE = 1,
    GROUP = 2,
    MAX_PARAM
  };

  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_ID;
  UNUSED(result);
  ObResourceManagerProxy proxy;
  ObString attr;
  ObString value;
  ObString group;
  sql::ObSQLSessionInfo *sess = GET_MY_SESSION(ctx);
  if (OB_ISNULL(sess) || params.count() < MAX_PARAM) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("err unexpected", K(params.count()), K(MAX_PARAM), K(ret));
  } else {
    tenant_id = sess->get_effective_tenant_id();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    ObObj &obj = params.at(i);
    if (ATTR == i) {
      ret = obj.get_string(attr);
    } else if (VALUE == i) {
      ret = obj.get_string(value);
    } else if (GROUP == i) {
      // note: group 不指定 (obj is null)
      // 时表示取消 value 对应的 group 绑定
      if (!obj.is_null()) {
        ret = obj.get_string(group);
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(proxy.replace_mapping_rule(tenant_id,
                                           attr,
                                           value,
                                           group,
                                           *sess))) {
      LOG_WARN("fail update plan directive", K(tenant_id), K(attr), K(value), K(group), K(ret));
    }
  }
  return ret;
}
