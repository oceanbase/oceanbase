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
    ObString resource_manager_plan;
    if (OB_FAIL(ObSchemaUtils::get_tenant_varchar_variable(
                  tenant_id,
                  SYS_VAR_RESOURCE_MANAGER_PLAN,
                  ctx.get_allocator(),
                  resource_manager_plan))) {
        LOG_WARN("fail get tenant variable", K(tenant_id), K(resource_manager_plan), K(ret));
    } else if (0 == plan.case_compare(resource_manager_plan)) {
      // this plan is active and cannot be deleted.
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "deleting active plan");
      LOG_WARN("active plan cannot be removed", K(plan));
    } else if (OB_FAIL(proxy.delete_plan(tenant_id, plan))) {
      LOG_WARN("fail delete plan", K(tenant_id), K(plan), K(ret));
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
  int ret = OB_SUCCESS;
  uint64_t tenant_id;
  UNUSED(result);
  ObResourceManagerProxy proxy;
  ObString plan;
  ObString group;
  sql::ObSQLSessionInfo *sess = GET_MY_SESSION(ctx);
  common::ObObj max_net_bandwidht_obj(int64_t(100));
  common::ObObj net_bandwidth_weight_obj(int64_t(0));

  if (OB_ISNULL(sess)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("err unexpected", K(params.count()), K(ret));
  } else if (params.count() < ObPlanDirectiveParamType::MAX_PARAM) {
    if (params.count() < ObPlanDirectiveParamType::MAX_NET_BANDWIDTH) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("err unexpected", K(params.count()), K(ObPlanDirectiveParamType::MAX_PARAM), K(ret));
    } else {
      LOG_INFO("use lower version of admin pkg", K(params.count()), K(ObPlanDirectiveParamType::MAX_PARAM), K(ret));
    }
  } else if (params.count() == ObPlanDirectiveParamType::MAX_PARAM) {
      max_net_bandwidht_obj.reset();
      net_bandwidth_weight_obj.reset();
      bool is_copy_all = true;
      params.at(MAX_NET_BANDWIDTH).copy_value_or_obj(max_net_bandwidht_obj, is_copy_all);
      params.at(NET_BANDWIDTH_WEIGHT).copy_value_or_obj(net_bandwidth_weight_obj, is_copy_all);
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < params.count() && i < ObPlanDirectiveParamType::COMMENT; ++i) {
    LOG_INFO("pl params", K(params.at(i)));
    ObObj &obj = params.at(i);
    if (ObPlanDirectiveParamType::PLAN == i) {
      ret = obj.get_string(plan);
    } else if (ObPlanDirectiveParamType::GROUP == i) {
      ret = obj.get_string(group);
    }
  }
  if (OB_SUCC(ret)) {
    tenant_id = sess->get_effective_tenant_id();
    if (OB_FAIL(proxy.create_plan_directive(tenant_id,
                                            plan,
                                            group,
                                            params.at(ObPlanDirectiveParamType::COMMENT),
                                            params.at(ObPlanDirectiveParamType::MGMT_P1),
                                            params.at(ObPlanDirectiveParamType::UTILIZATION_LIMIT),
                                            params.at(ObPlanDirectiveParamType::MIN_IOPS),
                                            params.at(ObPlanDirectiveParamType::MAX_IOPS),
                                            params.at(ObPlanDirectiveParamType::WEIGHT_IOPS),
                                            max_net_bandwidht_obj,
                                            net_bandwidth_weight_obj))) {
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
  int ret = OB_SUCCESS;
  uint64_t tenant_id;
  UNUSED(result);
  ObResourceManagerProxy proxy;
  ObString plan;
  ObString group;
  sql::ObSQLSessionInfo *sess = GET_MY_SESSION(ctx);
  common::ObObj max_net_bandwidht_obj(int64_t(100));
  common::ObObj net_bandwidth_weight_obj(int64_t(0));

  if (OB_ISNULL(sess)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("err unexpected", K(params.count()), K(ret));
  } else if (params.count() < ObPlanDirectiveParamType::MAX_PARAM) {
    if (params.count() < MAX_NET_BANDWIDTH) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("err unexpected", K(params.count()), K(ObPlanDirectiveParamType::MAX_PARAM), K(ret));
    } else {
      LOG_INFO("use lower version of admin pkg", K(params.count()), K(ObPlanDirectiveParamType::MAX_PARAM), K(ret));
    }
  } else if (params.count() == ObPlanDirectiveParamType::MAX_PARAM) {
    max_net_bandwidht_obj.reset();
    net_bandwidth_weight_obj.reset();
    bool is_copy_all = true;
    params.at(ObPlanDirectiveParamType::MAX_NET_BANDWIDTH).copy_value_or_obj(max_net_bandwidht_obj, is_copy_all);
    params.at(ObPlanDirectiveParamType::NET_BANDWIDTH_WEIGHT).copy_value_or_obj(net_bandwidth_weight_obj, is_copy_all);
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < params.count() && i < ObPlanDirectiveParamType::COMMENT; ++i) {
    ObObj &obj = params.at(i);
    if (ObPlanDirectiveParamType::PLAN == i) {
      ret = obj.get_string(plan);
    } else if (ObPlanDirectiveParamType::GROUP == i) {
      ret = obj.get_string(group);
    }
  }
  if (OB_SUCC(ret)) {
    tenant_id = sess->get_effective_tenant_id();
    if (OB_FAIL(proxy.update_plan_directive(tenant_id,
                                            plan,
                                            group,
                                            params.at(ObPlanDirectiveParamType::COMMENT),
                                            params.at(ObPlanDirectiveParamType::MGMT_P1),
                                            params.at(ObPlanDirectiveParamType::UTILIZATION_LIMIT),
                                            params.at(ObPlanDirectiveParamType::MIN_IOPS),
                                            params.at(ObPlanDirectiveParamType::MAX_IOPS),
                                            params.at(ObPlanDirectiveParamType::WEIGHT_IOPS),
                                            max_net_bandwidht_obj,
                                            net_bandwidth_weight_obj))) {
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
