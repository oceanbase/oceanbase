/**
 * Copyright (c) 2024 OceanBase
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

#include "pl/sys_package/ob_dbms_balance.h"
#include "share/ob_common_rpc_proxy.h"
#include "observer/ob_server_struct.h" // GCTX
#include "common/ob_timeout_ctx.h" // ObTimeoutCtx
#include "share/ob_balance_define.h"
#include "share/balance/ob_object_balance_weight_operator.h" // ObObjectBalanceWeightOperator
#include "share/schema/ob_part_mgr_util.h" // ObPartGetter
#include "rootserver/ob_tenant_event_def.h" // TENANT_EVENT

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace observer;
using namespace tenant_event;

namespace pl
{
int ObDBMSBalance::trigger_partition_balance(
    sql::ObExecContext &ctx,
    sql::ParamStore &params,
    common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  int32_t balance_timeout_s = 0;
  int64_t balance_timeout_us = 0;
  ObAddr leader;
  ObTimeoutCtx timeout_ctx;
  ObTriggerPartitionBalanceArg arg;
  UNUSED(result);
  bool is_supported = false;
  const int64_t begin_ts = ObTimeUtil::current_time();
  if (OB_ISNULL(GCTX.srv_rpc_proxy_)
      || OB_ISNULL(GCTX.location_service_)
      || OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", KR(ret), K(GCTX.srv_rpc_proxy_),
        K(GCTX.location_service_), K(ctx.get_my_session()));
  } else if (FALSE_IT(tenant_id = ctx.get_my_session()->get_effective_tenant_id())) {
  } else if (OB_FAIL(ObBalanceStrategy::check_compat_version(tenant_id, is_supported))) {
    LOG_WARN("check compat version failed", KR(ret), K(tenant_id), K(is_supported));
  } else if (!is_supported) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support to trigger partition balance", KR(ret), K(tenant_id), K(is_supported));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "trigger partition balance is");
  } else if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("not user tenant, trigger_partition_balance is not allowed", KR(ret), K(tenant_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "not user tenant, trigger partition balance is");
  } else if (OB_UNLIKELY(1 != params.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params", KR(ret), K(params));
  } else if (!params.at(0).is_null() && OB_FAIL(params.at(0).get_int32(balance_timeout_s))) {
    LOG_WARN("get int failed", KR(ret), K(params.at(0)));
  } else if (FALSE_IT(balance_timeout_us = balance_timeout_s * USECS_PER_SEC)) {
  } else if (OB_FAIL(arg.init(tenant_id, balance_timeout_us))) {
    LOG_WARN("init failed", KR(ret), K(tenant_id), K(balance_timeout_us), K(params.at(0)));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(timeout_ctx, GCONF.balancer_task_timeout))) {
    LOG_WARN("set default timeout ctx failed", KR(ret), K(ctx));
  } else {
    const int64_t RETRY_CNT_LIMIT = 3;
    int64_t retry_cnt = 0;
    do {
      if (OB_NOT_MASTER == ret) {
        ret = OB_SUCCESS;
        ob_usleep(1_s);
      }
      if (FAILEDx(GCTX.location_service_->get_leader_with_retry_until_timeout(
          GCONF.cluster_id,
          tenant_id,
          SYS_LS,
          leader))) { // default 1s timeout
        LOG_WARN("get leader failed", KR(ret), "cluster_id", GCONF.cluster_id.get_value(),
            K(tenant_id), K(leader));
      } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(leader)
                                              .by(tenant_id)
                                              .timeout(timeout_ctx.get_abs_timeout())
                                              .trigger_partition_balance(arg))) {
        LOG_WARN("trigger partition balance failed", KR(ret), K(arg));
      }
    } while (OB_NOT_MASTER == ret && ++retry_cnt <= RETRY_CNT_LIMIT);

    const int64_t end_ts = ObTimeUtil::current_time();
    TENANT_EVENT(tenant_id, DBMS_BALANCE, TRIGGER_PARTITION_BALANCE, end_ts, ret, end_ts - begin_ts, balance_timeout_s);
  }
  return ret;
}

int ObDBMSBalance::set_balance_weight(
    sql::ObExecContext &ctx,
    sql::ParamStore &params,
    common::ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  ObString database_name;
  ObString table_name;
  ObString partition_name;
  ObString subpartition_name;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  int32_t weight = 0;
  ObObjectID table_id = OB_INVALID_ID;
  ObObjectID part_id = OB_INVALID_ID;
  ObObjectID subpart_id = OB_INVALID_ID;
  ObObjectBalanceWeight obj_weight;
  const int64_t begin_ts = ObTimeUtil::current_time();
  const char *op_str = "set_balance_weight";

  if (OB_ISNULL(GCTX.sql_proxy_)
      || OB_ISNULL(GCTX.schema_service_)
      || OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", KR(ret), K(GCTX.sql_proxy_), K(GCTX.schema_service_), K(ctx.get_my_session()));
  } else if (FALSE_IT(tenant_id = ctx.get_my_session()->get_effective_tenant_id())) {
  } else if (OB_FAIL(pre_check_for_balance_weight_(tenant_id, op_str))) {
    LOG_WARN("pre check for balance weight failed", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(5 != params.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params", KR(ret), K(params));
  } else if (!params.at(0).is_null() && OB_FAIL(params.at(0).get_int32(weight))) {
    LOG_WARN("get weight failed", KR(ret), K(params.at(0)));
  } else if (!params.at(1).is_null() && OB_FAIL(params.at(1).get_varchar(database_name))) {
    LOG_WARN("get database_name failed", KR(ret), K(params.at(1)));
  } else if (!params.at(2).is_null() && OB_FAIL(params.at(2).get_varchar(table_name))) {
    LOG_WARN("get table_name failed", KR(ret), K(params.at(2)));
  } else if (!params.at(3).is_null() && OB_FAIL(params.at(3).get_varchar(partition_name))) {
    LOG_WARN("get partition_name failed", KR(ret), K(params.at(3)));
  } else if (!params.at(4).is_null() && OB_FAIL(params.at(4).get_varchar(subpartition_name))) {
    LOG_WARN("get partition_name failed", KR(ret), K(params.at(4)));
  } else if (OB_FAIL(get_id_by_name_(
      tenant_id,
      database_name,
      table_name,
      partition_name,
      subpartition_name,
      table_id,
      part_id,
      subpart_id))) {
    LOG_WARN("get id by name failed", KR(ret), K(tenant_id),
        K(database_name), K(table_name), K(partition_name), K(subpartition_name));
  } else if (OB_FAIL(obj_weight.init(tenant_id, table_id, part_id, subpart_id, weight))) {
    LOG_WARN("init failed", KR(ret), K(tenant_id),
        K(table_id), K(part_id), K(subpart_id), K(weight));
  } else if (OB_FAIL(ObObjectBalanceWeightOperator::update(*GCTX.sql_proxy_, obj_weight))) {
    LOG_WARN("update failed", KR(ret), K(obj_weight));
  }
  const int64_t end_ts = ObTimeUtil::current_time();
  TENANT_EVENT(tenant_id, DBMS_BALANCE, SET_BALANCE_WEIGHT, end_ts, ret, end_ts - begin_ts,
      weight, database_name, table_name, partition_name, subpartition_name);

  return ret;
}

int ObDBMSBalance::clear_balance_weight(
    sql::ObExecContext &ctx,
    sql::ParamStore &params,
    common::ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  ObString database_name;
  ObString table_name;
  ObString partition_name;
  ObString subpartition_name;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ObObjectID table_id = OB_INVALID_ID;
  ObObjectID part_id = OB_INVALID_ID;
  ObObjectID subpart_id = OB_INVALID_ID;
  ObObjectBalanceWeightKey obj_key;
  const int64_t begin_ts = ObTimeUtil::current_time();
  const char *op_str = "clear_balance_weight";

  if (OB_ISNULL(GCTX.sql_proxy_)
      || OB_ISNULL(GCTX.schema_service_)
      || OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", KR(ret), K(GCTX.sql_proxy_), K(GCTX.schema_service_), K(ctx.get_my_session()));
  } else if (FALSE_IT(tenant_id = ctx.get_my_session()->get_effective_tenant_id())) {
  } else if (OB_FAIL(pre_check_for_balance_weight_(tenant_id, op_str))) {
    LOG_WARN("pre check for balance weight failed", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(4 != params.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params", KR(ret), K(params));
  } else if (!params.at(0).is_null() && OB_FAIL(params.at(0).get_varchar(database_name))) {
    LOG_WARN("get database_name failed", KR(ret), K(params.at(0)));
  } else if (!params.at(1).is_null() && OB_FAIL(params.at(1).get_varchar(table_name))) {
    LOG_WARN("get table_name failed", KR(ret), K(params.at(1)));
  } else if (!params.at(2).is_null() && OB_FAIL(params.at(2).get_varchar(partition_name))) {
    LOG_WARN("get partition_name failed", KR(ret), K(params.at(2)));
  } else if (!params.at(3).is_null() && OB_FAIL(params.at(3).get_varchar(subpartition_name))) {
    LOG_WARN("get partition_name failed", KR(ret), K(params.at(3)));
  } else if (OB_FAIL(get_id_by_name_(
      tenant_id,
      database_name,
      table_name,
      partition_name,
      subpartition_name,
      table_id,
      part_id,
      subpart_id))) {
    LOG_WARN("get id by name failed", KR(ret), K(tenant_id),
        K(database_name), K(table_name), K(partition_name), K(subpartition_name));
  } else if (OB_FAIL(obj_key.init(tenant_id, table_id, part_id, subpart_id))) {
    LOG_WARN("init failed", KR(ret), K(tenant_id), K(table_id), K(part_id), K(subpart_id));
  } else if (OB_FAIL(ObObjectBalanceWeightOperator::remove(*GCTX.sql_proxy_, obj_key))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("object balance weight not exist", K(obj_key));
    } else {
      LOG_WARN("remove failed", KR(ret), K(obj_key));
    }
  }
  const int64_t end_ts = ObTimeUtil::current_time();
  TENANT_EVENT(tenant_id, DBMS_BALANCE, CLEAR_BALANCE_WEIGHT, end_ts, ret, end_ts - begin_ts,
      database_name, table_name, partition_name, subpartition_name);

  return ret;
}

int ObDBMSBalance::pre_check_for_balance_weight_(const uint64_t tenant_id, const char *op_str)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  char err_msg[OB_MAX_ERROR_MSG_LEN] = {0};
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(tenant_id), K(data_version));
  } else if (data_version < MOCK_DATA_VERSION_4_2_5_2
      || (data_version >= DATA_VERSION_4_3_0_0
          && data_version < DATA_VERSION_4_4_1_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support version", KR(ret), K(tenant_id), K(data_version), K(op_str));
    (void)snprintf(err_msg, sizeof(err_msg), "%s is", op_str);
    LOG_USER_ERROR(OB_NOT_SUPPORTED, err_msg);
  } else if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("not user tenant, not allowed", KR(ret), K(tenant_id), K(op_str));
    (void)snprintf(err_msg, sizeof(err_msg), "not user tenant, %s is", op_str);
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, err_msg);
  }
  return ret;
}

int ObDBMSBalance::get_id_by_name_(
    const uint64_t tenant_id,
    const common::ObString &database_name,
    const common::ObString &table_name,
    const common::ObString &partition_name,
    const common::ObString &subpartition_name,
    ObObjectID &table_id,
    ObObjectID &part_id,
    ObObjectID &subpart_id)
{
  int ret = OB_SUCCESS;
  table_id = OB_INVALID_ID;
  part_id = OB_INVALID_ID;
  subpart_id = OB_INVALID_ID;
  schema::ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  const char* table_type_str = NULL;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", KR(ret), K(GCTX.schema_service_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || database_name.empty()
      || table_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(database_name),
        K(table_name), K(partition_name), K(subpartition_name));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(
      tenant_id,
      database_name,
      table_name,
      false/*is_index*/,
      table_schema))) {
    LOG_WARN("get table schema failed", KR(ret), K(tenant_id),
        K(database_name), K(table_name), K(partition_name), K(subpartition_name));
  } else if (OB_ISNULL(table_schema)
      && OB_FAIL(schema_guard.get_table_schema(
          tenant_id,
          database_name,
          table_name,
          true/*is_index*/,
          table_schema))) { // support global index
    LOG_WARN("get table schema failed", KR(ret), K(tenant_id),
        K(database_name), K(table_name), K(partition_name), K(subpartition_name));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    ObCStringHelper helper;
    LOG_WARN("table not exist", KR(ret), K(tenant_id),
        K(database_name), K(table_name), K(partition_name), K(subpartition_name));
    LOG_USER_ERROR(OB_TABLE_NOT_EXIST, helper.convert(database_name), helper.convert(table_name));
  } else if (FALSE_IT(table_id = table_schema->get_table_id())) {
  } else if (!check_if_need_balance_table(*table_schema, table_type_str)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not need balance table", KR(ret), KPC(table_schema));
    char err_msg[OB_MAX_ERROR_MSG_LEN] = {0};
    (void)snprintf(err_msg, sizeof(err_msg), "%s is", table_type_str);
    LOG_USER_ERROR(OB_NOT_SUPPORTED, err_msg);
  } else if (PARTITION_LEVEL_TWO == table_schema->get_part_level()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("subpartition is not support", KR(ret), KPC(table_schema));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "subpartition table is");
  } else if (partition_name.empty()) {
    if (OB_UNLIKELY(!subpartition_name.empty())) {
      ret = OB_UNKNOWN_PARTITION;
      LOG_WARN("partition not exist", KR(ret), K(tenant_id),
        K(database_name), K(table_name), K(partition_name), K(subpartition_name));
      LOG_USER_ERROR(OB_UNKNOWN_PARTITION, partition_name.length(), partition_name.ptr(),
          table_name.length(), table_name.ptr());
    } else {
      // both partition_name and subpartition_name are empty, do nothing
    }
  } else {
    ObPartGetter part_getter(*table_schema);
    if (subpartition_name.empty()) {
      if (OB_FAIL(part_getter.get_part_id(partition_name, part_id))) {
        LOG_WARN("get part id failed", KR(ret), K(database_name),
            K(table_name), K(partition_name), K(subpartition_name));
        if (OB_UNKNOWN_PARTITION == ret) {
          LOG_USER_ERROR(OB_UNKNOWN_PARTITION, partition_name.length(), partition_name.ptr(),
              table_name.length(), table_name.ptr());
        }
      }
    } else if (OB_FAIL(part_getter.get_part_id_and_subpart_id(
        partition_name,
        subpartition_name,
        part_id,
        subpart_id))) {
      LOG_WARN("get part id failed", KR(ret), K(database_name),
          K(table_name), K(partition_name), K(subpartition_name));
      if (OB_UNKNOWN_SUBPARTITION == ret) {
        LOG_USER_ERROR(OB_UNKNOWN_SUBPARTITION);
      }
    }
  }

  return ret;
}

int ObDBMSBalance::set_tablegroup_balance_weight(
    sql::ObExecContext &ctx,
    sql::ParamStore &params,
    common::ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  ObString tablegroup_name;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ObObjectID tablegroup_id = OB_INVALID_ID;
  int32_t weight = 0;
  ObObjectBalanceWeight obj_weight;
  const int64_t begin_ts = ObTimeUtil::current_time();
  const char *op_str = "set_tablegroup_balance_weight";

  if (OB_ISNULL(GCTX.sql_proxy_)
      || OB_ISNULL(GCTX.schema_service_)
      || OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", KR(ret), K(GCTX.sql_proxy_), K(GCTX.schema_service_), K(ctx.get_my_session()));
  } else if (FALSE_IT(tenant_id = ctx.get_my_session()->get_effective_tenant_id())) {
  } else if (OB_FAIL(pre_check_for_tablegroup_balance_weight_(tenant_id, op_str))) {
    LOG_WARN("pre check for balance weight failed", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(2 != params.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params", KR(ret), K(params));
  } else if (!params.at(0).is_null() && OB_FAIL(params.at(0).get_int32(weight))) {
    LOG_WARN("get weight failed", KR(ret), K(params.at(0)));
  } else if (!params.at(1).is_null() && OB_FAIL(params.at(1).get_varchar(tablegroup_name))) {
    LOG_WARN("get tablegroup_name failed", KR(ret), K(params.at(1)));
  } else if (OB_FAIL(get_and_check_tablegroup_id_by_name_(tenant_id, tablegroup_name, tablegroup_id))) {
    LOG_WARN("get and check tablegroup id by name failed", KR(ret), K(tenant_id), K(tablegroup_name));
  } else if (OB_FAIL(obj_weight.init_tablegroup_weight(tenant_id, tablegroup_id, weight))) {
    LOG_WARN("init failed", KR(ret), K(tenant_id), K(tablegroup_id), K(weight));
  } else if (OB_FAIL(ObObjectBalanceWeightOperator::update(*GCTX.sql_proxy_, obj_weight))) {
    LOG_WARN("update failed", KR(ret), K(obj_weight));
  }
  const int64_t end_ts = ObTimeUtil::current_time();
  TENANT_EVENT(tenant_id, DBMS_BALANCE, SET_TABLEGROUP_BALANCE_WEIGHT, end_ts, ret, end_ts - begin_ts,
      weight, tablegroup_name);
  return ret;
}

int ObDBMSBalance::clear_tablegroup_balance_weight(
    sql::ObExecContext &ctx,
    sql::ParamStore &params,
    common::ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  ObString tablegroup_name;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ObObjectID tablegroup_id = OB_INVALID_ID;
  ObObjectBalanceWeightKey obj_key;
  const int64_t begin_ts = ObTimeUtil::current_time();
  const char *op_str = "clear_tablegroup_balance_weight";

  if (OB_ISNULL(GCTX.sql_proxy_)
      || OB_ISNULL(GCTX.schema_service_)
      || OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", KR(ret), K(GCTX.sql_proxy_), K(GCTX.schema_service_), K(ctx.get_my_session()));
  } else if (FALSE_IT(tenant_id = ctx.get_my_session()->get_effective_tenant_id())) {
  } else if (OB_FAIL(pre_check_for_tablegroup_balance_weight_(tenant_id, op_str))) {
    LOG_WARN("pre check for balance weight failed", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(1 != params.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params", KR(ret), K(params));
  } else if (!params.at(0).is_null() && OB_FAIL(params.at(0).get_varchar(tablegroup_name))) {
    LOG_WARN("get tablegroup_name failed", KR(ret), K(params.at(0)));
  } else if (OB_FAIL(get_and_check_tablegroup_id_by_name_(tenant_id, tablegroup_name, tablegroup_id))) {
    LOG_WARN("get and check tablegroup id by name failed", KR(ret), K(tenant_id), K(tablegroup_name));
  } else if (OB_FAIL(obj_key.init_tablegroup_key(tenant_id, tablegroup_id))) {
    LOG_WARN("init failed", KR(ret), K(tenant_id), K(tablegroup_id));
  } else if (OB_FAIL(ObObjectBalanceWeightOperator::remove(*GCTX.sql_proxy_, obj_key))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("object balance weight not exist", K(obj_key));
    } else {
      LOG_WARN("remove failed", KR(ret), K(obj_key));
    }
  }
  const int64_t end_ts = ObTimeUtil::current_time();
  TENANT_EVENT(tenant_id, DBMS_BALANCE, CLEAR_TABLEGROUP_BALANCE_WEIGHT, end_ts, ret, end_ts - begin_ts,
      tablegroup_name);
  return ret;
}

int ObDBMSBalance::pre_check_for_tablegroup_balance_weight_(
    const uint64_t tenant_id,
    const char *op_str)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  char err_msg[OB_MAX_ERROR_MSG_LEN] = {0};
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(tenant_id), K(data_version));
  } else if (data_version < DATA_VERSION_4_4_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support version", KR(ret), K(tenant_id), K(data_version), K(op_str));
    (void)snprintf(err_msg, sizeof(err_msg), "%s is", op_str);
    LOG_USER_ERROR(OB_NOT_SUPPORTED, err_msg);
  } else if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("not user tenant, not allowed", KR(ret), K(tenant_id), K(op_str));
    (void)snprintf(err_msg, sizeof(err_msg), "not user tenant, %s is", op_str);
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, err_msg);
  }
  return ret;
}

int ObDBMSBalance::get_and_check_tablegroup_id_by_name_(
    const uint64_t tenant_id,
    const common::ObString &tablegroup_name,
    ObObjectID &tablegroup_id)
{
  int ret = OB_SUCCESS;
  tablegroup_id = OB_INVALID_ID;
  schema::ObSchemaGetterGuard schema_guard;
  const ObSimpleTablegroupSchema *tablegroup_schema = nullptr;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", KR(ret), K(GCTX.schema_service_));
  } else if (OB_UNLIKELY(tablegroup_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(tablegroup_name));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tablegroup_id(tenant_id, tablegroup_name, tablegroup_id))) {
    LOG_WARN("get tablegroup id failed", KR(ret), K(tenant_id), K(tablegroup_name));
  } else if (OB_INVALID_ID == tablegroup_id) {
    ret = OB_TABLEGROUP_NOT_EXIST;
    LOG_WARN("tablegroup not exist", KR(ret), K(tablegroup_name));
    LOG_USER_ERROR(OB_TABLEGROUP_NOT_EXIST);
  } else if (OB_FAIL(schema_guard.get_tablegroup_schema(tenant_id, tablegroup_id, tablegroup_schema))) {
    LOG_WARN("fail to get tablegroup schema", KR(ret), K(tenant_id), K(tablegroup_id));
  } else if (OB_ISNULL(tablegroup_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", KR(ret), K(tablegroup_name), K(tablegroup_id), K(tablegroup_schema));
  } else if (!tablegroup_schema->is_sharding_none()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support sharding tablegroup", KR(ret), K(tablegroup_name), K(tablegroup_id));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "sharding partition or adaptive tablegroup are");
  } else {
    tablegroup_id = tablegroup_schema->get_tablegroup_id();
  }
  return ret;
}

} // end of pl
} // end oceanbase
