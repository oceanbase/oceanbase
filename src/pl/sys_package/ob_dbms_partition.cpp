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
#include "pl/sys_package/ob_dbms_partition.h"

#include "pl/ob_pl_package_manager.h"
#include "rootserver/ob_tenant_event_def.h"
#include "share/ob_dynamic_partition_manager.h"
#include "lib/string/ob_sql_string.h"
#include "pl/ob_pl.h"

namespace oceanbase
{
using namespace tenant_event;
using namespace share;
using namespace share::schema;

namespace pl
{

/**
 * @brief ObDBMSPartition::manage_dynamic_partition
 * @param pl_ctx
 * @param params
 *      0. precreate_time    VARCHAR2    DEFAULT NULL,
 *      1. time_unit         VARCHAR2    DEFAULT NULL,
 * @param result
 * @return
 *
 * precreate_time:
 *   Use the max value between specified precreate_time and table precreate_time for partition precreation.
 *   NULL means use table precreate_time.
 *
 * time_unit:
 *   Only tables with time_unit matching specified time_unit will perform dynamic partition manage.
 *   NULL means all dynamic partition tables will perform dynamic partition manage.
 *   We have an hourly scheduled task with time_unit = 'hour', and a daily scheduled task with time_unit = 'day,week,month,year'.
 */
int ObDBMSPartition::manage_dynamic_partition(ObPLExecCtx &pl_ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[DYNAMIC_PARTITION] start to manage dynamic partition");
  const int64_t begin_ts = ObTimeUtil::current_time();
  UNUSED(result);
  DEBUG_SYNC(BEFORE_MANAGE_DYNAMIC_PARTITION);

  uint64_t tenant_id = OB_INVALID_ID;
  bool is_valid_tenant = false;
  ObString precreate_time_str;
  ObArray<ObString> time_unit_strs;
  ObSchemaGetterGuard schema_guard;
  ObArray<const ObSimpleTableSchemaV2 *> table_schemas;
  if (OB_ISNULL(pl_ctx.exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec context is null", KR(ret));
  } else if (OB_ISNULL(pl_ctx.exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", KR(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret));
  } else if (FALSE_IT(tenant_id = pl_ctx.exec_ctx_->get_my_session()->get_effective_tenant_id())) {
  } else if (OB_FAIL(ObDynamicPartitionManager::check_tenant_is_valid_for_dynamic_partition(tenant_id, is_valid_tenant))) {
    LOG_WARN("fail to check tenant is valid for dynamic partition", KR(ret), K(tenant_id));
  } else if (!is_valid_tenant) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("invalid tenant for dynamic partition, manage_dynamic_partition is not allowed", KR(ret), K(tenant_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "invalid tenant for dynamic partition, manage_dynamic_partition is");
  } else if (OB_FAIL(get_and_check_params_(params, precreate_time_str, time_unit_strs))) {
    LOG_WARN("fail to check and get params", KR(ret), K(params));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id, table_schemas))) {
    LOG_WARN("fail to get table schemas in tenant", KR(ret), K(tenant_id));
  } else {
    // collect dynamic partition table ids
    ObArray<uint64_t> table_ids;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); i++) {
      const ObSimpleTableSchemaV2 *table_schema = table_schemas.at(i);
      const ObDatabaseSchema *database_schema = NULL;
      if (OB_ISNULL(table_schema)
          || !table_schema->is_user_table()
          || !table_schema->with_dynamic_partition_policy()
          || table_schema->is_in_recyclebin()
          || !table_schema->is_normal_schema()) {
        // skip
      } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, table_schema->get_database_id(), database_schema))) {
        LOG_WARN("fail to get database schema", KR(ret), K(tenant_id), K(table_schema->get_database_id()));
      } else if (OB_ISNULL(database_schema) || database_schema->is_in_recyclebin()) {
        // skip
      } else if (OB_FAIL(table_ids.push_back(table_schema->get_table_id()))) {
        LOG_WARN("fail to push back table_id", KR(ret), K(table_schema->get_table_id()));
      }
    }

    ObArray<uint64_t> success_table_ids;
    ObArray<uint64_t> failed_table_ids;
    // iterate dynamic partition tables to manage dynamic partition
    for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); i++) {
      const uint64_t table_id = table_ids.at(i);
      const ObTableSchema *table_schema = NULL;
      if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
        LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
      } else if (OB_ISNULL(table_schema)) {
        // table may be dropped, skip
      } else {
        DEBUG_SYNC(BEFORE_MANAGE_DYNAMIC_PARTITION_ON_TABLE);
        ObDynamicPartitionManager dynamic_partition_manager;
        int tmp_ret = OB_SUCCESS;
        bool skipped = false;
        if (OB_TMP_FAIL(dynamic_partition_manager.init(table_schema, pl_ctx.exec_ctx_->get_my_session()))) {
          LOG_WARN("fail to init dynamic partition manager", KR(tmp_ret), KPC(table_schema));
        } else if (OB_TMP_FAIL(dynamic_partition_manager.execute(precreate_time_str, time_unit_strs, skipped))) {
          LOG_WARN("fail to execute dynamic partition manage", KR(tmp_ret), K(precreate_time_str), K(time_unit_strs));
          ObSqlString warn_msg;
          int fmt_ret = OB_SUCCESS;
          if (OB_SUCCESS != (fmt_ret = warn_msg.append_fmt("dynamic partition management failed on table %.*s, reason: %s",
                                                           table_schema->get_table_name_str().length(),
                                                           table_schema->get_table_name_str().ptr(),
                                                           common::ob_strerror(tmp_ret)))) {
            LOG_WARN("fail to append fmt", KR(fmt_ret));
            // fallback if formatting failed, directly use the error message from ob_strerror
            FORWARD_USER_WARN(tmp_ret, common::ob_strerror(tmp_ret));
          } else {
            FORWARD_USER_WARN(tmp_ret, warn_msg.ptr());
          }
        }

        if (!skipped) {
          ObArray<uint64_t> &chosen_table_ids = OB_SUCCESS == tmp_ret ? success_table_ids : failed_table_ids;
          if (OB_FAIL(chosen_table_ids.push_back(table_id))) {
            LOG_WARN("fail to push back table id", KR(ret), K(table_id));
          }
        }
      }
    }
    const int64_t end_ts = ObTimeUtil::current_time();
    TENANT_EVENT(tenant_id, DBMS_PARTITION, MANAGE_DYNAMIC_PARTITION, end_ts, ret, end_ts - begin_ts,
                 success_table_ids,
                 failed_table_ids);
  }

  FLOG_INFO("[DYNAMIC_PARTITION] end manage dynamic partition", KR(ret));
  return ret;
}

/**
 * @param params
 *      0. precreate_time    VARCHAR2    DEFAULT NULL,
 *      1. time_unit         VARCHAR2    DEFAULT NULL,
 */
int ObDBMSPartition::get_and_check_params_(
  const ParamStore &params,
  ObString &precreate_time_str,
  ObArray<ObString> &time_unit_strs)
{
  int ret = OB_SUCCESS;
  ObDateUnitType time_unit = ObDateUnitType::DATE_UNIT_MAX;
  int64_t precreate_time_num = 0;
  ObDateUnitType precreate_time_unit = ObDateUnitType::DATE_UNIT_MAX;
  ObString time_unit_str;
  precreate_time_str.reset();
  time_unit_strs.reset();
  if (OB_UNLIKELY(2 != params.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params count", KR(ret), K(params));
  } else {
    // get and check precreate_time
    if (!params.at(0).is_null()) {
      if (OB_FAIL(params.at(0).get_string(precreate_time_str))) {
        LOG_WARN("fail to get string from params", KR(ret), K(params));
      } else if (OB_FAIL(ObDynamicPartitionManager::str_to_time(precreate_time_str,
                                                                precreate_time_num,
                                                                precreate_time_unit))) {
        LOG_WARN("fail to convert str to time", KR(ret), K(precreate_time_str));
      } else if (precreate_time_num < 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid precreate time", KR(ret), K(precreate_time_num));
      }
    }

    // get and check time_unit
    if (OB_SUCC(ret) && !params.at(1).is_null()) {
      if (OB_FAIL(params.at(1).get_string(time_unit_str))) {
        LOG_WARN("fail to get string from params", KR(ret), K(params));
      } else if (OB_FAIL(split_on(time_unit_str, ',', time_unit_strs))) {
        LOG_WARN("fail to split str", KR(ret), K(time_unit_str));
      } else if (time_unit_strs.empty()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("timt unit is empty", KR(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < time_unit_strs.count(); i++) {
          ObString str = time_unit_strs.at(i).trim();
          if (OB_FAIL(ObDynamicPartitionManager::str_to_time_unit(str, time_unit))) {
            LOG_WARN("fail to convert str to time unit", KR(ret), K(str));
          } else {
            time_unit_strs.at(i) = str;
          }
        }
      }
    }
  }

  return ret;
}

} // end of pl
} // end oceanbase
