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

#define USING_LOG_PREFIX RS

#include "lib/string/ob_sql_string.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_upgrade_utils.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "observer/ob_server_struct.h"
#include "rootserver/ob_root_service.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace share::schema;
using namespace rootserver;
using namespace sql;

namespace share
{
const uint64_t ObUpgradeChecker::UPGRADE_PATH[CLUTER_VERSION_NUM] = {
  CALC_CLUSTER_VERSION(1UL, 4UL, 0UL, 3UL),  // 1.4.3
  CALC_CLUSTER_VERSION(1UL, 4UL, 0UL, 40UL), // 1.4.40
  CALC_CLUSTER_VERSION(1UL, 4UL, 0UL, 50UL), // 1.4.50
  CALC_CLUSTER_VERSION(1UL, 4UL, 0UL, 51UL), // 1.4.51
  CALC_CLUSTER_VERSION(1UL, 4UL, 0UL, 60UL), // 1.4.60
  CALC_CLUSTER_VERSION(1UL, 4UL, 0UL, 61UL), // 1.4.61
  CALC_CLUSTER_VERSION(1UL, 4UL, 0UL, 70UL), // 1.4.70
  CALC_CLUSTER_VERSION(1UL, 4UL, 0UL, 71UL), // 1.4.71
  CALC_CLUSTER_VERSION(1UL, 4UL, 0UL, 72UL), // 1.4.72
  CALC_CLUSTER_VERSION(1UL, 4UL, 0UL, 76UL), // 1.4.76
  CALC_CLUSTER_VERSION(2UL, 0UL, 0UL, 0UL),  // 2.0.0
  CALC_CLUSTER_VERSION(2UL, 1UL, 0UL, 0UL),  // 2.1.0
  CALC_CLUSTER_VERSION(2UL, 1UL, 0UL, 1UL),  // 2.1.1
  CALC_CLUSTER_VERSION(2UL, 1UL, 0UL, 11UL), // 2.1.11
  CALC_CLUSTER_VERSION(2UL, 1UL, 0UL, 20UL), // 2.1.20
  CALC_CLUSTER_VERSION(2UL, 1UL, 0UL, 30UL), // 2.1.30
  CALC_CLUSTER_VERSION(2UL, 1UL, 0UL, 31UL), // 2.1.31
  CALC_CLUSTER_VERSION(2UL, 2UL, 0UL, 0UL),  // 2.2.0
  CALC_CLUSTER_VERSION(2UL, 2UL, 0UL, 1UL),  // 2.2.1
  CALC_CLUSTER_VERSION(2UL, 2UL, 0UL, 20UL), // 2.2.20
  CALC_CLUSTER_VERSION(2UL, 2UL, 0UL, 30UL), // 2.2.30
  CALC_CLUSTER_VERSION(2UL, 2UL, 0UL, 40UL), // 2.2.40
  CALC_CLUSTER_VERSION(2UL, 2UL, 0UL, 50UL), // 2.2.50
  CALC_CLUSTER_VERSION(2UL, 2UL, 0UL, 60UL), // 2.2.60
  CALC_CLUSTER_VERSION(2UL, 2UL, 0UL, 70UL), // 2.2.70
  CALC_CLUSTER_VERSION(2UL, 2UL, 0UL, 71UL), // 2.2.71
  CALC_CLUSTER_VERSION(2UL, 2UL, 0UL, 72UL), // 2.2.72
  CALC_CLUSTER_VERSION(2UL, 2UL, 0UL, 73UL), // 2.2.73
  CALC_CLUSTER_VERSION(2UL, 2UL, 0UL, 74UL), // 2.2.74
  CALC_CLUSTER_VERSION(2UL, 2UL, 0UL, 75UL), // 2.2.75
  CALC_CLUSTER_VERSION(2UL, 2UL, 0UL, 76UL), // 2.2.76
  CALC_CLUSTER_VERSION(2UL, 2UL, 0UL, 77UL), // 2.2.77
  CALC_CLUSTER_VERSION(3UL, 1UL, 0UL, 0UL),  // 3.1.0
  CALC_CLUSTER_VERSION(3UL, 1UL, 0UL, 1UL),  // 3.1.1
  CALC_CLUSTER_VERSION(3UL, 1UL, 0UL, 2UL),  // 3.1.2
  CALC_CLUSTER_VERSION(3UL, 2UL, 0UL, 0UL),  // 3.2.0
  CALC_CLUSTER_VERSION(3UL, 2UL, 0UL, 1UL),  // 3.2.1
  CALC_CLUSTER_VERSION(3UL, 2UL, 0UL, 2UL),  // 3.2.2
  CALC_CLUSTER_VERSION(3UL, 2UL, 3UL, 0UL),  // 3.2.3.0
  CALC_CLUSTER_VERSION(4UL, 0UL, 0UL, 0UL)   // 4.0.0.0
};

bool ObUpgradeChecker::check_cluster_version_exist(
     const uint64_t version)
{
  bool bret = false;
  OB_ASSERT(CLUTER_VERSION_NUM == ARRAYSIZEOF(UPGRADE_PATH));
  for (int64_t i = 0; !bret && i < ARRAYSIZEOF(UPGRADE_PATH); i++) {
    bret = (version == UPGRADE_PATH[i]);
  }
  return bret;
}

#define FORMAT_STR(str) ObHexEscapeSqlStr(str.empty() ? ObString("") : str)

/*
 * Upgrade script will insert failed record to __all_rootservice_job before upgrade job runs.
 * This function is used to check if specific upgrade job runs successfully. If we can't find
 * any records of specific upgrade job, we pretend that such upgrade job have run successfully.
 */
int ObUpgradeUtils::check_upgrade_job_passed(ObRsJobType job_type)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  bool success = false;
  if (JOB_TYPE_INVALID >= job_type
      || job_type >= JOB_TYPE_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job_type", K(ret), K(job_type));
  } else if (OB_FAIL(check_rs_job_exist(job_type, exist))) {
    LOG_WARN("fail to check rs job exist", K(ret), K(job_type));
  } else if (!exist) {
    // rs job not exist, see as passed
  } else if (OB_FAIL(check_rs_job_success(job_type, success))) {
    LOG_WARN("fail to check rs job success", K(ret), K(job_type));
  } else if (!success) {
    ret = OB_RUN_JOB_NOT_SUCCESS;
    LOG_WARN("run job not success yet", K(ret));
  } else {
    LOG_INFO("run job success", K(ret), K(job_type));
  }
  return ret;
}

/*
 * Can only run upgrade job when failed record of specific upgrade job exists.
 * 1. Upgrade script will insert failed record to __all_rootservice_job before upgrade job runs.
 *    If we can't find any records of specific upgrade job, it's no need to run such upgrade job.
 * 2. If specific upgrade job run successfully once, it's no need to run such upgrade job again.
 */
int ObUpgradeUtils::can_run_upgrade_job(ObRsJobType job_type, bool &can)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  bool success = false;
  can = false;
  if (JOB_TYPE_INVALID >= job_type
      || job_type >= JOB_TYPE_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job_type", K(ret), K(job_type));
  } else if (OB_FAIL(check_rs_job_exist(job_type, exist))) {
    LOG_WARN("fail to check rs job exist", K(ret), K(job_type));
  } else if (!exist) {
    LOG_WARN("can't run job while rs_job table is empty", K(ret));
  } else if (OB_FAIL(check_rs_job_success(job_type, success))) {
    LOG_WARN("fail to check rs job success", K(ret), K(job_type));
  } else if (success) {
    LOG_WARN("can't run job while rs_job table is empty", K(ret));
  } else {
    // task exists && not success yet
    can = true;
  }
  return ret;
}

int ObUpgradeUtils::check_rs_job_exist(ObRsJobType job_type, bool &exist)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
    int32_t count = OB_INVALID_COUNT;
    exist = false;
    if (JOB_TYPE_INVALID >= job_type
        || job_type >= JOB_TYPE_MAX) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid job_type", K(ret), K(job_type));
    } else if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql_proxy is null", K(ret));
    } else if (sql.assign_fmt("SELECT floor(count(*)) as count FROM %s WHERE job_type = '%s'",
                              OB_ALL_ROOTSERVICE_JOB_TNAME, ObRsJobTableOperator::get_job_type_str(job_type))) {
      LOG_WARN("fail to assign sql", K(ret));
    } else if (OB_FAIL(GCTX.sql_proxy_->read(res, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get sql result", K(ret));
    } else if (OB_FAIL((result->next()))) {
      LOG_WARN("fail to get result", K(ret));
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "count", count, int32_t);
      if (OB_FAIL(ret)) {
      } else if (count < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid count", K(ret), K(count));
      } else {
        exist = (count > 0);
      }
    }
  }
  return ret;
}

int ObUpgradeUtils::check_rs_job_success(ObRsJobType job_type, bool &success)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
    int32_t count = OB_INVALID_COUNT;
    success = false;
    if (JOB_TYPE_INVALID >= job_type
        || job_type >= JOB_TYPE_MAX) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid job_type", K(ret), K(job_type));
    } else if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql_proxy is null", K(ret));
    } else if (sql.assign_fmt("SELECT floor(count(*)) as count FROM %s "
                              "WHERE job_type = '%s' and job_status = 'SUCCESS'",
                              OB_ALL_ROOTSERVICE_JOB_TNAME, ObRsJobTableOperator::get_job_type_str(job_type))) {
      LOG_WARN("fail to assign sql", K(ret));
    } else if (OB_FAIL(GCTX.sql_proxy_->read(res, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get sql result", K(ret));
    } else if (OB_FAIL((result->next()))) {
      LOG_WARN("fail to get result", K(ret));
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "count", count, int32_t);
      if (OB_FAIL(ret)) {
      } else if (count < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid count", K(ret), K(count));
      } else {
        success = (count > 0);
      }
    }
  }
  return ret;
}

int ObUpgradeUtils::check_schema_sync(bool &is_sync)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
    int32_t count = OB_INVALID_COUNT;
    is_sync = false;
    if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql_proxy is null", K(ret));
    } else if (sql.assign_fmt("SELECT floor(count(*)) as count FROM %s AS a "
                              "JOIN %s AS b ON a.tenant_id = b.tenant_id "
                              "WHERE a.refreshed_schema_version != b.refreshed_schema_version",
                              OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_TNAME,
                              OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_TNAME)) {
      LOG_WARN("fail to assign sql", K(ret));
    } else if (OB_FAIL(GCTX.sql_proxy_->read(res, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get sql result", K(ret));
    } else if (OB_FAIL((result->next()))) {
      LOG_WARN("fail to get result", K(ret));
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "count", count, int32_t);
      if (OB_FAIL(ret)) {
      } else if (count < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid count", K(ret), K(count));
      } else {
        is_sync = (count == 0);
      }
    }
  }
  return ret;
}

/* =========== upgrade sys variable =========== */
//  C++ implement for exec_sys_vars_upgrade_dml() in python upgrade script.
int ObUpgradeUtils::upgrade_sys_variable(
    obrpc::ObCommonRpcProxy &rpc_proxy,
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> update_list; // sys_var_store_idx, sys var to modify
  ObArray<int64_t> add_list;    // sys_var_store_idx, sys var to add
  if (OB_INVALID_TENANT_ID == tenant_id
      || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(calc_diff_sys_var_(sql_client, tenant_id, update_list, add_list))) {
    LOG_WARN("fail to calc diff sys var", KR(ret), K(tenant_id));
  } else if (OB_FAIL(update_sys_var_(rpc_proxy, tenant_id, true, update_list))) {
    LOG_WARN("fail to update sys var", KR(ret), K(tenant_id));
  } else if (OB_FAIL(update_sys_var_(rpc_proxy, tenant_id, false, add_list))) {
    LOG_WARN("fail to add sys var", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObUpgradeUtils::calc_diff_sys_var_(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    common::ObArray<int64_t> &update_list,
    common::ObArray<int64_t> &add_list)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id
      || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else {
    ObArray<Name> fetch_names;
    ObSqlString sql;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.append_fmt(
                  "select name, data_type, value, info, flags, min_val, max_val from %s "
                  "where tenant_id = %lu and (tenant_id, zone, name, schema_version) in ( "
                  "select tenant_id, zone, name, max(schema_version) from %s "
                  "where tenant_id = %lu group by tenant_id, zone, name)",
                  OB_ALL_SYS_VARIABLE_HISTORY_TNAME,
                  ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                  OB_ALL_SYS_VARIABLE_HISTORY_TNAME,
                  ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)))) {
        LOG_WARN("fail to append fmt", KR(ret), K(tenant_id), K(sql));
      } else if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is not expected to be NULL", KR(ret), K(tenant_id), K(sql));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          ObString name;
          ObString info;
          ObString max_val;
          ObString min_val;
          int64_t data_type = OB_INVALID_ID;
          int64_t flags = OB_INVALID_ID;
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "name", name);
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "info", info);
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "max_val", max_val);
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "min_val", min_val);
          EXTRACT_INT_FIELD_MYSQL(*result, "data_type", data_type, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "flags", flags, int64_t);

          ObSysVarClassType sys_var_id = SYS_VAR_INVALID;
          int64_t var_store_idx = OB_INVALID_INDEX;
          if (FAILEDx(fetch_names.push_back(name))) {
            LOG_WARN("fail to push back name", KR(ret), K(tenant_id), K(name));
          } else if (SYS_VAR_INVALID == (sys_var_id =
                     ObSysVarFactory::find_sys_var_id_by_name(name))) {
            // maybe has unused sys variable in table, just ignore
            LOG_INFO("sys variable exist in table, but not hard code", KR(ret), K(tenant_id), K(name));
          } else if (OB_FAIL(ObSysVarFactory::calc_sys_var_store_idx(sys_var_id, var_store_idx))) {
            LOG_WARN("fail to calc sys var store idx", KR(ret), K(sys_var_id), K(name));
          } else if (false == ObSysVarFactory::is_valid_sys_var_store_idx(var_store_idx)) {
            ret = OB_SCHEMA_ERROR;
            LOG_WARN("calc sys var store idx success but store_idx is invalid", KR(ret), K(var_store_idx));
          } else {
            const ObString &hard_code_info = ObSysVariables::get_info(var_store_idx);
            const ObObjType &hard_code_type = ObSysVariables::get_type(var_store_idx);
            const ObString &hard_code_min_val = ObSysVariables::get_min(var_store_idx);
            const ObString &hard_code_max_val  = ObSysVariables::get_max(var_store_idx);
            const int64_t hard_code_flag = ObSysVariables::get_flags(var_store_idx);
            if (hard_code_flag != flags
                || static_cast<int64_t>(hard_code_type) != data_type
                || 0 != hard_code_info.compare(info)
                || 0 != hard_code_min_val.compare(min_val)
                || 0 != hard_code_max_val.compare(max_val)) {
              // sys var to modify
              LOG_INFO("[UPGRADE] sys var diff, need modify", K(tenant_id), K(name),
                       K(data_type), K(flags), K(min_val), K(max_val), K(info),
                       K(hard_code_type), K(hard_code_flag), K(hard_code_min_val),
                       K(hard_code_max_val), K(hard_code_info));
              if (OB_FAIL(update_list.push_back(var_store_idx))) {
                LOG_WARN("fail to push_back var_store_idx", KR(ret), K(tenant_id), K(name), K(var_store_idx));
              }
            }
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
          LOG_WARN("fail to iter result", KR(ret), K(tenant_id));
        }
      }
    }
    // sys var to add
    if (OB_SUCC(ret)) {
      int64_t sys_var_cnt = ObSysVariables::get_amount();
      for (int64_t i = 0; OB_SUCC(ret) && i < sys_var_cnt; i++) {
        const ObString &name = ObSysVariables::get_name(i);
        bool found = false;
        FOREACH_CNT_X(fetch_name, fetch_names, OB_SUCC(ret)) {
          if (OB_ISNULL(fetch_name)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("name is null", KR(ret), K(tenant_id));
          } else if (0 == name.compare(fetch_name->str())) {
            found = true;
            break;
          }
        }
        if (OB_FAIL(ret) || found) {
        } else if (OB_FAIL(add_list.push_back(i))) {
          LOG_WARN("fail to push back var_store_idx", KR(ret), K(tenant_id), K(name));
        } else {
          LOG_INFO("[UPGRADE] sys var miss, need add", K(tenant_id), K(name), K(i));
        }
      }
    }
  }
  return ret;
}

// modify & add sys var according by hard code schema
int ObUpgradeUtils::update_sys_var_(
    obrpc::ObCommonRpcProxy &rpc_proxy,
    const uint64_t tenant_id,
    const bool is_update,
    common::ObArray<int64_t> &update_list)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id
      || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else {
    const int64_t timeout = GCONF.internal_sql_execute_timeout;
    for (int64_t i = 0; OB_SUCC(ret) && i < update_list.count(); i++) {
      int64_t start_ts = ObTimeUtility::current_time();
      int64_t var_store_idx = update_list.at(i);
      const ObString &name = ObSysVariables::get_name(var_store_idx);
      const ObObjType &type = ObSysVariables::get_type(var_store_idx);
      const ObString &value = ObSysVariables::get_value(var_store_idx);
      const ObString &min = ObSysVariables::get_min(var_store_idx);
      const ObString &max = ObSysVariables::get_max(var_store_idx);
      const ObString &info = ObSysVariables::get_info(var_store_idx);
      const int64_t flag = ObSysVariables::get_flags(var_store_idx);
      const ObString zone("");
      ObSysParam sys_param;
      obrpc::ObAddSysVarArg arg;
      arg.exec_tenant_id_ = tenant_id;
      arg.if_not_exist_ = true; // not used
      arg.sysvar_.set_tenant_id(tenant_id);
      arg.update_sys_var_ = is_update;
      if (OB_FAIL(sys_param.init(tenant_id, zone, name.ptr(), type,
          value.ptr(), min.ptr(), max.ptr(), info.ptr(), flag))) {
        LOG_WARN("sys_param init failed", KR(ret), K(tenant_id), K(name),
                 K(type), K(value), K(min), K(max), K(info), K(flag));
      } else if (!sys_param.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("sys param is invalid", KR(ret), K(tenant_id), K(sys_param));
      } else if (OB_FAIL(ObSchemaUtils::convert_sys_param_to_sysvar_schema(sys_param, arg.sysvar_))) {
        LOG_WARN("convert sys param to sysvar schema failed", KR(ret));
      } else if (OB_FAIL(rpc_proxy.timeout(timeout).add_system_variable(arg))) {
        LOG_WARN("add system variable failed", KR(ret), K(timeout), K(arg));
      }
      LOG_INFO("[UPGRADE] finish upgrade system variable",
               KR(ret), K(tenant_id), K(name), "cost", ObTimeUtility::current_time() - start_ts);
    }
  }
  return ret;
}

/* =========== upgrade sys variable end =========== */

/* =========== upgrade sys stat =========== */
int ObUpgradeUtils::upgrade_sys_stat(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSysStat sys_stat;
  if (OB_INVALID_TENANT_ID == tenant_id
      || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sys_stat.set_initial_values(tenant_id))) {
    LOG_WARN("fail to init sys stat", KR(ret), K(tenant_id));
  } else if (OB_FAIL(filter_sys_stat(sql_client, tenant_id, sys_stat))) {
    LOG_WARN("fail to filter sys stat", KR(ret), K(tenant_id), K(sys_stat));
  } else if (OB_FAIL(ObDDLOperator::replace_sys_stat(tenant_id, sys_stat, sql_client))) {
    LOG_WARN("fail to add sys stat", KR(ret), K(tenant_id), K(sys_stat));
  } else {
    LOG_INFO("[UPGRADE] upgrade sys stat", KR(ret), K(tenant_id), K(sys_stat));
  }
  return ret;
}

int ObUpgradeUtils::filter_sys_stat(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    ObSysStat &sys_stat)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id
      || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      ObSqlString sql;
      if (OB_FAIL(sql.append_fmt("select distinct(name) name from %s", OB_ALL_SYS_STAT_TNAME))) {
        LOG_WARN("fail to append sql", KR(ret), K(tenant_id), K(sql));
      } else if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is not expected to be NULL", KR(ret), K(tenant_id), K(sql));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          ObString name;
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "name", name);
          DLIST_FOREACH_REMOVESAFE_X(node, sys_stat.item_list_, OB_SUCC(ret)) {
            if (OB_NOT_NULL(node)) {
              if (OB_ISNULL(node->name_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("name is null", KR(ret), K(tenant_id));
              } else if (0 == name.compare(node->name_)) {
                // filter sys stat which exist in __all_sys_stat
                ObSysStat::Item *item = sys_stat.item_list_.remove(node);
                if (OB_ISNULL(item) || 0 != name.compare(item->name_)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("fail to remove node", KR(ret), K(tenant_id), KPC(node));
                } else {
                  break;
                }
              }
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("node is null", KR(ret), K(tenant_id));
            }
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
          LOG_WARN("fail to iter result", KR(ret), K(tenant_id));
        }
      }
    }
  }
  return ret;
}
/* =========== upgrade sys stat end=========== */

/* =========== upgrade processor ============= */
ObUpgradeProcesserSet::ObUpgradeProcesserSet()
  : inited_(false), allocator_("UpgProcSet"),
    processor_list_(OB_MALLOC_NORMAL_BLOCK_SIZE,
                    ModulePageAllocator(allocator_))
{
}

ObUpgradeProcesserSet::~ObUpgradeProcesserSet()
{
  for (int64_t i = 0; i < processor_list_.count(); i++) {
    if (OB_NOT_NULL(processor_list_.at(i))) {
      processor_list_.at(i)->~ObBaseUpgradeProcessor();
    }
  }
}

int ObUpgradeProcesserSet::init(
    ObBaseUpgradeProcessor::UpgradeMode mode,
    common::ObMySQLProxy &sql_proxy,
    obrpc::ObSrvRpcProxy &rpc_proxy,
    obrpc::ObCommonRpcProxy &common_proxy,
    share::schema::ObMultiVersionSchemaService &schema_service,
    share::ObCheckStopProvider &check_server_provider)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
#define INIT_PROCESSOR_BY_VERSION(MAJOR, MINOR, MAJOR_PATCH, MINOR_PATCH) \
    if (OB_SUCC(ret)) { \
      void *buf = NULL; \
      ObBaseUpgradeProcessor *processor = NULL; \
      int64_t version = static_cast<int64_t>(cal_version((MAJOR), (MINOR), (MAJOR_PATCH), (MINOR_PATCH))); \
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObUpgradeFor##MAJOR##MINOR##MAJOR_PATCH##MINOR_PATCH##Processor)))) { \
        ret = OB_ALLOCATE_MEMORY_FAILED; \
        LOG_WARN("fail to alloc upgrade processor", KR(ret)); \
      } else if (OB_ISNULL(processor = new(buf)ObUpgradeFor##MAJOR##MINOR##MAJOR_PATCH##MINOR_PATCH##Processor)) { \
        ret = OB_NOT_INIT; \
        LOG_WARN("fail to new upgrade processor", KR(ret)); \
      } else if (OB_FAIL(processor->init(version, mode, sql_proxy, rpc_proxy, common_proxy, \
                                         schema_service, check_server_provider))) { \
        LOG_WARN("fail to init processor", KR(ret), K(version)); \
      } else if (OB_FAIL(processor_list_.push_back(processor))) { \
        LOG_WARN("fail to push back processor", KR(ret), K(version)); \
      } \
      if (OB_FAIL(ret)) { \
        if (OB_NOT_NULL(processor)) { \
          processor->~ObBaseUpgradeProcessor(); \
          allocator_.free(buf); \
          processor = NULL; \
          buf = NULL; \
        } else if (OB_NOT_NULL(buf)) { \
          allocator_.free(buf); \
          buf = NULL; \
        } \
      } \
    }
    // order by cluster version asc
    INIT_PROCESSOR_BY_VERSION(2, 2, 0, 60);
    INIT_PROCESSOR_BY_VERSION(2, 2, 0, 70);
    INIT_PROCESSOR_BY_VERSION(2, 2, 0, 71);
    INIT_PROCESSOR_BY_VERSION(2, 2, 0, 72);
    INIT_PROCESSOR_BY_VERSION(2, 2, 0, 73);
    INIT_PROCESSOR_BY_VERSION(2, 2, 0, 74);
    INIT_PROCESSOR_BY_VERSION(2, 2, 0, 75);
    INIT_PROCESSOR_BY_VERSION(2, 2, 0, 76);
    INIT_PROCESSOR_BY_VERSION(2, 2, 0, 77);
    INIT_PROCESSOR_BY_VERSION(3, 1, 0, 0);
    INIT_PROCESSOR_BY_VERSION(3, 1, 0, 1);
    INIT_PROCESSOR_BY_VERSION(3, 1, 0, 2);
    INIT_PROCESSOR_BY_VERSION(3, 2, 0, 0);
    INIT_PROCESSOR_BY_VERSION(3, 2, 0, 1);
    INIT_PROCESSOR_BY_VERSION(3, 2, 0, 2);
    INIT_PROCESSOR_BY_VERSION(3, 2, 3, 0);
    INIT_PROCESSOR_BY_VERSION(4, 0, 0, 0);
#undef INIT_PROCESSOR_BY_VERSION
    inited_ = true;
  }
  return ret;
}

int ObUpgradeProcesserSet::check_inner_stat() const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init yet", KR(ret));
  } else if (processor_list_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("processer_list cnt is less than 0", KR(ret));
  }
  return ret;
}

int ObUpgradeProcesserSet::get_processor_by_idx(
    const int64_t idx,
    ObBaseUpgradeProcessor *&processor) const
{
  int ret = OB_SUCCESS;
  int64_t cnt = processor_list_.count();
  processor = NULL;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (idx >= cnt || idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid idx", KR(ret), K(idx), K(cnt));
  } else {
    processor = processor_list_.at(idx);
    if (OB_ISNULL(processor)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("processor is null", KR(ret), K(idx));
    } else {
      processor->set_tenant_id(OB_INVALID_ID); // reset
    }
  }
  return ret;
}

int ObUpgradeProcesserSet::get_processor_by_version(
    const int64_t version,
    ObBaseUpgradeProcessor *&processor) const
{
  int ret = OB_SUCCESS;
  int64_t idx = OB_INVALID_INDEX;
  if (OB_FAIL(get_processor_idx_by_version(version, idx))) {
    LOG_WARN("fail to get processor idx by version", KR(ret), K(version));
  } else if (OB_FAIL(get_processor_by_idx(idx, processor))) {
    LOG_WARN("fail to get processor by idx", KR(ret), K(version));
  }
  return ret;
}

// run upgrade processor by (start_version, end_version]
int ObUpgradeProcesserSet::get_processor_idx_by_range(
    const int64_t start_version,
    const int64_t end_version,
    int64_t &start_idx,
    int64_t &end_idx)
{
  int ret = OB_SUCCESS;
  start_idx = OB_INVALID_INDEX;
  end_idx = OB_INVALID_INDEX;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (start_version <= 0 || end_version <= 0 || start_version > end_version) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid version", KR(ret), K(start_version), K(end_version));
  } else if (OB_FAIL(get_processor_idx_by_version(start_version, start_idx))) {
    LOG_WARN("fail to get processor idx by version", KR(ret), K(start_version));
  } else if (OB_FAIL(get_processor_idx_by_version(end_version, end_idx))) {
    LOG_WARN("fail to get processor idx by version", KR(ret), K(end_version));
  }
  return ret;
}

int ObUpgradeProcesserSet::get_processor_idx_by_version(
    const int64_t version,
    int64_t &idx) const
{
  int ret = OB_SUCCESS;
  idx = OB_INVALID_INDEX;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid version", KR(ret), K(version));
  } else if (processor_list_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("processor_list cnt shoud greator than 0", KR(ret));
  } else {
    int64_t start = 0;
    int64_t end = processor_list_.count() - 1;
    while (OB_SUCC(ret) && start <= end) {
      int64_t mid = (start + end) / 2;
      if (OB_ISNULL(processor_list_.at(mid))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("processor is null", KR(ret), K(mid));
      } else if (processor_list_.at(mid)->get_version() == version) {
        idx = mid;
        break;
      } else if (processor_list_.at(mid)->get_version() > version) {
        end = mid - 1;
      } else {
        start = mid + 1;
      }
    }
    if (OB_SUCC(ret) && OB_INVALID_INDEX == idx) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("fail to find processor by version", KR(ret), K(version));
    }
  }
  return ret;
}

ObBaseUpgradeProcessor::ObBaseUpgradeProcessor()
  : inited_(false), cluster_version_(OB_INVALID_VERSION),
    tenant_id_(common::OB_INVALID_ID), mode_(UPGRADE_MODE_INVALID),
    sql_proxy_(NULL), rpc_proxy_(NULL), common_proxy_(NULL), schema_service_(NULL),
    check_stop_provider_(NULL)
{
}

// Standby cluster runs sys tenant's upgrade process only.
int ObBaseUpgradeProcessor::check_inner_stat() const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init yet", KR(ret));
  } else if (cluster_version_ <= 0
             || tenant_id_ == OB_INVALID_ID
             || UPGRADE_MODE_INVALID == mode_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid processor status",
             KR(ret), K_(cluster_version), K_(tenant_id), K_(mode));
  } else if (GCTX.is_standby_cluster() && OB_SYS_TENANT_ID != tenant_id_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("run upgrade job for non-sys tenant in standby cluster is not supported",
             KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(check_stop_provider_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("check_stop_provider is null", KR(ret));
  } else if (OB_FAIL(check_stop_provider_->check_stop())) {
    LOG_WARN("check stop", KR(ret));
  }
  return ret;
}

int ObBaseUpgradeProcessor::init(
    int64_t cluster_version,
    UpgradeMode mode,
    common::ObMySQLProxy &sql_proxy,
    obrpc::ObSrvRpcProxy &rpc_proxy,
    obrpc::ObCommonRpcProxy &common_proxy,
    share::schema::ObMultiVersionSchemaService &schema_service,
    share::ObCheckStopProvider &check_server_provider)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    mode_ = mode;
    cluster_version_ = cluster_version;
    sql_proxy_ = &sql_proxy;
    rpc_proxy_ = &rpc_proxy;
    common_proxy_ = &common_proxy;
    schema_service_ = &schema_service;
    check_stop_provider_ = &check_server_provider;
    inited_ = true;
  }
  return ret;
}

/* =========== 22070 upgrade processor start ============= */
int ObUpgradeFor22070Processor::pre_upgrade()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(modify_trigger_package_source_body())) {
    LOG_WARN("fail to modify trigger package source body", KR(ret));
  } else {
    // TODO:(xiaoyi.xy) rename oracle's database('__public' to 'PUBLIC') by ddl
  }
  return ret;
}

int ObUpgradeFor22070Processor::post_upgrade()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(rebuild_subpart_partition_gc_info())) {
    LOG_WARN("fail to rebuild subpart partition gc info", KR(ret));
  }
  return ret;
}

// bugfix: https://aone.alibaba-inc.com/task/30451372
int ObUpgradeFor22070Processor::modify_trigger_package_source_body()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = get_tenant_id();
  int64_t start_ts = ObTimeUtility::current_time();
  LOG_INFO("start modify_trigger_package_source_body", K(tenant_id));
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    const static char* update_all_tenant_trigger_str =
    "update %s "
    "set "
    "package_spec_source = replace("
    "package_spec_source,"
    "'FUNCTION UPDATING(column VARCHAR2 := NULL) RETURN BOOL;',"
    "'FUNCTION UPDATING(column_name VARCHAR2 := NULL) RETURN BOOL;'"
    "),"
    "package_body_source = replace(replace("
    "package_body_source,"
    "'\n"
    "PROCEDURE init_trigger(update_columns IN STRINGARRAY) IS\n"
    "BEGIN\n"
    "  NULL;\n"
    "END;\n"
    "',"
    "'\n"
    "PROCEDURE init_trigger(update_columns IN STRINGARRAY) IS\n"
    "BEGIN\n"
    "  update_columns_ := STRINGARRAY();\n"
    "  update_columns_.EXTEND(update_columns.COUNT);\n"
    "  FOR i IN 1 .. update_columns.COUNT LOOP\n"
    "    update_columns_(i) := update_columns(i);\n"
    "  END LOOP;\n"
    "END;\n"
    "'),"
    "'\n"
    "FUNCTION UPDATING(column VARCHAR2 := NULL) RETURN BOOL IS\n"
    "BEGIN\n"
    "  RETURN (dml_event_ = 4);\n"
    "END;\n"
    "',"
    "'\n"
    "FUNCTION UPDATING(column_name VARCHAR2 := NULL) RETURN BOOL IS\n"
    "  is_updating BOOL;\n"
    "BEGIN\n"
    "  is_updating := (dml_event_ = 4);\n"
    "  IF (is_updating AND column_name IS NOT NULL) THEN\n"
    "    is_updating := FALSE;\n"
    "    FOR i IN 1 .. update_columns_.COUNT LOOP\n"
    "      IF (UPPER(update_columns_(i)) = UPPER(column_name)) THEN is_updating := TRUE; EXIT; END IF;\n"
    "    END LOOP;\n"
    "  END IF;\n"
    "  RETURN is_updating;\n"
    "END;\n"
    "');";

    const static char* update_all_tenant_trigger_history_str =
    "update %s "
    "set "
    "package_spec_source = replace("
    "package_spec_source,"
    "'FUNCTION UPDATING(column VARCHAR2 := NULL) RETURN BOOL;',"
    "'FUNCTION UPDATING(column_name VARCHAR2 := NULL) RETURN BOOL;'"
    "),"
    "package_body_source = replace(replace("
    "package_body_source,"
    "'\n"
    "PROCEDURE init_trigger(update_columns IN STRINGARRAY) IS\n"
    "BEGIN\n"
    "  NULL;\n"
    "END;\n"
    "',"
    "'\n"
    "PROCEDURE init_trigger(update_columns IN STRINGARRAY) IS\n"
    "BEGIN\n"
    "  update_columns_ := STRINGARRAY();\n"
    "  update_columns_.EXTEND(update_columns.COUNT);\n"
    "  FOR i IN 1 .. update_columns.COUNT LOOP\n"
    "    update_columns_(i) := update_columns(i);\n"
    "  END LOOP;\n"
    "END;\n"
    "'),"
    "'\n"
    "FUNCTION UPDATING(column VARCHAR2 := NULL) RETURN BOOL IS\n"
    "BEGIN\n"
    "  RETURN (dml_event_ = 4);\n"
    "END;\n"
    "',"
    "'\n"
    "FUNCTION UPDATING(column_name VARCHAR2 := NULL) RETURN BOOL IS\n"
    "  is_updating BOOL;\n"
    "BEGIN\n"
    "  is_updating := (dml_event_ = 4);\n"
    "  IF (is_updating AND column_name IS NOT NULL) THEN\n"
    "    is_updating := FALSE;\n"
    "    FOR i IN 1 .. update_columns_.COUNT LOOP\n"
    "      IF (UPPER(update_columns_(i)) = UPPER(column_name)) THEN is_updating := TRUE; EXIT; END IF;\n"
    "    END LOOP;\n"
    "  END IF;\n"
    "  RETURN is_updating;\n"
    "END;\n"
    "') "
    "where is_deleted = 0;";

    ObSqlString sql;
    int64_t affected_rows = 0;
    // update __all_tenant_tigger
    if (OB_FAIL(sql.assign_fmt(update_all_tenant_trigger_str, OB_ALL_TENANT_TRIGGER_TNAME))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else if (OB_FAIL(sql_proxy_->write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute", KR(ret), K(affected_rows), K(sql));
    } else {
      LOG_INFO("update __all_tenant_trigger",
               KR(ret), K(tenant_id), K(affected_rows), K(sql));
    }
    // update __all_tenant_tigger_history
    if (OB_SUCC(ret)) {
      sql.reset();
      affected_rows = 0;
      if (OB_FAIL(sql.assign_fmt(update_all_tenant_trigger_history_str,
                                 OB_ALL_TENANT_TRIGGER_HISTORY_TNAME))) {
        LOG_WARN("fail to assign sql", KR(ret));
      } else if (OB_FAIL(sql_proxy_->write(tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("failed to execute", KR(ret), K(affected_rows), K(sql));
      } else {
        LOG_INFO("update __all_tenant_trigger_history",
                 KR(ret), K(tenant_id), K(affected_rows), K(sql));
      }
    }
  }
  LOG_INFO("finish modify_trigger_package_source_body",
            KR(ret), K(tenant_id), "cost", ObTimeUtility::current_time() - start_ts);
  return ret;
}

int ObUpgradeFor22070Processor::modify_oracle_public_database_name()
{

  int ret = OB_SUCCESS;
  const uint64_t tenant_id = get_tenant_id();
  int64_t start_ts = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] start modify_oracle_public_database_name", K(tenant_id));
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    ObSqlString sql;
    int64_t affected_rows = 0;
    uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    uint64_t tenant_id_in_sql = ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id);
    uint64_t db_id_in_sql = ObSchemaUtils::get_extract_schema_id(exec_tenant_id, OB_PUBLIC_SCHEMA_ID);

    const static char* upd_all_database_sql =
      "update %s set database_name = 'PUBLIC' where tenant_id = %ld and database_id = %ld;";
    const static char* upd_all_database_history_sql =
      "update %s set database_name = 'PUBLIC' where tenant_id = %ld and \
       database_id = %ld and database_name = '__public';";

    lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
    if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(exec_tenant_id, compat_mode))) {
      LOG_WARN("fail to get tenant mode", K(ret), K(exec_tenant_id));
    }

    if (OB_SUCC(ret) && lib::Worker::CompatMode::ORACLE == compat_mode) {
      // update __all_database
      if (OB_FAIL(sql.assign_fmt(upd_all_database_sql,
                                 OB_ALL_DATABASE_TNAME,
                                 tenant_id_in_sql,
                                 db_id_in_sql))) {
        LOG_WARN("fail to assign sql", K(ret));
      } else if (OB_FAIL(sql_proxy_->write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("failed to execute", K(ret), K(affected_rows), K(sql));
      } else {
        LOG_INFO("[UPGRADE] update __all_database",
                 KR(ret), K(exec_tenant_id), K(affected_rows), K(sql));
      }

      // update __all_database_history
      if (OB_SUCC(ret)) {
        sql.reset();
        affected_rows = 0;
        if (OB_FAIL(sql.assign_fmt(upd_all_database_history_sql,
                                   OB_ALL_DATABASE_HISTORY_TNAME,
                                   tenant_id_in_sql,
                                   db_id_in_sql))) {
          LOG_WARN("fail to assign sql", K(ret));
        } else if (OB_FAIL(sql_proxy_->write(exec_tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("failed to execute", K(ret), K(affected_rows), K(sql));
        } else {
          LOG_INFO("[UPGRADE] update __all_database_history",
                   KR(ret), K(exec_tenant_id), K(affected_rows), K(sql));
        }
      }
    }
  }
  LOG_INFO("[UPGRADE] finish modify_oracle_public_database_name",
            KR(ret), K(tenant_id), "cost", ObTimeUtility::current_time() - start_ts);
  return ret;
}

// bugfix: https://aone.alibaba-inc.com/risk/29845662
int ObUpgradeFor22070Processor::rebuild_subpart_partition_gc_info()
{
  return OB_NOT_SUPPORTED;
}

/* =========== 22070 upgrade processor end ============= */

int ObUpgradeFor22075Processor::post_upgrade()
{
  ObSchemaGetterGuard guard;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;

  int ret = OB_SUCCESS;
  const uint64_t tenant_id = get_tenant_id();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_full_schema_guard(tenant_id, guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
    LOG_WARN("fail to get tenant mode", K(ret), K(tenant_id));
  } else if (compat_mode == lib::Worker::CompatMode::ORACLE) {
    ObSqlString sql;
    int64_t affected_rows = 0;

    OZ (sql.assign_fmt("grant create table, create type, create trigger, "
                     "create procedure, create sequence to resource"));
    CK (sql_proxy_ != NULL);
    OZ (sql_proxy_->write(tenant_id, sql.ptr(), affected_rows, ORACLE_MODE));
  }
  return ret;
}

int ObUpgradeFor22077Processor::post_upgrade()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(create_inner_keystore_for_sys_tenant())) {
    LOG_WARN("fail to create inner keystore", KR(ret));
  } else if (OB_FAIL(alter_default_profile())) {
    LOG_WARN("fail to alter default profile", K(ret));
  }
  return ret;
}

// TDE is forbidden in sys tenant, so there isn't an inner keystore. The master key of sys tenant
// is used in encrypted zone since 22077. TDE remains forbidden by add check in create tablespace
int ObUpgradeFor22077Processor::create_inner_keystore_for_sys_tenant()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = get_tenant_id();
  ObSchemaGetterGuard guard;
  const ObKeystoreSchema *ks_schema = NULL;
  LOG_INFO("create inner keystore for sys tenant start", K(tenant_id), K(ret));
  if (OB_SYS_TENANT_ID != tenant_id) {
    // do nothing
  } else if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_full_schema_guard(tenant_id, guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(guard.get_keystore_schema(tenant_id, ks_schema))) {
    LOG_WARN("fail to get keystore schema", KR(ret));
  } else if (OB_NOT_NULL(ks_schema)) {
    /*do nothing*/
  } else {
    obrpc::ObKeystoreDDLArg arg;
    ObKeystoreSchema &keystore_schema = arg.schema_;
    arg.exec_tenant_id_ = tenant_id;
    arg.type_ = obrpc::ObKeystoreDDLArg::DDLType::CREATE_KEYSTORE;
    int64_t keystore_id = OB_MYSQL_TENANT_INNER_KEYSTORE_ID;
    keystore_schema.set_keystore_id(keystore_id);
    keystore_schema.set_tenant_id(tenant_id);
    keystore_schema.set_status(2);
    keystore_schema.set_keystore_name("mysql_keystore");
    if (OB_FAIL(common_proxy_->do_keystore_ddl(arg))) {
      LOG_WARN("create keystore error", K(ret));
    }
  }
  LOG_INFO("create inner keystore for sys tenant end", K(tenant_id), K(ret));
  return ret;
}

int ObUpgradeFor22077Processor::alter_default_profile()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = get_tenant_id();
  const char* ddl_string = "alter profile default limit FAILED_LOGIN_ATTEMPTS unlimited"
                           " PASSWORD_LOCK_TIME 1";
  ObSchemaGetterGuard guard;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_full_schema_guard(tenant_id, guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
    LOG_WARN("fail to get tenant mode", K(ret), K(tenant_id));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (compat_mode == lib::Worker::CompatMode::ORACLE) {
    int64_t affected_rows = 0;

    LOG_INFO("alter_default_profile start", K(tenant_id), K(ret));
    OZ (sql_proxy_->write(tenant_id, ddl_string, affected_rows, ObCompatibilityMode::ORACLE_MODE));
    LOG_INFO("alter_default_profile end", K(tenant_id), K(ret));
  }
  return ret;
}

/* =========== 3100 upgrade processor start ============= */
int ObUpgradeFor3100Processor::pre_upgrade()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  }
  return ret;
}
int ObUpgradeFor3100Processor::post_upgrade()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(revise_check_cst_schema())) {
    LOG_WARN("fail to rebuild subpart partition gc info", KR(ret));
  }
  return ret;
}
// fill the info of check constraint columns into __all_tenant_constraint_column
// which is a new inner table from 3.1.0
int ObUpgradeFor3100Processor::revise_check_cst_schema()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = get_tenant_id();
  ObSchemaGetterGuard schema_guard;
  ObSArray<uint64_t> table_ids;
  ObSArray<int64_t> check_cst_counts_in_each_tbl;
  if (OB_FAIL(schema_service_->get_tenant_full_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(compute_check_cst_counts_in_each_table(
                     tenant_id, table_ids, check_cst_counts_in_each_tbl))) {
    LOG_WARN("compute check cst count in each table failed",
             K(ret), K(tenant_id), K(table_ids), K(check_cst_counts_in_each_tbl));
  } else if (OB_FAIL(update_check_csts(
                     tenant_id, table_ids, check_cst_counts_in_each_tbl, schema_guard))) {
    LOG_WARN("update check csts failed", K(ret), K(check_cst_counts_in_each_tbl));
  }
  return ret;
}
int ObUpgradeFor3100Processor::compute_check_cst_counts_in_each_table(
    const uint64_t tenant_id,
    ObSArray<uint64_t> &table_ids,
    ObSArray<int64_t> &check_cst_counts_in_each_table)
{
  int ret = OB_SUCCESS;
  table_ids.reset();
  check_cst_counts_in_each_table.reset();
  uint64_t table_id = OB_INVALID_ID;
  int64_t check_cst_count = 0;
  ObSqlString sql;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = NULL;
    /*
     sql is :
      SELECT table_id, count(1) AS check_csts_count
      FROM __all_constraint
      WHERE tenant_id = XXX and constraint_type = 3
      GROUP BY(table_id);
    */
    if (OB_FAIL(sql.append_fmt("SELECT table_id, count(1) as check_csts_count FROM"))) {
      LOG_WARN("failed to append sql", K(ret));
    } else if (OB_FAIL(sql.append_fmt(
                          " %s WHERE tenant_id = %lu and CONSTRAINT_TYPE = %d GROUP BY(table_id)",
                          OB_ALL_CONSTRAINT_TNAME,
                          ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                          CONSTRAINT_TYPE_CHECK))) {
      // Why need to use tenant_id to filter while revising cst schemas ?
      // Because old datas in inner table of sys tenant had't been removed after schema-split.
      // https://work.aone.alibaba-inc.com/issue/35188973
      LOG_WARN("failed to append sql for sys tenant",
               K(ret), K(tenant_id), K(OB_ALL_CONSTRAINT_TNAME));
    } else if (OB_FAIL(sql_proxy_->read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("failed to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get result", K(ret));
    } else {
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        EXTRACT_INT_FIELD_MYSQL_WITH_TENANT_ID(*result, "table_id", table_id, tenant_id);
        EXTRACT_INT_FIELD_MYSQL(*result, "check_csts_count", check_cst_count, int64_t);
        if (OB_SUCC(ret)) {
          if (OB_FAIL(table_ids.push_back(table_id))) {
            LOG_WARN("push_back table_id into array failed", K(ret), K(table_id));
          } else if (OB_FAIL(check_cst_counts_in_each_table.push_back(check_cst_count))) {
            LOG_WARN("push_back check_cst_count into array failed", K(ret), K(check_cst_count));
          }
        }
      }
      if (OB_LIKELY(OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
      } else if (OB_FAIL(ret)) {
        LOG_WARN("fail to get sql result, iter quit", K(ret), K(sql));
      } else if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ret should be OB_ITER_END", K(ret), K(sql));
      }
    }
  }
  return ret;
}
int ObUpgradeFor3100Processor::generate_constraint_schema(
    obrpc::ObSchemaReviseArg &arg,
    share::schema::ObSchemaGetterGuard &schema_guard,
    ObIAllocator &allocator,
    bool &is_need_to_revise)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg.tenant_id_;
  uint64_t table_id = arg.table_id_;
  ObRawExprFactory expr_factory(allocator);
  ObRawExpr *expr = NULL;
  ObSqlString sql;
  is_need_to_revise = true; // for reentrant
  SMART_VAR(sql::ObSQLSessionInfo, session) {
    if (OB_FAIL(session.init(0 /*default session id*/,
                             0 /*default proxy id*/,
                             &allocator))) {
      LOG_WARN("init session failed", K(ret));
    } else if (OB_FAIL(session.load_default_sys_variable(false, false))) {
      LOG_WARN("session load default system variable failed", K(ret));
    } else {
      ObResolverParams params;
      params.expr_factory_ = &expr_factory;
      params.allocator_ = &allocator;
      params.session_info_ = &session;
      const ObTableSchema *table_schema = NULL;
      bool is_oracle_mode = false;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
        LOG_WARN("get table schema failed", K(ret), K(table_id));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table_schema is null", K(ret));
      } else if (OB_FAIL(table_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
        LOG_WARN("fail to check oracle comapt mode", KR(ret), KPC(table_schema));
      } else {
        lib::CompatModeGuard g(is_oracle_mode ?
                          lib::Worker::CompatMode::ORACLE :
                          lib::Worker::CompatMode::MYSQL);
        for (ObTableSchema::const_constraint_iterator iter = table_schema->constraint_begin();
             OB_SUCC(ret) && (iter != table_schema->constraint_end());
             ++iter) {
          if (CONSTRAINT_TYPE_CHECK == (*iter)->get_constraint_type()) {
            if ((*iter)->get_column_cnt() > 0) {
              is_need_to_revise = false; // The csts in this table have been revised already.
              break;
            } else {
              ObConstraint cst;
              const ParseNode *node = NULL;
              ObRawExpr *check_constraint_expr = NULL;
              if (OB_FAIL(cst.assign(**iter))) {
                LOG_WARN("fail to assign ObConstraint", K(ret));
              } else if (OB_FAIL(ObRawExprUtils::parse_bool_expr_node_from_str(
                          cst.get_check_expr_str(), *params.allocator_, node))) {
                LOG_WARN("parse expr node from string failed", K(ret), K(cst));
              } else if (OB_FAIL(ObResolverUtils::resolve_check_constraint_expr(
                          params, node, *table_schema, cst, check_constraint_expr))) {
                LOG_WARN("resolve check constraint expr", K(ret), K(cst));
              } else if (0 == cst.get_column_cnt()) {
                // no need to revise the cst if the const check expr likes '123 > 123'
                continue;
              } else if (OB_FAIL(arg.csts_array_.push_back(cst))) {
                LOG_WARN("push back cst to csts failed", K(ret), K(cst));
              }
            }
          }
        }
        if (OB_SUCC(ret) && 0 == arg.csts_array_.count()) {
          // no need to revise if all the csts in this table are const check exprs
          is_need_to_revise = false;
        }
      }
    }
  }
  return ret;
}
// 对拥有 check 约束的表补充 check 约束的列信息
int ObUpgradeFor3100Processor::update_check_csts(
    const uint64_t tenant_id,
    ObSArray<uint64_t> &table_ids,
    ObSArray<int64_t> &check_cst_counts_in_each_tbl,
    share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("UpdateCheckCst");
  const int64_t DEFAULT_TIMEOUT = 10 * 1000 * 1000L; // 10s
  const int64_t TIMEOUT_PER_RPC = GCONF.rpc_timeout; // default rpc timeout is 2s
  if (table_ids.count() != check_cst_counts_in_each_tbl.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of table_ids is not equal to the count of check_cst_counts_in_each_tbl",
             K(ret), K(tenant_id), K(table_ids.count()), K(check_cst_counts_in_each_tbl.count()));
  } else {
    bool need_to_revise = true; // to guarantee reentrant
    for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
      obrpc::ObSchemaReviseArg arg;
      need_to_revise = true;
      arg.exec_tenant_id_ = tenant_id;
      arg.tenant_id_ = tenant_id;
      arg.table_id_ = table_ids.at(i);
      arg.type_ = obrpc::ObSchemaReviseArg::SchemaReviseType::REVISE_CONSTRAINT_COLUMN_INFO;
      arg.csts_array_.reset();
      if (OB_FAIL(arg.csts_array_.reserve(check_cst_counts_in_each_tbl.at(i)))) {
        LOG_WARN("reserve space for csts failed",
                 K(ret), K(table_ids.at(i)), K(check_cst_counts_in_each_tbl.at(i)));
      } else if (OB_FAIL(generate_constraint_schema(arg, schema_guard, allocator, need_to_revise))) {
        LOG_WARN("generate constraint schemas failed", K(ret), K(arg));
      } else if (need_to_revise) {
        if (arg.csts_array_.count() != check_cst_counts_in_each_tbl.at(i)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the count of csts in arg is not equal to check_cst_count_in_each_tbl",
                   K(ret), K(tenant_id), K(table_ids.at(i)),
                   K(arg.csts_array_.count()), K(check_cst_counts_in_each_tbl.at(i)));
        } else if (OB_ISNULL(rpc_proxy_)) {
          ret = OB_NOT_INIT;
          LOG_WARN("rpc proxy is not inited");
        } else if (OB_FAIL(rpc_proxy_->timeout(max(DEFAULT_TIMEOUT, TIMEOUT_PER_RPC)).schema_revise(arg))) {
          LOG_WARN("schema revise failed", K(ret), K(arg));
        }
      }
    }
  }
  return ret;
}
/* =========== 3100 upgrade processor end ============= */

/* =========== 3102 upgrade processor start ============= */

int ObUpgradeFor3102Processor::pre_upgrade()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  }
  return ret;
}

int ObUpgradeFor3102Processor::post_upgrade()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(revise_not_null_cst_schema())) {
    LOG_ERROR("fail to revise not null cst schema", KR(ret), K(get_tenant_id()));
  }
  return ret;
}

int ObUpgradeFor3102Processor::revise_not_null_cst_schema()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = get_tenant_id();
  ObSchemaGetterGuard schema_guard;
  ObSArray<uint64_t> table_ids;

  oceanbase::lib::Worker::CompatMode compat_mode;
  if (OB_FAIL(schema_service_->get_tenant_full_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_compat_mode(tenant_id, compat_mode))) {
    LOG_WARN("get tenant compat mode failed", K(ret), K(tenant_id));
  } else if (lib::Worker::CompatMode::MYSQL == compat_mode) {
    // do nothing for mysql tenant
  } else if (OB_FAIL(get_all_table_with_not_null_column(tenant_id, schema_guard, table_ids))) {
    LOG_WARN("get not null column ids of each table failed",
             K(ret), K(tenant_id), K(table_ids));
  } else {
    const int64_t DEFAULT_TIMEOUT = 10 * 1000 * 1000L; // 10s
    const int64_t TIMEOUT_PER_RPC = GCONF.rpc_timeout; // default rpc timeout is 2s
    for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
      obrpc::ObSchemaReviseArg arg;
      arg.exec_tenant_id_ = tenant_id;
      arg.tenant_id_ = tenant_id;
      arg.table_id_ = table_ids.at(i);
      arg.type_ = obrpc::ObSchemaReviseArg::SchemaReviseType::REVISE_NOT_NULL_CONSTRAINT;
    if (OB_FAIL(rpc_proxy_->timeout(max(DEFAULT_TIMEOUT, TIMEOUT_PER_RPC)).schema_revise(arg))) {
        LOG_WARN("schema revise failed", K(ret), K(arg));
      }
    }
  }

  return ret;
}

int ObUpgradeFor3102Processor::get_all_table_with_not_null_column(
    const uint64_t tenant_id,
    ObSchemaGetterGuard &schema_guard,
    ObSArray<uint64_t> &table_ids)
{
  return OB_NOT_SUPPORTED;
}

/* =========== 3102 upgrade processor end ============= */

/* =========== 3200 upgrade processor start ============= */

int ObUpgradeFor3200Processor::pre_upgrade()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  }
  return ret;
}

int ObUpgradeFor3200Processor::post_upgrade()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = get_tenant_id();
  oceanbase::lib::Worker::CompatMode compat_mode;
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_full_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_compat_mode(tenant_id, compat_mode))) {
    LOG_WARN("get tenant compat mode failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(grant_directory_privilege_for_dba_role(compat_mode, tenant_id))) {
    LOG_WARN("fail to grant directory privilege for dba role", K(ret), K(compat_mode), K(tenant_id));
  }
  return ret;
}

int ObUpgradeFor3200Processor::grant_directory_privilege_for_dba_role(
    const lib::Worker::CompatMode compat_mode,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (lib::Worker::CompatMode::ORACLE == compat_mode) {
    // only grant privilege under oracle mode
    ObSqlString sql;
    int64_t affected_rows = 0;
    OZ (sql.assign_fmt("grant create any directory, drop any directory to dba"));
    CK (sql_proxy_ != NULL);
    OZ (sql_proxy_->write(tenant_id, sql.ptr(), affected_rows, ORACLE_MODE));
  }
  return ret;
}

/* =========== 3200 upgrade processor end ============= */

/* =========== 3201 upgrade processor start ============= */

int ObUpgradeFor3201Processor::pre_upgrade()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  }
  return ret;
}

int ObUpgradeFor3201Processor::post_upgrade()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = get_tenant_id();
  oceanbase::lib::Worker::CompatMode compat_mode;
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_full_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_compat_mode(tenant_id, compat_mode))) {
    LOG_WARN("get tenant compat mode failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(init_tenant_optstat_global_prefs(compat_mode, tenant_id))) {
    LOG_WARN("failed to init tenant optstat global prefs", K(ret), K(compat_mode), K(tenant_id));
  }
  return ret;
}

int ObUpgradeFor3201Processor::init_tenant_optstat_global_prefs(
    const lib::Worker::CompatMode compat_mode,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (lib::Worker::CompatMode::ORACLE == compat_mode) {
    ObSqlString raw_sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(ObDbmsStatsPreferences::gen_init_global_prefs_sql(raw_sql))) {
      LOG_WARN("failed gen init global prefs sql", K(ret), K(raw_sql));
    } else if (OB_UNLIKELY(raw_sql.empty()) || OB_ISNULL(sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(raw_sql), K(sql_proxy_));
    } else if (OB_FAIL(sql_proxy_->write(tenant_id, raw_sql.ptr(), affected_rows))) {
      LOG_WARN("failed to exec sql", K(ret), K(raw_sql));
    }
  }
  return ret;
}

/* =========== 3201 upgrade processor end ============= */

/* =========== 4000 upgrade processor start ============= */
int ObUpgradeFor4000Processor::pre_upgrade()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  }
  return ret;
}

int ObUpgradeFor4000Processor::post_upgrade()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = get_tenant_id();
  oceanbase::lib::Worker::CompatMode compat_mode;
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_full_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_compat_mode(tenant_id, compat_mode))) {
    LOG_WARN("get tenant compat mode failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(grant_debug_privilege_for_dba_role(compat_mode, tenant_id))) {
    LOG_WARN("fail to grant directory privilege for dba role", K(ret), K(compat_mode), K(tenant_id));
  } else if (OB_FAIL(grant_context_privilege_for_dba_role(compat_mode, tenant_id))) {
    LOG_WARN("fail to grant context privilege for dba role", K(ret), K(compat_mode), K(tenant_id));
  }
  return ret;
}

int ObUpgradeFor4000Processor::grant_debug_privilege_for_dba_role(
    const lib::Worker::CompatMode compat_mode,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (lib::Worker::CompatMode::ORACLE == compat_mode) {
    // only grant privilege under oracle mode
    ObSqlString sql;
    int64_t affected_rows = 0;
    OZ (sql.assign_fmt("grant debug connect session, debug any procedure to dba"));
    CK (sql_proxy_ != NULL);
    OZ (sql_proxy_->write(tenant_id, sql.ptr(), affected_rows, ORACLE_MODE));
  }
  return ret;
}

int ObUpgradeFor4000Processor::grant_context_privilege_for_dba_role(
    const lib::Worker::CompatMode compat_mode,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (lib::Worker::CompatMode::ORACLE == compat_mode) {
    // only grant privilege under oracle mode
    ObSqlString sql;
    int64_t affected_rows = 0;
    OZ (sql.assign_fmt("grant create any context, drop any context to dba"));
    CK (sql_proxy_ != NULL);
    OZ (sql_proxy_->write(tenant_id, sql.ptr(), affected_rows, ORACLE_MODE));
  }
  return ret;
}

#undef FORMAT_STR

/* =========== 4000 upgrade processor end ============= */

/* =========== upgrade processor end ============= */
} // end share
} // end oceanbase
