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

#define USING_LOG_PREFIX SHARE

#include "lib/string/ob_sql_string.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_upgrade_utils.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "observer/ob_server_struct.h"
#include "rootserver/ob_root_service.h"

namespace oceanbase {
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace share::schema;
using namespace rootserver;

namespace share {
#define CALC_CLUSTER_VERSION(major, minor, patch) (((major) << 32) + ((minor) << 16) + (patch))
const uint64_t ObUpgradeChecker::UPGRADE_PATH[CLUTER_VERSION_NUM] = {
    CALC_CLUSTER_VERSION(3UL, 1UL, 1UL),    //3.1.1
    CALC_CLUSTER_VERSION(3UL, 1UL, 2UL),   //3.1.2
    CALC_CLUSTER_VERSION(3UL, 1UL, 3UL),   //3.1.3
    CALC_CLUSTER_VERSION(3UL, 1UL, 4UL)   //3.1.4
};

bool ObUpgradeChecker::check_cluster_version_exist(const uint64_t version)
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
 * 1. Once sys tenant's system table is created, schema of system table in tenant space is avaliable.
 *    After that, create other tenants' system table will cause error because schema is exist.
 *    So, sys tenant's system tables must be created at last.
 * 2. Check if system table is once created failed from inner tables(schema related and meta table).
 *    If system table is created failed, manual intervention is needed.
 */
int ObUpgradeUtils::create_tenant_tables(
    share::schema::ObMultiVersionSchemaService& schema_service, const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  ObSchemaGetterGuard schema_guard;
  if (table_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_id is invalid", K(ret), K(table_id));
  } else if (OB_FAIL(schema_service.get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("get tenant ids failed", K(ret));
  } else {
    ObTableSchema table_schema;
    const schema_create_func* creator_ptr_arrays[] = {sys_table_schema_creators};
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(creator_ptr_arrays); ++i) {
      for (const schema_create_func* creator_ptr = creator_ptr_arrays[i]; OB_SUCCESS == ret && NULL != *creator_ptr;
           ++creator_ptr) {
        table_schema.reset();
        if (OB_FAIL((*creator_ptr)(table_schema))) {
          LOG_WARN("construct_schema failed", K(table_schema), K(ret));
        } else if (table_id == extract_pure_id(table_schema.get_table_id())) {
          break;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (table_id != extract_pure_id(table_schema.get_table_id())) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("fail to find tenant table", K(ret), K(table_id), K(table_schema.get_table_id()));
    } else if (tenant_ids.size() > 0 && OB_SYS_TENANT_ID != tenant_ids.at(0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fisrt tenant should be sys_tenant", K(ret), K(tenant_ids.at(0)));
    } else {
      bool allow_sys_create_table = true;
      bool in_sync = true;
      for (int64_t i = tenant_ids.size() - 1; i >= 0 && OB_SUCC(ret); i--) {
        if (OB_FAIL(create_tenant_table(tenant_ids.at(i), table_schema, in_sync, allow_sys_create_table))) {
          LOG_WARN("create new tenant table failed", K(ret), "tenant_id", tenant_ids.at(i));
        }
      }
    }
  }
  return ret;
}

int ObUpgradeUtils::create_tenant_table(
    const uint64_t tenant_id, ObTableSchema& table_schema, bool in_sync, bool& allow_sys_create_table)
{
  int ret = OB_SUCCESS;
  if (tenant_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", K(ret), K(tenant_id));
  } else if (OB_SYS_TENANT_ID == tenant_id && !allow_sys_create_table) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can't create sys tenant table now", K(ret), K(tenant_id));
  } else {
    obrpc::ObCreateTableArg arg;
    uint64_t table_id = OB_INVALID_ID;
    obrpc::ObCreateTableRes res;
    table_id = combine_id(tenant_id, extract_pure_id(table_schema.get_table_id()));
    bool exist = false;
    if (OB_FAIL(check_table_exist(table_id, exist))) {
      LOG_WARN("fail to check table exist", K(ret), K(table_id));
    } else if (exist) {
      // schema exist(from inner table), just skip
      LOG_INFO("table exists, just skip", K(ret), K(table_id));
    } else if (OB_FAIL(check_table_partition_exist(table_id, exist))) {
      LOG_WARN("fail to check table partition exist", K(ret), K(table_id));
    } else if (exist) {
      // create table failed or create tenant during upgrade.
      // To continue create other tenants' system table, we skip here.
      LOG_ERROR(
          "table partition exist, maybe create table failed or tenant new created, just skip", K(ret), K(table_id));
    } else {
      arg.if_not_exist_ = true;
      if (OB_FAIL(arg.schema_.assign(table_schema))) {
        LOG_WARN("fail to assign schema", K(ret));
      } else {
        arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
        arg.schema_.set_tenant_id(tenant_id);
        arg.schema_.set_table_id(table_id);
        arg.schema_.set_database_id(combine_id(tenant_id, OB_SYS_DATABASE_ID));
        arg.schema_.set_tablegroup_id(combine_id(tenant_id, OB_SYS_TABLEGROUP_ID));
        arg.schema_.set_tablegroup_name(OB_SYS_TABLEGROUP_NAME);
        arg.schema_.set_table_type(USER_TABLE);  // just to make it work
        arg.db_name_ = OB_SYS_DATABASE_NAME;
        arg.create_mode_ = obrpc::ObCreateTableMode::OB_CREATE_TABLE_MODE_LOOSE;
        arg.schema_.set_primary_zone(ObString());
        arg.schema_.set_locality(ObString());
        arg.schema_.set_previous_locality(ObString());
        ObArray<ObString> empty_zones;
        arg.schema_.set_zone_list(empty_zones);
        arg.is_inner_ = true;
        if (arg.schema_.has_partition()) {
          // reset partition option
          arg.schema_.reset_partition_schema();
          arg.schema_.get_part_option().set_max_used_part_id(0);
          arg.schema_.get_part_option().set_partition_cnt_within_partition_table(0);
        }
        // Because run job cmd will run in ddl thread, we can just call create_table() directly.
        const int64_t start = ObTimeUtility::current_time();
        LOG_INFO("[UPGRADE] create tenant space tables start", K(tenant_id), K(table_id), K(start));
        if (in_sync) {
          if (OB_ISNULL(GCTX.root_service_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid global context", K(ret), K(GCTX));
          } else if (OB_FAIL(GCTX.root_service_->create_table(arg, res))) {
            LOG_WARN("fail to create table", K(ret), K(arg), K(res));
          }
        } else {
          if (OB_ISNULL(GCTX.rs_rpc_proxy_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid global context", K(ret), K(GCTX));
          } else if (OB_FAIL(GCTX.rs_rpc_proxy_->create_table(arg, res))) {
            LOG_WARN("fail to create table", K(ret), K(arg), K(res));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (table_id != res.table_id_) {
        LOG_WARN("table_id not match", K(ret), K(table_id), K(res.table_id_));
      } else {
        LOG_INFO("[UPGRADE] create tenant space table end", K(ret), K(tenant_id), K(table_id));
      }
    }
  }
  allow_sys_create_table = OB_SUCC(ret);
  return ret;
}

int ObUpgradeUtils::check_table_exist(uint64_t table_id, bool& exist)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult* result = NULL;
    uint64_t tenant_id = extract_tenant_id(table_id);
    exist = false;
    const char* table_name = NULL;
    if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql_proxy is null", K(ret));
    } else if (table_id <= 0 || !is_sys_table(table_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("table_id is invalid", K(ret), K(table_id));
    } else if (OB_FAIL(ObSchemaUtils::get_all_table_name(OB_SYS_TENANT_ID, table_name))) {
      LOG_WARN("fail to get all table name", K(ret));
    } else if (OB_FAIL(sql.assign_fmt("SELECT floor(count(*)) as count FROM %s "
                                      "WHERE tenant_id = '%ld' and table_id = '%ld'",
                   table_name,
                   tenant_id,
                   table_id))) {
      LOG_WARN("fail to append sql", K(ret));
    } else if (OB_FAIL(GCTX.sql_proxy_->read(res, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get sql result", K(ret));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("fail to get result", K(ret));
    } else {
      int32_t count = OB_INVALID_COUNT;
      EXTRACT_INT_FIELD_MYSQL(*result, "count", count, int32_t);
      if (OB_SUCC(ret)) {
        if (0 == count) {
          exist = false;
        } else if (1 == count) {
          exist = true;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("more than one row", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObUpgradeUtils::check_table_partition_exist(uint64_t table_id, bool& exist)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult* result = NULL;
    uint64_t tenant_id = extract_tenant_id(table_id);
    exist = false;
    if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql_proxy is null", K(ret));
    } else if (table_id <= 0 || !is_sys_table(table_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("table_id is invalid", K(ret), K(table_id));
    } else if (OB_FAIL(sql.assign_fmt("SELECT floor(count(*)) as count FROM %s "
                                      "WHERE tenant_id = '%ld' and table_id = '%ld'",
                   OB_ALL_ROOT_TABLE_TNAME,
                   tenant_id,
                   table_id))) {
      LOG_WARN("fail to append sql", K(ret));
    } else if (OB_FAIL(GCTX.sql_proxy_->read(res, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get sql result", K(ret));
    } else if (OB_FAIL((result->next()))) {
      LOG_WARN("fail to get result", K(ret));
    } else {
      int32_t count = OB_INVALID_COUNT;
      EXTRACT_INT_FIELD_MYSQL(*result, "count", count, int32_t);
      if (OB_SUCC(ret)) {
        exist = (count > 0);
      }
    }
  }
  return ret;
}

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
  if (JOB_TYPE_INVALID >= job_type || job_type >= JOB_TYPE_MAX) {
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
int ObUpgradeUtils::can_run_upgrade_job(ObRsJobType job_type, bool& can)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  bool success = false;
  can = false;
  if (JOB_TYPE_INVALID >= job_type || job_type >= JOB_TYPE_MAX) {
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

int ObUpgradeUtils::check_rs_job_exist(ObRsJobType job_type, bool& exist)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult* result = NULL;
    int32_t count = OB_INVALID_COUNT;
    exist = false;
    if (JOB_TYPE_INVALID >= job_type || job_type >= JOB_TYPE_MAX) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid job_type", K(ret), K(job_type));
    } else if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql_proxy is null", K(ret));
    } else if (sql.assign_fmt("SELECT floor(count(*)) as count FROM %s WHERE job_type = '%s'",
                   OB_ALL_ROOTSERVICE_JOB_TNAME,
                   ObRsJobTableOperator::get_job_type_str(job_type))) {
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

int ObUpgradeUtils::check_rs_job_success(ObRsJobType job_type, bool& success)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult* result = NULL;
    int32_t count = OB_INVALID_COUNT;
    success = false;
    if (JOB_TYPE_INVALID >= job_type || job_type >= JOB_TYPE_MAX) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid job_type", K(ret), K(job_type));
    } else if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql_proxy is null", K(ret));
    } else if (sql.assign_fmt("SELECT floor(count(*)) as count FROM %s "
                              "WHERE job_type = '%s' and job_status = 'SUCCESS'",
                   OB_ALL_ROOTSERVICE_JOB_TNAME,
                   ObRsJobTableOperator::get_job_type_str(job_type))) {
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

int ObUpgradeUtils::force_create_tenant_table(
    const uint64_t tenant_id, const uint64_t table_id, const uint64_t last_replay_log_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id || !is_tenant_table(table_id) || OB_INVALID_ID == last_replay_log_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id or table_id or last_replay_log_id",
        K(ret),
        K(tenant_id),
        K(table_id),
        K(last_replay_log_id));
  } else {
    ObTableSchema table_schema;
    const schema_create_func* creator_ptr_arrays[] = {sys_table_schema_creators};
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(creator_ptr_arrays); ++i) {
      for (const schema_create_func* creator_ptr = creator_ptr_arrays[i]; OB_SUCCESS == ret && NULL != *creator_ptr;
           ++creator_ptr) {
        table_schema.reset();
        if (OB_FAIL((*creator_ptr)(table_schema))) {
          LOG_WARN("construct_schema failed", K(table_schema), K(ret));
        } else if (table_id == extract_pure_id(table_schema.get_table_id())) {
          break;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (table_id != extract_pure_id(table_schema.get_table_id())) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("fail to find tenant table", K(ret), K(table_id), K(table_schema.get_table_id()));
    } else {
      obrpc::ObCreateTableArg arg;
      uint64_t table_id = OB_INVALID_ID;
      obrpc::ObCreateTableRes res;
      table_id = combine_id(tenant_id, extract_pure_id(table_schema.get_table_id()));

      arg.if_not_exist_ = true;
      if (OB_FAIL(arg.schema_.assign(table_schema))) {
        LOG_WARN("fail to assign schema", K(ret));
      } else {
        arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
        arg.schema_.set_tenant_id(tenant_id);
        arg.schema_.set_table_id(table_id);
        arg.schema_.set_database_id(combine_id(tenant_id, OB_SYS_DATABASE_ID));
        arg.schema_.set_tablegroup_id(combine_id(tenant_id, OB_SYS_TABLEGROUP_ID));
        arg.schema_.set_tablegroup_name(OB_SYS_TABLEGROUP_NAME);
        arg.schema_.set_table_type(USER_TABLE);  // just to make it work
        arg.db_name_ = OB_SYS_DATABASE_NAME;
        arg.create_mode_ = obrpc::ObCreateTableMode::OB_CREATE_TABLE_MODE_LOOSE;
        arg.last_replay_log_id_ = last_replay_log_id;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(GCTX.root_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid global context", K(ret), K(GCTX));
      } else if (OB_FAIL(GCTX.root_service_->create_table(arg, res))) {
        LOG_WARN("fail to create table", K(ret), K(arg), K(res));
      } else if (table_id != res.table_id_) {
        LOG_WARN("table_id not match", K(ret), K(table_id), K(res.table_id_));
      } else {
        LOG_INFO("create table", K(ret), K(table_id));
      }
    }
  }
  return ret;
}

int ObUpgradeUtils::check_schema_sync(bool& is_sync)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult* result = NULL;
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
int ObUpgradeUtils::upgrade_sys_variable(common::ObISQLClient& sql_client, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> update_list;  // sys_var_store_idx, sys var to modify
  ObArray<int64_t> add_list;     // sys_var_store_idx, sys var to add
  if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(calc_diff_sys_var(sql_client, tenant_id, update_list, add_list))) {
    LOG_WARN("fail to calc diff sys var", KR(ret), K(tenant_id));
  } else if (OB_FAIL(update_sys_var(sql_client, tenant_id, update_list))) {
    LOG_WARN("fail to update sys var", KR(ret), K(tenant_id));
  } else if (OB_FAIL(add_sys_var(sql_client, tenant_id, add_list))) {
    LOG_WARN("fail to add sys var", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObUpgradeUtils::calc_diff_sys_var(common::ObISQLClient& sql_client, const uint64_t tenant_id,
    common::ObArray<int64_t>& update_list, common::ObArray<int64_t>& add_list)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else {
    ObArray<Name> fetch_names;
    ObSqlString sql;
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      ObMySQLResult* result = NULL;
      if (OB_FAIL(sql.append_fmt("select name, data_type, value, info, flags, min_val, max_val from %s "
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
          } else if (SYS_VAR_INVALID == (sys_var_id = ObSysVarFactory::find_sys_var_id_by_name(name))) {
            // maybe has unused sys variable in table, just ignore
            LOG_INFO("sys variable exist in table, but not hard code", KR(ret), K(tenant_id), K(name));
          } else if (OB_FAIL(ObSysVarFactory::calc_sys_var_store_idx(sys_var_id, var_store_idx))) {
            LOG_WARN("fail to calc sys var store idx", KR(ret), K(sys_var_id), K(name));
          } else if (false == ObSysVarFactory::is_valid_sys_var_store_idx(var_store_idx)) {
            ret = OB_SCHEMA_ERROR;
            LOG_WARN("calc sys var store idx success but store_idx is invalid", KR(ret), K(var_store_idx));
          } else {
            const ObString& hard_code_info = ObSysVariables::get_info(var_store_idx);
            const ObObjType& hard_code_type = ObSysVariables::get_type(var_store_idx);
            const ObString& hard_code_min_val = ObSysVariables::get_min(var_store_idx);
            const ObString& hard_code_max_val = ObSysVariables::get_max(var_store_idx);
            const int64_t hard_code_flag = ObSysVariables::get_flags(var_store_idx);
            if (hard_code_flag != flags || static_cast<int64_t>(hard_code_type) != data_type ||
                0 != hard_code_info.compare(info) || 0 != hard_code_min_val.compare(min_val) ||
                0 != hard_code_max_val.compare(max_val)) {
              // sys var to modify
              LOG_INFO("sys var diff, need modify",
                  K(tenant_id),
                  K(name),
                  K(data_type),
                  K(flags),
                  K(min_val),
                  K(max_val),
                  K(info),
                  K(hard_code_type),
                  K(hard_code_flag),
                  K(hard_code_min_val),
                  K(hard_code_max_val),
                  K(hard_code_info));
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
        const ObString& name = ObSysVariables::get_name(i);
        bool found = false;
        FOREACH_CNT_X(fetch_name, fetch_names, OB_SUCC(ret))
        {
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
          LOG_INFO("sys var miss, need add", K(tenant_id), K(name), K(i));
        }
      }
    }
  }
  return ret;
}

int ObUpgradeUtils::update_sys_var(
    common::ObISQLClient& sql_client, const uint64_t tenant_id, common::ObArray<int64_t>& update_list)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < update_list.count(); i++) {
      int64_t var_store_idx = update_list.at(i);
      const ObString& name = ObSysVariables::get_name(var_store_idx);
      const ObObjType& type = ObSysVariables::get_type(var_store_idx);
      const ObString& value = ObSysVariables::get_value(var_store_idx);
      const ObString& min = ObSysVariables::get_min(var_store_idx);
      const ObString& max = ObSysVariables::get_max(var_store_idx);
      const ObString& info = ObSysVariables::get_info(var_store_idx);
      const int64_t flag = ObSysVariables::get_flags(var_store_idx);
      const ObString zone("");
      ObSysParam sys_param;
      if (OB_FAIL(
              sys_param.init(tenant_id, zone, name.ptr(), type, value.ptr(), min.ptr(), max.ptr(), info.ptr(), flag))) {
        LOG_WARN("sys_param init failed",
            KR(ret),
            K(tenant_id),
            K(name),
            K(type),
            K(value),
            K(min),
            K(max),
            K(info),
            K(flag));
      } else if (!sys_param.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("sys param is invalid", KR(ret), K(tenant_id), K(sys_param));
      } else if (OB_FAIL(execute_update_sys_var_sql(sql_client, tenant_id, sys_param))) {
        LOG_WARN("fail to execute update sys var sql", KR(ret), K(tenant_id));
      } else if (OB_FAIL(execute_update_sys_var_history_sql(sql_client, tenant_id, sys_param))) {
        LOG_WARN("fail to execute update sys var history sql", KR(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObUpgradeUtils::execute_update_sys_var_sql(
    common::ObISQLClient& sql_client, const uint64_t tenant_id, const ObSysParam& sys_param)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else {
    int64_t affected_rows = 0;
    ObDMLSqlSplicer dml;
    ObDMLExecHelper exec(sql_client, tenant_id);
    if (OB_FAIL(gen_basic_sys_variable_dml(tenant_id, sys_param, dml))) {
      LOG_WARN("fail to gen dml", KR(ret), K(tenant_id), K(sys_param));
    } else if (OB_FAIL(exec.exec_update(OB_ALL_SYS_VARIABLE_TNAME, dml, affected_rows))) {
      LOG_WARN("execute insert failed", KR(ret));
    } else if (!is_zero_row(affected_rows) && !is_single_row(affected_rows)) {
      LOG_WARN("invalid affected_rows", KR(ret), K(tenant_id), K(affected_rows));
    } else {
      LOG_INFO("[UPGRADE] modify sys var", KR(ret), K(tenant_id), K(sys_param));
    }
  }
  return ret;
}

int ObUpgradeUtils::execute_update_sys_var_history_sql(
    common::ObISQLClient& sql_client, const uint64_t tenant_id, const ObSysParam& sys_param)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else {
    int64_t schema_version = OB_INVALID_VERSION;
    {
      SMART_VAR(ObMySQLProxy::MySQLResult, res)
      {
        ObMySQLResult* result = NULL;
        ObSqlString sql;
        if (OB_FAIL(sql.append_fmt("select schema_version from %s where tenant_id = %lu"
                                   " and zone = '' and name = '%s' order by schema_version desc limit 1",
                OB_ALL_SYS_VARIABLE_HISTORY_TNAME,
                ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                sys_param.name_))) {
          LOG_WARN("fail to append sql", KR(ret), K(tenant_id), K(sql));
        } else if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
          LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
        } else if (NULL == (result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is not expected to be NULL", KR(ret), K(tenant_id), K(sql));
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("fail to get row", KR(ret), K(tenant_id), K(sql));
        } else {
          EXTRACT_INT_FIELD_MYSQL(*result, "schema_version", schema_version, int64_t);
        }
      }
    }
    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      ObDMLSqlSplicer dml;
      ObDMLExecHelper exec(sql_client, tenant_id);
      if (OB_INVALID_VERSION == schema_version) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid schema_version", KR(ret), K(tenant_id), K(sys_param));
      } else if (OB_FAIL(gen_basic_sys_variable_dml(tenant_id, sys_param, dml))) {
        LOG_WARN("fail to gen dml", KR(ret), K(tenant_id), K(sys_param));
      } else if (OB_FAIL(dml.add_pk_column("schema_version", schema_version))) {
        LOG_WARN("fail to add column", KR(ret), K(tenant_id), K(schema_version));
      } else if (OB_FAIL(exec.exec_update(OB_ALL_SYS_VARIABLE_HISTORY_TNAME, dml, affected_rows))) {
        LOG_WARN("execute insert failed", KR(ret));
      } else if (!is_zero_row(affected_rows) && !is_single_row(affected_rows)) {
        LOG_WARN("invalid affected_rows", KR(ret), K(tenant_id), K(affected_rows));
      } else {
        LOG_INFO("[UPGRADE] modify sys var history", KR(ret), K(tenant_id), K(sys_param));
      }
    }
  }
  return ret;
}

int ObUpgradeUtils::add_sys_var(
    common::ObISQLClient& sql_client, const uint64_t tenant_id, common::ObArray<int64_t>& add_list)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else {
    ObArenaAllocator allocator("AddSysVar");
    for (int64_t i = 0; OB_SUCC(ret) && i < add_list.count(); i++) {
      int64_t var_store_idx = add_list.at(i);
      const ObString& name = ObSysVariables::get_name(var_store_idx);
      const ObObjType& type = ObSysVariables::get_type(var_store_idx);
      const ObString& min = ObSysVariables::get_min(var_store_idx);
      const ObString& max = ObSysVariables::get_max(var_store_idx);
      const ObString& info = ObSysVariables::get_info(var_store_idx);
      const int64_t flag = ObSysVariables::get_flags(var_store_idx);
      const ObString zone("");
      ObSysParam sys_param;
      ObString value;
      if (OB_FAIL(convert_sys_variable_value(var_store_idx, allocator, value))) {
        LOG_WARN("fail to get sys variable value", KR(ret), K(tenant_id), K(var_store_idx));
      } else if (OB_FAIL(sys_param.init(
                     tenant_id, zone, name.ptr(), type, value.ptr(), min.ptr(), max.ptr(), info.ptr(), flag))) {
        LOG_WARN("sys_param init failed",
            KR(ret),
            K(tenant_id),
            K(name),
            K(type),
            K(value),
            K(min),
            K(max),
            K(info),
            K(flag));
      } else if (!sys_param.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("sys param is invalid", KR(ret), K(tenant_id), K(sys_param));
      } else if (OB_FAIL(execute_add_sys_var_sql(sql_client, tenant_id, sys_param))) {
        LOG_WARN("fail to execute add sys var sql", KR(ret), K(tenant_id));
      } else if (OB_FAIL(execute_add_sys_var_history_sql(sql_client, tenant_id, sys_param))) {
        LOG_WARN("fail to execute add sys var history sql", KR(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

// C++ implement for special_update_sys_vars_for_tenant() in python upgrade script.
int ObUpgradeUtils::convert_sys_variable_value(
    const int64_t var_store_idx, common::ObIAllocator& allocator, ObString& value)
{
  int ret = OB_SUCCESS;
  const ObString& name = ObSysVariables::get_name(var_store_idx);
  if (0 == name.compare("nls_date_format")) {
    if (OB_FAIL(ob_write_string(allocator, ObString("YYYY-MM-DD HH24:MI:SS"), value))) {
      LOG_WARN("fail to write string", KR(ret), K(name));
    }
  } else if (0 == name.compare("nls_timestamp_format")) {
    if (OB_FAIL(ob_write_string(allocator, ObString("YYYY-MM-DD HH24:MI:SS.FF"), value))) {
      LOG_WARN("fail to write string", KR(ret), K(name));
    }
  } else if (0 == name.compare("nls_timestamp_tz_format")) {
    if (OB_FAIL(ob_write_string(allocator, ObString("YYYY-MM-DD HH24:MI:SS.FF TZR TZD"), value))) {
      LOG_WARN("fail to write string", KR(ret), K(name));
    }
  } else {
    const ObString& ori_value = ObSysVariables::get_value(var_store_idx);
    value.assign_ptr(ori_value.ptr(), ori_value.length());
  }
  return ret;
}

int ObUpgradeUtils::execute_add_sys_var_sql(
    common::ObISQLClient& sql_client, const uint64_t tenant_id, const ObSysParam& sys_param)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else {
    int64_t affected_rows = 0;
    ObDMLSqlSplicer dml;
    ObDMLExecHelper exec(sql_client, tenant_id);
    if (OB_FAIL(gen_basic_sys_variable_dml(tenant_id, sys_param, dml))) {
      LOG_WARN("fail to gen dml", KR(ret), K(tenant_id), K(sys_param));
    } else if (OB_FAIL(dml.add_column("value", FORMAT_STR(ObString(sys_param.value_))))) {
      LOG_WARN("fail to gen dml", KR(ret), K(tenant_id), K(sys_param));
    } else if (OB_FAIL(exec.exec_replace(OB_ALL_SYS_VARIABLE_TNAME, dml, affected_rows))) {
      LOG_WARN("execute insert failed", KR(ret));
    } else if (!is_zero_row(affected_rows) && !is_single_row(affected_rows) && !is_double_row(affected_rows)) {
      LOG_WARN("invalid affected_rows", KR(ret), K(tenant_id), K(affected_rows));
    } else {
      LOG_INFO("[UPGRADE] add sys var", KR(ret), K(tenant_id), K(sys_param));
    }
  }
  return ret;
}

int ObUpgradeUtils::execute_add_sys_var_history_sql(
    common::ObISQLClient& sql_client, const uint64_t tenant_id, const ObSysParam& sys_param)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else {
    int64_t schema_version = OB_INVALID_VERSION;
    {
      SMART_VAR(ObMySQLProxy::MySQLResult, res)
      {
        ObMySQLResult* result = NULL;
        ObSqlString sql;
        if (OB_FAIL(sql.append_fmt("select schema_version from %s where tenant_id = %lu"
                                   " order by schema_version asc limit 1",
                OB_ALL_SYS_VARIABLE_HISTORY_TNAME,
                ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)))) {
          LOG_WARN("fail to append sql", KR(ret), K(tenant_id), K(sql));
        } else if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
          LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
        } else if (NULL == (result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is not expected to be NULL", KR(ret), K(tenant_id), K(sql));
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("fail to get row", KR(ret), K(tenant_id), K(sql));
        } else {
          EXTRACT_INT_FIELD_MYSQL(*result, "schema_version", schema_version, int64_t);
        }
      }
    }
    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      ObDMLSqlSplicer dml;
      ObDMLExecHelper exec(sql_client, tenant_id);
      if (OB_INVALID_VERSION == schema_version) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid schema_version", KR(ret), K(tenant_id), K(sys_param));
      } else if (OB_FAIL(gen_basic_sys_variable_dml(tenant_id, sys_param, dml))) {
        LOG_WARN("fail to gen dml", KR(ret), K(tenant_id), K(sys_param));
      } else if (OB_FAIL(dml.add_pk_column("schema_version", schema_version)) ||
                 OB_FAIL(dml.add_column("value", FORMAT_STR(ObString(sys_param.value_)))) ||
                 OB_FAIL(dml.add_column("is_deleted", 0))) {
        LOG_WARN("fail to add column", KR(ret), K(tenant_id), K(schema_version));
      } else if (OB_FAIL(exec.exec_replace(OB_ALL_SYS_VARIABLE_HISTORY_TNAME, dml, affected_rows))) {
        LOG_WARN("execute insert failed", KR(ret));
      } else if (!is_zero_row(affected_rows) && !is_single_row(affected_rows) && !is_double_row(affected_rows)) {
        LOG_WARN("invalid affected_rows", KR(ret), K(tenant_id), K(affected_rows));
      } else {
        LOG_INFO("[UPGRADE] add sys var history", KR(ret), K(tenant_id), K(sys_param));
      }
    }
  }
  return ret;
}

int ObUpgradeUtils::gen_basic_sys_variable_dml(
    const uint64_t tenant_id, const ObSysParam& sys_param, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id))) ||
             OB_FAIL(dml.add_pk_column("zone", "")) ||
             OB_FAIL(dml.add_pk_column("name", FORMAT_STR(ObString(sys_param.name_)))) ||
             OB_FAIL(dml.add_column("data_type", sys_param.data_type_)) ||
             OB_FAIL(dml.add_column("min_val", FORMAT_STR(ObString(sys_param.min_val_)))) ||
             OB_FAIL(dml.add_column("max_val", FORMAT_STR(ObString(sys_param.max_val_)))) ||
             OB_FAIL(dml.add_column("info", FORMAT_STR(ObString(sys_param.info_)))) ||
             OB_FAIL(dml.add_column("flags", sys_param.flags_))) {
    LOG_WARN("fail to add column", KR(ret), K(tenant_id), K(sys_param));
  }
  return ret;
}
/* =========== upgrade sys variable end =========== */

/* =========== upgrade sys stat =========== */
int ObUpgradeUtils::upgrade_sys_stat(common::ObISQLClient& sql_client, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSysStat sys_stat;
  if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == tenant_id) {
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

int ObUpgradeUtils::filter_sys_stat(common::ObISQLClient& sql_client, const uint64_t tenant_id, ObSysStat& sys_stat)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      ObMySQLResult* result = NULL;
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
          DLIST_FOREACH_REMOVESAFE_X(node, sys_stat.item_list_, OB_SUCC(ret))
          {
            if (OB_NOT_NULL(node)) {
              if (OB_ISNULL(node->name_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("name is null", KR(ret), K(tenant_id));
              } else if (0 == name.compare(node->name_)) {
                // filter sys stat which exist in __all_sys_stat
                ObSysStat::Item* item = sys_stat.item_list_.remove(node);
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
    : inited_(false),
      allocator_("UpgProcSet"),
      processor_list_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator_))
{}

ObUpgradeProcesserSet::~ObUpgradeProcesserSet()
{
  for (int64_t i = 0; i < processor_list_.count(); i++) {
    if (OB_NOT_NULL(processor_list_.at(i))) {
      processor_list_.at(i)->~ObBaseUpgradeProcessor();
    }
  }
}

int ObUpgradeProcesserSet::init(ObBaseUpgradeProcessor::UpgradeMode mode, common::ObMySQLProxy& sql_proxy,
    obrpc::ObSrvRpcProxy& rpc_proxy, share::schema::ObMultiVersionSchemaService& schema_service,
    share::ObCheckStopProvider& check_server_provider)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
#define INIT_PROCESSOR_BY_VERSION(MAJOR, MINOR, PATCH)                                                                 \
  if (OB_SUCC(ret)) {                                                                                                  \
    void* buf = NULL;                                                                                                  \
    ObBaseUpgradeProcessor* processor = NULL;                                                                          \
    int64_t version = static_cast<int64_t>(cal_version((MAJOR), (MINOR), (PATCH)));                                    \
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObUpgradeFor##MAJOR##MINOR##PATCH##Processor)))) {                     \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                                                 \
      LOG_WARN("fail to alloc upgrade processor", KR(ret));                                                            \
    } else if (OB_ISNULL(processor = new (buf) ObUpgradeFor##MAJOR##MINOR##PATCH##Processor)) {                        \
      ret = OB_NOT_INIT;                                                                                               \
      LOG_WARN("fail to new upgrade processor", KR(ret));                                                              \
    } else if (OB_FAIL(processor->init(version, mode, sql_proxy, rpc_proxy, schema_service, check_server_provider))) { \
      LOG_WARN("fail to init processor", KR(ret), K(version));                                                         \
    } else if (OB_FAIL(processor_list_.push_back(processor))) {                                                        \
      LOG_WARN("fail to push back processor", KR(ret), K(version));                                                    \
    }                                                                                                                  \
    if (OB_FAIL(ret)) {                                                                                                \
      if (OB_NOT_NULL(processor)) {                                                                                    \
        processor->~ObBaseUpgradeProcessor();                                                                          \
        allocator_.free(buf);                                                                                          \
        processor = NULL;                                                                                              \
        buf = NULL;                                                                                                    \
      } else if (OB_NOT_NULL(buf)) {                                                                                   \
        allocator_.free(buf);                                                                                          \
        buf = NULL;                                                                                                    \
      }                                                                                                                \
    }                                                                                                                  \
  }
    // order by cluster version asc
    INIT_PROCESSOR_BY_VERSION(3, 1, 1);
    INIT_PROCESSOR_BY_VERSION(3, 1, 2);
    INIT_PROCESSOR_BY_VERSION(3, 1, 3);
    INIT_PROCESSOR_BY_VERSION(3, 1, 4);
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

int ObUpgradeProcesserSet::get_processor_by_idx(const int64_t idx, ObBaseUpgradeProcessor*& processor) const
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
      processor->set_tenant_id(OB_INVALID_ID);  // reset
    }
  }
  return ret;
}

int ObUpgradeProcesserSet::get_processor_by_version(const int64_t version, ObBaseUpgradeProcessor*& processor) const
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
    const int64_t start_version, const int64_t end_version, int64_t& start_idx, int64_t& end_idx)
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

int ObUpgradeProcesserSet::get_processor_idx_by_version(const int64_t version, int64_t& idx) const
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
    : inited_(false),
      cluster_version_(OB_INVALID_VERSION),
      tenant_id_(common::OB_INVALID_ID),
      mode_(UPGRADE_MODE_INVALID),
      sql_proxy_(NULL),
      rpc_proxy_(NULL),
      schema_service_(NULL),
      check_stop_provider_(NULL)
{}

// Standby cluster runs sys tenant's upgrade process only.
int ObBaseUpgradeProcessor::check_inner_stat() const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init yet", KR(ret));
  } else if (cluster_version_ <= 0 || tenant_id_ == OB_INVALID_ID || UPGRADE_MODE_INVALID == mode_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid processor status", KR(ret), K_(cluster_version), K_(tenant_id), K_(mode));
  } else if (GCTX.is_standby_cluster() && OB_SYS_TENANT_ID != tenant_id_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("run upgrade job for non-sys tenant in standby cluster is not supported", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(check_stop_provider_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("check_stop_provider is null", KR(ret));
  } else if (OB_FAIL(check_stop_provider_->check_stop())) {
    LOG_WARN("check stop", KR(ret));
  }
  return ret;
}

int ObBaseUpgradeProcessor::init(int64_t cluster_version, UpgradeMode mode, common::ObMySQLProxy& sql_proxy,
    obrpc::ObSrvRpcProxy& rpc_proxy, share::schema::ObMultiVersionSchemaService& schema_service,
    share::ObCheckStopProvider& check_server_provider)
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
    schema_service_ = &schema_service;
    check_stop_provider_ = &check_server_provider;
    inited_ = true;
  }
  return ret;
}

/* =========== 2270 upgrade processor start ============= */
int ObUpgradeFor2270Processor::pre_upgrade()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(modify_trigger_package_source_body())) {
    LOG_WARN("fail to modify trigger package source body", KR(ret));
  } else if (OB_FAIL(modify_oracle_public_database_name())) {
    LOG_WARN("fail to modify public db name", KR(ret));
  }
  return ret;
}

int ObUpgradeFor2270Processor::post_upgrade()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(rebuild_subpart_partition_gc_info())) {
    LOG_WARN("fail to rebuild subpart partition gc info", KR(ret));
  }
  return ret;
}

int ObUpgradeFor2270Processor::modify_trigger_package_source_body()
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
      LOG_INFO("update __all_tenant_trigger", KR(ret), K(tenant_id), K(affected_rows), K(sql));
    }
    // update __all_tenant_tigger_history
    if (OB_SUCC(ret)) {
      sql.reset();
      affected_rows = 0;
      if (OB_FAIL(sql.assign_fmt(update_all_tenant_trigger_history_str, OB_ALL_TENANT_TRIGGER_HISTORY_TNAME))) {
        LOG_WARN("fail to assign sql", KR(ret));
      } else if (OB_FAIL(sql_proxy_->write(tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("failed to execute", KR(ret), K(affected_rows), K(sql));
      } else {
        LOG_INFO("update __all_tenant_trigger_history", KR(ret), K(tenant_id), K(affected_rows), K(sql));
      }
    }
  }
  LOG_INFO("finish modify_trigger_package_source_body",
      KR(ret),
      K(tenant_id),
      "cost",
      ObTimeUtility::current_time() - start_ts);
  return ret;
}

int ObUpgradeFor2270Processor::modify_oracle_public_database_name()
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
    uint64_t db_id_in_sql =
        ObSchemaUtils::get_extract_schema_id(exec_tenant_id, combine_id(exec_tenant_id, OB_PUBLIC_SCHEMA_ID));
    const static char* upd_all_database_sql =
        "update %s set database_name = 'PUBLIC' where tenant_id = %ld and database_id = %ld;";
    const static char* upd_all_database_history_sql =
        "update %s set database_name = 'PUBLIC' where tenant_id = %ld and \
       database_id = %ld and database_name = '__public';";
    ObWorker::CompatMode compat_mode = ObWorker::CompatMode::INVALID;
    if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(exec_tenant_id, compat_mode))) {
      LOG_WARN("fail to get tenant mode", K(ret), K(exec_tenant_id));
    }
    if (OB_SUCC(ret) && ObWorker::CompatMode::ORACLE == compat_mode) {
      // update __all_database
      if (OB_FAIL(sql.assign_fmt(upd_all_database_sql, OB_ALL_DATABASE_TNAME, tenant_id_in_sql, db_id_in_sql))) {
        LOG_WARN("fail to assign sql", K(ret));
      } else if (OB_FAIL(sql_proxy_->write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("failed to execute", K(ret), K(affected_rows), K(sql));
      } else {
        LOG_INFO("[UPGRADE] update __all_database", KR(ret), K(exec_tenant_id), K(affected_rows), K(sql));
      }
      // update __all_database_history
      if (OB_SUCC(ret)) {
        sql.reset();
        affected_rows = 0;
        if (OB_FAIL(sql.assign_fmt(
                upd_all_database_history_sql, OB_ALL_DATABASE_HISTORY_TNAME, tenant_id_in_sql, db_id_in_sql))) {
          LOG_WARN("fail to assign sql", K(ret));
        } else if (OB_FAIL(sql_proxy_->write(exec_tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("failed to execute", K(ret), K(affected_rows), K(sql));
        } else {
          LOG_INFO("[UPGRADE] update __all_database_history", KR(ret), K(exec_tenant_id), K(affected_rows), K(sql));
        }
      }
    }
  }
  LOG_INFO("[UPGRADE] finish modify_oracle_public_database_name",
      KR(ret),
      K(tenant_id),
      "cost",
      ObTimeUtility::current_time() - start_ts);
  return ret;
}

int ObUpgradeFor2270Processor::rebuild_subpart_partition_gc_info()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = get_tenant_id();
  ObSchemaGetterGuard guard;
  int64_t start_ts = ObTimeUtility::current_time();
  LOG_INFO("start rebuild_subpart_partition_gc_info", K(tenant_id));
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_full_schema_guard(tenant_id, guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else {
    ObArray<ObPartitionKey> keys;
    ObPartitionKey key;
    const int64_t BATCH_REPLACE_CNT = 100;
    {  // iter table
      ObArray<const ObSimpleTableSchemaV2*> tables;
      if (OB_FAIL(guard.get_table_schemas_in_tenant(tenant_id, tables))) {
        LOG_WARN("fail to get table schemas", KR(ret), K(tenant_id));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); i++) {
        const ObSimpleTableSchemaV2* table = tables.at(i);
        if (OB_FAIL(check_inner_stat())) {
          LOG_WARN("fail to check inner stat", KR(ret));
        } else if (OB_ISNULL(table)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table is null", KR(ret), K(tenant_id));
        } else if (is_inner_table(table->get_table_id()) || PARTITION_LEVEL_TWO != table->get_part_level() ||
                   !table->has_self_partition()) {
          // skip
        } else {
          ObPartitionKeyIter iter(table->get_table_id(), *table, true /*check_dropped_schema*/);
          while (OB_SUCC(ret) && OB_SUCC(iter.next_partition_key_v2(key))) {
            if (OB_FAIL(keys.push_back(key))) {
              LOG_WARN("fail to push back key", KR(ret), K(key));
            } else if (keys.count() < BATCH_REPLACE_CNT) {
              // skip
            } else if (OB_FAIL(batch_replace_partition_gc_info(keys))) {
              LOG_WARN("fail to batch replace partition gc info", KR(ret), K(tenant_id));
            } else {
              keys.reset();
            }
          }  // end while
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
            LOG_WARN("iter failed", KR(ret), K(tenant_id));
          }
        }
      }  // iter table end
    }
    if (OB_SUCC(ret)) {  // iter tablegroup
      ObArray<const ObTablegroupSchema*> tablegroups;
      if (OB_FAIL(guard.get_tablegroup_schemas_in_tenant(tenant_id, tablegroups))) {
        LOG_WARN("fail to get tablegroups schemas", KR(ret), K(tenant_id));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < tablegroups.count(); i++) {
        const ObTablegroupSchema* tablegroup = tablegroups.at(i);
        if (OB_FAIL(check_inner_stat())) {
          LOG_WARN("fail to check inner stat", KR(ret));
        } else if (OB_ISNULL(tablegroup)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablrgroup is null", KR(ret), K(tenant_id));
        } else if (PARTITION_LEVEL_TWO != tablegroup->get_part_level() || !tablegroup->has_self_partition()) {
          // skip
        } else {
          ObPartitionKeyIter iter(tablegroup->get_tablegroup_id(), *tablegroup, true /*check_dropped_schema*/);
          while (OB_SUCC(ret) && OB_SUCC(iter.next_partition_key_v2(key))) {
            if (OB_FAIL(keys.push_back(key))) {
              LOG_WARN("fail to push back key", KR(ret), K(key));
            } else if (keys.count() < BATCH_REPLACE_CNT) {
              // skip
            } else if (OB_FAIL(batch_replace_partition_gc_info(keys))) {
              LOG_WARN("fail to batch replace partition gc info", KR(ret), K(tenant_id));
            } else {
              keys.reset();
            }
          }  // end while
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
            LOG_WARN("iter failed", KR(ret), K(tenant_id));
          }
        }
      }  // iter tablegroup end
    }
    if (OB_SUCC(ret) && keys.count() > 0) {  // remain
      if (OB_FAIL(batch_replace_partition_gc_info(keys))) {
        LOG_WARN("fail to batch replace partition gc info", KR(ret), K(tenant_id));
      }
    }
  }
  LOG_INFO("finish rebuild_subpart_partition_gc_info",
      KR(ret),
      K(tenant_id),
      "cost",
      ObTimeUtility::current_time() - start_ts);
  return ret;
}

int ObUpgradeFor2270Processor::batch_replace_partition_gc_info(const common::ObIArray<ObPartitionKey>& keys)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = get_tenant_id();
  ObSchemaGetterGuard guard;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (keys.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("keys cnt should be greator than zero", KR(ret), K(tenant_id), "cnt", keys.count());
  } else {
    ObDMLSqlSplicer dml;
    for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); i++) {
      const ObPartitionKey& key = keys.at(i);
      uint64_t tid = ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
      uint64_t tbid = ObSchemaUtils::get_extract_schema_id(tenant_id, key.get_table_id());
      int64_t pid = key.get_partition_id();
      if (OB_FAIL(check_inner_stat())) {
        LOG_WARN("fail to check inner stat", KR(ret));
      } else if (OB_FAIL(dml.add_pk_column("tenant_id", tid)) || OB_FAIL(dml.add_pk_column("table_id", tbid)) ||
                 OB_FAIL(dml.add_pk_column("partition_id", pid))) {
        LOG_WARN("fail to add row", KR(ret), K(key));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("fail to finish row", KR(ret), K(tenant_id));
      }
    }  // end for
    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      ObSqlString sql;
      if (OB_FAIL(check_inner_stat())) {
        LOG_WARN("fail to check inner stat", KR(ret), K(tenant_id));
      } else if (OB_FAIL(dml.splice_batch_replace_sql_without_plancache(OB_ALL_TENANT_GC_PARTITION_INFO_TNAME, sql))) {
        LOG_WARN("fail to gen sql", KR(ret), K(tenant_id));
      } else if (OB_FAIL(sql_proxy_->write(tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(sql));
      } else {
        // no need to check affected_rows
        LOG_INFO("replace partition gc info", KR(ret), K(tenant_id), K(sql));
      }
    }
  }
  return ret;
}
/* =========== 2270 upgrade processor end ============= */

int ObUpgradeFor2275Processor::post_upgrade()
{
  ObSchemaGetterGuard guard;
  ObWorker::CompatMode compat_mode = ObWorker::CompatMode::INVALID;

  int ret = OB_SUCCESS;
  const uint64_t tenant_id = get_tenant_id();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_full_schema_guard(tenant_id, guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
    LOG_WARN("fail to get tenant mode", K(ret), K(tenant_id));
  } else if (compat_mode == ObWorker::CompatMode::ORACLE) {
    ObSqlString sql;
    int64_t affected_rows = 0;

    OZ(sql.assign_fmt("grant create table, create type, create trigger, "
                      "create procedure, create sequence to resource"));
    CK(sql_proxy_ != NULL);
    OZ(sql_proxy_->write(tenant_id, sql.ptr(), affected_rows, ORACLE_MODE));
  }
  return ret;
}

/* =========== upgrade processor end ============= */
}  // namespace share
}  // namespace oceanbase
