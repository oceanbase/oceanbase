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
#include "share/ob_service_epoch_proxy.h"
#include "observer/ob_server_struct.h"
#include "rootserver/ob_root_service.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "share/ob_rpc_struct.h"
#include "share/ls/ob_ls_status_operator.h"//get max ls id
#include "share/ob_tenant_info_proxy.h"//update max ls id
#include "share/balance/ob_scheduled_trigger_partition_balance.h" // ObScheduledTriggerPartitionBalance
#include "ob_upgrade_utils.h"
#include "share/stat/ob_dbms_stats_maintenance_window.h"
#include "share/stat/ob_dbms_stats_preferences.h"
#include "share/config/ob_config_helper.h"
#include "share/ncomp_dll/ob_flush_ncomp_dll_task.h"
#include "logservice/data_dictionary/ob_data_dict_scheduler.h"    // ObDataDictScheduler

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

void ObUpgradePath::reset()
{
  upgrade_versions_.reset();
  update_current_version_.reset();
}

bool ObUpgradePath::is_valid() const
{
  bool valid = true;
  if (upgrade_versions_.count() != update_current_version_.count()) {
    valid = false;
  }
  return valid;
}

int64_t ObUpgradePath::count() const
{
  return upgrade_versions_.count();
}

int ObUpgradePath::add_version(const uint64_t version, const bool update)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(upgrade_versions_.push_back(version)) || OB_FAIL(update_current_version_.push_back(update))) {
    LOG_WARN("failed to push_back", KR(ret), K(update_current_version_), K(upgrade_versions_),
        KDV(version), K(update));
  }
  return ret;
}

int ObUpgradePath::get_version(const int64_t idx, uint64_t &version, bool &update) const
{
  int ret = OB_SUCCESS;
  if (idx >= upgrade_versions_.count() || idx >= update_current_version_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("index out of range", KR(ret), K(idx), K(upgrade_versions_), K(update_current_version_));
  } else {
    version = upgrade_versions_[idx];
    update = update_current_version_[idx];
  }
  return ret;
}
// UPGRADE_PATH_CURRENT 表示当前版本序列的升级路径(42x)，所有版本从小到大排列
// UPGRADE_PATH_LAST 表示上一个版本序列的升级路径(421)，每一项由二维数组表示，二维数组分别是 {version, next_upgrade_version}
//  version表示当前版本
//  next_upgrade_version表示UPGRADE_PATH_CURRENT中第一个可以由当前版本升级的版本
//  next_upgrade_version可以为OB_INVALID_VERSION，但是UPGRADE_PATH_LAST最后一项中的next_upgrade_version必须是UPGRADE_PATH_CURRENT中的合法版本号
// 样例:
//  UPGRADE_PATH_LAST[][2]={
//    {a1, b2},
//    {a2, OB_INVALID_VERSION},
//    {a3, b3},
//  };
// UPGRADE_PATH_CURRENT={
//  b1,
//  b2,
//  b3,
//  b4,
//  b5,
// };
// 上述例子表示现在由两个版本升级序列，分别是 [a1, a2, a3]和[b1, b2, b3]，版本升级序列内部的低版本可以升级到高版本
// 版本a1可以升级到版本b2，版本a3可以升级到版本b3，但是都会先将版本号推至a3，然后直接推到b3，然后推到b4、b5
//
// 添加新版本号：
// 1. UPGRADE_PATH_CURRENT添加新版本号b6：直接在UPGRADE_PATH_CURRENT尾部添加
// 2. UPGRADE_PATH_LAST添加新版本号a4：
//  a. a4可以升级到UPGRADE_PATH_CURRENT中的版本b6：UPGRADE_PATH_LAST中添加 {a4, b6}
//  b. a4无法升级到UPGRADE_PATH_CURRENT中的版本：不添加，等支持升级到UPGRADE_PATH_CURRENT中的版本出现后再添加{a4, OB_INVALID_VERSION}

const uint64_t ObUpgradeChecker::UPGRADE_PATH_LAST[][2] = {
  { CALC_VERSION(4UL, 2UL, 1UL, 10UL), CALC_VERSION(4UL, 2UL, 5UL, 1UL) },  // the next version of 4.2.1.10 is 4.2.5.1
  { CALC_VERSION(4UL, 2UL, 1UL, 11UL), CALC_VERSION(4UL, 2UL, 5UL, 3UL) },  // the next version of 4.2.1.11 is 4.2.5.3
  // !!! add new 421 versions here
};
const uint64_t ObUpgradeChecker::UPGRADE_PATH_CURRENT[] = {
  CALC_VERSION(4UL, 2UL, 2UL, 0UL),  // 4.2.2.0
  CALC_VERSION(4UL, 2UL, 2UL, 1UL),  // 4.2.2.1
  CALC_VERSION(4UL, 2UL, 3UL, 0UL),  // 4.2.3.0
  CALC_VERSION(4UL, 2UL, 3UL, 1UL),  // 4.2.3.1
  CALC_VERSION(4UL, 2UL, 4UL, 0UL),  // 4.2.4.0
  CALC_VERSION(4UL, 2UL, 5UL, 0UL),  // 4.2.5.0
  CALC_VERSION(4UL, 2UL, 5UL, 1UL),  // 4.2.5.1
  CALC_VERSION(4UL, 2UL, 5UL, 2UL),  // 4.2.5.2
  CALC_VERSION(4UL, 2UL, 5UL, 3UL),  // 4.2.5.3
  CALC_VERSION(4UL, 2UL, 5UL, 4UL),  // 4.2.5.4
  CALC_VERSION(4UL, 2UL, 5UL, 5UL),  // 4.2.5.5
  CALC_VERSION(4UL, 2UL, 5UL, 6UL),  // 4.2.5.6
  CALC_VERSION(4UL, 2UL, 5UL, 7UL),  // 4.2.5.7
  CALC_VERSION(4UL, 2UL, 5UL, 8UL),  // 4.2.5.8
  // add 425 versions here
};

int ObUpgradeChecker::get_data_version_by_cluster_version(
    const uint64_t cluster_version,
    uint64_t &data_version)
{
  int ret = OB_SUCCESS;
  switch (cluster_version) {
#define CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(CLUSTER_VERSION, DATA_VERSION) \
    case CLUSTER_VERSION : { \
      data_version = DATA_VERSION; \
      break; \
    }
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(CLUSTER_VERSION_4_0_0_0, DATA_VERSION_4_0_0_0)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(CLUSTER_VERSION_4_1_0_0, DATA_VERSION_4_1_0_0)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(CLUSTER_VERSION_4_1_0_1, DATA_VERSION_4_1_0_1)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(CLUSTER_VERSION_4_1_0_2, DATA_VERSION_4_1_0_2)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(CLUSTER_VERSION_4_2_0_0, DATA_VERSION_4_2_0_0)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(CLUSTER_VERSION_4_2_1_0, DATA_VERSION_4_2_1_0)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(CLUSTER_VERSION_4_2_1_1, DATA_VERSION_4_2_1_1)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(CLUSTER_VERSION_4_2_1_2, DATA_VERSION_4_2_1_2)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(MOCK_CLUSTER_VERSION_4_2_1_3, MOCK_DATA_VERSION_4_2_1_3)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(MOCK_CLUSTER_VERSION_4_2_1_4, MOCK_DATA_VERSION_4_2_1_4)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(MOCK_CLUSTER_VERSION_4_2_1_5, MOCK_DATA_VERSION_4_2_1_5)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(MOCK_CLUSTER_VERSION_4_2_1_6, MOCK_DATA_VERSION_4_2_1_6)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(MOCK_CLUSTER_VERSION_4_2_1_7, MOCK_DATA_VERSION_4_2_1_7)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(MOCK_CLUSTER_VERSION_4_2_1_8, MOCK_DATA_VERSION_4_2_1_8)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(MOCK_CLUSTER_VERSION_4_2_1_9, MOCK_DATA_VERSION_4_2_1_9)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(MOCK_CLUSTER_VERSION_4_2_1_10, MOCK_DATA_VERSION_4_2_1_10)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(MOCK_CLUSTER_VERSION_4_2_1_11, MOCK_DATA_VERSION_4_2_1_11)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(CLUSTER_VERSION_4_2_2_0, DATA_VERSION_4_2_2_0)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(CLUSTER_VERSION_4_2_2_1, DATA_VERSION_4_2_2_1)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(CLUSTER_VERSION_4_2_3_0, DATA_VERSION_4_2_3_0)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(CLUSTER_VERSION_4_2_3_1, DATA_VERSION_4_2_3_1)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(CLUSTER_VERSION_4_2_4_0, DATA_VERSION_4_2_4_0)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(CLUSTER_VERSION_4_2_5_0, DATA_VERSION_4_2_5_0)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(CLUSTER_VERSION_4_2_5_1, DATA_VERSION_4_2_5_1)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(CLUSTER_VERSION_4_2_5_2, DATA_VERSION_4_2_5_2)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(CLUSTER_VERSION_4_2_5_3, DATA_VERSION_4_2_5_3)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(CLUSTER_VERSION_4_2_5_4, DATA_VERSION_4_2_5_4)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(CLUSTER_VERSION_4_2_5_5, DATA_VERSION_4_2_5_5)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(CLUSTER_VERSION_4_2_5_6, DATA_VERSION_4_2_5_6)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(CLUSTER_VERSION_4_2_5_7, DATA_VERSION_4_2_5_7)
    CONVERT_CLUSTER_VERSION_TO_DATA_VERSION(CLUSTER_VERSION_4_2_5_8, DATA_VERSION_4_2_5_8)
#undef CONVERT_CLUSTER_VERSION_TO_DATA_VERSION
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid cluster_version", KR(ret), KCV(cluster_version));
    }
  }
  return ret;
}

bool ObUpgradeChecker::check_data_version_exist(
     const uint64_t version)
{
  return check_in_last_version_list_(version) || check_in_current_version_list_(version);
}

bool ObUpgradeChecker::check_in_last_version_list_(const uint64_t version)
{
  bool bret = false;
  for (int64_t i = 0; !bret && i < ARRAYSIZEOF(UPGRADE_PATH_LAST); i++) {
    bret = (version == UPGRADE_PATH_LAST[i][0]);
  }
  return bret;
}

bool ObUpgradeChecker::check_in_current_version_list_(const uint64_t version)
{
  bool bret = false;
  for (int64_t i = 0; !bret && i < ARRAYSIZEOF(UPGRADE_PATH_CURRENT); i++) {
    bret = (version == UPGRADE_PATH_CURRENT[i]);
  }
  return bret;
}

int64_t ObUpgradeChecker::get_upgrade_path_last_size_()
{
  return ARRAYSIZEOF(UPGRADE_PATH_LAST);
}

int64_t ObUpgradeChecker::get_upgrade_path_current_size_()
{
  return ARRAYSIZEOF(UPGRADE_PATH_CURRENT);
}

int ObUpgradeChecker::get_upgrade_path(const uint64_t version, ObUpgradePath &path)
{
  int ret = OB_SUCCESS;
  path.reset();
  if (check_in_last_version_list_(version)) {
    const uint64_t next_upgrade_version = UPGRADE_PATH_LAST[ARRAYSIZEOF(UPGRADE_PATH_LAST) - 1][1];
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(UPGRADE_PATH_LAST); i++) {
      const uint64_t data_version = UPGRADE_PATH_LAST[i][0];
      if (data_version > version) {
        if (OB_FAIL(path.add_version(data_version, true /* update_current_data_version */))) {
          LOG_WARN("failed to add version", KR(ret), KDV(data_version));
        }
      }
    }
    if (FAILEDx(add_upgrade_versions_(version, UPGRADE_PATH_CURRENT, ARRAYSIZEOF(UPGRADE_PATH_CURRENT),
            false /*force_update_current_version*/, next_upgrade_version, path))) {
      LOG_WARN("failed to add current lts versions to path", KR(ret));
    }
  } else if (check_in_current_version_list_(version)) {
    if (OB_FAIL(add_upgrade_versions_(version, UPGRADE_PATH_CURRENT, ARRAYSIZEOF(UPGRADE_PATH_CURRENT),
            true /*force_update_current_version*/, 0 /*next_upgrade_version*/, path))) {
      LOG_WARN("failed to add current lts versions to path", KR(ret));
    }
  } else {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("data version not exists in upgrade path", KR(ret), KDV(version));
  }
  return ret;
}

int ObUpgradeChecker::add_upgrade_versions_(const uint64_t current_version,
    const uint64_t *upgrade_path,
    const int64_t data_version_num,
    const bool force_update_current_version,
    const uint64_t next_upgrade_version,
    ObUpgradePath &path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(upgrade_path)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is NULL", KR(ret), KP(upgrade_path));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < data_version_num; i++) {
    const uint64_t data_version = upgrade_path[i];
    if (data_version > current_version) {
      bool update = (force_update_current_version || data_version >= next_upgrade_version);
      if (OB_FAIL(path.add_version(data_version, update))) {
        LOG_WARN("failed to add version", KR(ret), KDV(data_version), K(update));
      }
    }
  }
  return ret;
}

// the version allow to backup to current version should be the same as the version can upgrade to current version
bool ObUpgradeChecker::check_data_version_valid_for_backup(const uint64_t data_version)
{
  return check_data_version_exist(data_version);
}

//FIXME:(yanmu.ztl) cluster version should be discrete.
bool ObUpgradeChecker::check_cluster_version_exist(
     const uint64_t version)
{
  return version >= CLUSTER_VERSION_4_0_0_0
         && version <= CLUSTER_CURRENT_VERSION;
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
    } else if (OB_FAIL(sql.assign_fmt("SELECT floor(count(*)) as count FROM %s WHERE job_type = '%s'",
                              OB_ALL_ROOTSERVICE_JOB_TNAME, ObRsJobTableOperator::get_job_type_str(job_type)))) {
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
    } else if (OB_FAIL(sql.assign_fmt("SELECT floor(count(*)) as count FROM %s "
                              "WHERE job_type = '%s' and job_status = 'SUCCESS'",
                              OB_ALL_ROOTSERVICE_JOB_TNAME, ObRsJobTableOperator::get_job_type_str(job_type)))) {
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

// tenant_id == OB_INVALID_TENANT_ID: check all tenants' schema statuses
int ObUpgradeUtils::check_schema_sync(
    const uint64_t tenant_id,
    bool &is_sync)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
    int32_t count = OB_INVALID_COUNT;
    is_sync = false;
    if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql_proxy is null", KR(ret));
    } else if (OB_FAIL(sql.assign_fmt(
               "SELECT floor(count(*)) as count FROM %s AS a "
               "JOIN %s AS b ON a.tenant_id = b.tenant_id "
               "WHERE (a.refreshed_schema_version != b.refreshed_schema_version "
               "       OR (a.refreshed_schema_version mod %ld) != 0) ",
               OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_TNAME,
               OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_TNAME,
               schema::ObSchemaVersionGenerator::SCHEMA_VERSION_INC_STEP))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else if (OB_INVALID_TENANT_ID != tenant_id
               && OB_FAIL(sql.append_fmt(" AND a.tenant_id = %ld", tenant_id))) {
      LOG_WARN("fail to append sql", KR(ret), K(tenant_id));
    } else if (OB_FAIL(GCTX.sql_proxy_->read(res, sql.ptr()))) {
      LOG_WARN("fail to execute sql", KR(ret), K(sql));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get sql result", K(ret));
    } else if (OB_FAIL((result->next()))) {
      LOG_WARN("fail to get result", KR(ret));
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "count", count, int32_t);
      if (OB_FAIL(ret)) {
      } else if (count < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid count", KR(ret), K(count));
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
  } else if (OB_FAIL(batch_update_sys_var_(rpc_proxy, tenant_id, true, update_list))) {
    LOG_WARN("fail to update sys var", KR(ret), K(tenant_id));
  } else if (OB_FAIL(batch_update_sys_var_(rpc_proxy, tenant_id, false, add_list))) {
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

int ObUpgradeUtils::get_sys_param_(const uint64_t &tenant_id, const int64_t &var_store_idx,
    share::schema::ObSysVarSchema &sys_var)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (var_store_idx < 0 || var_store_idx >= ObSysVariables::get_all_sys_var_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("var_store_idx out of range", KR(ret), K(var_store_idx),
        "count", ObSysVariables::get_all_sys_var_count());
  } else {
    ObSysParam sys_param;
    const ObString &name = ObSysVariables::get_name(var_store_idx);
    const ObObjType &type = ObSysVariables::get_type(var_store_idx);
    const ObString &value = ObSysVariables::get_value(var_store_idx);
    const ObString &min = ObSysVariables::get_min(var_store_idx);
    const ObString &max = ObSysVariables::get_max(var_store_idx);
    const ObString &info = ObSysVariables::get_info(var_store_idx);
    const int64_t flag = ObSysVariables::get_flags(var_store_idx);
    const ObString zone("");
    sys_var.set_tenant_id(tenant_id);
    if (OB_FAIL(sys_param.init(tenant_id, zone, name.ptr(), type,
            value.ptr(), min.ptr(), max.ptr(), info.ptr(), flag))) {
      LOG_WARN("sys_param init failed", KR(ret), K(tenant_id), K(name),
          K(type), K(value), K(min), K(max), K(info), K(flag));
    } else if (!sys_param.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("sys param is invalid", KR(ret), K(tenant_id), K(sys_param));
    } else if (OB_FAIL(ObSchemaUtils::convert_sys_param_to_sysvar_schema(sys_param, sys_var))) {
      LOG_WARN("convert sys param to sysvar schema failed", KR(ret));
    }
  }
  return ret;
}

int ObUpgradeUtils::batch_update_sys_var_(
    obrpc::ObCommonRpcProxy &rpc_proxy,
    const uint64_t tenant_id,
    const bool is_update,
    common::ObArray<int64_t> &update_list)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else {
    int64_t start_ts = ObTimeUtility::current_time();
    const int64_t timeout = GCONF._ob_ddl_timeout;
    ObArray<share::schema::ObSysVarSchema> sysvars;
    ObAddSysVarArg args;
    bool if_not_exist = true; // not used
    if (OB_FAIL(sysvars.prepare_allocate(update_list.count()))) {
      LOG_WARN("failed to prepare_allocate sysvars", KR(ret), "count", update_list.count());
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < update_list.count(); i++) {
        int64_t var_store_idx = update_list.at(i);
        if (OB_FAIL(get_sys_param_(tenant_id, var_store_idx, sysvars.at(i)))) {
          LOG_WARN("failed to get sys param", KR(ret), K(tenant_id), K(var_store_idx));
        }
      }
    }
    if (FAILEDx(args.init(is_update, if_not_exist, tenant_id, sysvars))) {
      LOG_WARN("failed to init args", KR(ret), K(sysvars), K(if_not_exist), K(is_update),
          K(tenant_id));
    } else if (OB_FAIL(rpc_proxy.timeout(timeout).add_system_variable(args))) {
      LOG_WARN("add system variable failed", KR(ret), K(timeout), K(args));
    }
    LOG_INFO("[UPGRADE] finish batch upgrade system variables",
             KR(ret), K(tenant_id), K(args), "cost", ObTimeUtility::current_time() - start_ts);
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
    ObAddSysVarArg arg;
    for (int64_t i = 0; OB_SUCC(ret) && i < update_list.count(); i++) {
      int64_t start_ts = ObTimeUtility::current_time();
      int64_t var_store_idx = update_list.at(i);
      ObSysVarSchema sysvar;
      if (OB_FAIL(get_sys_param_(tenant_id, var_store_idx, sysvar))) {
        LOG_WARN("failed to get sys param", KR(ret), K(var_store_idx));
      } else if (OB_FAIL(arg.init(is_update, true /* if_not_exist */, tenant_id, sysvar))) {
      } else if (OB_FAIL(rpc_proxy.timeout(timeout).add_system_variable(arg))) {
        LOG_WARN("add system variable failed", KR(ret), K(timeout), K(arg));
      }
      LOG_INFO("[UPGRADE] finish upgrade system variable",
               KR(ret), K(tenant_id), K(sysvar), "cost", ObTimeUtility::current_time() - start_ts);
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
  : inited_(false), allocator_(ObMemAttr(MTL_CTX() ? MTL_ID() : OB_SERVER_TENANT_ID,
                                         "UpgProcSet")),
    processor_list_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator_)),
    all_version_upgrade_processor_(NULL)
{
}

ObUpgradeProcesserSet::~ObUpgradeProcesserSet()
{
  for (int64_t i = 0; i < processor_list_.count(); i++) {
    if (OB_NOT_NULL(processor_list_.at(i))) {
      processor_list_.at(i)->~ObBaseUpgradeProcessor();
    }
  }
  if (OB_NOT_NULL(all_version_upgrade_processor_)) {
    all_version_upgrade_processor_->~ObBaseUpgradeProcessor();
  }
}

int ObUpgradeProcesserSet::init(
    ObBaseUpgradeProcessor::UpgradeMode mode,
    common::ObMySQLProxy &sql_proxy,
    common::ObOracleSqlProxy &oracle_sql_proxy,
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
#define INIT_PROCESSOR_BY_NAME_AND_VERSION(PROCESSOR_NAME, VERSION, processor) \
    if (OB_SUCC(ret)) { \
      void *buf = NULL; \
      int64_t version = VERSION; \
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(PROCESSOR_NAME)))) { \
        ret = OB_ALLOCATE_MEMORY_FAILED; \
        LOG_WARN("fail to alloc upgrade processor", KR(ret)); \
      } else if (OB_ISNULL(processor = new(buf)PROCESSOR_NAME)) { \
        ret = OB_NOT_INIT; \
        LOG_WARN("fail to new upgrade processor", KR(ret)); \
      } else if (OB_FAIL(processor->init(version, mode, sql_proxy, oracle_sql_proxy, rpc_proxy, common_proxy, \
                                         schema_service, check_server_provider))) { \
        LOG_WARN("fail to init processor", KR(ret), KDV(version)); \
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

#define INIT_PROCESSOR_BY_VERSION(MAJOR, MINOR, MAJOR_PATCH, MINOR_PATCH) \
    if (OB_SUCC(ret)) { \
      ObBaseUpgradeProcessor *processor = NULL; \
      int64_t data_version = cal_version(MAJOR, MINOR, MAJOR_PATCH, MINOR_PATCH); \
      INIT_PROCESSOR_BY_NAME_AND_VERSION(ObUpgradeFor##MAJOR##MINOR##MAJOR_PATCH##MINOR_PATCH##Processor,  \
          data_version, processor); \
      if (FAILEDx(processor_list_.push_back(processor))) { \
        LOG_WARN("fail to push back processor", KR(ret), KDV(data_version)); \
      } \
    }

    INIT_PROCESSOR_BY_NAME_AND_VERSION(ObUpgradeForAllVersionProcessor, DATA_CURRENT_VERSION,
        all_version_upgrade_processor_);

    // order by data version asc
    INIT_PROCESSOR_BY_VERSION(4, 2, 1, 10);
    INIT_PROCESSOR_BY_VERSION(4, 2, 1, 11);
    INIT_PROCESSOR_BY_VERSION(4, 2, 2, 0);
    INIT_PROCESSOR_BY_VERSION(4, 2, 2, 1);
    INIT_PROCESSOR_BY_VERSION(4, 2, 3, 0);
    INIT_PROCESSOR_BY_VERSION(4, 2, 3, 1);
    INIT_PROCESSOR_BY_VERSION(4, 2, 4, 0);
    INIT_PROCESSOR_BY_VERSION(4, 2, 5, 0);
    INIT_PROCESSOR_BY_VERSION(4, 2, 5, 1);
    INIT_PROCESSOR_BY_VERSION(4, 2, 5, 2);
    INIT_PROCESSOR_BY_VERSION(4, 2, 5, 3);
    INIT_PROCESSOR_BY_VERSION(4, 2, 5, 4);
    INIT_PROCESSOR_BY_VERSION(4, 2, 5, 5);
    INIT_PROCESSOR_BY_VERSION(4, 2, 5, 6);
    INIT_PROCESSOR_BY_VERSION(4, 2, 5, 7);
    INIT_PROCESSOR_BY_VERSION(4, 2, 5, 8);

#undef INIT_PROCESSOR_BY_NAME_AND_VERSION
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
    LOG_WARN("fail to get processor idx by version", KR(ret), KDV(version));
  } else if (OB_FAIL(get_processor_by_idx(idx, processor))) {
    LOG_WARN("fail to get processor by idx", KR(ret), KDV(version));
  }
  return ret;
}

int ObUpgradeProcesserSet::get_all_version_processor(ObBaseUpgradeProcessor *&processor) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_ISNULL(all_version_upgrade_processor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("processor is NULL", KR(ret), KP(all_version_upgrade_processor_));
  } else {
    processor = all_version_upgrade_processor_;
    processor->set_tenant_id(OB_INVALID_ID); // reset
  }
  return ret;
}

// start_idx --> the processor of start_version in processor_set array
// end_idx --> the processor of end_version in processor_set array
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
    LOG_WARN("invalid version", KR(ret), KDV(start_version), KDV(end_version));
  } else if (OB_FAIL(get_processor_idx_by_version(start_version, start_idx))) {
    LOG_WARN("fail to get processor idx by version", KR(ret), KDV(start_version));
  } else if (OB_FAIL(get_processor_idx_by_version(end_version, end_idx))) {
    LOG_WARN("fail to get processor idx by version", KR(ret), KDV(end_version));
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
    LOG_WARN("invalid version", KR(ret), KDV(version));
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
      LOG_WARN("fail to find processor by version", KR(ret), KDV(version));
    }
  }
  return ret;
}

ObBaseUpgradeProcessor::ObBaseUpgradeProcessor()
  : inited_(false), data_version_(OB_INVALID_VERSION),
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
  } else if (data_version_ <= 0
             || tenant_id_ == OB_INVALID_ID
             || UPGRADE_MODE_INVALID == mode_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid processor status",
             KR(ret), KDV(data_version_), K_(tenant_id), K_(mode));
  } else if (OB_ISNULL(check_stop_provider_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("check_stop_provider is null", KR(ret));
  } else if (OB_FAIL(check_stop_provider_->check_stop())) {
    LOG_WARN("check stop", KR(ret));
  }
  return ret;
}

int ObBaseUpgradeProcessor::init(
    int64_t data_version,
    UpgradeMode mode,
    common::ObMySQLProxy &sql_proxy,
    common::ObOracleSqlProxy &oracle_sql_proxy,
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
    data_version_ = data_version;
    sql_proxy_ = &sql_proxy;
    oracle_sql_proxy_ = &oracle_sql_proxy;
    rpc_proxy_ = &rpc_proxy;
    common_proxy_ = &common_proxy;
    schema_service_ = &schema_service;
    check_stop_provider_ = &check_server_provider;
    inited_ = true;
  }
  return ret;
}

#undef FORMAT_STR

/* =========== special upgrade processor start ============= */
int ObUpgradeForAllVersionProcessor::post_upgrade()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(flush_ncomp_dll_job())) {
    LOG_WARN("fail to flush ncomp dll job", KR(ret));
  } else if (OB_FAIL(replace_unit_group_id_with_unit_list_())) {
    LOG_WARN("failed to replace unit group id with unit list", KR(ret));
  }

  return ret;
}

int ObUpgradeForAllVersionProcessor::flush_ncomp_dll_job()
{
  int ret = OB_SUCCESS;

  bool is_primary_tenant= false;
  ObSchemaGetterGuard schema_guard;
  const ObSysVariableSchema *sys_variable_schema = NULL;
  if (OB_ISNULL(sql_proxy_) || OB_ISNULL(schema_service_) || !is_valid_tenant_id(tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), KP(sql_proxy_), KP(schema_service_), K(tenant_id_));
  } else if (!is_user_tenant(tenant_id_)) {
    LOG_INFO("not user tenant, ignore", K(tenant_id_));
  } else if (OB_FAIL(ObAllTenantInfoProxy::is_primary_tenant(sql_proxy_, tenant_id_, is_primary_tenant))) {
    LOG_WARN("check is standby tenant failed", KR(ret), K(tenant_id_));
  } else if (!is_primary_tenant) {
    LOG_INFO("not primary tenant, ignore", K(tenant_id_));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_sys_variable_schema(tenant_id_, sys_variable_schema))) {
    LOG_WARN("get sys variable schema failed", KR(ret), K(tenant_id_));
  } else if (OB_ISNULL(sys_variable_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys variable schema is null", KR(ret));
  } else {
    START_TRANSACTION(sql_proxy_, tenant_id_);
    if (FAILEDx(ObFlushNcompDll::create_flush_ncomp_dll_job(
        *sys_variable_schema,
        tenant_id_,
        true/*is_enabled*/,
        trans))) { // insert ignore
      LOG_WARN("create flush ncomp dll job failed", KR(ret), K(tenant_id_));
    }
    END_TRANSACTION(trans);
    LOG_INFO("post upgrade for create flush ncomp dll finished", KR(ret), K(tenant_id_));
  }

  return ret;
}

int ObUpgradeForAllVersionProcessor::replace_unit_group_id_with_unit_list_()
{
  int ret = OB_SUCCESS;
  ObArray<ObLSStatusInfo> ls_status_array;
  ObLSStatusOperator ls_status_operator;
  ObUnitTableOperator unit_operator;
  ObArray<ObUnit> unit_array;
  ObUnitIDList unit_id_list;
  const int64_t start_time = ObTimeUtility::current_time();
  int64_t duration = 0;
  if (OB_ISNULL(sql_proxy_) || !is_valid_tenant_id(tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(sql_proxy_), K(tenant_id_));
  } else if (!is_meta_tenant(tenant_id_)) {
    LOG_INFO("not meta tenant, ignore", K(tenant_id_));
  } else if (OB_FAIL(ls_status_operator.get_all_ls_status_by_order(
                         gen_user_tenant_id(tenant_id_), ls_status_array, *sql_proxy_))) {
    LOG_WARN("fail to get all ls status from table", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(unit_operator.init(*sql_proxy_))) {
    LOG_WARN("fail to init unit table operator", KR(ret), KP(sql_proxy_));
  } else {
    for (int64_t index = 0; index < ls_status_array.count() && OB_SUCC(ret); ++index) {
      const ObLSStatusInfo &ls_status_info = ls_status_array.at(index);
      unit_array.reset();
      unit_id_list.reset();
      if (OB_UNLIKELY(!ls_status_info.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(ls_status_info));
      } else if (0 != ls_status_info.unit_group_id_) {
        if (OB_FAIL(unit_operator.get_units_by_unit_group_id(ls_status_info.unit_group_id_, unit_array))) {
          LOG_WARN("fail to get units by unit group id", KR(ret), K(ls_status_info));
        } else {
          for (int64_t unit_index = 0; unit_index < unit_array.count() && OB_SUCC(ret); ++unit_index) {
            const uint64_t &unit_id = unit_array.at(unit_index).unit_id_;
            if (OB_FAIL(unit_id_list.push_back(ObDisplayUnitID(unit_id)))) {
              LOG_WARN("fail to push back unit_id into list", KR(ret),
                       K(unit_id_list), K(unit_index), K(unit_id));
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(ls_status_operator.alter_ls_group_id(
                                 gen_user_tenant_id(tenant_id_), ls_status_info.ls_id_,
                                 ls_status_info.ls_group_id_/*old_one*/,
                                 ls_status_info.ls_group_id_/*new_one*/,
                                 ls_status_info.get_unit_list()/*old_one*/,
                                 unit_id_list/*new_one*/, *sql_proxy_))) {
            LOG_WARN("fail to alter unit id list", KR(ret), "user_tenant_id",
                     gen_user_tenant_id(tenant_id_), K(ls_status_info), K(unit_id_list));
          } else {
            FLOG_INFO("finish adjust unit_group_id to unit_list", KR(ret), "user_tenant_id",
                      gen_user_tenant_id(tenant_id_), K(ls_status_info), K(unit_id_list));
          }
        }
      }
    }
  }
  duration = ObTimeUtility::current_time() - start_time;
  FLOG_INFO("replace_unit_group_id_with_unit_list finished",
            KR(ret), K(tenant_id_), K(start_time), K(duration));
  return ret;
}


int ObUpgradeFor4220Processor::post_upgrade()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(post_upgrade_for_create_replication_role_in_oracle())) {
    LOG_WARN("fail to create standby replication role in oracle", KR(ret));
  }
  return ret;
}

int ObUpgradeFor4220Processor::post_upgrade_for_create_replication_role_in_oracle()
{
  int ret = OB_SUCCESS;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  bool is_standby = false;
  if (OB_ISNULL(sql_proxy_) || OB_ISNULL(oracle_sql_proxy_) || OB_ISNULL(schema_service_) || !is_valid_tenant_id(tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), KP_(sql_proxy), KP_(schema_service), K_(tenant_id));
  } else if (!is_user_tenant(tenant_id_)) {
    LOG_INFO("meta and sys tenant no need to update replicate role", K_(tenant_id));
  } else if (OB_FAIL(ObAllTenantInfoProxy::is_standby_tenant(sql_proxy_, tenant_id_, is_standby))) {
    LOG_WARN("check is standby tenant failed", KR(ret), K_(tenant_id));
  } else if (is_standby) {
    LOG_INFO("standby tenant no need to update replicate role", K_(tenant_id));
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id_, compat_mode))) {
    LOG_WARN("failed to get tenant compat mode", KR(ret), K_(tenant_id));
  } else if (lib::Worker::CompatMode::ORACLE == compat_mode) {
    ObSchemaGetterGuard schema_guard;
    ObSqlString role_sql;
    ObSqlString sys_priv_sql;
    int64_t affected_rows = 0;
    bool is_user_exist = false;
    // check and create standby replication role
    if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("failed to get tenant schema guard", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(schema_guard.check_user_exist(tenant_id_, OB_ORA_STANDBY_REPLICATION_ROLE_ID, is_user_exist))) {
      LOG_WARN("fail to check user exist", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(schema_guard.reset())) {
      LOG_WARN("fail to reset schema guard", KR(ret));
    } else if (!is_user_exist) {
      if (OB_FAIL(role_sql.assign_fmt("CREATE ROLE %s", OB_ORA_STANDBY_REPLICATION_ROLE_NAME))) {
        LOG_WARN("fail to assign create role sql", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(oracle_sql_proxy_->write(tenant_id_, role_sql.ptr(), affected_rows))) {
        LOG_WARN("fail to write create role sql", KR(ret), K(role_sql), K_(tenant_id));
      }
    } else {
      LOG_INFO("standy replication role already exist");
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(sys_priv_sql.assign_fmt("GRANT CREATE SESSION TO %s", OB_ORA_STANDBY_REPLICATION_ROLE_NAME))) {
        LOG_WARN("fail to assign sql", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(oracle_sql_proxy_->write(tenant_id_, sys_priv_sql.ptr(), affected_rows))) {
        LOG_WARN("fail to write sql", KR(ret), K(sys_priv_sql), K_(tenant_id));
      }
    }
#define GRANT_OBJ_PRIVS_TO_USER(PRIV, DB_NAME, TABLE_NAME, USER_NAME)                                                       \
    if (OB_SUCC(ret)) {                                                                                                     \
      ObSqlString tab_priv_sql;                                                                                             \
      if (OB_FAIL(tab_priv_sql.assign_fmt("GRANT %s on %s.%s to %s", PRIV, DB_NAME, TABLE_NAME, USER_NAME))) {              \
        LOG_WARN("fail to assign sql", KR(ret));                                                                            \
      } else if (OB_FAIL(oracle_sql_proxy_->write(tenant_id_, tab_priv_sql.ptr(), affected_rows))) {                        \
        LOG_WARN("fail to write sql", KR(ret), K(tab_priv_sql));                                                            \
      }                                                                                                                     \
    }
    GRANT_OBJ_PRIVS_TO_USER("SELECT", OB_ORA_SYS_SCHEMA_NAME, OB_DBA_OB_TENANTS_ORA_TNAME, OB_ORA_STANDBY_REPLICATION_ROLE_NAME)
    GRANT_OBJ_PRIVS_TO_USER("SELECT", OB_ORA_SYS_SCHEMA_NAME, OB_DBA_OB_ACCESS_POINT_ORA_TNAME, OB_ORA_STANDBY_REPLICATION_ROLE_NAME)
    GRANT_OBJ_PRIVS_TO_USER("SELECT", OB_ORA_SYS_SCHEMA_NAME, OB_DBA_OB_LS_ORA_TNAME, OB_ORA_STANDBY_REPLICATION_ROLE_NAME)
    GRANT_OBJ_PRIVS_TO_USER("SELECT", OB_ORA_SYS_SCHEMA_NAME, OB_DBA_OB_LS_HISTORY_ORA_TNAME, OB_ORA_STANDBY_REPLICATION_ROLE_NAME)
    GRANT_OBJ_PRIVS_TO_USER("SELECT", OB_ORA_SYS_SCHEMA_NAME, OB_GV_OB_PARAMETERS_ORA_TNAME, OB_ORA_STANDBY_REPLICATION_ROLE_NAME)
    GRANT_OBJ_PRIVS_TO_USER("SELECT", OB_ORA_SYS_SCHEMA_NAME, OB_GV_OB_LOG_STAT_ORA_TNAME, OB_ORA_STANDBY_REPLICATION_ROLE_NAME)
    GRANT_OBJ_PRIVS_TO_USER("SELECT", OB_ORA_SYS_SCHEMA_NAME, OB_GV_OB_UNITS_ORA_TNAME, OB_ORA_STANDBY_REPLICATION_ROLE_NAME)
#undef GRANT_OBJ_PRIVS_TO_USER
    if (OB_FAIL(ret)) {
      LOG_WARN("[UPGRADE] upgrade user tenant create replication role failed", KR(ret), K_(tenant_id));
    } else {
      LOG_INFO("[UPGRADE] upgrade user tenant create replication role success", K_(tenant_id));
    }
  }
  return ret;
}
/* =========== 4220 upgrade processor end ============= */

/* =========== 4240 upgrade processor start ============= */
int ObUpgradeFor4240Processor::post_upgrade()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(post_upgrade_for_optimizer_stats())) {
    LOG_WARN("failed to post upgrade for optimizer stats", K(ret));
  } else if (OB_FAIL(post_upgrade_for_service_name())) {
    LOG_WARN("post upgrade for service name failed", KR(ret));
  } else if (OB_FAIL(post_upgrade_for_scheduled_trigger_partition_balance())) {
    LOG_WARN("post for upgrade scheduled trigger partition balance failed", KR(ret));
  } else if (OB_FAIL(post_upgrade_for_spm())) {
    LOG_WARN("post for upgrade for spm", K(ret));
  }
  return ret;
}

int ObUpgradeFor4240Processor::post_upgrade_for_optimizer_stats()
{
  int ret = OB_SUCCESS;
  ObSqlString extra_stats_perfs_sql;
  ObSqlString add_async_stats_job_sql;
  int64_t affected_rows = 0;
  bool is_primary_tenant = false;
  if (sql_proxy_ == NULL) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", K(ret), K(tenant_id_));
  } else if (OB_FAIL(ObAllTenantInfoProxy::is_primary_tenant(sql_proxy_, tenant_id_, is_primary_tenant))) {
    LOG_WARN("check is standby tenant failed", K(ret), K(tenant_id_));
  } else if (!is_primary_tenant) {
    LOG_INFO("tenant isn't primary standby, no refer to gather stats, skip", K(tenant_id_));
  } else if (OB_FAIL(ObDbmsStatsPreferences::get_extra_stats_perfs_for_upgrade(extra_stats_perfs_sql))) {
    LOG_WARN("failed to get extra stats perfs for upgrade", K(ret));
  } else if (OB_FAIL(sql_proxy_->write(tenant_id_, extra_stats_perfs_sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write", K(ret));
  } else if (OB_FAIL(ObDbmsStatsMaintenanceWindow::get_async_gather_stats_job_for_upgrade(sql_proxy_,
                                                                                          tenant_id_,
                                                                                          add_async_stats_job_sql))) {
    LOG_WARN("failed to get async gather stats job for upgrade", K(ret));
  } else if (OB_UNLIKELY(add_async_stats_job_sql.empty())) {
    LOG_INFO("failed to add async stats job in upgrade, perhaps the join already exists, need check after the upgrade.");
  } else if (OB_FAIL(sql_proxy_->write(tenant_id_, add_async_stats_job_sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write", K(ret));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("[UPGRADE] post upgrade for optimizer stats failed", KR(ret), K_(tenant_id));
  } else {
    LOG_INFO("[UPGRADE] post upgrade for optimizer stats succeed", K_(tenant_id));
  }
  return ret;
}
int ObUpgradeFor4240Processor::post_upgrade_for_service_name()
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected", KR(ret), KP(sql_proxy_));
  } else if (!is_meta_tenant(tenant_id_)) {
    LOG_INFO("not meta tenant, skip", K(tenant_id_));
  } else {
    ObSqlString sql;
    uint64_t user_tenant_id = gen_user_tenant_id(tenant_id_);
    if (OB_FAIL(sql.assign_fmt("INSERT IGNORE INTO %s (tenant_id, name, value) VALUES (%lu, '%s', 0)",
        OB_ALL_SERVICE_EPOCH_TNAME, user_tenant_id, ObServiceEpochProxy::SERVICE_NAME_EPOCH))) {
      LOG_WARN("fail to assign sql assign", KR(ret));
    } else if (OB_FAIL(sql_proxy_->write(tenant_id_, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(sql));
    } else {}
  }
  FLOG_INFO("insert service name epoch", KR(ret), K(tenant_id_), K(affected_rows));
  return ret;
}

// The tenant upgraded from old version still relies on partition_balance_schedule_interval.
// SCHEDULED_TRIGGER_PARTITION_BALANCE is disabled.
int ObUpgradeFor4240Processor::post_upgrade_for_scheduled_trigger_partition_balance()
{
  int ret = OB_SUCCESS;
  bool is_primary_tenant= false;
  ObSchemaGetterGuard schema_guard;
  const ObSysVariableSchema *sys_variable_schema = NULL;
  if (OB_ISNULL(sql_proxy_) || OB_ISNULL(schema_service_) || !is_valid_tenant_id(tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), KP(sql_proxy_), KP(schema_service_), K(tenant_id_));
  } else if (!is_user_tenant(tenant_id_)) {
    LOG_INFO("not user tenant, ignore", K(tenant_id_));
  } else if (OB_FAIL(ObAllTenantInfoProxy::is_primary_tenant(sql_proxy_, tenant_id_, is_primary_tenant))) {
    LOG_WARN("check is standby tenant failed", KR(ret), K(tenant_id_));
  } else if (!is_primary_tenant) {
    LOG_INFO("not primary tenant, ignore", K(tenant_id_));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_sys_variable_schema(tenant_id_, sys_variable_schema))) {
    LOG_WARN("get sys variable schema failed", KR(ret), K(tenant_id_));
  } else if (OB_ISNULL(sys_variable_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys variable schema is null", KR(ret));
  } else {
    START_TRANSACTION(sql_proxy_, tenant_id_);
    if (FAILEDx(ObScheduledTriggerPartitionBalance::create_scheduled_trigger_partition_balance_job(
        *sys_variable_schema,
        tenant_id_,
        false/*is_enabled*/,
        trans))) { // insert ignore
      LOG_WARN("create scheduled trigger partition balance job failed", KR(ret), K(tenant_id_));
    }
    END_TRANSACTION(trans);
    LOG_INFO("post upgrade for scheduled_trigger_partition_balance finished", KR(ret), K(tenant_id_));
  }
  return ret;
}

int ObUpgradeFor4240Processor::post_upgrade_for_spm()
{
  int ret = OB_SUCCESS;
  int64_t start = ObTimeUtility::current_time();
  ObSchemaGetterGuard schema_guard;
  const ObSysVariableSchema *var_schema = NULL;
  const ObSysVarSchema *spm_var = NULL;
  common::ObObj var_value;
  ObString sql("alter system set sql_plan_management_mode = 'OnlineEvolve';");
  int64_t affected_rows = 0;

  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (tenant_config.is_valid()) {
    int64_t spm_mode = ObSqlPlanManagementModeChecker::get_spm_mode_by_string(
        tenant_config->sql_plan_management_mode.get_value_string());
    if (0 == spm_mode) {
      if (OB_ISNULL(schema_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ptr", K(ret));
      } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
        LOG_WARN("failed to get schema guard", K(ret));
      } else if (OB_FAIL(schema_guard.get_sys_variable_schema(tenant_id_, var_schema))) {
        LOG_WARN("fail to get sys variable schema", KR(ret), K_(tenant_id));
      } else if (OB_NOT_NULL(var_schema)) {
        ObArenaAllocator alloc("UpgradeAlloc", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
        if (OB_FAIL(var_schema->get_sysvar_schema(ObSysVarClassType::SYS_VAR_OPTIMIZER_USE_SQL_PLAN_BASELINES,
                                                  spm_var))) {
          LOG_WARN("failed to get sysvar schema", K(ret));
        } else if (OB_ISNULL(spm_var)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null sysvar schema", K(ret));
        } else if (OB_FAIL(spm_var->get_value(&alloc, NULL, var_value))) {
          LOG_WARN("failed to get sys variable value", K(ret));
        } else if (!var_value.get_bool()) {
          // do nothing
        } else if (OB_ISNULL(sql_proxy_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null ptr", K(ret));
        } else if (OB_FAIL(sql_proxy_->write(tenant_id_, sql.ptr(), affected_rows))) {
          LOG_WARN("execute sql failed", K(ret), K(sql));
        } else {
          LOG_TRACE("execute sql", KR(ret), K(tenant_id_), K(sql), K(affected_rows));
        }
      }
    }
  }

  LOG_INFO("set spm parameter based on sys variable", K(tenant_id_), K(var_value), "cost", ObTimeUtility::current_time() - start);

  return ret;
}
/* =========== 4240 upgrade processor end ============= */

/* =========== 4250 upgrade processor start ============= */

int ObUpgradeFor4250Processor::post_upgrade()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(add_spm_stats_scheduler_job())) {
    LOG_WARN("fail to create standby replication role in oracle", KR(ret));
  } else if (OB_FAIL(post_upgrade_for_persitent_routine())) {
    LOG_WARN("post for upgrade for spm", K(ret));
  } else if (OB_FAIL(post_upgrade_for_optimizer_stats())) {
    LOG_WARN("fail to post upgrade for optimizer stats", KR(ret));
  }
  return ret;
}

int ObUpgradeFor4250Processor::add_spm_stats_scheduler_job()
{
  int ret = OB_SUCCESS;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  bool is_primary_tenant = false;
  bool job_exists = false;
  ObSchemaGetterGuard schema_guard;
  const ObSysVariableSchema *var_schema = NULL;
  ObSqlString insert_sql;
  int64_t affected_rows = 0;
  if (OB_ISNULL(sql_proxy_) || OB_ISNULL(schema_service_) || !is_valid_tenant_id(tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), KP_(sql_proxy), KP_(schema_service), K_(tenant_id));
  } else if (OB_FAIL(ObAllTenantInfoProxy::is_primary_tenant(sql_proxy_, tenant_id_, is_primary_tenant))) {
    LOG_WARN("check is standby tenant failed", K(ret), K(tenant_id_));
  } else if (!is_primary_tenant) {
    LOG_INFO("tenant isn't primary tenant", K(tenant_id_));
  } else if (OB_FAIL(ObDbmsStatsMaintenanceWindow::check_job_exists(sql_proxy_,
                                                                    tenant_id_,
                                                                    "SPM_STATS_MANAGER",
                                                                    job_exists))) {
    LOG_WARN("failed to check job exists");
  } else if (job_exists) {
    LOG_INFO("spm schedular job already exists", K(tenant_id_), "job_name", "SPM_STATS_MANAGER");
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_sys_variable_schema(tenant_id_, var_schema))) {
    LOG_WARN("fail to get sys variable schema", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id_, compat_mode))) {
    LOG_WARN("failed to get tenant compat mode", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObDbmsStatsMaintenanceWindow::get_spm_stats_upgrade_jobs_sql(sql_proxy_,
                                                                                  *var_schema,
                                                                                  tenant_id_,
                                                                                  lib::Worker::CompatMode::ORACLE == compat_mode,
                                                                                  insert_sql))) {
    LOG_WARN("failed to get spm stats upgrade jobs sql");
  } else if (OB_FAIL(sql_proxy_->write(tenant_id_, insert_sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write spm stats job", K(ret), K(tenant_id_), K(affected_rows), K(insert_sql));
  }

  return ret;
}

int ObUpgradeFor4250Processor::post_upgrade_for_persitent_routine()
{
  int ret = OB_SUCCESS;

  bool is_primary_tenant= false;
  ObSchemaGetterGuard schema_guard;
  const ObSysVariableSchema *sys_variable_schema = NULL;
  if (OB_ISNULL(sql_proxy_) || OB_ISNULL(schema_service_) || !is_valid_tenant_id(tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), KP(sql_proxy_), KP(schema_service_), K(tenant_id_));
  } else if (!is_user_tenant(tenant_id_)) {
    LOG_INFO("not user tenant, ignore", K(tenant_id_));
  } else if (OB_FAIL(ObAllTenantInfoProxy::is_primary_tenant(sql_proxy_, tenant_id_, is_primary_tenant))) {
    LOG_WARN("check is standby tenant failed", KR(ret), K(tenant_id_));
  } else if (!is_primary_tenant) {
    LOG_INFO("not primary tenant, ignore", K(tenant_id_));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_sys_variable_schema(tenant_id_, sys_variable_schema))) {
    LOG_WARN("get sys variable schema failed", KR(ret), K(tenant_id_));
  } else if (OB_ISNULL(sys_variable_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys variable schema is null", KR(ret));
  } else {
    START_TRANSACTION(sql_proxy_, tenant_id_);
    if (FAILEDx(ObFlushNcompDll::create_flush_ncomp_dll_job_for_425(
        *sys_variable_schema,
        tenant_id_,
        false/*is_enabled*/,
        trans))) { // insert ignore
      LOG_WARN("create flush ncomp dll job failed", KR(ret), K(tenant_id_));
    }
    END_TRANSACTION(trans);
    LOG_INFO("post upgrade for create flush ncomp dll finished", KR(ret), K(tenant_id_));
  }

  return ret;
}

int ObUpgradeFor4250Processor::post_upgrade_for_optimizer_stats()
{
  int ret = OB_SUCCESS;
  ObSqlString extra_stats_perfs_sql;
  ObSqlString add_async_stats_job_sql;
  int64_t affected_rows = 0;
  bool is_primary_tenant = false;
  if (sql_proxy_ == NULL) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", K(ret), K(tenant_id_));
  } else if (OB_FAIL(ObAllTenantInfoProxy::is_primary_tenant(sql_proxy_, tenant_id_, is_primary_tenant))) {
    LOG_WARN("check is standby tenant failed", K(ret), K(tenant_id_));
  } else if (!is_primary_tenant) {
    LOG_INFO("tenant isn't primary standby, no refer to gather stats, skip", K(tenant_id_));
  } else if (OB_FAIL(ObDbmsStatsPreferences::get_extra_stats_perfs_for_upgrade_425(extra_stats_perfs_sql))) {
    LOG_WARN("failed to get extra stats perfs for upgrade", K(ret));
  } else if (OB_FAIL(sql_proxy_->write(tenant_id_, extra_stats_perfs_sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write", K(ret));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("[UPGRADE] post upgrade for optimizer stats failed", KR(ret), K_(tenant_id));
  } else {
    LOG_INFO("[UPGRADE] post upgrade for optimizer stats succeed", K_(tenant_id));
  }
  return ret;
}
/* =========== 4250 upgrade processor end ============= */

/* =========== 4251 upgrade processor start ============= */

int ObUpgradeFor4251Processor::post_upgrade()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(post_upgrade_for_scheduled_trigger_dump_data_dict())) {
    LOG_WARN("fail to post upgrade for scheduled trigger dump data dict", KR(ret));
  }
  return ret;
}

int ObUpgradeFor4251Processor::post_upgrade_for_scheduled_trigger_dump_data_dict()
{
  int ret = OB_SUCCESS;
  bool is_primary_tenant= false;
  ObSchemaGetterGuard schema_guard;
  const ObSysVariableSchema *sys_variable_schema = NULL;

  if (OB_ISNULL(sql_proxy_) || OB_ISNULL(schema_service_) || !is_valid_tenant_id(tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), KP(sql_proxy_), KP(schema_service_), K(tenant_id_));
  } else if (!is_user_tenant(tenant_id_)) {
    LOG_INFO("not user tenant, ignore", K(tenant_id_));
  } else if (OB_FAIL(ObAllTenantInfoProxy::is_primary_tenant(sql_proxy_, tenant_id_, is_primary_tenant))) {
    LOG_WARN("check is standby tenant failed", KR(ret), K(tenant_id_));
  } else if (!is_primary_tenant) {
    LOG_INFO("not primary tenant, ignore", K(tenant_id_));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_sys_variable_schema(tenant_id_, sys_variable_schema))) {
    LOG_WARN("get sys variable schema failed", KR(ret), K(tenant_id_));
  } else if (OB_ISNULL(sys_variable_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys variable schema is null", KR(ret));
  } else {
    START_TRANSACTION(sql_proxy_, tenant_id_);
    if (FAILEDx(datadict::ObDataDictScheduler::create_scheduled_trigger_dump_data_dict_job(
        *sys_variable_schema,
        tenant_id_,
        true/*is_enabled*/,
        trans))) { // insert ignore
      LOG_WARN("create scheduled trigger dump_data_dict job failed", KR(ret), K(tenant_id_));
    }
    END_TRANSACTION(trans);
    LOG_INFO("post upgrade for create_scheduled_trigger_dump_data_dict_job finished", KR(ret), K(tenant_id_));
  }
  return ret;
}

/* =========== 4251 upgrade processor end ============= */

/* =========== special upgrade processor end   ============= */
} // end share
} // end oceanbase
