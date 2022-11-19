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

#include "rootserver/ob_upgrade_executor.h"
#include "rootserver/ob_rs_job_table_operator.h"
#include "observer/ob_server_struct.h"
#include "rootserver/ob_root_inspection.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace share::schema;

namespace rootserver
{

int64_t ObUpgradeTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

ObAsyncTask *ObUpgradeTask::deep_copy(char *buf, const int64_t buf_size) const
{
  ObAsyncTask *task = NULL;
  int ret = OB_SUCCESS;
  const int64_t need_size = get_deep_copy_size();
  if (NULL == buf) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null", KR(ret));
  } else if (buf_size < need_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is not long enough", K(need_size), K(buf_size), KR(ret));
  } else {
    task = new(buf) ObUpgradeTask(*upgrade_executor_, action_, version_);
  }
  return task;
}

int ObUpgradeTask::process()
{
  const int64_t start = ObTimeUtility::current_time();
  FLOG_INFO("[UPGRADE] start to do execute upgrade task",
            K(start), K_(action), K_(version));
  int ret = OB_SUCCESS;
  if (OB_ISNULL(upgrade_executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("upgrade_executor_ is null", KR(ret));
  } else if (OB_FAIL(upgrade_executor_->execute(action_, version_))) {
    LOG_WARN("fail to execute upgrade task", KR(ret), K_(action), K_(version));
  }
  FLOG_INFO("[UPGRADE] finish execute upgrade task",
            KR(ret), K_(action), K_(version),
            "cost_us", ObTimeUtility::current_time() - start);
  return ret;
}

ObUpgradeExecutor::ObUpgradeExecutor()
    : inited_(false), stopped_(false), execute_(false), rwlock_(),
      sql_proxy_(NULL), rpc_proxy_(NULL), common_rpc_proxy_(NULL), schema_service_(NULL),
      upgrade_processors_()
{}

int ObUpgradeExecutor::init(
    share::schema::ObMultiVersionSchemaService &schema_service,
    common::ObMySQLProxy &sql_proxy,
    obrpc::ObSrvRpcProxy &rpc_proxy,
    obrpc::ObCommonRpcProxy &common_proxy)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can't init twice", KR(ret));
  } else if (OB_FAIL(upgrade_processors_.init(
                     ObBaseUpgradeProcessor::UPGRADE_MODE_OB,
                     sql_proxy, rpc_proxy, common_proxy, schema_service, *this))) {
    LOG_WARN("fail to init upgrade processors", KR(ret));
  } else {
    schema_service_ = &schema_service;
    sql_proxy_ = &sql_proxy;
    rpc_proxy_ = &rpc_proxy;
    common_rpc_proxy_ = &common_proxy;
    stopped_ = false;
    execute_ = false;
    inited_ = true;
  }
  return ret;
}

void ObUpgradeExecutor::start()
{
  SpinWLockGuard guard(rwlock_);
  stopped_ = false;
}

int ObUpgradeExecutor::stop()
{
  int ret = OB_SUCCESS;
  const uint64_t WAIT_US = 100 * 1000L; //100ms
  const uint64_t MAX_WAIT_US = 10 * 1000 * 1000L; //10s
  const int64_t start = ObTimeUtility::current_time();
  {
    SpinWLockGuard guard(rwlock_);
    stopped_ = true;
  }
  while (OB_SUCC(ret)) {
    if (ObTimeUtility::current_time() - start > MAX_WAIT_US) {
      ret = OB_TIMEOUT;
      LOG_WARN("use too much time", KR(ret), "cost_us", ObTimeUtility::current_time() - start);
    } else if (!check_execute()) {
      break;
    } else {
      ob_usleep(WAIT_US);
    }
  }
  return ret;
}

int ObUpgradeExecutor::check_stop() const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(rwlock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (stopped_) {
    ret = OB_CANCELED;
    LOG_WARN("executor should stopped", KR(ret));
  }
  return ret;
}

bool ObUpgradeExecutor::check_execute() const
{
  SpinRLockGuard guard(rwlock_);
  bool bret = execute_;
  return bret;
}

int ObUpgradeExecutor::set_execute_mark_()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(rwlock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (stopped_ || execute_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can't run job at the same time", KR(ret));
  } else {
    execute_ = true;
  }
  return ret;
}

int ObUpgradeExecutor::can_execute()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(rwlock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (stopped_ || execute_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("status not matched", KR(ret),
             "stopped", stopped_ ? "true" : "false",
             "build", execute_ ? "true" : "false");
  }
  return ret;
}

int ObUpgradeExecutor::check_inner_stat_() const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_ISNULL(schema_service_)
             || OB_ISNULL(sql_proxy_)
             || OB_ISNULL(rpc_proxy_)
             || OB_ISNULL(common_rpc_proxy_)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP_(schema_service),
             KP_(sql_proxy), KP_(rpc_proxy), KP_(common_rpc_proxy));
  }
  return ret;
}

// wait schema sync in cluster
int ObUpgradeExecutor::check_schema_sync_()
{
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] start to check schema sync", K(start));
  int ret = OB_SUCCESS;
  bool enable_ddl = GCONF.enable_ddl;
  bool enable_sys_table_ddl = GCONF.enable_sys_table_ddl;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (enable_ddl || enable_sys_table_ddl) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("ddl should disable now", KR(ret), K(enable_ddl), K(enable_sys_table_ddl));
  } else {
    const int64_t WAIT_US = 1000 * 1000L; // 1 second
    bool is_sync = false;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(check_stop())) {
        LOG_WARN("executor is stop", KR(ret));
      } else if (OB_FAIL(ObUpgradeUtils::check_schema_sync(is_sync))) {
        LOG_WARN("fail to check schema sync", KR(ret));
      } else if (is_sync) {
        break;
      } else {
        LOG_INFO("schema not sync, should wait", KR(ret));
        ob_usleep(static_cast<useconds_t>((WAIT_US)));
      }
    }
  }
  LOG_INFO("[UPGRADE] check schema sync finish", KR(ret),
           "cost_us", ObTimeUtility::current_time() - start);
  return ret;
}

// Ensure primary cluster's schema_version is not greator than standby clusters'.
int ObUpgradeExecutor::check_schema_sync_(
    obrpc::ObTenantSchemaVersions &primary_schema_versions,
    obrpc::ObTenantSchemaVersions &standby_schema_versions,
    bool &schema_sync)
{
  int ret = OB_SUCCESS;
  int64_t primary_cnt = primary_schema_versions.tenant_schema_versions_.count();
  int64_t standby_cnt = standby_schema_versions.tenant_schema_versions_.count();
  if (primary_cnt <= 0 || standby_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cnt", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", KR(ret));
  } else {
    schema_sync = true;
    for (int64_t i = 0; schema_sync && OB_SUCC(ret) && i < primary_cnt; i++) {
      bool find = false;
      TenantIdAndSchemaVersion &primary = primary_schema_versions.tenant_schema_versions_.at(i);
      // check normal tenant only
      if (OB_SYS_TENANT_ID == primary.tenant_id_) {
        continue;
      } else {
        for (int64_t j = 0; !find && OB_SUCC(ret) && j < standby_cnt; j++) {
          TenantIdAndSchemaVersion &standby = standby_schema_versions.tenant_schema_versions_.at(j);
          if (OB_FAIL(check_stop())) {
            LOG_WARN("executor should stopped", KR(ret));
          } else if (primary.tenant_id_ == standby.tenant_id_) {
            find = true;
            schema_sync = (primary.schema_version_ <= standby.schema_version_);
            LOG_INFO("check if tenant schema is sync",
                     KR(ret), K(primary), K(standby), K(schema_sync));
          }
        }
        if (OB_SUCC(ret) && !find) {
          schema_sync = false;
        }
      }
    }
  }
  return ret;
}

//TODO:
//1. Run upgrade job by tenant.
//2. Check tenant role/tenant status before run upgrade job.
int ObUpgradeExecutor::execute(
    const obrpc::ObUpgradeJobArg::Action action,
    const int64_t version)
{
  ObCurTraceId::init(GCONF.self_addr_);
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(set_execute_mark_())) {
    LOG_WARN("fail to set execute mark", KR(ret));
  } else {
    if (obrpc::ObUpgradeJobArg::UPGRADE_POST_ACTION == action) {
      if (OB_FAIL(run_upgrade_post_job_(version))) {
        LOG_WARN("fail to run upgrade post job", KR(ret), K(version));
      }
    } else if (obrpc::ObUpgradeJobArg::UPGRADE_SYSTEM_VARIABLE == action) {
      if (OB_FAIL(run_upgrade_system_variable_job_())) {
        LOG_WARN("fail to run upgrade system variable job", KR(ret));
      }
    } else if (obrpc::ObUpgradeJobArg::UPGRADE_SYSTEM_TABLE == action) {
      if (OB_FAIL(run_upgrade_system_table_job_())) {
        LOG_WARN("fail to run upgrade system table job", KR(ret));
      }
    }
    execute_ = false;
  }
  return ret;
}

// Python upgrade script may set enable_ddl = false before it run upgrade job.
// TODO:
// 1. support run upgrade post action from `COMPATIBLE` to current data version.
int ObUpgradeExecutor::run_upgrade_post_job_(const int64_t version)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", KR(ret));
  } else if (!ObUpgradeChecker::check_data_version_exist(version)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported version to run upgrade job", KR(ret), K(version));
  } else {
    ObArray<uint64_t> tenant_ids;
    ObBaseUpgradeProcessor *processor = NULL;
    int64_t job_id = OB_INVALID_ID;
    ObRsJobType job_type = ObRsJobType::JOB_TYPE_UPGRADE_POST_ACTION;
    char version_str[common::OB_CLUSTER_VERSION_LENGTH] = {0};
    int64_t len = ObClusterVersion::print_version_str(
                  version_str, common::OB_CLUSTER_VERSION_LENGTH, version);
    int tmp_ret = OB_SUCCESS;
    int64_t backup_ret = OB_SUCCESS;
    if (OB_FAIL(RS_JOB_CREATE_WITH_RET(job_id, job_type, *sql_proxy_,
                                       "tenant_id", 0,
                                       "extra_info", ObString(len, version_str)))) {
      LOG_WARN("fail to create rs job", KR(ret));
    } else if (job_id <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("job_id is invalid", KR(ret), K(job_id));
    } else if (OB_FAIL(schema_service_->get_tenant_ids(tenant_ids))) {
      LOG_WARN("fail to get tenant_ids", KR(ret), K(version));
    } else if (OB_FAIL(upgrade_processors_.get_processor_by_version(
                       version, processor))) {
      LOG_WARN("fail to get processor by version", KR(ret), K(version));
    } else {
      for (int64_t i = tenant_ids.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
        const uint64_t tenant_id = tenant_ids.at(i);
        int64_t start_ts = ObTimeUtility::current_time();
        int64_t current_version = processor->get_version();
        processor->set_tenant_id(tenant_id);
        FLOG_INFO("[UPGRADE] start to run post upgrade job by version",
                  K(tenant_id), K(current_version));
        if (OB_FAIL(check_stop())) {
          LOG_WARN("executor should stopped", KR(ret));
        } else if (OB_TMP_FAIL(processor->post_upgrade())) {
          LOG_WARN("run post upgrade by version failed",
                   KR(tmp_ret), K(tenant_id), K(current_version));
          backup_ret = OB_SUCCESS == backup_ret ? tmp_ret : backup_ret;
        }
        FLOG_INFO("[UPGRADE] finish post upgrade job by version",
                  KR(tmp_ret), K(tenant_id), K(current_version),
                  "cost", ObTimeUtility::current_time() - start_ts);
      } // end for
    }
    ret = OB_SUCC(ret) ? backup_ret : ret;
    if (job_id > 0) {
      if (OB_SUCCESS != (tmp_ret = RS_JOB_COMPLETE(job_id, ret, *sql_proxy_))) {
        LOG_ERROR("fail to complete job", K(tmp_ret), KR(ret), K(job_id));
        ret = OB_FAIL(ret) ? ret : tmp_ret;
      }
    }
  }
  return ret;
}

int ObUpgradeExecutor::run_upgrade_system_variable_job_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", KR(ret));
  } else {
    ObArray<uint64_t> tenant_ids;
    int64_t job_id = OB_INVALID_ID;
    ObRsJobType job_type = ObRsJobType::JOB_TYPE_UPGRADE_SYSTEM_VARIABLE;
    int tmp_ret = OB_SUCCESS;
    int backup_ret = OB_SUCCESS;
    if (OB_FAIL(RS_JOB_CREATE_WITH_RET(job_id, job_type, *sql_proxy_, "tenant_id", 0))) {
      LOG_WARN("fail to create rs job", KR(ret));
    } else if (job_id <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("job_id is invalid", KR(ret), K(job_id));
    } else if (OB_FAIL(schema_service_->get_tenant_ids(tenant_ids))) {
      LOG_WARN("fail to get tenant_ids", KR(ret));
    } else {
      for (int64_t i = tenant_ids.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
        const uint64_t tenant_id = tenant_ids.at(i);
        int64_t start_ts = ObTimeUtility::current_time();
        FLOG_INFO("[UPGRADE] start to run upgrade system variable job", K(tenant_id));
        if (OB_FAIL(check_stop())) {
          LOG_WARN("executor should stopped", KR(ret));
        } else if (OB_TMP_FAIL(ObUpgradeUtils::upgrade_sys_variable(*common_rpc_proxy_, *sql_proxy_, tenant_id))) {
          LOG_WARN("fail to upgrade sys variable", KR(tmp_ret), K(tenant_id));
          backup_ret = OB_SUCCESS == backup_ret ? tmp_ret : backup_ret;
        }
        FLOG_INFO("[UPGRADE] finish run upgrade system variable job",
                  KR(tmp_ret), K(tenant_id), "cost", ObTimeUtility::current_time() - start_ts);
      } // end for
      ret = OB_SUCC(ret) ? backup_ret : ret;
    }
    if (job_id > 0) {
      if (OB_SUCCESS != (tmp_ret = RS_JOB_COMPLETE(job_id, ret, *sql_proxy_))) {
        LOG_ERROR("fail to complete job", K(tmp_ret), KR(ret), K(job_id));
        ret = OB_FAIL(ret) ? ret : tmp_ret;
      }
    }
  }
  return ret;
}

// NOTICE: enable_sys_table_ddl should be true before run this job.
int ObUpgradeExecutor::run_upgrade_system_table_job_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", KR(ret));
  } else {
    ObArray<uint64_t> tenant_ids;
    int64_t job_id = OB_INVALID_ID;
    ObRsJobType job_type = ObRsJobType::JOB_TYPE_UPGRADE_SYSTEM_TABLE;
    int tmp_ret = OB_SUCCESS;
    int backup_ret = OB_SUCCESS;
    if (OB_FAIL(RS_JOB_CREATE_WITH_RET(job_id, job_type, *sql_proxy_, "tenant_id", 0))) {
      LOG_WARN("fail to create rs job", KR(ret));
    } else if (job_id <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("job_id is invalid", KR(ret), K(job_id));
    } else if (OB_FAIL(schema_service_->get_tenant_ids(tenant_ids))) {
      LOG_WARN("fail to get tenant_ids", KR(ret));
    } else {
      for (int64_t i = tenant_ids.count() - 1; i >= 0; i--) {
        const uint64_t tenant_id = tenant_ids.at(i);
        int64_t start_ts = ObTimeUtility::current_time();
        FLOG_INFO("[UPGRADE] start to run upgrade system table job", K(tenant_id));
        if (OB_FAIL(check_stop())) {
          LOG_WARN("executor should stopped", KR(ret));
        } else if (OB_TMP_FAIL(upgrade_system_table_(tenant_id))) {
          LOG_WARN("fail to upgrade system table", KR(tmp_ret), K(tenant_id));
          backup_ret = OB_SUCCESS == backup_ret ? tmp_ret : backup_ret;
        }
        FLOG_INFO("[UPGRADE] finish run upgrade system table job",
                  KR(tmp_ret), K(tenant_id), "cost", ObTimeUtility::current_time() - start_ts);
      } // end for
      ret = OB_SUCC(ret) ? backup_ret : ret;
    }
    if (job_id > 0) {
      if (OB_SUCCESS != (tmp_ret = RS_JOB_COMPLETE(job_id, ret, *sql_proxy_))) {
        LOG_ERROR("fail to complete job", K(tmp_ret), KR(ret), K(job_id));
        ret = OB_FAIL(ret) ? ret : tmp_ret;
      }
    }
  }
  return ret;
}

int ObUpgradeExecutor::upgrade_system_table_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", KR(ret));
  } else {
    ObArray<uint64_t> upgrade_table_ids; // miss or mismatch
    // Only core/system tables can be upgraded here.
    // 1. __all_core_table can't be altered.
    // 2. sys index table and sys lob table will be added with sys data table, and can't be altered.
    const schema_create_func *creator_ptr_array[] = {
      share::core_table_schema_creators,
      share::sys_table_schema_creators, NULL };

    // check system table
    ObTableSchema table_schema;
    bool exist = false;
    for (const schema_create_func **creator_ptr_ptr = creator_ptr_array;
         OB_SUCC(ret) && OB_NOT_NULL(*creator_ptr_ptr); ++creator_ptr_ptr) {
      for (const schema_create_func *creator_ptr = *creator_ptr_ptr;
           OB_SUCC(ret) && OB_NOT_NULL(*creator_ptr); ++creator_ptr) {
        table_schema.reset();
        if (OB_FAIL(check_stop())) {
          LOG_WARN("check_cancel failed", KR(ret));
        } else if (OB_FAIL((*creator_ptr)(table_schema))) {
          LOG_WARN("create table schema failed", KR(ret));
        } else if (!is_sys_tenant(tenant_id)
                   && OB_FAIL(ObSchemaUtils::construct_tenant_space_full_table(
                                  tenant_id, table_schema))) {
          LOG_WARN("fail to construct tenant space table", KR(ret), K(tenant_id));
        } else if (OB_FAIL(ObSysTableChecker::is_inner_table_exist(
                   tenant_id, table_schema, exist))) {
          LOG_WARN("fail to check inner table exist",
                   KR(ret), K(tenant_id), K(table_schema));
        } else if (!exist) {
          // skip
        } else if (OB_FAIL(check_table_schema_(tenant_id, table_schema))) {
          const uint64_t table_id = table_schema.get_table_id();
          if (OB_SCHEMA_ERROR != ret) {
            LOG_WARN("check_table_schema failed", KR(ret), K(tenant_id), K(table_id));
          } else {
            FLOG_INFO("[UPGRADE] table need upgrade", K(tenant_id), K(table_id),
                      "table_name", table_schema.get_table_name());
            if (OB_FAIL(upgrade_table_ids.push_back(table_id))) { // overwrite ret
              LOG_WARN("fail to push back upgrade table ids", KR(ret), K(tenant_id), K(table_id));
            }
          }
        }
      } // end for
    } // end for

    int tmp_ret = OB_SUCCESS;
    int backup_ret = OB_SUCCESS;
    // upgrade system table(create or alter)
    obrpc::ObUpgradeTableSchemaArg arg;
    const int64_t timeout = GCONF.internal_sql_execute_timeout;
    for (int64_t i = 0; OB_SUCC(ret) && i < upgrade_table_ids.count(); i++) {
      const uint64_t table_id = upgrade_table_ids.at(i);
      int64_t start_ts = ObTimeUtility::current_time();
      FLOG_INFO("[UPGRADE] start upgrade system table", K(tenant_id), K(table_id));
      if (OB_FAIL(check_stop())) {
        LOG_WARN("check_cancel failed", KR(ret));
      } else if (OB_FAIL(arg.init(tenant_id, table_id))) {
        LOG_WARN("fail to init arg", KR(ret), K(tenant_id), K(table_id));
      } else if (OB_TMP_FAIL(common_rpc_proxy_->timeout(timeout).upgrade_table_schema(arg))) {
        LOG_WARN("fail to uggrade table schema", KR(tmp_ret), K(timeout), K(arg));
        backup_ret = OB_SUCCESS == backup_ret ? tmp_ret : backup_ret;
      }
      FLOG_INFO("[UPGRADE] finish upgrade system table",
                KR(tmp_ret), K(tenant_id), K(table_id), "cost", ObTimeUtility::current_time() - start_ts);
    } // end for
    ret = OB_SUCC(ret) ? backup_ret : ret;
  }
  return ret;
}

int ObUpgradeExecutor::check_table_schema_(const uint64_t tenant_id, const ObTableSchema &hard_code_table)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table = NULL;
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(
             tenant_id, hard_code_table.get_table_id(), table))) {
    LOG_WARN("get_table_schema failed", KR(ret), K(tenant_id),
             "table_id", hard_code_table.get_table_id(),
             "table_name", hard_code_table.get_table_name());
  } else if (OB_ISNULL(table)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table should not be null", KR(ret), K(tenant_id),
             "table_id", hard_code_table.get_table_id(),
             "table_name", hard_code_table.get_table_name());
  } else if (OB_FAIL(ObRootInspection::check_table_schema(hard_code_table, *table))) {
    LOG_WARN("fail to check table schema", KR(ret), K(tenant_id), K(hard_code_table), KPC(table));
  }
  return ret;
}

}//end rootserver
}//end oceanbase
