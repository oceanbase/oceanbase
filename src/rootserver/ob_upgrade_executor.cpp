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
#include "rootserver/ob_ls_service_helper.h"
#include "rootserver/tenant_snapshot/ob_tenant_snapshot_util.h" //ObTenantSnapshotUtil
#include "observer/ob_server_struct.h"
#include "share/ob_global_stat_proxy.h"
#include "share/ob_cluster_event_history_table_operator.h"//CLUSTER_EVENT_INSTANCE
#include "share/ob_primary_standby_service.h" // ObPrimaryStandbyService
#include "share/ob_tenant_info_proxy.h" //ObAllTenantInfoProxy
#include "observer/ob_service.h"

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
    task = new(buf) ObUpgradeTask(*upgrade_executor_);
    if (OB_FAIL(static_cast<ObUpgradeTask *>(task)->init(arg_))) {
      LOG_WARN("fail to init task", KR(ret), K_(arg));
    }
  }
  return task;
}

int ObUpgradeTask::init(const obrpc::ObUpgradeJobArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(arg_.assign(arg))) {
    LOG_WARN("fail to assign arg", KR(ret));
  }
  return ret;
}

int ObUpgradeTask::process()
{
  const int64_t start = ObTimeUtility::current_time();
  FLOG_INFO("[UPGRADE] start to do execute upgrade task", K(start), K_(arg));
  int ret = OB_SUCCESS;
  if (OB_ISNULL(upgrade_executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("upgrade_executor_ is null", KR(ret));
  } else if (OB_FAIL(upgrade_executor_->execute(arg_))) {
    LOG_WARN("fail to execute upgrade task", KR(ret), K_(arg));
  }
  FLOG_INFO("[UPGRADE] finish execute upgrade task",
            KR(ret), K_(arg), "cost_us", ObTimeUtility::current_time() - start);
  return ret;
}

ObUpgradeExecutor::ObUpgradeExecutor()
    : inited_(false), stopped_(false), execute_(false), rwlock_(ObLatchIds::DEFAULT_SPIN_RWLOCK),
      sql_proxy_(NULL), rpc_proxy_(NULL), common_rpc_proxy_(NULL),
      schema_service_(NULL), root_inspection_(NULL),
      upgrade_processors_()
{}

int ObUpgradeExecutor::init(
    share::schema::ObMultiVersionSchemaService &schema_service,
    rootserver::ObRootInspection &root_inspection,
    common::ObMySQLProxy &sql_proxy,
    common::ObOracleSqlProxy &oracle_sql_proxy,
    obrpc::ObSrvRpcProxy &rpc_proxy,
    obrpc::ObCommonRpcProxy &common_proxy)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can't init twice", KR(ret));
  } else if (OB_FAIL(upgrade_processors_.init(
                     ObBaseUpgradeProcessor::UPGRADE_MODE_OB,
                     sql_proxy, oracle_sql_proxy, rpc_proxy, common_proxy, schema_service, *this))) {
    LOG_WARN("fail to init upgrade processors", KR(ret));
  } else {
    schema_service_ = &schema_service;
    root_inspection_ = &root_inspection;
    sql_proxy_ = &sql_proxy;
    oralce_sql_proxy_ = &oracle_sql_proxy;
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
             || OB_ISNULL(root_inspection_)
             || OB_ISNULL(sql_proxy_)
             || OB_ISNULL(rpc_proxy_)
             || OB_ISNULL(common_rpc_proxy_)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP_(schema_service), KP_(root_inspection),
             KP_(sql_proxy), KP_(rpc_proxy), KP_(common_rpc_proxy));
  }
  return ret;
}

// wait schema sync in cluster
int ObUpgradeExecutor::check_schema_sync_(const uint64_t tenant_id)
{
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] start to check schema sync", K(tenant_id), K(start));
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    const int64_t WAIT_US = 1000 * 1000L; // 1 second
    bool is_sync = false;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(check_stop())) {
        LOG_WARN("executor is stop", KR(ret));
      } else if (OB_FAIL(ObUpgradeUtils::check_schema_sync(tenant_id, is_sync))) {
        LOG_WARN("fail to check schema sync", KR(ret), K(tenant_id));
      } else if (is_sync) {
        break;
      } else {
        LOG_INFO("schema not sync, should wait", KR(ret), K(tenant_id));
        ob_usleep(static_cast<useconds_t>((WAIT_US)));
      }
    }
  }
  LOG_INFO("[UPGRADE] check schema sync finish", KR(ret), K(tenant_id),
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
    const obrpc::ObUpgradeJobArg &arg)
{
  ObCurTraceId::init(GCONF.self_addr_);
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  obrpc::ObUpgradeJobArg::Action action = arg.action_;
  int64_t version = arg.version_;
  ObRsJobType job_type = convert_to_job_type_(arg.action_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (JOB_TYPE_INVALID == job_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job type", KR(ret), K(arg));
  } else if (version > 0 && !ObUpgradeChecker::check_data_version_exist(version)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported version to run upgrade job", KR(ret), K(arg));
  } else if (OB_FAIL(construct_tenant_ids_(arg.tenant_ids_, tenant_ids))) {
    LOG_WARN("fail to construct tenant_ids", KR(ret), K(arg));
  } else if (OB_FAIL(set_execute_mark_())) {
    LOG_WARN("fail to set execute mark", KR(ret));
    // NOTICE: don't add any `else if` after set_execute_mark_().
  } else {
    const uint64_t tenant_id = (1 == tenant_ids.count()) ?  tenant_ids.at(0) : 0;
    const int64_t BUF_LEN = common::MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH;
    char extra_buf[BUF_LEN] = {'\0'};
    int64_t job_id = OB_INVALID_ID;
    uint64_t current_data_version = 0;
    if (0 != tenant_id && OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, current_data_version))) {
      LOG_WARN("fail to get min data version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(fill_extra_info_(tenant_id, version,
               current_data_version, BUF_LEN, extra_buf))) {
      LOG_WARN("fail to fill extra info", KR(ret),
               K(tenant_id), K(version), K(current_data_version));
    } else if (OB_FAIL(RS_JOB_CREATE_WITH_RET(
               job_id, job_type, *sql_proxy_, "tenant_id", tenant_id,
               "extra_info", ObHexEscapeSqlStr(ObString(strlen(extra_buf), extra_buf))))) {
      LOG_WARN("fail to create rs job", KR(ret));
    } else if (job_id <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("job_id is invalid", KR(ret), K(job_id));
    } else {
      switch (action) {
        case obrpc::ObUpgradeJobArg::UPGRADE_POST_ACTION: {
          if (OB_FAIL(run_upgrade_post_job_(tenant_ids, version))) {
            LOG_WARN("fail to run upgrade post job", KR(ret), K(version));
          }
          break;
        }
        case obrpc::ObUpgradeJobArg::UPGRADE_BEGIN: {
          if (OB_FAIL(run_upgrade_begin_action_(tenant_ids))) {
            LOG_WARN("fail to run upgrade begin job", KR(ret));
          }
          break;
        }
        case obrpc::ObUpgradeJobArg::UPGRADE_SYSTEM_VARIABLE: {
          if (OB_FAIL(run_upgrade_system_variable_job_(tenant_ids))) {
            LOG_WARN("fail to run upgrade system variable job", KR(ret));
          }
          break;
        }
        case obrpc::ObUpgradeJobArg::UPGRADE_SYSTEM_TABLE: {
          if (OB_FAIL(run_upgrade_system_table_job_(tenant_ids))) {
            LOG_WARN("fail to run upgrade system table job", KR(ret));
          }
          break;
        }
        case obrpc::ObUpgradeJobArg::UPGRADE_VIRTUAL_SCHEMA: {
          if (OB_FAIL(run_upgrade_virtual_schema_job_(tenant_ids))) {
            LOG_WARN("fail to run upgrade virtual schema job", KR(ret));
          }
          break;
        }
        case obrpc::ObUpgradeJobArg::UPGRADE_SYSTEM_PACKAGE: {
          if (OB_FAIL(run_upgrade_system_package_job_())) {
            LOG_WARN("fail to run upgrade system package job", KR(ret));
          }
          break;
        }
        case obrpc::ObUpgradeJobArg::UPGRADE_ALL_POST_ACTION: {
          if (OB_FAIL(run_upgrade_all_post_action_(tenant_ids))) {
            LOG_WARN("fail to run upgrade all post action", KR(ret));
          }
          break;
        }
        case obrpc::ObUpgradeJobArg::UPGRADE_INSPECTION: {
          if (OB_FAIL(run_upgrade_inspection_job_(tenant_ids))) {
            LOG_WARN("fail to run upgrade inspection job", KR(ret));
          }
          break;
        }
        case obrpc::ObUpgradeJobArg::UPGRADE_END: {
          if (OB_FAIL(run_upgrade_end_action_(tenant_ids))) {
            LOG_WARN("fail to run upgrade end job", KR(ret));
          }
          break;
        }
        case obrpc::ObUpgradeJobArg::UPGRADE_ALL: {
          if (OB_FAIL(run_upgrade_all_(tenant_ids))) {
            LOG_WARN("fail to run upgrade all action", KR(ret));
          }
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support upgrade job type", KR(ret), K(action));
          break;
        }
      }
    }

    if (OB_SUCC(ret)) {
      const int64_t BUF_LEN = OB_SERVER_VERSION_LENGTH;
      char min_cluster_version_str[BUF_LEN] = {'\0'};
      const uint64_t min_cluster_version = GET_MIN_CLUSTER_VERSION();
      char targe_data_version_str[BUF_LEN] = {'\0'};
      const uint64_t target_data_version = DATA_CURRENT_VERSION;
      share::ObServerInfoInTable::ObBuildVersion build_version;
      if (OB_INVALID_INDEX == ObClusterVersion::print_version_str(
          min_cluster_version_str, BUF_LEN, min_cluster_version)) {
         ret = OB_SIZE_OVERFLOW;
         LOG_WARN("fail to print version str", KR(ret), K(min_cluster_version));
      } else if (OB_INVALID_INDEX == ObClusterVersion::print_version_str(
                 targe_data_version_str, BUF_LEN, target_data_version)) {
         ret = OB_SIZE_OVERFLOW;
         LOG_WARN("fail to print version str", KR(ret), K(target_data_version));
      } else if (OB_FAIL(observer::ObService::get_build_version(build_version))) {
        LOG_WARN("fail to get build version", KR(ret));
      } else if (0 != tenant_id) {
        char current_data_version_str[BUF_LEN] = {'\0'};
        if (OB_INVALID_INDEX == ObClusterVersion::print_version_str(
            current_data_version_str, BUF_LEN, current_data_version)) {
           ret = OB_SIZE_OVERFLOW;
           LOG_WARN("fail to print version str", KR(ret), K(current_data_version));
        }
        CLUSTER_EVENT_SYNC_ADD("UPGRADE",
                               ObRsJobTableOperator::get_job_type_str(job_type),
                               "cluster_version", min_cluster_version_str,
                               "build_version", build_version.ptr(),
                               "target_data_version", targe_data_version_str,
                               "current_data_version", current_data_version_str,
                               "tenant_id", tenant_id)
      } else {
        CLUSTER_EVENT_SYNC_ADD("UPGRADE",
                               ObRsJobTableOperator::get_job_type_str(job_type),
                               "cluster_version", min_cluster_version_str,
                               "build_version", build_version.ptr(),
                               "target_data_version", targe_data_version_str);
      }
    }

    if (job_id > 0) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = RS_JOB_COMPLETE(job_id, ret, *sql_proxy_))) {
        LOG_ERROR("fail to complete job", K(tmp_ret), KR(ret), K(job_id));
        ret = OB_FAIL(ret) ? ret : tmp_ret;
      }
    }
    execute_ = false;
  }
  return ret;
}

int ObUpgradeExecutor::fill_extra_info_(
    const uint64_t tenant_id,
    const int64_t specified_version,
    const uint64_t current_data_version,
    const int64_t buf_len,
    char *buf)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  const int64_t VERSION_LEN = common::OB_CLUSTER_VERSION_LENGTH;
  char version_buf[VERSION_LEN] = {'\0'};
  int64_t version_len = 0;
  if (specified_version > 0) {
    if (OB_INVALID_INDEX == (version_len = ObClusterVersion::print_version_str(
        version_buf, VERSION_LEN, static_cast<uint64_t>(specified_version)))) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("fail to print version", KR(ret), K(specified_version));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, len,
               "SPECIFIED_DATA_VERSION: '%s'", version_buf))) {
      LOG_WARN("fail to print string", KR(ret), K(len));
    }
  } else {
    if (OB_SUCC(ret)) {
      uint64_t target_data_version = DATA_CURRENT_VERSION;
      if (OB_INVALID_INDEX == (version_len = ObClusterVersion::print_version_str(
          version_buf, VERSION_LEN, target_data_version))) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("fail to print version", KR(ret), K(target_data_version));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, len,
                 "TARGET_DATA_VERSION: '%s'", version_buf))) {
        LOG_WARN("fail to print string", KR(ret), K(len));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (0 != tenant_id) {
      // record current data version when upgrade single tenant
      if (OB_UNLIKELY(len < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("str should not be empty", KR(ret), K(len));
      } else if (OB_INVALID_INDEX == (version_len = ObClusterVersion::print_version_str(
          version_buf, VERSION_LEN, current_data_version))) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("fail to print version", KR(ret), K(current_data_version));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, len,
                 ", CURRENT_DATA_VERSION: '%s'", version_buf))) {
        LOG_WARN("fail to print string", KR(ret), K(len));
      }
    }
  }
  return ret;
}

// Python upgrade script may set enable_ddl = false before it run upgrade job.
// this function won't raise current_data_version
int ObUpgradeExecutor::run_upgrade_post_job_(
    const common::ObIArray<uint64_t> &tenant_ids,
    const int64_t version)
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
    ObBaseUpgradeProcessor *processor = NULL;
    int64_t backup_ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    if (OB_FAIL(upgrade_processors_.get_processor_by_version(
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
  }
  return ret;
}

int ObUpgradeExecutor::run_upgrade_begin_action_(
    const common::ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  common::hash::ObHashMap<uint64_t, share::SCN> tenants_sys_ls_target_scn;
  lib::ObMemAttr attr(OB_SYS_TENANT_ID, "UPGRADE");
  const int BUCKET_NUM  = hash::cal_next_prime(tenant_ids.count());
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", KR(ret));
  } else if (OB_FAIL(tenants_sys_ls_target_scn.create(BUCKET_NUM, attr))) {
    LOG_WARN("fail to create tenants_sys_ls_target_scn", KR(ret));
  } else {
    int64_t backup_ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    tenants_sys_ls_target_scn.clear();
    for (int64_t i = tenant_ids.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
      const uint64_t tenant_id = tenant_ids.at(i);
      int64_t start_ts = ObTimeUtility::current_time();
      FLOG_INFO("[UPGRADE] start to run upgrade begin action", K(tenant_id));
      if (OB_FAIL(check_stop())) {
        LOG_WARN("executor should stopped", KR(ret));
      } else if (OB_TMP_FAIL(run_upgrade_begin_action_(tenant_id, tenants_sys_ls_target_scn))) {
        LOG_WARN("fail to upgrade begin action", KR(ret), K(tenant_id));
        backup_ret = OB_SUCCESS == backup_ret ? tmp_ret : backup_ret;
      }
      FLOG_INFO("[UPGRADE] finish run upgrade begin action step 1/2, write upgrade barrier log",
                KR(ret), K(tenant_id), "cost", ObTimeUtility::current_time() - start_ts);
    } // end for
    ret = OB_SUCC(ret) ? backup_ret : ret;
    if (OB_SUCC(ret)) {
      int64_t start_ts_step2 = ObTimeUtility::current_time();
      ret = ObLSServiceHelper::wait_all_tenants_user_ls_sync_scn(tenants_sys_ls_target_scn);
      FLOG_INFO("[UPGRADE] finish run upgrade begin action step 2/2, wait all tenants' sync_scn",
          KR(ret), "cost", ObTimeUtility::current_time() - start_ts_step2);
    }
  }
  return ret;
}

int ObUpgradeExecutor::run_upgrade_begin_action_(
    const uint64_t tenant_id,
    common::hash::ObHashMap<uint64_t, share::SCN> &tenants_sys_ls_target_scn)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  share::SCN sys_ls_target_scn = SCN::invalid_scn();
  ObConflictCaseWithClone case_to_check(ObConflictCaseWithClone::UPGRADE);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", KR(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_, tenant_id))) {
    LOG_WARN("fail to start trans", KR(ret), K(tenant_id));
  } else {
    ObGlobalStatProxy proxy(trans, tenant_id);
    // get target_data_version
    uint64_t target_data_version = 0;
    const uint64_t DEFAULT_DATA_VERSION = DATA_VERSION_4_0_0_0;
    bool for_update = true;
    if (OB_FAIL(proxy.get_target_data_version(for_update, target_data_version))) {
      if (OB_ERR_NULL_VALUE == ret
          && GET_MIN_CLUSTER_VERSION() <= CLUSTER_VERSION_4_1_0_0) {
        // 4.0 -> 4.1
        uint64_t current_data_version = 0;
        ret = proxy.get_current_data_version(current_data_version);
        if (OB_ERR_NULL_VALUE != ret) {
          ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
          LOG_WARN("current data version should be not exist",
                   KR(ret), K(tenant_id), K(current_data_version));
        } else if (OB_FAIL(proxy.update_current_data_version(DEFAULT_DATA_VERSION))) {
          // overwrite ret
          LOG_WARN("fail to init current data version",
                   KR(ret), K(tenant_id), K(DEFAULT_DATA_VERSION));
        } else {
          target_data_version = DEFAULT_DATA_VERSION;
          LOG_INFO("[UPGRADE] init missing current data version",
                   KR(ret), K(tenant_id), K(DEFAULT_DATA_VERSION));
        }
      } else {
        LOG_WARN("fail to get target data version", KR(ret), K(tenant_id));
      }
    }
    // check tenant not in cloning procedure in trans
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObTenantSnapshotUtil::check_tenant_not_in_cloning_procedure(tenant_id, case_to_check))) {
      LOG_WARN("fail to check whether tenant is in cloning produre", KR(ret), K(tenant_id));
    }
    // try update target_data_version
    if (OB_FAIL(ret)) {
    } else if (target_data_version >= DATA_CURRENT_VERSION) {
      LOG_INFO("[UPGRADE] target data version is new enough, just skip",
               KR(ret), K(tenant_id), K(target_data_version));
    } else if (OB_FAIL(proxy.update_target_data_version(DATA_CURRENT_VERSION))) {
      LOG_WARN("fail to update target data version",
               KR(ret), K(tenant_id), "version", DATA_CURRENT_VERSION);
    } else if (is_user_tenant(tenant_id)
               && OB_FAIL(OB_PRIMARY_STANDBY_SERVICE.write_upgrade_barrier_log(
                                                     trans, tenant_id, DATA_CURRENT_VERSION))) {
      LOG_WARN("fail to write_upgrade_barrier_log",
               KR(ret), K(tenant_id), "version", DATA_CURRENT_VERSION);
    } else {
      LOG_INFO("[UPGRADE] update target data version",
               KR(ret), K(tenant_id), "version", DATA_CURRENT_VERSION);
    }
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", KR(tmp_ret), K(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!is_user_tenant(tenant_id)) {
    // skip
  } else if (OB_FAIL(ObGlobalStatProxy::get_target_data_version_ora_rowscn(tenant_id, sys_ls_target_scn))) {
    LOG_WARN("fail to get sys_ls_target_scn", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tenants_sys_ls_target_scn.set_refactored(
      tenant_id,
      sys_ls_target_scn,
      0 /* flag:  0 shows that not cover existing object. */))) {
    LOG_WARN("fail to push an element into tenants_sys_ls_target_scn", KR(ret), K(tenant_id),
        K(sys_ls_target_scn));
  }
  return ret;
}

int ObUpgradeExecutor::run_upgrade_system_variable_job_(
    const common::ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", KR(ret));
  } else {
    int backup_ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = tenant_ids.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
      const uint64_t tenant_id = tenant_ids.at(i);
      int64_t start_ts = ObTimeUtility::current_time();
      FLOG_INFO("[UPGRADE] start to run upgrade system variable job", K(tenant_id));
      if (OB_FAIL(check_stop())) {
        LOG_WARN("executor should stopped", KR(ret));
      } else if (OB_FAIL(check_schema_sync_(tenant_id))) {
        LOG_WARN("fail to check schema sync", KR(ret), K(tenant_id));
      } else if (OB_TMP_FAIL(ObUpgradeUtils::upgrade_sys_variable(*common_rpc_proxy_, *sql_proxy_, tenant_id))) {
        LOG_WARN("fail to upgrade sys variable", KR(tmp_ret), K(tenant_id));
        backup_ret = OB_SUCCESS == backup_ret ? tmp_ret : backup_ret;
      }
      FLOG_INFO("[UPGRADE] finish run upgrade system variable job",
                KR(tmp_ret), K(tenant_id), "cost", ObTimeUtility::current_time() - start_ts);
    } // end for
    ret = OB_SUCC(ret) ? backup_ret : ret;
  }
  return ret;
}

// NOTICE: enable_sys_table_ddl should be true before run this job.
int ObUpgradeExecutor::run_upgrade_system_table_job_(
    const common::ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", KR(ret));
  } else {
    int backup_ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = tenant_ids.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
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
    bool upgrade_virtual_schema = false;
    const int64_t timeout = GCONF._ob_ddl_timeout;
    for (int64_t i = 0; OB_SUCC(ret) && i < upgrade_table_ids.count(); i++) {
      const uint64_t table_id = upgrade_table_ids.at(i);
      int64_t start_ts = ObTimeUtility::current_time();
      FLOG_INFO("[UPGRADE] start upgrade system table", K(tenant_id), K(table_id));
      if (OB_FAIL(check_stop())) {
        LOG_WARN("check_cancel failed", KR(ret));
      } else if (OB_FAIL(arg.init(tenant_id, table_id, upgrade_virtual_schema))) {
        LOG_WARN("fail to init arg", KR(ret), K(tenant_id), K(table_id));
      } else if (OB_FAIL(check_schema_sync_(tenant_id))) {
        LOG_WARN("fail to check schema sync", KR(ret), K(tenant_id));
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


int ObUpgradeExecutor::run_upgrade_virtual_schema_job_(
    const common::ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", KR(ret));
  } else {
    int backup_ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    obrpc::ObUpgradeTableSchemaArg arg;
    uint64_t invalid_table_id = OB_INVALID_ID;
    bool upgrade_virtual_schema = true;
    // TODO:(yanmu.ztl) upgrade single virtual table/sys view
    int64_t timeout = GCONF._ob_ddl_timeout;
    for (int64_t i = tenant_ids.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
      const uint64_t tenant_id = tenant_ids.at(i);
      int64_t start_ts = ObTimeUtility::current_time();
      FLOG_INFO("[UPGRADE] start to run upgrade virtual schema job", K(tenant_id));
      if (OB_FAIL(check_stop())) {
        LOG_WARN("executor should stopped", KR(ret));
      } else if (OB_FAIL(arg.init(tenant_id, invalid_table_id, upgrade_virtual_schema))) {
        LOG_WARN("fail to init arg", KR(ret), K(tenant_id));
      } else if (OB_FAIL(check_schema_sync_(tenant_id))) {
        LOG_WARN("fail to check schema sync", KR(ret), K(tenant_id));
      } else if (OB_TMP_FAIL(common_rpc_proxy_->timeout(timeout).upgrade_table_schema(arg))) {
        LOG_WARN("fail to upgrade virtual schema", KR(tmp_ret), K(arg));
        backup_ret = OB_SUCCESS == backup_ret ? tmp_ret : backup_ret;
      }
      FLOG_INFO("[UPGRADE] finish run upgrade virtual schema job",
                KR(tmp_ret), K(tenant_id), "cost", ObTimeUtility::current_time() - start_ts);
    } // end for
    ret = OB_SUCC(ret) ? backup_ret : ret;
  }
  return ret;
}

int ObUpgradeExecutor::run_upgrade_system_package_job_()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(check_schema_sync_(tenant_id))) {
    LOG_WARN("fail to check schema sync", KR(ret), K(tenant_id));
  } else if (OB_FAIL(upgrade_mysql_system_package_job_())) {
    LOG_WARN("fail to upgrade mysql system package", KR(ret));
#ifdef OB_BUILD_ORACLE_PL
  } else if (OB_FAIL(check_schema_sync_(tenant_id))) {
    LOG_WARN("fail to check schema sync", KR(ret), K(tenant_id));
  } else if (OB_FAIL(upgrade_oracle_system_package_job_())) {
    LOG_WARN("fail to upgrade mysql system package", KR(ret));
#endif
  }
  return ret;
}

int ObUpgradeExecutor::upgrade_mysql_system_package_job_()
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  FLOG_INFO("[UPGRADE] start to run upgrade mysql system package job");
  int64_t timeout = GCONF._ob_ddl_timeout;
  const char *create_package_sql =
        "CREATE OR REPLACE PACKAGE __DBMS_UPGRADE \
           PROCEDURE UPGRADE(package_name VARCHAR(1024)); \
           PROCEDURE UPGRADE_ALL(); \
         END;";
  const char *create_package_body_sql =
        "CREATE OR REPLACE PACKAGE BODY __DBMS_UPGRADE \
           PROCEDURE UPGRADE(package_name VARCHAR(1024)); \
             PRAGMA INTERFACE(c, UPGRADE_SINGLE); \
           PROCEDURE UPGRADE_ALL(); \
             PRAGMA INTERFACE(c, UPGRADE_ALL); \
         END;";
  const char *upgrade_sql = "CALL __DBMS_UPGRADE.UPGRADE_ALL();";
  ObTimeoutCtx ctx;
  int64_t affected_rows = 0;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(ctx.set_timeout(timeout))) {
    LOG_WARN("fail to set timeout", KR(ret));
  } else if (OB_FAIL(sql_proxy_->write(
             OB_SYS_TENANT_ID, create_package_sql, affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), "sql", create_package_sql);
  } else if (0 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows expected to be zero", KR(ret), K(affected_rows));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor is stop", KR(ret));
  } else if (OB_FAIL(ctx.set_timeout(timeout))) {
    LOG_WARN("fail to set timeout", KR(ret));
  } else if (OB_FAIL(sql_proxy_->write(
             OB_SYS_TENANT_ID, create_package_body_sql, affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), "sql", create_package_body_sql);
  } else if (0 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows expected to be zero", KR(ret), K(affected_rows));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor is stop", KR(ret));
  } else if (OB_FAIL(ctx.set_timeout(timeout))) {
    LOG_WARN("fail to set timeout", KR(ret));
  } else if (OB_FAIL(sql_proxy_->write(
             OB_SYS_TENANT_ID, upgrade_sql, affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), "sql", upgrade_sql);
  } else if (0 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows expected to be zero", KR(ret), K(affected_rows));
  }
  FLOG_INFO("[UPGRADE] finish run upgrade mysql system package job",
            KR(ret), "cost", ObTimeUtility::current_time() - start_ts);
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObUpgradeExecutor::upgrade_oracle_system_package_job_()
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  FLOG_INFO("[UPGRADE] start to run upgrade oracle system package job");
  ObCompatibilityMode mode = ObCompatibilityMode::ORACLE_MODE;
  int64_t timeout = GCONF._ob_ddl_timeout;
  const char *create_package_sql =
        "CREATE OR REPLACE PACKAGE \"__DBMS_UPGRADE\" IS \
           PROCEDURE UPGRADE(package_name VARCHAR2); \
           PROCEDURE UPGRADE_ALL; \
         END;";
  const char *create_package_body_sql =
        "CREATE OR REPLACE PACKAGE BODY \"__DBMS_UPGRADE\" IS \
           PROCEDURE UPGRADE(package_name VARCHAR2); \
             PRAGMA INTERFACE(c, UPGRADE_SINGLE); \
           PROCEDURE UPGRADE_ALL; \
             PRAGMA INTERFACE(c, UPGRADE_ALL); \
         END;";
  const char *upgrade_sql = "CALL \"__DBMS_UPGRADE\".UPGRADE_ALL();";
  ObTimeoutCtx ctx;
  int64_t affected_rows = 0;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(ctx.set_timeout(timeout))) {
    LOG_WARN("fail to set timeout", KR(ret));
  } else if (OB_FAIL(sql_proxy_->write(
             OB_SYS_TENANT_ID, create_package_sql,
             affected_rows, static_cast<int64_t>(mode)))) {
    LOG_WARN("fail to execute sql", KR(ret), "sql", create_package_sql);
  } else if (0 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows expected to be zero", KR(ret), K(affected_rows));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor is stop", KR(ret));
  } else if (OB_FAIL(ctx.set_timeout(timeout))) {
    LOG_WARN("fail to set timeout", KR(ret));
  } else if (OB_FAIL(sql_proxy_->write(
             OB_SYS_TENANT_ID, create_package_body_sql,
             affected_rows, static_cast<int64_t>(mode)))) {
    LOG_WARN("fail to execute sql", KR(ret), "sql", create_package_body_sql);
  } else if (0 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows expected to be zero", KR(ret), K(affected_rows));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor is stop", KR(ret));
  } else if (OB_FAIL(ctx.set_timeout(timeout))) {
    LOG_WARN("fail to set timeout", KR(ret));
  } else if (OB_FAIL(sql_proxy_->write(
             OB_SYS_TENANT_ID, upgrade_sql,
             affected_rows, static_cast<int64_t>(mode)))) {
    LOG_WARN("fail to execute sql", KR(ret), "sql", upgrade_sql);
  } else if (0 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows expected to be zero", KR(ret), K(affected_rows));
  }
  FLOG_INFO("[UPGRADE] finish run upgrade oracle system package job",
            KR(ret), "cost", ObTimeUtility::current_time() - start_ts);
  return ret;
}
#endif

int ObUpgradeExecutor::run_upgrade_all_post_action_(
    const common::ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", KR(ret));
  } else {
    int64_t backup_ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = tenant_ids.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
      const uint64_t tenant_id = tenant_ids.at(i);
      int64_t start_ts = ObTimeUtility::current_time();
      FLOG_INFO("[UPGRADE] start to run upgrade all post action", K(tenant_id));
      if (OB_FAIL(check_stop())) {
        LOG_WARN("executor should stopped", KR(ret));
      } else if (OB_TMP_FAIL(run_upgrade_all_post_action_(tenant_id))) {
        LOG_WARN("fail to upgrade all post action", KR(ret), K(tenant_id));
        backup_ret = OB_SUCCESS == backup_ret ? tmp_ret : backup_ret;
      }
      FLOG_INFO("[UPGRADE] finish run upgrade all post action",
                KR(ret), K(tenant_id), "cost", ObTimeUtility::current_time() - start_ts);
    } // end for
    ret = OB_SUCC(ret) ? backup_ret : ret;
  }
  return ret;
}

int ObUpgradeExecutor::run_upgrade_all_post_action_(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", KR(ret));
  } else {
    uint64_t current_data_version = 0;
    int64_t start_idx = OB_INVALID_INDEX;
    int64_t end_idx = OB_INVALID_INDEX;
    ObGlobalStatProxy proxy(*sql_proxy_, tenant_id);
    if (OB_FAIL(proxy.get_current_data_version(current_data_version))) {
      LOG_WARN("fail to get current data version",
               KR(ret), K(tenant_id), K(current_data_version));
    } else if (OB_FAIL(upgrade_processors_.get_processor_idx_by_range(
                       current_data_version, DATA_CURRENT_VERSION,
                       start_idx, end_idx))) {
      LOG_WARN("fail to get processor by version", KR(ret), K(current_data_version));
    }
    int64_t version = OB_INVALID_VERSION;
    for (int64_t i = start_idx + 1; OB_SUCC(ret) && i <= end_idx; i++) {
      ObBaseUpgradeProcessor *processor  = NULL;
      if (OB_FAIL(check_stop())) {
        LOG_WARN("executor should stopped", KR(ret));
      } else if (OB_FAIL(upgrade_processors_.get_processor_by_idx(i, processor))) {
        LOG_WARN("fail to get processor", KR(ret), K(current_data_version), K(i));
      } else if (FALSE_IT(version = processor->get_version())) {
      } else if (FALSE_IT(processor->set_tenant_id(tenant_id))) {
      } else if (OB_FAIL(check_schema_sync_(tenant_id))) {
        LOG_WARN("fail to check schema sync", KR(ret), K(tenant_id));
      } else if (OB_FAIL(processor->post_upgrade())) {
        LOG_WARN("run post upgrade by version failed", KR(ret), K(tenant_id), K(version));
      } else if (OB_FAIL(check_schema_sync_(tenant_id))) {
        LOG_WARN("fail to check schema sync", KR(ret), K(tenant_id));
      } else if (i < end_idx) {
        if (OB_FAIL(proxy.update_current_data_version(version))) {
          LOG_WARN("fail to update current data version", KR(ret), K(tenant_id), K(version));
        }
      } else if (OB_FAIL(update_final_current_data_version_(tenant_id, version))) {
        LOG_WARN("fail to update final current data version", KR(ret), K(tenant_id), K(version));
      }
    } // end for
  }
  return ret;
}

// for the final version, we need to write a data version barrier log when updating the
// current_data_version
int ObUpgradeExecutor::update_final_current_data_version_(const uint64_t tenant_id,
                                                          const int64_t version)
{
  int ret = OB_SUCCESS;

  ObMySQLTransaction trans;
  if (OB_FAIL(trans.start(sql_proxy_, tenant_id))) {
    LOG_WARN("fail to start trans", KR(ret), K(tenant_id));
  } else {
    ObGlobalStatProxy end_proxy(trans, tenant_id);
    if (OB_FAIL(end_proxy.update_current_data_version(version))) {
      LOG_WARN("fail to update current data version", KR(ret), K(tenant_id), K(version));
    } else if (is_user_tenant(tenant_id) &&
               OB_FAIL(OB_PRIMARY_STANDBY_SERVICE.write_upgrade_data_version_barrier_log(
                   trans, tenant_id, version))) {
      LOG_WARN("fail to write_upgrade_data_version_barrier_log", KR(ret), K(tenant_id), K(version));
    }
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", KR(tmp_ret), K(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }

  return ret;
}

int ObUpgradeExecutor::run_upgrade_inspection_job_(
    const common::ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", KR(ret));
  } else {
    int backup_ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = tenant_ids.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
      const uint64_t tenant_id = tenant_ids.at(i);
      int64_t start_ts = ObTimeUtility::current_time();
      FLOG_INFO("[UPGRADE] start to run upgrade inspection job", K(tenant_id));
      if (OB_FAIL(check_stop())) {
        LOG_WARN("executor should stopped", KR(ret));
      } else if (OB_FAIL(check_schema_sync_(tenant_id))) {
        LOG_WARN("fail to check schema sync", KR(ret), K(tenant_id));
      } else if (OB_TMP_FAIL(root_inspection_->check_tenant(tenant_id))) {
        LOG_WARN("fail to do upgrade inspection", KR(tmp_ret), K(tenant_id));
        backup_ret = OB_SUCCESS == backup_ret ? tmp_ret : backup_ret;
      }
      FLOG_INFO("[UPGRADE] finish run upgrade inspection job",
                KR(tmp_ret), K(tenant_id), "cost", ObTimeUtility::current_time() - start_ts);
    } // end for
    ret = OB_SUCC(ret) ? backup_ret : ret;
  }
  return ret;
}

int ObUpgradeExecutor::run_upgrade_end_action_(
    const common::ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", KR(ret));
  } else {
    int64_t backup_ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = tenant_ids.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
      const uint64_t tenant_id = tenant_ids.at(i);
      int64_t start_ts = ObTimeUtility::current_time();
      FLOG_INFO("[UPGRADE] start to run upgrade end action", K(tenant_id));
      if (OB_FAIL(check_stop())) {
        LOG_WARN("executor should stopped", KR(ret));
      } else if (OB_FAIL(check_schema_sync_(tenant_id))) {
        LOG_WARN("fail to check schema sync", KR(ret), K(tenant_id));
      } else if (OB_TMP_FAIL(run_upgrade_end_action_(tenant_id))) {
        LOG_WARN("fail to upgrade end action", KR(ret), K(tenant_id));
        backup_ret = OB_SUCCESS == backup_ret ? tmp_ret : backup_ret;
      }
      FLOG_INFO("[UPGRADE] finish run upgrade end action",
                KR(ret), K(tenant_id), "cost", ObTimeUtility::current_time() - start_ts);
    } // end for
    ret = OB_SUCC(ret) ? backup_ret : ret;
  }
  return ret;
}

int ObUpgradeExecutor::run_upgrade_end_action_(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", KR(ret));
  } else {
    ObGlobalStatProxy proxy(*sql_proxy_, tenant_id);
    uint64_t target_data_version = 0;
    uint64_t current_data_version = 0;
    bool for_update = false;
    uint64_t data_version = 0;
    if (OB_FAIL(proxy.get_target_data_version(for_update, target_data_version))) {
      LOG_WARN("fail to get target data version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(proxy.get_current_data_version(current_data_version))) {
      LOG_WARN("fail to get current data version", KR(ret), K(tenant_id));
    } else if (target_data_version != current_data_version
               || target_data_version != DATA_CURRENT_VERSION) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("data_version not match, upgrade process should be run",
               KR(ret), K(tenant_id), K(target_data_version), K(current_data_version));
    } else {
      // target_data_version == current_data_version == DATA_CURRENT_VERSION
      if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
        LOG_WARN("fail to get min data version", KR(ret), K(tenant_id));
      } else if (data_version >= current_data_version) {
        LOG_INFO("[UPGRADE] data version is not less than current data version, just skip",
                 K(tenant_id), K(data_version), K(current_data_version));
      } else {
        HEAP_VAR(obrpc::ObAdminSetConfigItem, item) {
        ObSchemaGetterGuard guard;
        const ObSimpleTenantSchema *tenant = NULL;
        obrpc::ObAdminSetConfigArg arg;
        item.exec_tenant_id_ = OB_SYS_TENANT_ID;
        const int64_t timeout = GCONF.internal_sql_execute_timeout;
        int64_t pos = ObClusterVersion::print_version_str(
                      item.value_.ptr(), item.value_.capacity(),
                      current_data_version);
        if (pos <= 0) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("current_data_version is invalid",
                   KR(ret), K(tenant_id), K(current_data_version));
        } else if (OB_FAIL(GSCHEMASERVICE.get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
          LOG_WARN("fail to get schema guard", KR(ret));
        } else if (OB_FAIL(guard.get_tenant_info(tenant_id, tenant))) {
          LOG_WARN("fail to get tenant info", KR(ret), K(tenant_id));
        } else if (OB_ISNULL(tenant)) {
          ret = OB_TENANT_NOT_EXIST;
          LOG_WARN("tenant not exist", KR(ret), K(tenant_id));
        } else if (OB_FAIL(item.tenant_name_.assign(tenant->get_tenant_name()))) {
          LOG_WARN("fail to assign tenant name", KR(ret), K(tenant_id));
        } else if (OB_FAIL(item.name_.assign("compatible"))) {
          LOG_WARN("fail to assign config name", KR(ret), K(tenant_id));
        } else if (OB_FAIL(arg.items_.push_back(item))) {
          LOG_WARN("fail to push back item", KR(ret), K(item));
        } else if (OB_FAIL(common_rpc_proxy_->timeout(timeout).admin_set_config(arg))) {
          LOG_WARN("fail to set config", KR(ret), K(arg), K(timeout));
        } else {
          int64_t start_ts = ObTimeUtility::current_time();
          while (OB_SUCC(ret)) {
            if (OB_FAIL(check_stop())) {
              LOG_WARN("executor should stopped", KR(ret));
            } else if (ObTimeUtility::current_time() - start_ts >= timeout) {
              ret = OB_TIMEOUT;
              LOG_WARN("wait config taking effective failed",
                       KR(ret), K(tenant_id), K(timeout));
            } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
              LOG_WARN("fail to get min data version", KR(ret), K(tenant_id));
            } else if (data_version >= current_data_version) {
              LOG_INFO("[UPGRADE] config take effective", K(tenant_id),
                       "cost", ObTimeUtility::current_time() - start_ts);
              break;
            } else {
              LOG_INFO("[UPGRADE] config doesn't take effective", K(tenant_id));
              usleep(1 * 1000 * 1000L); // 1s
            }
          }
        }
        } // end HEAP_VAR
      }
    }
  }
  return ret;
}

int ObUpgradeExecutor::run_upgrade_all_(
    const common::ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  FLOG_INFO("[UPGRADE] start to run upgrade all action");
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", KR(ret));
  } else if (OB_FAIL(run_upgrade_begin_action_(tenant_ids))) {
    LOG_WARN("fail to run upgrade begin job", KR(ret));
  } else if (OB_FAIL(run_upgrade_system_variable_job_(tenant_ids))) {
    LOG_WARN("fail to run upgrade system variable job", KR(ret));
  } else if (OB_FAIL(run_upgrade_system_table_job_(tenant_ids))) {
    LOG_WARN("fail to run upgrade system table job", KR(ret));
  } else if (OB_FAIL(run_upgrade_virtual_schema_job_(tenant_ids))) {
    LOG_WARN("fail to run upgrade virtual schema job", KR(ret));
  } else if (has_exist_in_array(tenant_ids, OB_SYS_TENANT_ID)
             && OB_FAIL(run_upgrade_system_package_job_())) {
    LOG_WARN("fail to run upgrade system package job", KR(ret));
  } else if (OB_FAIL(run_upgrade_all_post_action_(tenant_ids))) {
    LOG_WARN("fail to run upgrade all post action", KR(ret));
  } else if (OB_FAIL(run_upgrade_inspection_job_(tenant_ids))) {
    LOG_WARN("fail to run upgrade inspection job", KR(ret));
  } else if (OB_FAIL(run_upgrade_end_action_(tenant_ids))) {
    LOG_WARN("fail to run upgrade end job", KR(ret));
  }
  FLOG_INFO("[UPGRADE] finish run upgrade all action",
            KR(ret), "cost", ObTimeUtility::current_time() - start_ts);
  return ret;
}

int ObUpgradeExecutor::construct_tenant_ids_(
    const common::ObIArray<uint64_t> &src_tenant_ids,
    common::ObIArray<uint64_t> &dst_tenant_ids)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> standby_tenants;
  ObTenantRole tenant_role(share::ObTenantRole::INVALID_TENANT);
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get sys tenant schema guard", KR(ret));
  } else if (src_tenant_ids.count() > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < src_tenant_ids.count(); i++) {
      const uint64_t tenant_id = src_tenant_ids.at(i);
      const ObSimpleTenantSchema *tenant_schema = nullptr;
      if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
        LOG_WARN("fail to get tenant info", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(tenant_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant schema is null", KR(ret), KP(tenant_schema));
      } else if (!tenant_schema->is_normal()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("tenant is not normal, can not do upgrade", KR(ret), K(tenant_id), KPC(tenant_schema));
      } else if (OB_FAIL(ObAllTenantInfoProxy::get_tenant_role(sql_proxy_, tenant_id, tenant_role))) {
        LOG_WARN("fail to get tenant role", KR(ret), K(tenant_id), K(tenant_role));
      } else if (!tenant_role.is_primary()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support to upgrade a non-primary tenant", KR(ret), K(tenant_id), K(tenant_role));
      }
    } // end for
    // tenant_list is specified
    if (FAILEDx(dst_tenant_ids.assign(src_tenant_ids))) {
      LOG_WARN("fail to assign tenant_ids", KR(ret));
    }
  } else {
    ObArray<uint64_t> tenant_ids;
    if (OB_FAIL(schema_service_->get_tenant_ids(tenant_ids))) {
      LOG_WARN("fail to get tenant_ids", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); i++) {
      const uint64_t tenant_id = tenant_ids.at(i);
      const ObSimpleTenantSchema *tenant_schema = nullptr;
      if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
        LOG_WARN("fail to get tenant info", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(tenant_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant schema is null", KR(ret), KP(tenant_schema));
      } else if (!tenant_schema->is_normal()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("tenant is not normal, can not do upgrade", KR(ret), K(tenant_id), KPC(tenant_schema));
      } else if (OB_FAIL(ObAllTenantInfoProxy::get_tenant_role(sql_proxy_, tenant_id, tenant_role))) {
        LOG_WARN("fail to get tenant role", KR(ret), K(tenant_id), K(tenant_role));
      } else if (tenant_role.is_standby()) {
        // skip
      } else if (!tenant_role.is_primary()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support do upgrade with tenant role is neither primary nor standby",
                 KR(ret), K(tenant_id), K(tenant_role));
      } else if (OB_FAIL(dst_tenant_ids.push_back(tenant_id))) {
        LOG_WARN("fail to push back tenant_id", KR(ret), K(tenant_id));
      }
    } // end for
  }
  return ret;
}

ObRsJobType ObUpgradeExecutor::convert_to_job_type_(
  const obrpc::ObUpgradeJobArg::Action &action)
{
  ObRsJobType job_type = JOB_TYPE_INVALID;
  switch (action) {
    case obrpc::ObUpgradeJobArg::UPGRADE_POST_ACTION: {
      job_type = ObRsJobType::JOB_TYPE_UPGRADE_POST_ACTION;
      break;
    }
    case obrpc::ObUpgradeJobArg::UPGRADE_BEGIN: {
      job_type = ObRsJobType::JOB_TYPE_UPGRADE_BEGIN;
      break;
    }
    case obrpc::ObUpgradeJobArg::UPGRADE_SYSTEM_VARIABLE: {
      job_type = ObRsJobType::JOB_TYPE_UPGRADE_SYSTEM_VARIABLE;
      break;
    }
    case obrpc::ObUpgradeJobArg::UPGRADE_SYSTEM_TABLE: {
      job_type = ObRsJobType::JOB_TYPE_UPGRADE_SYSTEM_TABLE;
      break;
    }
    case obrpc::ObUpgradeJobArg::UPGRADE_VIRTUAL_SCHEMA: {
      job_type = ObRsJobType::JOB_TYPE_UPGRADE_VIRTUAL_SCHEMA;
      break;
    }
    case obrpc::ObUpgradeJobArg::UPGRADE_SYSTEM_PACKAGE: {
      job_type = ObRsJobType::JOB_TYPE_UPGRADE_SYSTEM_PACKAGE;
      break;
    }
    case obrpc::ObUpgradeJobArg::UPGRADE_ALL_POST_ACTION: {
      job_type = ObRsJobType::JOB_TYPE_UPGRADE_ALL_POST_ACTION;
      break;
    }
    case obrpc::ObUpgradeJobArg::UPGRADE_INSPECTION: {
      job_type = ObRsJobType::JOB_TYPE_UPGRADE_INSPECTION;
      break;
    }
    case obrpc::ObUpgradeJobArg::UPGRADE_END: {
      job_type = ObRsJobType::JOB_TYPE_UPGRADE_END;
      break;
    }
    case obrpc::ObUpgradeJobArg::UPGRADE_ALL: {
      job_type = ObRsJobType::JOB_TYPE_UPGRADE_ALL;
      break;
    }
    default: {
      job_type = JOB_TYPE_INVALID;
      break;
    }
  }
  return job_type;
}

}//end rootserver
}//end oceanbase
