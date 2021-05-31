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

namespace oceanbase {
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace share::schema;

namespace rootserver {

int64_t ObUpgradeTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

ObAsyncTask* ObUpgradeTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObAsyncTask* task = NULL;
  int ret = OB_SUCCESS;
  const int64_t need_size = get_deep_copy_size();
  if (NULL == buf) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null", KR(ret));
  } else if (buf_size < need_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is not long enough", K(need_size), K(buf_size), KR(ret));
  } else {
    task = new (buf) ObUpgradeTask(*upgrade_executor_, version_);
  }
  return task;
}

int ObUpgradeTask::process()
{
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] start to do execute upgrade task", K(start), K_(version));
  int ret = OB_SUCCESS;
  if (OB_ISNULL(upgrade_executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("upgrade_executor_ is null", KR(ret));
  } else if (OB_FAIL(upgrade_executor_->execute(version_))) {
    LOG_WARN("fail to execute upgrade task", KR(ret));
  }
  LOG_INFO(
      "[UPGRADE] finish execute upgrade task", KR(ret), K_(version), "cost_us", ObTimeUtility::current_time() - start);
  return ret;
}

ObUpgradeExecutor::ObUpgradeExecutor()
    : inited_(false),
      stopped_(false),
      execute_(false),
      rwlock_(),
      sql_proxy_(NULL),
      rpc_proxy_(NULL),
      schema_service_(NULL),
      upgrade_processors_()
{}

int ObUpgradeExecutor::init(share::schema::ObMultiVersionSchemaService& schema_service, common::ObMySQLProxy& sql_proxy,
    obrpc::ObSrvRpcProxy& rpc_proxy)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can't init twice", KR(ret));
  } else if (OB_FAIL(upgrade_processors_.init(
                 ObBaseUpgradeProcessor::UPGRADE_MODE_OB, sql_proxy, rpc_proxy, schema_service, *this))) {
    LOG_WARN("fail to init upgrade processors", KR(ret));
  } else {
    schema_service_ = &schema_service;
    sql_proxy_ = &sql_proxy;
    rpc_proxy_ = &rpc_proxy;
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
  const uint64_t WAIT_US = 100 * 1000L;            // 100ms
  const uint64_t MAX_WAIT_US = 10 * 1000 * 1000L;  // 10s
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
      usleep(WAIT_US);
    }
  }
  return ret;
}

int ObUpgradeExecutor::check_stop() const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(rwlock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
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

int ObUpgradeExecutor::set_execute_mark()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(rwlock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
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
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (stopped_ || execute_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN(
        "status not matched", KR(ret), "stopped", stopped_ ? "true" : "false", "build", execute_ ? "true" : "false");
  }
  return ret;
}

// wait schema sync in cluster
int ObUpgradeExecutor::check_schema_sync()
{
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] start to check schema sync", K(start));
  int ret = OB_SUCCESS;
  bool enable_ddl = GCONF.enable_ddl;
  bool enable_sys_table_ddl = GCONF.enable_sys_table_ddl;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (enable_ddl || enable_sys_table_ddl) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("ddl should disable now", KR(ret), K(enable_ddl), K(enable_sys_table_ddl));
  } else {
    const int64_t WAIT_US = 1000 * 1000L;  // 1 second
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
        usleep(static_cast<useconds_t>((WAIT_US)));
      }
    }
  }
  LOG_INFO("[UPGRADE] check schema sync finish", KR(ret), "cost_us", ObTimeUtility::current_time() - start);
  return ret;
}

int ObUpgradeExecutor::get_tenant_ids(common::ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", KR(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else if (OB_FAIL(guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get tenant_ids", KR(ret));
  }
  return ret;
}

int ObUpgradeExecutor::execute(const int64_t version)
{
  ObCurTraceId::init(GCONF.self_addr_);
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(set_execute_mark())) {
    LOG_WARN("fail to set execute mark", KR(ret));
  } else {
    int64_t job_id = OB_INVALID_ID;
    ObRsJobType job_type = ObRsJobType::JOB_TYPE_RUN_UPGRADE_POST_JOB;
    char version_str[common::ObClusterVersion::MAX_VERSION_ITEM] = {0};
    int64_t len = ObClusterVersion::print_version_str(version_str, common::ObClusterVersion::MAX_VERSION_ITEM, version);
    if (OB_ISNULL(sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql_proxy_ is null", KR(ret));
    } else if (OB_FAIL(check_stop())) {
      LOG_WARN("executor should stopped", KR(ret));
    } else if (OB_FAIL(
                   RS_JOB_CREATE_WITH_RET(job_id, job_type, *sql_proxy_, "extra_info", ObString(len, version_str)))) {
      LOG_WARN("fail to create rs job", KR(ret));
    } else if (job_id <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("job_id is invalid", KR(ret), K(job_id));
    } else if (OB_FAIL(run_upgrade_job(version))) {
      LOG_WARN("fail to run upgrade job", KR(ret), K(job_id), K(version));
    }
    int tmp_ret = OB_SUCCESS;
    if (job_id > 0) {
      if (OB_SUCCESS != (tmp_ret = RS_JOB_COMPLETE(job_id, ret, *sql_proxy_))) {
        LOG_ERROR("fail to complete job", K(tmp_ret), KR(ret), K(job_id));
        ret = OB_FAIL(ret) ? ret : tmp_ret;
      }
    }
    execute_ = false;
  }
  return ret;
}

// Python upgrade script will set enable_ddl = false before it run upgrade job.
int ObUpgradeExecutor::run_upgrade_job(const int64_t version)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  ObBaseUpgradeProcessor* processor = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", KR(ret));
  } else if (version < CLUSTER_VERSION_2270 || !ObUpgradeChecker::check_cluster_version_exist(version)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported version to run upgrade job", KR(ret), K(version));
  } else if (OB_FAIL(get_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get tenant_ids", KR(ret), K(version));
  } else if (OB_FAIL(upgrade_processors_.get_processor_by_version(version, processor))) {
    LOG_WARN("fail to get processor by version", KR(ret), K(version));
  } else {
    // 1. Run upgrade jobs(by tenant_id desc) in primary cluster.
    // 2. Only run sys tenant's upgrade job in standby clusters.
    for (int64_t i = tenant_ids.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
      const uint64_t tenant_id = tenant_ids.at(i);
      if (OB_SYS_TENANT_ID == tenant_id || !GCTX.is_standby_cluster()) {
        int64_t start_ts = ObTimeUtility::current_time();
        int64_t current_version = processor->get_version();
        processor->set_tenant_id(tenant_id);
        LOG_INFO("[UPGRADE] start to run post upgrade job by version", K(tenant_id), K(current_version));
        if (OB_FAIL(processor->post_upgrade())) {
          LOG_WARN("run post upgrade by version failed", KR(ret), K(tenant_id), K(current_version));
        }
        LOG_INFO("[UPGRADE] finish post upgrade job by version",
            KR(ret),
            K(tenant_id),
            K(current_version),
            "cost",
            ObTimeUtility::current_time() - start_ts);
      }
    }
  }
  return ret;
}

}  // namespace rootserver
}  // namespace oceanbase
