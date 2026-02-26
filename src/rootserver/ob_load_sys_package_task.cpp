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

#include "src/rootserver/ob_load_sys_package_task.h"
#include "src/rootserver/ob_root_service.h"
#include "src/pl/ob_pl_package_manager.h"

namespace oceanbase
{
namespace rootserver
{

ObLoadSysPackageTask::ObLoadSysPackageTask(
    ObRootService &root_service,
    int64_t fail_count)
  : ObAsyncTimerTask(root_service.task_queue_), root_service_(root_service),
  fail_count_(fail_count)
{
  // retry until success
  set_retry_times(INT64_MAX);
}

ObAsyncTask *ObLoadSysPackageTask::deep_copy(char *buf, const int64_t buf_size) const
{
  ObLoadSysPackageTask *task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN_RET(OB_BUF_NOT_ENOUGH, "buffer not large enough", K(buf_size), KP(buf));
  } else {
    task = new (buf) ObLoadSysPackageTask(root_service_, fail_count_);
  }
  return task;
}
ERRSIM_POINT_DEF(ERRSIM_LOAD_PACKAGE_ERROR);
int ObLoadSysPackageTask::load_package(const ObCompatibilityMode &compat_mode)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  FLOG_INFO("[LOAD_SYS_PACKAGE] load sys package begin", K(compat_mode));
  ObMySQLProxy& sql_proxy = root_service_.get_sql_proxy();
  int64_t job_id = OB_INVALID_ID;
  if (compat_mode == ObCompatibilityMode::MYSQL_MODE) {
    ret = RS_JOB_FIND(LOAD_MYSQL_SYS_PACKAGE, job_id, sql_proxy, "tenant_id", OB_SYS_TENANT_ID);
  } else if (compat_mode == ObCompatibilityMode::ORACLE_MODE) {
    ret = RS_JOB_FIND(LOAD_ORACLE_SYS_PACKAGE, job_id, sql_proxy, "tenant_id", OB_SYS_TENANT_ID);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not support compat_mode", KR(ret), K(compat_mode));
  }
  if (OB_FAIL(ret)) {
    if (ret == OB_ENTRY_NOT_EXIST) {
      // success rs job cannot be found by RS_JOB_FIND
      LOG_INFO("[LOAD_SYS_PACKAGE] rs job is not INPROGRESS, no need to load package", KR(ret), K(compat_mode));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("[LOAD_SYS_PACKAGE] failed to get INPROGRESS rs job", KR(ret), K(compat_mode));
    }
  } else if (OB_FAIL(pl::ObPLPackageManager::load_all_common_sys_package(sql_proxy, compat_mode,
          false/*from_file*/))) {
    LOG_WARN("failed to load package", KR(ret), K(compat_mode));
  } else if (OB_FAIL(ERRSIM_LOAD_PACKAGE_ERROR)) {
    LOG_WARN("ERRSIM_LOAD_PACKAGE_ERROR", KR(ret));
  } else if (OB_FAIL(RS_JOB_COMPLETE(job_id, 0/*result_code*/, sql_proxy))) {
    LOG_WARN("failed to complete rs job", KR(ret), K(job_id));
  }
  FLOG_INFO("[LOAD_SYS_PACKAGE] load sys package end", K(ret), K(compat_mode));
  return ret;
}

int ObLoadSysPackageTask::process()
{
  int ret = OB_SUCCESS;
  ObMySQLProxy& sql_proxy = root_service_.get_sql_proxy();
  if (OB_FAIL(load_package(ObCompatibilityMode::MYSQL_MODE))) {
    LOG_WARN("failed to load mysql sys package", KR(ret));
  } else if (OB_FAIL(load_package(ObCompatibilityMode::ORACLE_MODE))) {
    LOG_WARN("failed to load oracle sys package", KR(ret));
  }
  if (OB_FAIL(ret)) {
    fail_count_++;
    const int MAX_WARN_FAIL_COUNT = 3;
    if (fail_count_ >= MAX_WARN_FAIL_COUNT) {
      LOG_ERROR("failed to load sys package, will retry", KR(ret));
    } else {
      LOG_WARN("failed to load sys package, will retry", KR(ret));
    }
  }
  FLOG_INFO("[LOAD_SYS_PACKAGE] load all sys package finished", KR(ret));
  return ret;
}

// mode == mysql will only wait mysql job
// mode == oracle will only wait oracle job
// otherwise it will wait all job
int ObLoadSysPackageTask::wait_sys_package_ready(
    ObMySQLProxy &sql_proxy,
    const ObTimeoutCtx &ctx,
    ObCompatibilityMode mode)
{
  int ret = OB_SUCCESS;
  const int64_t retry_interval_us = 500l * 1000l; // 500ms
  int64_t job_id = OB_INVALID_ID;
  bool finish = false;
  int64_t inprogress_job_count = 0;
  while (OB_SUCC(ret) && !finish) {
    int tmp_ret = OB_SUCCESS;
    if (ctx.is_timeouted()) {
      ret = OB_TIMEOUT;
      LOG_WARN("wait sys package ready failed", KR(ret), K(mode));
    } else {
      inprogress_job_count = 0;
      if (mode != ObCompatibilityMode::ORACLE_MODE &&
          OB_ENTRY_NOT_EXIST != (tmp_ret = RS_JOB_FIND(LOAD_MYSQL_SYS_PACKAGE, job_id, sql_proxy,
            "tenant_id", OB_SYS_TENANT_ID))) {
          inprogress_job_count++;
          LOG_WARN("mysql job is not ready", K(tmp_ret), K(mode));
      }
      if (mode != ObCompatibilityMode::MYSQL_MODE &&
          OB_ENTRY_NOT_EXIST != (tmp_ret = RS_JOB_FIND(LOAD_ORACLE_SYS_PACKAGE, job_id, sql_proxy,
            "tenant_id", OB_SYS_TENANT_ID))) {
          inprogress_job_count++;
          LOG_WARN("oracle job is not ready", K(tmp_ret), K(mode));
      }
      if (inprogress_job_count == 0) {
        // RS_JOB_FIND will only find INPROGRESS job
        finish = true;
        LOG_INFO("sys_package is ready", KR(ret), K(mode));
      } else {
        LOG_WARN("sys_package is not ready", KR(ret), KR(tmp_ret), K(mode));
        ob_usleep(retry_interval_us);
      }
    }
  }
  return ret;
}

}
}
