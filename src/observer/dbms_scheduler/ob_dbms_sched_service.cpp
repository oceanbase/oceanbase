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

#include "observer/dbms_scheduler/ob_dbms_sched_service.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_utils.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/ob_get_compat_mode.h"
#define USING_LOG_PREFIX SERVER

namespace oceanbase
{
using namespace common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sqlclient;
namespace rootserver
{

void ObDBMSSchedService::ObOrphanJobCleanTask::runTimerTask()
{
  if (OB_NOT_NULL(service_)) {
    int ret = service_->run_orphan_job_clean_task_();
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to run orphan job clean task", KR(ret), K(service_->tenant_id_));
    }
  }
}

int ObDBMSSchedService::mtl_init(ObDBMSSchedService *&dbms_sched_service)
{
  return dbms_sched_service->init();
}

int ObDBMSSchedService::init()
{
  int ret = OB_SUCCESS;
  tenant_id_ = MTL_ID();
  if (!is_user_tenant(tenant_id_) && !is_sys_tenant(tenant_id_) && !is_meta_tenant(tenant_id_)) {
    // do nothing
  } else if (job_master_.is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", KR(ret), "tenant_id", tenant_id_);
  } else if (OB_FAIL(job_master_.init(GCTX.sql_proxy_, GCTX.schema_service_, tenant_id_))) {
    LOG_WARN("[DBMS_SCHED_SERVICE] job master init failed", "tenant_id", tenant_id_);
  } else if (OB_FAIL(ObTenantThreadHelper::create(
      "DBMSSched",
      lib::TGDefIDs::DBMSSchedService,
      *this))) {
    LOG_WARN("[DBMS_SCHED_SERVICE] fail to create thread", KR(ret));
  } else {
    orphan_job_clean_task_.set_service(this);
    LOG_INFO("[DBMS_SCHED_SERVICE] ObDBMSSchedService init success", "tenant_id", tenant_id_);
  }
  return ret;
}

int ObDBMSSchedService::schedule_orphan_job_clean_task_()
{
  int ret = OB_SUCCESS;
  omt::ObSharedTimer *timer = MTL(omt::ObSharedTimer *);
  if (!is_user_tenant(tenant_id_)) {
    // do nothing
  } else if (OB_ISNULL(timer)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("shared timer is NULL", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(TG_SCHEDULE(timer->get_tg_id(), orphan_job_clean_task_, 10L * 60 * 1000 * 1000, true))) {
    LOG_WARN("failed to schedule orphan job clean task", KR(ret), K(tenant_id_));
  } else {
    orphan_job_clean_scheduled_ = true;
  }
  return ret;
}

void ObDBMSSchedService::cancel_orphan_job_clean_task_()
{
  omt::ObSharedTimer *timer = MTL(omt::ObSharedTimer *);
  if (orphan_job_clean_scheduled_ && OB_NOT_NULL(timer)) {
    TG_CANCEL_TASK(timer->get_tg_id(), orphan_job_clean_task_);
  }
}

void ObDBMSSchedService::trigger_orphan_job_clean()
{
  int ret = OB_SUCCESS;
  omt::ObSharedTimer *timer = MTL(omt::ObSharedTimer *);
  if (!orphan_job_clean_scheduled_ || OB_ISNULL(timer)) {
    // not scheduled yet, ignore
  } else if (OB_FAIL(TG_SCHEDULE(timer->get_tg_id(), orphan_job_clean_task_,
                                  0 /*delay*/, false /*repeat*/))) {
    // 一次性任务，delay=0 立即触发，不影响原有周期任务。
    LOG_WARN("failed to trigger orphan job clean task once", KR(ret), K(tenant_id_));
  } else {
    LOG_INFO("trigger orphan job clean", K(tenant_id_));
  }
}

void ObDBMSSchedService::wait_orphan_job_clean_task_()
{
  omt::ObSharedTimer *timer = MTL(omt::ObSharedTimer *);
  if (orphan_job_clean_scheduled_ && OB_NOT_NULL(timer)) {
    TG_WAIT_TASK(timer->get_tg_id(), orphan_job_clean_task_);
  }
}

int ObDBMSSchedService::run_orphan_job_clean_task_()
{
  int ret = OB_SUCCESS;
  bool is_oracle_tenant = false;
  uint64_t compat_version = 0;

  // 版本门控：4.4.2.4 之前不启用，避免对升级前的存量数据产生意外影响。
  if (!is_user_tenant(tenant_id_) || !is_leader()) {
    // do nothing
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, compat_version))) {
    LOG_WARN("failed to get min data version", KR(ret), K(tenant_id_));
  } else if (compat_version < DATA_VERSION_4_4_2_4) {
    // do nothing
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id_, is_oracle_tenant))) {
    LOG_WARN("failed to check tenant compat mode", KR(ret), K(tenant_id_));
  } else if (!is_oracle_tenant) {
    // do nothing
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sql_proxy", KR(ret), K(tenant_id_));
  } else {
    if (!ATOMIC_BCAS(&in_orphan_job_clean_, false, true)) {
      return ret;
    }
    DEFER(ATOMIC_STORE(&in_orphan_job_clean_, false));
    ObSqlString sql;
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id_);
    const int64_t extract_tid = ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id_);
    // 扫描 Oracle 租户所有 scheduler job 主行：
    // 1. 只处理 user_id 有效（!= OB_INVALID_ID）且不属于内置用户（>= OB_MAX_INNER_USER_ID）的 job；
    //    内置用户（SYS/LBACSYS 等）的 user_id 在 [OB_MIN_INNER_USER_ID, OB_MAX_INNER_USER_ID) 范围内，
    //    不存在于 __all_user 中，若不排除会被误判为 orphan。
    // 2. 历史老数据（user_id == OB_INVALID_ID）跳过，规避 RENAME USER 后误删风险。
    if (OB_FAIL(sql.assign_fmt(
                   "SELECT j.job_name FROM %s j "
                   "LEFT JOIN %s u ON u.tenant_id = j.tenant_id AND u.user_id = j.user_id "
                   "WHERE j.tenant_id = %ld AND j.job > 0 "
                   "AND j.user_id != %lu AND j.user_id >= %lu AND u.user_id IS NULL",
                   OB_ALL_TENANT_SCHEDULER_JOB_TNAME,
                   OB_ALL_USER_TNAME,
                   extract_tid,
                   OB_INVALID_ID,
                   OB_MAX_INNER_USER_ID))) {
      LOG_WARN("failed to build orphan job scan sql", KR(ret), K(tenant_id_));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        if (OB_FAIL(GCTX.sql_proxy_->read(res, exec_tenant_id, sql.ptr()))) {
          LOG_WARN("failed to scan scheduler jobs", KR(ret), K(tenant_id_), K(sql));
        } else if (OB_NOT_NULL(res.get_result())) {
          while (OB_SUCC(ret)) {
            if (OB_FAIL(res.get_result()->next())) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
              }
              break;
            }
            ObString raw_job_name;
            EXTRACT_VARCHAR_FIELD_MYSQL(*(res.get_result()), "job_name", raw_job_name);
            // 先 stop job（发 KILLED + kill session），再删定义行；
            {
              ObArenaAllocator allocator("SchedOrphan");
              dbms_scheduler::ObDBMSSchedJobInfo job_info;
              int tmp_ret = OB_SUCCESS;
              if (OB_TMP_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::get_dbms_sched_job_info(
                  *GCTX.sql_proxy_, tenant_id_, true /*is_oracle_tenant*/, raw_job_name, allocator, job_info))) {
                LOG_WARN("failed to get job info", K(tmp_ret), K(tenant_id_), K(raw_job_name));
              } else if (OB_TMP_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::stop_dbms_sched_job(
                  *GCTX.sql_proxy_, job_info, true /*is_delete_after_stop*/))) {
                if (OB_ENTRY_NOT_EXIST != tmp_ret) {
                  LOG_WARN("failed to stop orphan job", K(tmp_ret), K(tenant_id_), K(raw_job_name));
                } else {
                  tmp_ret = OB_SUCCESS;
                }
              }
              if (OB_SUCCESS != tmp_ret) {
                //do nothing
              } else if (OB_TMP_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::remove_dbms_sched_job(
                  *GCTX.sql_proxy_, tenant_id_, raw_job_name, true /*if_exists*/))) {
                LOG_WARN("failed to remove orphan scheduler job", K(tmp_ret), K(tenant_id_), K(raw_job_name));
              } else {
                LOG_INFO("remove orphan scheduler job", K(tenant_id_), K(raw_job_name));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDBMSSchedService::start()
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(tenant_id_) && !is_sys_tenant(tenant_id_) && !is_meta_tenant(tenant_id_)) {
    // do nothing
  } else if (!job_master_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(job_master_.is_inited()));
  } else if (OB_FAIL(job_master_.start())) {
    LOG_WARN("[DBMS_SCHED_SERVICE] job master start failed", K(ret), "tenant_id", tenant_id_);
  } else if (OB_FAIL(schedule_orphan_job_clean_task_())) {
    LOG_WARN("[DBMS_SCHED_SERVICE] failed to start orphan job cleaner", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(ObTenantThreadHelper::start())) {
    LOG_WARN("[DBMS_SCHED_SERVICE] failed to start thread", KR(ret));
  } else {
    LOG_INFO("[DBMS_SCHED_SERVICE] ObDBMSSchedService start success", "tenant_id", tenant_id_);
  }
  return ret;
}

void ObDBMSSchedService::do_work()
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(tenant_id_) && !is_sys_tenant(tenant_id_) && !is_meta_tenant(tenant_id_)) {
    // do nothing
  } else if (!job_master_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(job_master_.is_inited()));
  } else if (OB_FAIL(job_master_.scheduler())) {
    LOG_WARN("[DBMS_SCHED_SERVICE] job master sched failed", K(ret), "tenant_id", tenant_id_);
  }
}

void ObDBMSSchedService::stop()
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(tenant_id_) && !is_sys_tenant(tenant_id_) && !is_meta_tenant(tenant_id_)) {
    // do nothing
  } else if (!job_master_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(job_master_.is_inited()));
  } else if (OB_FAIL(job_master_.stop())) {
    LOG_INFO("[DBMS_SCHED_SERVICE] ObDBMSSchedService stop failure", "tenant_id", tenant_id_);
  } else {
    cancel_orphan_job_clean_task_();
    ObTenantThreadHelper::stop();
    LOG_INFO("[DBMS_SCHED_SERVICE] ObDBMSSchedService stop success", "tenant_id", tenant_id_);
  }
}

void ObDBMSSchedService::wait()
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(tenant_id_) && !is_sys_tenant(tenant_id_) && !is_meta_tenant(tenant_id_)) {
    // do nothing
  } else if (!job_master_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(job_master_.is_inited()));
  } else {
    ObTenantThreadHelper::wait();
    wait_orphan_job_clean_task_();
    LOG_INFO("[DBMS_SCHED_SERVICE] ObDBMSSchedService wait success", "tenant_id", tenant_id_);
  }
}

void ObDBMSSchedService::destroy()
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(tenant_id_) && !is_sys_tenant(tenant_id_) && !is_meta_tenant(tenant_id_)) {
    // do nothing
  } else if (job_master_.is_inited()) {
    if (OB_FAIL(job_master_.destroy())) {
      LOG_WARN("[DBMS_SCHED_SERVICE] job master destroy failed", K(ret), "tenant_id", tenant_id_);
    } else {
      LOG_INFO("[DBMS_SCHED_SERVICE] job master destroy success", "tenant_id", tenant_id_);
    }
    ObTenantThreadHelper::destroy();
  }
  LOG_INFO("[DBMS_SCHED_SERVICE] ObDBMSSchedService destroy success", "tenant_id", tenant_id_);
}

void ObDBMSSchedService::switch_to_follower_forcedly()
{
  switch_to_follower_gracefully();
}
int ObDBMSSchedService::switch_to_leader()
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(tenant_id_) && !is_sys_tenant(tenant_id_) && !is_meta_tenant(tenant_id_)) {
    // do nothing
  } else if (job_master_.is_inited()) {
    job_master_.switch_to_leader();
    ObTenantThreadHelper::switch_to_leader();
    LOG_INFO("[DBMS_SCHED_SERVICE] ObDBMSSchedService switch leader", "tenant_id", tenant_id_);
  }
  return ret;
}
int ObDBMSSchedService::switch_to_follower_gracefully()
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(tenant_id_) && !is_sys_tenant(tenant_id_) && !is_meta_tenant(tenant_id_)) {
    // do nothing
  } else if (job_master_.is_inited()) {
    job_master_.switch_to_follower();
    ObTenantThreadHelper::switch_to_follower_gracefully();
    LOG_INFO("[DBMS_SCHED_SERVICE] ObDBMSSchedService switch follower", "tenant_id", tenant_id_);
  }
  return ret;
}
int ObDBMSSchedService::resume_leader()
{
  int ret = OB_SUCCESS;
  if (!is_leader()) {
    if (OB_FAIL(switch_to_leader())) {
       LOG_INFO("[DBMS_SCHED_SERVICE] resume leader failed", "tenant_id", tenant_id_);
    }
  }
  return ret;
}

}  // namespace rootserver
}  // namespace oceanbase
