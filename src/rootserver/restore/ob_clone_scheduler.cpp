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

#define USING_LOG_PREFIX RS_RESTORE

#include "ob_clone_scheduler.h"
#include "share/ls/ob_ls_operator.h"
#include "share/ob_max_id_fetcher.h"
#include "rootserver/ob_common_ls_service.h"
#include "rootserver/restore/ob_restore_common_util.h"
#include "share/restore/ob_log_restore_source_mgr.h"
#include "share/backup/ob_archive_persist_helper.h"
#include "rootserver/restore/ob_tenant_clone_util.h"
#include "rootserver/tenant_snapshot/ob_tenant_snapshot_util.h"
#include "share/tenant_snapshot/ob_tenant_snapshot_table_operator.h"
#include "rootserver/ob_ddl_service.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"
#include "observer/ob_inner_sql_connection.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_master_key_getter.h"
#endif
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase
{
namespace rootserver
{
using namespace common;
using namespace share;
using namespace transaction::tablelock;

ObCloneScheduler::ObCloneScheduler()
  : is_inited_(false),
    schema_service_(NULL),
    sql_proxy_(NULL),
    rpc_proxy_(NULL),
    srv_rpc_proxy_(NULL),
    idle_time_us_(1),
    work_immediately_(false)
{
}

ObCloneScheduler::~ObCloneScheduler()
{
  if (!has_set_stop()) {
    stop();
    wait();
  }
}

void ObCloneScheduler::destroy()
{
  ObTenantThreadHelper::destroy();
  is_inited_ = false;
}

int ObCloneScheduler::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)
            || OB_ISNULL(GCTX.sql_proxy_)
            || OB_ISNULL(GCTX.rs_rpc_proxy_)
            || OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.schema_service_),
              KP(GCTX.sql_proxy_), KP(GCTX.rs_rpc_proxy_), KP(GCTX.srv_rpc_proxy_));
    //TODO: SimpleLSService
  } else if (OB_FAIL(ObTenantThreadHelper::create("CloneSche", lib::TGDefIDs::SimpleLSService, *this))) {
    LOG_WARN("failed to create thread", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::start())) {
    LOG_WARN("fail to start thread", KR(ret));
  } else {
    schema_service_ = GCTX.schema_service_;
    sql_proxy_ = GCTX.sql_proxy_;
    rpc_proxy_ = GCTX.rs_rpc_proxy_;
    srv_rpc_proxy_ = GCTX.srv_rpc_proxy_;
    is_inited_ = true;
  }
  return ret;
}

int ObCloneScheduler::idle()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (work_immediately_) {
    work_immediately_ = false;
  } else {
    ObTenantThreadHelper::idle(idle_time_us_);
    idle_time_us_ = is_sys_tenant(tenant_id)? DEFAULT_IDLE_TIME : PROCESS_IDLE_TIME;
  }
  return ret;
}

void ObCloneScheduler::wakeup()
{
  ObTenantThreadHelper::wakeup();
}

void ObCloneScheduler::do_work()
{
  LOG_INFO("[RESTORE] clone scheduler start");
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else {
    const uint64_t tenant_id = MTL_ID();
    idle_time_us_ = is_sys_tenant(tenant_id)? DEFAULT_IDLE_TIME : PROCESS_IDLE_TIME;
    ob_setup_default_tsi_warning_buffer();
    while (!has_set_stop()) {
      ObCurTraceId::init(GCTX.self_addr());
      ob_reset_tsi_warning_buffer();
      ObArray<ObCloneJob> clone_jobs;
      ObTenantCloneTableOperator clone_op;
      if (!is_sys_tenant(tenant_id) && !is_meta_tenant(tenant_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected tenant id", KR(ret), K(tenant_id));
      } else if (is_sys_tenant(tenant_id) && OB_FAIL(check_sys_tenant_(tenant_id))) {
        LOG_WARN("sys tenant can not do cloning operation", KR(ret), K(tenant_id));
      } else if (is_meta_tenant(tenant_id) && OB_FAIL(check_meta_tenant_(tenant_id))) {
        LOG_WARN("meta tenant can not do cloning operation", KR(ret), K(tenant_id));
      } else if (OB_FAIL(clone_op.init(gen_user_tenant_id(tenant_id), sql_proxy_))) {
        LOG_WARN("fail to init", KR(ret), K(tenant_id));
      } else if (OB_FAIL(clone_op.get_all_clone_jobs(clone_jobs))) {
        LOG_WARN("fail to get clone jobs", KR(ret), K(tenant_id));
      } else {
        FOREACH_CNT_X(clone_job, clone_jobs, !has_set_stop()) { // ignore ret
          if (OB_ISNULL(clone_job)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("clone job is null", KR(ret));
          } else if (!clone_job->is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("clone job is not valid", KR(ret), KPC(clone_job));
          } else if (is_sys_tenant(tenant_id)) {
            if (OB_FAIL(process_sys_clone_job(*clone_job))) {
              LOG_WARN("fail to process sys clone job", KR(ret), KPC(clone_job));
            }
          } else if (OB_FAIL(process_user_clone_job(*clone_job))) {
            LOG_WARN("fail to process user clone job", KR(ret), KPC(clone_job));
          }
          idle_time_us_ = PROCESS_IDLE_TIME;
        }
      }
      ret = OB_SUCCESS;
      idle();
    }
  }
  LOG_INFO("[RESTORE] clone scheduler quit");
}

int ObCloneScheduler::process_sys_clone_job(const share::ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_UNLIKELY(!is_sys_tenant(tenant_id)
                         || tenant_id != job.get_tenant_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tenant id", KR(ret), K(tenant_id), K(job));
  } else {
    ObTraceIdGuard trace_guard(job.get_trace_id());
    switch (job.get_status()) {
      case ObTenantCloneStatus::Status::CLONE_SYS_LOCK:
        ret = clone_lock(job);
        break;
      case ObTenantCloneStatus::Status::CLONE_SYS_CREATE_INNER_RESOURCE_POOL:
        ret = clone_create_resource_pool(job);
        break;
      case ObTenantCloneStatus::Status::CLONE_SYS_CREATE_SNAPSHOT:
        ret = clone_create_snapshot_for_fork_tenant(job);
        break;
      case ObTenantCloneStatus::Status::CLONE_SYS_WAIT_CREATE_SNAPSHOT:
        ret = clone_wait_create_snapshot_for_fork_tenant(job);
        break;
      case ObTenantCloneStatus::Status::CLONE_SYS_CREATE_TENANT:
        ret = clone_create_tenant(job);
        break;
      case ObTenantCloneStatus::Status::CLONE_SYS_WAIT_TENANT_RESTORE_FINISH:
        ret = clone_wait_tenant_restore_finish(job);
        break;
      case ObTenantCloneStatus::Status::CLONE_SYS_RELEASE_RESOURCE:
        ret = clone_release_resource(job);
        break;
      case ObTenantCloneStatus::Status::CLONE_SYS_SUCCESS:
        ret = clone_sys_finish(job);
        break;
      case ObTenantCloneStatus::Status::CLONE_SYS_LOCK_FAIL:
      case ObTenantCloneStatus::Status::CLONE_SYS_CREATE_INNER_RESOURCE_POOL_FAIL:
      case ObTenantCloneStatus::Status::CLONE_SYS_CREATE_SNAPSHOT_FAIL:
      case ObTenantCloneStatus::Status::CLONE_SYS_WAIT_CREATE_SNAPSHOT_FAIL:
      case ObTenantCloneStatus::Status::CLONE_SYS_CREATE_TENANT_FAIL:
      case ObTenantCloneStatus::Status::CLONE_SYS_WAIT_TENANT_RESTORE_FINISH_FAIL:
      case ObTenantCloneStatus::Status::CLONE_SYS_RELEASE_RESOURCE_FAIL:
      case ObTenantCloneStatus::Status::CLONE_SYS_CANCELING:
        ret = clone_recycle_failed_job(job);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status", KR(ret), K(job));
        break;
    }
  }
  return ret;
}

int ObCloneScheduler::process_user_clone_job(const share::ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_UNLIKELY(!is_meta_tenant(tenant_id)
                         || gen_user_tenant_id(tenant_id) != job.get_tenant_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tenant id", KR(ret), K(tenant_id), K(job));
  } else {
    ObTraceIdGuard trace_guard(job.get_trace_id());
    switch (job.get_status()) {
      case ObTenantCloneStatus::Status::CLONE_USER_PREPARE:
        ret = clone_prepare(job);
        break;
      case ObTenantCloneStatus::Status::CLONE_USER_CREATE_INIT_LS:
        ret = clone_init_ls(job);
        break;
      case ObTenantCloneStatus::Status::CLONE_USER_WAIT_LS:
        ret = clone_wait_ls_finish(job);
        break;
      case ObTenantCloneStatus::Status::CLONE_USER_POST_CHECK:
        ret = clone_post_check(job);
        break;
      case ObTenantCloneStatus::Status::CLONE_USER_SUCCESS:
        ret = clone_user_finish(job);
        break;
      case ObTenantCloneStatus::Status::CLONE_USER_FAIL:
        ret = clone_user_finish(job);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status", KR(ret), K(job));
        break;
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_CLONE_LOCK_ERROR);
int ObCloneScheduler::clone_lock(const share::ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(HANG_IN_CLONE_SYS_LOCK);
  const uint64_t tenant_id = job.get_tenant_id();
  const uint64_t source_tenant_id = job.get_source_tenant_id();
  const int64_t job_id = job.get_job_id();
  const ObTenantSnapshotID &snapshot_id = job.get_tenant_snapshot_id();
  ObTenantCloneJobType job_type = job.get_job_type();
  int32_t loop_cnt = 0;
  const ObTenantSnapshotUtil::TenantSnapshotOp op = ObTenantCloneJobType::RESTORE == job_type ?
                                                    ObTenantSnapshotUtil::RESTORE_OP :
                                                    ObTenantSnapshotUtil::FORK_OP;
  if (OB_UNLIKELY(ERRSIM_CLONE_LOCK_ERROR)) {
    ret = ERRSIM_CLONE_LOCK_ERROR;
    LOG_WARN("mock clone lock failed", KR(ret), K(job));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (has_set_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("clone scheduler stopped", KR(ret));
  } else if (!is_sys_tenant(tenant_id) || !is_user_tenant(source_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(source_tenant_id), K(snapshot_id));
  } else if (!snapshot_id.is_valid() && ObTenantCloneJobType::RESTORE == job_type) {
    // if the job_type is FORK, the according snapshot has not been created；
    // if the job_type is RESTORE, the according snapshot must already be created
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(source_tenant_id), K(snapshot_id));
  } else if (ObTenantCloneJobType::RESTORE != job_type && ObTenantCloneJobType::FORK != job_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_type));
  } else {
    while (OB_SUCC(ret)) {
      ObMySQLTransaction trans;
      ObTenantSnapStatus original_global_state_status = ObTenantSnapStatus::MAX;
      bool need_wait = false;
      if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(source_tenant_id)))) {
        LOG_WARN("failed to start trans", KR(ret), K(source_tenant_id));
      } else if (OB_FAIL(ObTenantSnapshotUtil::trylock_tenant_snapshot_simulated_mutex(trans,
                          source_tenant_id, op, job_id,
                          original_global_state_status))) {
        if (OB_TENANT_SNAPSHOT_LOCK_CONFLICT != ret) {
          LOG_WARN("trylock tenant snapshot simulated mutex failed", KR(ret), K(source_tenant_id));
        } else {
          if (ObTenantSnapStatus::CREATING == original_global_state_status) {
            ret = OB_SUCCESS;
            need_wait = true;
            LOG_INFO("need wait for current tenant snapshot creation", KR(ret), K(source_tenant_id));
          } else {
            LOG_WARN("GLOBAL_STATE snapshot lock conflict", KR(ret), K(source_tenant_id),
                                                                K(original_global_state_status));
          }
        }
      } else if (OB_FAIL(ObTenantSnapshotUtil::check_tenant_has_no_conflict_tasks(source_tenant_id))) {
        LOG_WARN("fail to check tenant has conflict tasks", KR(ret), K(source_tenant_id));
      } else if (ObTenantCloneJobType::RESTORE == job_type &&
                 OB_FAIL(ObTenantSnapshotUtil::add_clone_tenant_task(trans, source_tenant_id,
                                                                     snapshot_id))) {
        // if job_type is FORK, the snapshot will be updated as CLONING when it is created successful
        LOG_WARN("failed to add clone tenant snapshot task", KR(ret), K(source_tenant_id), K(snapshot_id));
      }
      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
          LOG_WARN("trans end failed", "is_commit", (OB_SUCCESS == ret), KR(tmp_ret));
          ret = (OB_SUCC(ret)) ? tmp_ret : ret;
        }
      }
      if (OB_SUCC(ret)) {
        if (need_wait) {
          if ((++loop_cnt) >= MAX_RETRY_CNT) {
            ret = OB_OP_NOT_ALLOW;
            LOG_WARN("not allow to lock", KR(ret), K(job));
          } else {
            ob_usleep(2 * 1000 * 1000L);
          }
        } else {
          break;
        }
      }
    }
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(try_update_job_status_(ret, job))) {
    LOG_WARN("fail to update job status", KR(ret), KR(tmp_ret), K(job));
  }
  LOG_INFO("[RESTORE] clone lock", KR(ret), K(job));
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_CLONE_RESOURCE_POOL_ERROR);
int ObCloneScheduler::clone_create_resource_pool(const share::ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(HANG_IN_CLONE_SYS_CREATE_INNER_RESOURCE_POOL);
  obrpc::ObCloneResourcePoolArg arg;
  int64_t timeout = GCONF._ob_ddl_timeout;
  uint64_t resource_pool_id = job.get_resource_pool_id();
  const int64_t job_id = job.get_job_id();

  if (OB_UNLIKELY(ERRSIM_CLONE_RESOURCE_POOL_ERROR)) {
    ret = ERRSIM_CLONE_RESOURCE_POOL_ERROR;
    LOG_WARN("mock clone resource pool failed", KR(ret), K(job));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (has_set_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("clone scheduler stopped", KR(ret));
  } else if (OB_ISNULL(GCTX.rs_mgr_) || OB_ISNULL(rpc_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job), KP(rpc_proxy_));
  } else if (OB_INVALID_ID == resource_pool_id) {
    ObMaxIdFetcher id_fetcher(*sql_proxy_);
    if (OB_FAIL(id_fetcher.fetch_new_max_id(OB_SYS_TENANT_ID,
                                            ObMaxIdType::OB_MAX_USED_RESOURCE_POOL_ID_TYPE,
                                            resource_pool_id))) {
      LOG_WARN("fetch resource pool id failed", KR(ret), K(job));
    } else if (OB_INVALID_ID == resource_pool_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid resource pool id", KR(ret), K(job));
    } else if (OB_FAIL(ObTenantCloneUtil::update_resource_pool_id_of_clone_job(*sql_proxy_,
                                                                               job_id,
                                                                               resource_pool_id))) {
      LOG_WARN("fail to update resource pool id of clone job", KR(ret), K(job), K(arg));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(fill_clone_resource_pool_arg_(job, resource_pool_id, arg))) {
    LOG_WARN("fail to fill clone resource pool arg", KR(ret), K(job), K(arg));
  } else if (OB_FAIL(rpc_proxy_->to_rs(*GCTX.rs_mgr_).timeout(timeout).clone_resource_pool(arg))) {
    LOG_WARN("fail to clone resource pool", KR(ret), K(arg), K(timeout));
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(try_update_job_status_(ret, job))) {
    LOG_WARN("fail to update job status", KR(ret), KR(tmp_ret), K(job));
  }
  LOG_INFO("[RESTORE] clone create resource pool", KR(ret), K(arg), K(job));
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_CLONE_CREATE_SNAPSHOT_ERROR);
int ObCloneScheduler::clone_create_snapshot_for_fork_tenant(const share::ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(HANG_IN_CLONE_SYS_CREATE_SNAPSHOT);
  int tmp_ret = OB_SUCCESS;
  const uint64_t tenant_id = job.get_tenant_id();
  const uint64_t source_tenant_id = job.get_source_tenant_id();
  const ObString &source_tenant_name = job.get_source_tenant_name();
  const ObTenantCloneJobType type = job.get_job_type();
  const int64_t job_id = job.get_job_id();
  ObTenantSnapItem item;
  ObMySQLTransaction trans;
  ObSqlString snapshot_name;
  ObTenantSnapshotID tenant_snapshot_id;

  if (OB_UNLIKELY(ERRSIM_CLONE_CREATE_SNAPSHOT_ERROR)) {
    ret = ERRSIM_CLONE_CREATE_SNAPSHOT_ERROR;
    LOG_WARN("mock clone create snapshot failed", KR(ret), K(job));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (has_set_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("clone scheduler stopped", KR(ret));
  } else if (!is_sys_tenant(tenant_id) || !is_user_tenant(source_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job));
  } else if (OB_UNLIKELY(source_tenant_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty source tenant name", KR(ret), K(job));
  } else if (ObTenantCloneJobType::FORK != type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job type", KR(ret), K(job));
  } else if (!job.get_tenant_snapshot_id().is_valid()) {
    if (OB_FAIL(rootserver::ObTenantSnapshotUtil::generate_tenant_snapshot_name(
                                                      source_tenant_id, snapshot_name, true))) {
      LOG_WARN("failed to generate new tenant snapshot name", KR(ret), K(source_tenant_id));
    } else if (snapshot_name.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid job", KR(ret), K(job));
    } else if (OB_FAIL(rootserver::ObTenantSnapshotUtil::generate_tenant_snapshot_id(
                                                        source_tenant_id, tenant_snapshot_id))) {
      LOG_WARN("failed to generate snapshot id", KR(ret), K(source_tenant_id));
    } else if (!tenant_snapshot_id.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid job", KR(ret), K(job));
    } else if (OB_FAIL(ObTenantCloneUtil::update_snapshot_info_for_fork_job(*sql_proxy_,
                                                                            job_id,
                                                                            tenant_snapshot_id,
                                                                            snapshot_name.string()))) {
      LOG_WARN("fail to update snapshot info", KR(ret), K(job), K(tenant_snapshot_id), K(snapshot_name));
    }
  } else { // job.tenant_snapshot_id_.is_valid() == true
    if (job.get_tenant_snapshot_name().empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid job", KR(ret), K(job));
    } else if (OB_FAIL(snapshot_name.assign(job.get_tenant_snapshot_name()))) {
      LOG_WARN("fail to assign", KR(ret), K(job));
    } else {
      tenant_snapshot_id = job.get_tenant_snapshot_id();
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(source_tenant_id)))) {
    LOG_WARN("failed to start trans", KR(ret), K(source_tenant_id));
  } else if (OB_FAIL(rootserver::ObTenantSnapshotUtil::get_tenant_snapshot_info(trans,
                                                                                source_tenant_id,
                                                                                tenant_snapshot_id,
                                                                                item))) {
    if (OB_TENANT_SNAPSHOT_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(rootserver::ObTenantSnapshotUtil::create_fork_tenant_snapshot(trans,
                                                                                source_tenant_id,
                                                                                source_tenant_name,
                                                                                snapshot_name.string(),
                                                                                tenant_snapshot_id))) {
        LOG_WARN("create tenant snapshot failed", KR(ret), K(job), K(tenant_snapshot_id), K(snapshot_name));
      }
    } else {
      LOG_WARN("get tenant snapshot failed", KR(ret), K(source_tenant_id), K(snapshot_name));
    }
  } else {
    // do nothing, tenant snapshot has been created
  }

  if (trans.is_started()) {
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", "is_commit", (OB_SUCCESS == ret), K(tmp_ret));
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
    }
  }

  if (OB_TMP_FAIL(try_update_job_status_(ret, job))) {
    LOG_WARN("fail to update job status", KR(ret), KR(tmp_ret), K(job));
  }
  LOG_INFO("[RESTORE] create fork tenant snapshot", KR(ret), K(job));
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_CLONE_WAIT_CREATE_SNAPSHOT_ERROR);
int ObCloneScheduler::clone_wait_create_snapshot_for_fork_tenant(const share::ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(HANG_IN_CLONE_SYS_WAIT_CREATE_SNAPSHOT);
  const uint64_t tenant_id = job.get_tenant_id();
  const uint64_t source_tenant_id = job.get_source_tenant_id();
  const ObTenantCloneJobType type = job.get_job_type();
  const ObTenantSnapshotID tenant_snapshot_id = job.get_tenant_snapshot_id();
  const int64_t job_id = job.get_job_id();
  ObTenantSnapItem item;
  ObMySQLTransaction trans;
  bool need_wait = false;

  if (OB_UNLIKELY(ERRSIM_CLONE_WAIT_CREATE_SNAPSHOT_ERROR)) {
    ret = ERRSIM_CLONE_WAIT_CREATE_SNAPSHOT_ERROR;
    LOG_WARN("mock clone wait create snapshot failed", KR(ret), K(job));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (has_set_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("clone scheduler stopped", KR(ret));
  } else if (!is_sys_tenant(tenant_id)
             || !is_user_tenant(source_tenant_id)
             || !tenant_snapshot_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(source_tenant_id), K(tenant_snapshot_id));
  } else if (ObTenantCloneJobType::FORK != type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job));
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(source_tenant_id)))) {
    LOG_WARN("failed to start trans", KR(ret), K(source_tenant_id));
  } else if (OB_FAIL(rootserver::ObTenantSnapshotUtil::get_tenant_snapshot_info(*sql_proxy_,
                                                                                source_tenant_id,
                                                                                tenant_snapshot_id,
                                                                                item))) {
    LOG_WARN("get tenant snapshot failed", KR(ret), K(source_tenant_id), K(tenant_snapshot_id));
  } else if (ObTenantSnapStatus::CREATING == item.get_status() ||
             ObTenantSnapStatus::DECIDED == item.get_status()) {
    need_wait = true;
  } else if (ObTenantSnapStatus::CLONING == item.get_status()) {
    // no need to update snapshot status
  } else if (ObTenantSnapStatus::FAILED == item.get_status()) {
    ret = OB_ERR_TENANT_SNAPSHOT;
    LOG_WARN("create snapshot for fork tenant failed", KR(ret), K(source_tenant_id),
                                                        K(tenant_snapshot_id), K(item));
  } else if (ObTenantSnapStatus::NORMAL != item.get_status()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid status for fork tenant snapshot", KR(ret), K(source_tenant_id),
                                                        K(tenant_snapshot_id), K(item));
  } else if (OB_FAIL(rootserver::ObTenantSnapshotUtil::add_clone_tenant_task(trans, item))) {
    LOG_WARN("fail to update fork tenant snapshot to cloning", KR(ret), K(item));
  }

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", "is_commit", (OB_SUCCESS == ret), K(tmp_ret));
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
    }
  }

  if (!need_wait) {
    const bool need_update_clone_job_scn = !job.get_restore_scn().is_valid();
    if (OB_FAIL(ret)) {
    } else if (need_update_clone_job_scn &&
               OB_FAIL(ObTenantCloneUtil::update_restore_scn_for_fork_job(*sql_proxy_,
                                                                          job_id,
                                                                          item.get_snapshot_scn()))) {
      LOG_WARN("fail to update snapshot scn", KR(ret), K(job), K(item));
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(try_update_job_status_(ret, job))) {
      LOG_WARN("fail to update job status", KR(ret), KR(tmp_ret), K(job));
    }
  }

  LOG_INFO("[RESTORE] wait create fork tenant snapshot", KR(ret), K(need_wait), K(job));
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_CLONE_CREATE_TENANT_ERROR);
int ObCloneScheduler::clone_create_tenant(const share::ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(HANG_IN_CLONE_SYS_CREATE_TENANT);
  obrpc::ObCreateTenantArg arg;
  uint64_t clone_tenant_id = job.get_clone_tenant_id();
  const uint64_t tenant_id = job.get_tenant_id();
  const int64_t job_id = job.get_job_id();
  obrpc::UInt64 unused_res;
  const int64_t timeout = GCONF._ob_ddl_timeout;
  ObTenantCloneTableOperator clone_op;

  if (OB_UNLIKELY(ERRSIM_CLONE_CREATE_TENANT_ERROR)) {
    ret = ERRSIM_CLONE_CREATE_TENANT_ERROR;
    LOG_WARN("mock clone create tenant failed", KR(ret), K(job));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (has_set_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("clone scheduler stopped", KR(ret));
  } else if (OB_INVALID_TENANT_ID == clone_tenant_id) {
    ObMaxIdFetcher id_fetcher(*sql_proxy_);
    uint64_t max_id = OB_INVALID_ID;
    if (OB_FAIL(id_fetcher.fetch_new_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_TENANT_ID_TYPE, max_id))) {
      LOG_WARN("get new schema id failed", KR(ret));
    } else if (OB_INVALID_ID == max_id || OB_INVALID_TENANT_ID == max_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid id", KR(ret), K(max_id), K(job));
    } else if (OB_FAIL(clone_op.init(tenant_id, sql_proxy_))) {
      LOG_WARN("fail to init", KR(ret), K(tenant_id));
    } else if (OB_FAIL(clone_op.update_job_clone_tenant_id(job_id, max_id))) {
      LOG_WARN("fail to update clone tenant id", KR(ret), K(max_id), K(job));
    } else {
      clone_tenant_id = max_id;
      FLOG_INFO("fetch clone_tenant_id success", K(clone_tenant_id), K(job));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(fill_create_tenant_arg_(job, clone_tenant_id, arg))) {
    LOG_WARN("fail to fill create tenant arg", KR(ret), K(job));
  } else if (OB_FAIL(rpc_proxy_->timeout(timeout).create_tenant(arg, unused_res))) {
    LOG_WARN("fail to create tenant", KR(ret), K(arg));
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(try_update_job_status_(ret, job))) {
    LOG_WARN("fail to update job status", KR(ret), KR(tmp_ret), K(job));
  }
  LOG_INFO("[RESTORE] clone create tenant", KR(ret), K(arg), K(job));
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_CLONE_WAIT_CREATE_TENANT_ERROR);
int ObCloneScheduler::clone_wait_tenant_restore_finish(const ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(HANG_IN_CLONE_SYS_WAIT_TENANT_RESTORE_FINISH);
  bool user_finished = false;

  ObTenantCloneTableOperator clone_op;
  ObCloneJob user_job_history;
  const uint64_t clone_tenant_id = job.get_clone_tenant_id();
  bool need_wait = false;

  if (OB_UNLIKELY(ERRSIM_CLONE_WAIT_CREATE_TENANT_ERROR)) {
    ret = ERRSIM_CLONE_WAIT_CREATE_TENANT_ERROR;
    need_wait = OB_EAGAIN == ret ? true : false;
    LOG_WARN("mock clone wait create tenant failed", KR(ret), K(job));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (has_set_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("clone scheduler stopped", KR(ret));
  } else if (OB_FAIL(clone_op.init(clone_tenant_id, sql_proxy_))) {
    LOG_WARN("fail to init clone op", KR(ret), K(clone_tenant_id));
  } else if (OB_FAIL(clone_op.get_user_clone_job_history(user_job_history))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get clone job history", KR(ret), K(clone_tenant_id));
    } else {
      need_wait = true;
      LOG_INFO("need wait user tenant restore finish", KR(ret), K(clone_tenant_id));
    }
  } else if (user_job_history.get_status().is_user_success_status()) {
    ObTenantSchema tenant_schema;
    if (OB_FAIL(ObTenantThreadHelper::get_tenant_schema(clone_tenant_id, tenant_schema))) {
      LOG_WARN("failed to get tenant schema", KR(ret), K(clone_tenant_id));
    } else if (tenant_schema.is_restore_tenant_status() || tenant_schema.is_normal()) {
      if (tenant_schema.is_restore_tenant_status()) {
        const int64_t timeout = GCONF.internal_sql_execute_timeout;
        // try finish restore status
        obrpc::ObCreateTenantEndArg arg;
        arg.tenant_id_ = clone_tenant_id;
        arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
        if (has_set_stop()) {
          ret = OB_CANCELED;
          LOG_WARN("clone scheduler stopped", KR(ret));
        } else if (OB_FAIL(check_data_version_before_finish_clone_(job.get_source_tenant_id()))) {
          LOG_WARN("fail to check data version before finish snapshot creation", KR(ret), K(job));
        } else if (OB_FAIL(rpc_proxy_->timeout(timeout).create_tenant_end(arg))) {
          need_wait = true;
          LOG_WARN("fail to create tenant end", KR(ret), K(arg), K(timeout));
        }
      }
    } else {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("tenant status not match", KR(ret), K(tenant_schema));
    }
  } else {
    user_finished = true;
    int user_ret_code = user_job_history.get_ret_code();
    if (OB_SUCCESS == user_ret_code) {
      ret = OB_ERR_CLONE_TENANT;
      LOG_WARN("user job is not in success status, but it's ret_code is OB_SUCCESS",
          KR(ret), K(user_job_history));
    } else {
      ret = user_ret_code;
    }
    LOG_WARN("user_job_history status is not SUCCESS", KR(ret), K(user_job_history));
  }

  if (OB_FAIL(ret) && !user_finished) {
    need_wait = true;
  }

  if (!need_wait) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(try_update_job_status_(ret, job))) {
      LOG_WARN("fail to update job status", KR(ret), KR(tmp_ret), K(job));
    }
  }
  LOG_INFO("[RESTORE] clone wait tenant restore finish", KR(ret), K(job));
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_CLONE_RELEASE_RESOURCE_ERROR);
int ObCloneScheduler::clone_release_resource(const share::ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(HANG_IN_CLONE_SYS_RELEASE_RESOURCE);
  const uint64_t tenant_id = job.get_tenant_id();
  const uint64_t source_tenant_id = job.get_source_tenant_id();
  const ObTenantSnapshotID snapshot_id = job.get_tenant_snapshot_id();
  const ObTenantCloneJobType job_type = job.get_job_type();
  int tmp_ret = OB_SUCCESS;
  bool need_retry = false;

  if (OB_UNLIKELY(ERRSIM_CLONE_RELEASE_RESOURCE_ERROR)) {
    ret = ERRSIM_CLONE_RELEASE_RESOURCE_ERROR;
    need_retry = OB_EAGAIN == ret ? true : false;
    LOG_WARN("mock clone release resource failed", KR(ret), K(job));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (has_set_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("clone scheduler stopped", KR(ret));
  } else if (!is_sys_tenant(tenant_id)
             || !is_user_tenant(source_tenant_id)
             || !snapshot_id.is_valid()
             || OB_ISNULL(sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(source_tenant_id), K(snapshot_id), KP(sql_proxy_));
  } else if (ObTenantCloneJobType::RESTORE != job_type && ObTenantCloneJobType::FORK != job_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job));
  } else if (OB_FAIL(ObTenantCloneUtil::release_source_tenant_resource_of_clone_job(*sql_proxy_, job))) {
    need_retry = true;
    LOG_WARN("failed to release source tenant resource", KR(ret), K(job));
  }

  if (OB_FAIL(ret) && need_retry) {
  } else if (OB_TMP_FAIL(try_update_job_status_(ret, job))) {
    LOG_WARN("fail to update job status", KR(ret), KR(tmp_ret), K(job));
  }
  LOG_INFO("[RESTORE] clone_release_resource", KR(ret), K(need_retry), K(job));
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_CLONE_SYS_FINISH_ERROR);
int ObCloneScheduler::clone_sys_finish(const share::ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(HANG_IN_CLONE_SYS_SUCCESS);
  bool clone_tenant_exist = true;
  const uint64_t tenant_id = job.get_tenant_id(); //sys tenant id
  const uint64_t clone_tenant_id = job.get_clone_tenant_id();
  const uint64_t source_tenant_id = job.get_source_tenant_id();
  const ObTenantSnapshotID &snapshot_id = job.get_tenant_snapshot_id();

  if (OB_UNLIKELY(ERRSIM_CLONE_SYS_FINISH_ERROR)) {
    ret = ERRSIM_CLONE_SYS_FINISH_ERROR;
    LOG_WARN("mock clone sys finish failed", KR(ret), K(job));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (has_set_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("clone scheduler stopped", KR(ret));
  } else if (OB_FAIL(ObRestoreCommonUtil::check_tenant_is_existed(schema_service_,
                                            clone_tenant_id, clone_tenant_exist))) {
    LOG_WARN("fail to check whether tenant is existed", KR(ret), K(clone_tenant_id), K(job));
  } else if (!clone_tenant_exist) {
    LOG_WARN("clone tenant is not exist", KR(ret), K(clone_tenant_id));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(sql_proxy_));
  } else if (OB_FAIL(ObTenantCloneUtil::recycle_clone_job(*sql_proxy_, job))) {
    LOG_WARN("recycle clone job failed", KR(ret), K(job));
  }

  LOG_INFO("[RESTORE] clone sys finish", KR(ret), K(job));
  const char *status_str = ObTenantCloneStatus::get_clone_status_str(job.get_status());
  ROOTSERVICE_EVENT_ADD("clone", "clone_sys_finish",
                        "job_id", job.get_job_id(),
                        K(ret),
                        "cur_clone_status", status_str);
  return ret;
}

int ObCloneScheduler::clone_prepare(const share::ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  const uint64_t user_tenant_id = job.get_tenant_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_UNLIKELY(!job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job));
  } else if (has_set_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("clone scheduler stopped", KR(ret));
  } else if (OB_FAIL(clone_root_key_(job))) {
    LOG_WARN("fail to clone root key", KR(ret), K(job));
  } else if (OB_FAIL(clone_keystore_(job))) {
    LOG_WARN("fail to clone keystore", KR(ret), K(job));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(try_update_job_status_(ret, job))) {
      LOG_WARN("fail to update job status", KR(ret), KR(tmp_ret), K(job));
    }
  }
  LOG_INFO("[RESTORE] clone prepare", KR(ret), K(job));
  return ret;
}

//Note: the role of cloning tenant is CLONING_TENANT,
//      and the status of cloning tenant is TENANT_STATUS_RESTORE.
int ObCloneScheduler::clone_init_ls(const share::ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const uint64_t user_tenant_id = job.get_tenant_id();
  const uint64_t source_tenant_id = job.get_source_tenant_id();
  schema::ObTenantSchema tenant_schema;
  ObAllTenantInfo all_tenant_info;
  ObSArray<ObLSAttr> ls_attr_array;
  bool need_wait = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (has_set_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("clone scheduler stopped", KR(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(sql_proxy_));
  } else if (OB_FAIL(ObTenantThreadHelper::get_tenant_schema(user_tenant_id, tenant_schema))) {
    LOG_WARN("failed to get tenant schema", KR(ret), K(user_tenant_id));
  } else if (!tenant_schema.is_restore_tenant_status()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant is not in restore status", KR(ret), K(tenant_schema));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(user_tenant_id, sql_proxy_,
                     false/*for_update*/, all_tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(user_tenant_id));
  } else if (!all_tenant_info.is_clone()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant is not in clone role", KR(ret), K(all_tenant_info));
  } else if (OB_FAIL(get_ls_attrs_from_source_(job, ls_attr_array))) {
    LOG_WARN("fail to get ls attrs from source", KR(ret), K(job));
  } else {
    const SCN &sync_scn = job.get_restore_scn();
    ObLSRecoveryStatOperator ls_recovery;
    const uint64_t exec_tenant_id = get_private_table_exec_tenant_id(user_tenant_id);
    START_TRANSACTION(sql_proxy_, exec_tenant_id)
    LOG_INFO("start to create ls and set sync scn", K(sync_scn), K(ls_attr_array), K(source_tenant_id));
    if (FAILEDx(ls_recovery.update_sys_ls_sync_scn(user_tenant_id, trans, sync_scn))) {
      LOG_WARN("failed to update sync ls sync scn", KR(ret), K(sync_scn));
    }
    END_TRANSACTION(trans)
  }

  if (OB_SUCC(ret)) {
    share::ObBackupPathString log_path;
    ObLogRestoreSourceMgr restore_source_mgr;

    if (OB_FAIL(create_all_ls_(user_tenant_id, tenant_schema, ls_attr_array, source_tenant_id))) {
      LOG_WARN("failed to create all ls", KR(ret), K(user_tenant_id), K(tenant_schema),
                                              K(ls_attr_array), K(source_tenant_id));
    } else if (OB_FAIL(wait_all_ls_created_(tenant_schema, job))) {
      LOG_WARN("failed to wait all ls created", KR(ret), K(tenant_schema), K(job));
    } else if (OB_FAIL(finish_create_ls_(tenant_schema, ls_attr_array))) {
      LOG_WARN("failed to finish create ls", KR(ret), K(tenant_schema), K(ls_attr_array));
    } else if (OB_FAIL(get_source_tenant_archive_log_path_(source_tenant_id, log_path))) {
      LOG_WARN("failed to get source tenant archive log path", KR(ret), K(source_tenant_id));
    } else if (OB_FAIL(restore_source_mgr.init(user_tenant_id, sql_proxy_))) {
      LOG_WARN("failed to init restore_source_mgr", KR(ret), K(user_tenant_id));
    } else if (OB_FAIL(restore_source_mgr.add_location_source(job.get_restore_scn(), log_path.str()))) {
      LOG_WARN("failed to add log restore source", KR(ret), K(job), K(log_path));
    }
  }
  if (OB_FAIL(ret)) {
    need_wait = true;
  }

  if (!need_wait) {
    if (OB_TMP_FAIL(try_update_job_status_(ret, job))) {
      LOG_WARN("fail to update job status", KR(ret), KR(tmp_ret), K(job));
    }
  }

  LOG_INFO("[RESTORE] clone init ls", KR(ret), KR(tmp_ret), K(job), K(need_wait));
  return ret;
}

int ObCloneScheduler::get_tenant_snap_ls_replica_simple_items_(
    const share::ObCloneJob &job,
    ObArray<share::ObTenantSnapLSReplicaSimpleItem>& ls_snapshot_array)
{
  int ret = OB_SUCCESS;
  ls_snapshot_array.reset();

  ObTenantSnapshotTableOperator snap_op;
  if (!is_user_tenant(job.get_source_tenant_id()) || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("source tenant is not user_tenant or sql proxy is null",
        KR(ret), K(job.get_source_tenant_id()), KP(sql_proxy_));
  } else {
    MTL_SWITCH(OB_SYS_TENANT_ID) {
      if (OB_FAIL(snap_op.init(job.get_source_tenant_id(), sql_proxy_))) {
        LOG_WARN("fail to init snap op", KR(ret), K(job));
      } else if (OB_FAIL(snap_op.get_tenant_snap_ls_replica_simple_items(
                         job.get_tenant_snapshot_id(),
                         ls_snapshot_array))) {
        LOG_WARN("fail to get_tenant_snap_ls_replica_simple_items", KR(ret), K(job));
      }
    }
  }
  return ret;
}

int ObCloneScheduler::check_one_ls_restore_finish_(
    const share::ObCloneJob& job,
    const ObLSStatusInfo& ls_status_info,
    const ObArray<ObLSInfo>& ls_info_array,
    const ObArray<ObTenantSnapLSReplicaSimpleItem>& ls_snapshot_array,
    TenantRestoreStatus &tenant_restore_status) /*a valid value in the outer func, do not reset it*/
{
  int ret = OB_SUCCESS;

  bool found_in_ls_meta_table = false;
  const ObLSInfo *ls_info_ptr = nullptr;
  for (int64_t i = 0; i < ls_info_array.count(); ++i) {
    if (ls_info_array.at(i).get_ls_id() == ls_status_info.get_ls_id()) {
      ls_info_ptr = &ls_info_array.at(i);
      found_in_ls_meta_table = true;
      break;
    }
  }

  if (!found_in_ls_meta_table) {
    ret = OB_NEED_WAIT;
    LOG_WARN("ls in __all_ls_status does not appear in __all_ls_meta_table", KR(ret),
                                                        K(ls_status_info), K(ls_info_array));
  } else if (OB_ISNULL(ls_info_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ls_info_ptr", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !tenant_restore_status.is_failed() && i < ls_info_ptr->get_replicas().count(); ++i) {
      const ObLSReplica &replica = ls_info_ptr->get_replicas().at(i);

      bool found_in_ls_snapshot = false;
      share::ObLSRestoreStatus ls_restore_status;
      for (int64_t j = 0; OB_SUCC(ret) && j < ls_snapshot_array.count(); ++j) {
        if (replica.get_server() == ls_snapshot_array.at(j).get_addr()) {
          if (ls_snapshot_array.at(j).get_status() != ObLSSnapStatus::NORMAL) {
            LOG_INFO("ls in __all_ls_meta_table does not be normal status in ls snapshot table",
                K(replica), K(ls_snapshot_array.at(j)));
          } else {
            if (OB_FAIL(ls_restore_status.set_status(replica.get_restore_status()))) {
              LOG_WARN("fail to set ls restore status", KR(ret), K(replica));
            }
            found_in_ls_snapshot = true;
          }
          break;
        }
      }

      if (OB_FAIL(ret)) {
      } else if (!found_in_ls_snapshot) {
        LOG_INFO("ls in __all_ls_meta_table does not be found in ls snapshot table or not be normal status",
            K(replica), K(ls_snapshot_array));
      } else if (!ls_restore_status.is_in_clone_or_none() || ls_restore_status.is_failed()) {
        tenant_restore_status = TenantRestoreStatus::FAILED;
        ROOTSERVICE_EVENT_ADD("clone", "clone_ls_replica_failed",
                              "job_id", job.get_job_id(),
                              "ls_restore_status", share::ObLSRestoreStatus::get_restore_status_str(ls_restore_status),
                              "ls_id", replica.get_ls_id().id(),
                              "server", replica.get_server());
        LOG_WARN("ls is restore failed or unexpected status",
            K(replica), K(ls_restore_status), K(tenant_restore_status));
      } else if (!ls_restore_status.is_none() && tenant_restore_status.is_success()) {
        tenant_restore_status = TenantRestoreStatus::IN_PROGRESS;
      }
    }
  }
  return ret;
}

int ObCloneScheduler::check_all_ls_restore_finish_(const share::ObCloneJob &job,
                                                   TenantRestoreStatus &tenant_restore_status)
{
  int ret = OB_SUCCESS;
  const uint64_t user_tenant_id = job.get_tenant_id();

  ObArray<ObLSInfo> ls_info_array;
  ObLSStatusInfoArray ls_array;
  ObArray<ObTenantSnapLSReplicaSimpleItem> ls_snapshot_array;

  ObLSStatusOperator status_op;
  tenant_restore_status = TenantRestoreStatus::SUCCESS;
  if (!is_user_tenant(user_tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the target tenant of clone job is not user tenant", KR(ret), K(job));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is unexpected nullptr", KR(ret), K(job));
  } else if (OB_ISNULL(GCTX.lst_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.lst_operator_ is unexpected nullptr", KR(ret), K(job));
  } else if (OB_FAIL(GCTX.lst_operator_->get_by_tenant(user_tenant_id, false, ls_info_array))) {
    LOG_WARN("fail to execute get_by_tenant", KR(ret), K(job));
  } else if (OB_FAIL(status_op.get_all_ls_status_by_order(user_tenant_id, ls_array, *sql_proxy_))) {
    LOG_WARN("failed to get all ls status", KR(ret), K(job));
  } else if (OB_FAIL(get_tenant_snap_ls_replica_simple_items_(job, ls_snapshot_array))) {
    LOG_WARN("fail to get_tenant_snap_ls_replica_simple_items_", KR(ret), K(job));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !tenant_restore_status.is_failed() && i < ls_array.count(); ++i) {
      const ObLSStatusInfo& ls_status_info = ls_array.at(i);
      if (OB_FAIL(check_one_ls_restore_finish_(job,
                                               ls_status_info,
                                               ls_info_array,
                                               ls_snapshot_array,
                                               tenant_restore_status))) {
        LOG_WARN("fail to check_one_ls_restore_finish_",
            KR(ret), K(ls_status_info), K(ls_info_array), K(ls_snapshot_array), K(tenant_restore_status), K(job));
      }
    }
    if (!tenant_restore_status.is_success()) {
      LOG_INFO("check all ls restore not finish, just wait",
          KR(ret), K(tenant_restore_status), K(job));
    }
  }

  return ret;
}

int ObCloneScheduler::clone_wait_ls_finish(const share::ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  const uint64_t user_tenant_id = job.get_tenant_id();
  TenantRestoreStatus tenant_restore_status;
  ObTenantSchema tenant_schema;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (has_set_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("clone scheduler stopped", KR(ret));
  } else if (!is_user_tenant(user_tenant_id) || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not user tenant or sql proxy is null", KR(ret), K(user_tenant_id), KP(sql_proxy_));
  } else if (OB_FAIL(ObTenantThreadHelper::get_tenant_schema(user_tenant_id, tenant_schema))) {
    LOG_WARN("failed to get tenant schema", KR(ret), K(user_tenant_id));
  } else if (!tenant_schema.is_restore_tenant_status()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant is not in restore status", KR(ret), K(tenant_schema));
  } else if (OB_FAIL(check_all_ls_restore_finish_(job, tenant_restore_status))) {
    LOG_WARN("failed to check all ls restore finish", KR(ret), K(job));
  } else if (tenant_restore_status.is_finish()) {
    int tmp_ret = OB_SUCCESS;
    int tenant_restore_result = OB_LS_RESTORE_FAILED;
    if (tenant_restore_status.is_success()) {
      tenant_restore_result = OB_SUCCESS;
    }
    if (OB_TMP_FAIL(try_update_job_status_(tenant_restore_result, job))) {
      LOG_WARN("fail to update job status", KR(tmp_ret), KR(tenant_restore_result), K(job));
    }
    LOG_INFO("[RESTORE] clone wait all ls finish", KR(ret), KR(tenant_restore_result), K(job));
  }
  return ret;
}

int ObCloneScheduler::clone_post_check(const share::ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  const uint64_t user_tenant_id = job.get_tenant_id();
  const uint64_t source_tenant_id = job.get_source_tenant_id();
  bool sync_satisfied = true;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (!is_user_tenant(user_tenant_id) || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not user tenant or sql proxy is null", KR(ret), K(user_tenant_id), KP(sql_proxy_));
  } else if (has_set_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("clone scheduler stopped", KR(ret));
  } else if (OB_FAIL(ObRestoreCommonUtil::try_update_tenant_role(sql_proxy_, user_tenant_id,
                  job.get_restore_scn(), true /*is_clone*/, sync_satisfied))) {
    LOG_WARN("failed to try update tenant role", KR(ret), K(user_tenant_id), K(job));
  } else if (!sync_satisfied) {
    ret = OB_NEED_WAIT;
    LOG_WARN("tenant sync scn not equal to restore scn, need wait", KR(ret), K(job));
  }

  if (FAILEDx(ObRestoreCommonUtil::process_schema(sql_proxy_, user_tenant_id))) {
    LOG_WARN("failed to process schema", KR(ret), K(user_tenant_id));
  } else if (OB_FAIL(convert_parameters_(user_tenant_id, source_tenant_id))) {
    LOG_WARN("failed to convert parameters", KR(ret), K(user_tenant_id), K(source_tenant_id));
  }

  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(try_update_job_status_(ret, job))) {
      LOG_WARN("fail to update job status", KR(ret), KR(tmp_ret), K(job));
    }
  }
  LOG_INFO("[RESTORE] clone post check", KR(ret), K(job));
  return ret;
}

int ObCloneScheduler::clone_user_finish(const share::ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  uint64_t user_tenant_id = job.get_tenant_id();
  bool sync_satisfied = true;

  DEBUG_SYNC(BEFORE_CREATE_CLONE_TENANT_END);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (has_set_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("clone scheduler stopped", KR(ret));
  } else if (OB_UNLIKELY(!is_user_tenant(user_tenant_id)
                         || OB_ISNULL(sql_proxy_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not user tenant or sql proxy is null", KR(ret), K(user_tenant_id), KP(sql_proxy_));
  } else if (OB_FAIL(ObTenantCloneUtil::recycle_clone_job(*sql_proxy_, job))) {
    LOG_WARN("fail to recycle clone job", KR(ret), K(user_tenant_id));
  }

  LOG_INFO("[RESTORE] clone user finish", KR(ret), K(job));
  const char *status_str = ObTenantCloneStatus::get_clone_status_str(job.get_status());
  ROOTSERVICE_EVENT_ADD("clone", "clone_user_finish",
                        "job_id", job.get_job_id(),
                        K(ret),
                        "cur_clone_status", status_str);
  return ret;
}

// 1. for clone_tenant, gc the resource of resource_pool and clone_tenant
// 2. for source_tenant, release global_lock and tenant snapshot
// 3. for sys_tenant, finish the clone job
ERRSIM_POINT_DEF(ERRSIM_CLONE_RECYCLE_FAILED_JOB_ERROR);
int ObCloneScheduler::clone_recycle_failed_job(const share::ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(HANG_IN_CLONE_SYS_FAILED_STATUS);
  const uint64_t tenant_id = job.get_tenant_id();
  const uint64_t source_tenant_id = job.get_source_tenant_id();
  const ObTenantCloneStatus job_status = job.get_status();

  if (OB_UNLIKELY(ERRSIM_CLONE_RECYCLE_FAILED_JOB_ERROR)) {
    ret = ERRSIM_CLONE_RECYCLE_FAILED_JOB_ERROR;
    LOG_WARN("mock clone recycle failed job failed", KR(ret), K(job));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (has_set_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("clone scheduler stopped", KR(ret));
  } else if (!is_sys_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!job.is_valid())
                         || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid clone job", KR(ret), K(job), KP(sql_proxy_));
  } else if (!job_status.is_sys_failed_status()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("try to recycle a processing or successful job", KR(ret), K(job));
  } else if (job_status.is_sys_release_clone_resource_status() &&
             OB_FAIL(ObTenantCloneUtil::release_clone_tenant_resource_of_clone_job(job))) {
    LOG_WARN("fail to release resource of clone tenant", KR(ret), K(job));
  } else if (OB_FAIL(ObTenantCloneUtil::release_source_tenant_resource_of_clone_job(*sql_proxy_, job))) {
    LOG_WARN("fail to release resource of source tenant", KR(ret), K(job));
  } else if (job_status.is_sys_canceling_status()) {
    const_cast<ObCloneJob&>(job).set_status(ObTenantCloneStatus::Status::CLONE_SYS_CANCELED);
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTenantCloneUtil::recycle_clone_job(*sql_proxy_, job))) {
      LOG_WARN("fail to recycle clone job", KR(ret), K(job));
    }
    const char *status_str = ObTenantCloneStatus::get_clone_status_str(job.get_status());
    ROOTSERVICE_EVENT_ADD("clone", "clone_recycle_failed_job_finish",
                          "job_id", job.get_job_id(),
                          K(ret),
                          "cur_clone_status", status_str);
  }
  LOG_INFO("[RESTORE] clone recycle failed job", KR(ret), K(job));
  return ret;
}

int ObCloneScheduler::try_update_job_status_(
    const int return_ret,
    const ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTenantCloneTableOperator clone_op;
  const uint64_t tenant_id = job.get_tenant_id();
  const ObTenantCloneStatus cur_status = job.get_status();
  ObTenantCloneStatus next_status;
  ObMySQLTransaction trans;
  ObString err_msg;
  ObArenaAllocator str_alloc;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (has_set_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("clone scheduler stopped", KR(ret));
  } else if (OB_FAIL(deep_copy_ob_string(str_alloc, common::ob_get_tsi_err_msg(return_ret), err_msg))) {
    LOG_WARN("fail to deep copy", KR(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id)))) {
    LOG_WARN("failed to start trans in meta tenant", KR(ret), K(gen_meta_tenant_id(tenant_id)));
  } else if (OB_FAIL(clone_op.init(gen_user_tenant_id(tenant_id), &trans))) {
    LOG_WARN("fail to init clone_op", KR(ret), K(tenant_id));
  } else {
    if (is_sys_tenant(tenant_id)) {
      next_status = get_sys_next_status_(return_ret, cur_status, job.get_job_type());
    } else {
      next_status = get_user_next_status_(return_ret, cur_status);
    }
    if (!next_status.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status", KR(ret), K(next_status));
    } else if (OB_FAIL(clone_op.update_job_status(job.get_job_id(), job.get_status(), next_status))) {
      LOG_WARN("fail to update job status", KR(ret), K(job), K(next_status));
    } else if (OB_UNLIKELY(OB_SUCCESS != return_ret)) {
      if (OB_FAIL(clone_op.update_job_failed_info(job.get_job_id(), return_ret, err_msg))) {
        LOG_WARN("fail to update job failed info", KR(ret), K(job), K(return_ret), K(err_msg));
      }
    }
  }

  if (trans.is_started()) {
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", "is_commit", (OB_SUCCESS == ret), KR(tmp_ret));
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
    }
  }
  if (OB_SUCC(ret)) {
    work_immediately_ = true;
    (void)ObTenantCloneUtil::try_to_record_clone_status_change_rs_event(
          job, cur_status, next_status, return_ret, ObCancelCloneJobReason());
  }
  return ret;
}

ObTenantCloneStatus ObCloneScheduler::get_sys_next_status_(int return_ret,
                                                           const share::ObTenantCloneStatus current_status,
                                                           const share::ObTenantCloneJobType job_type)
{
  ObTenantCloneStatus next_status = OB_SUCCESS == return_ret ?
                                    get_sys_next_status_in_normal_(current_status, job_type) :
                                    get_sys_next_status_in_failed_(current_status);

  return next_status;
}

share::ObTenantCloneStatus ObCloneScheduler::get_sys_next_status_in_normal_(const share::ObTenantCloneStatus current_status,
                                                                            const share::ObTenantCloneJobType job_type)
{
  ObTenantCloneStatus next_status;

  switch (current_status) {
    case ObTenantCloneStatus::Status::CLONE_SYS_LOCK:
      next_status = ObTenantCloneStatus::Status::CLONE_SYS_CREATE_INNER_RESOURCE_POOL;
      break;
    case ObTenantCloneStatus::Status::CLONE_SYS_CREATE_INNER_RESOURCE_POOL:
      next_status = share::ObTenantCloneJobType::RESTORE == job_type ?
                    ObTenantCloneStatus::Status::CLONE_SYS_CREATE_TENANT :
                    ObTenantCloneStatus::Status::CLONE_SYS_CREATE_SNAPSHOT;
      break;
    case ObTenantCloneStatus::Status::CLONE_SYS_CREATE_SNAPSHOT:
      next_status = ObTenantCloneStatus::Status::CLONE_SYS_WAIT_CREATE_SNAPSHOT;
      break;
    case ObTenantCloneStatus::Status::CLONE_SYS_WAIT_CREATE_SNAPSHOT:
      next_status = ObTenantCloneStatus::Status::CLONE_SYS_CREATE_TENANT;
      break;
    case ObTenantCloneStatus::Status::CLONE_SYS_CREATE_TENANT:
      next_status = ObTenantCloneStatus::Status::CLONE_SYS_WAIT_TENANT_RESTORE_FINISH;
      break;
    case ObTenantCloneStatus::Status::CLONE_SYS_WAIT_TENANT_RESTORE_FINISH:
      next_status = ObTenantCloneStatus::Status::CLONE_SYS_RELEASE_RESOURCE;
      break;
    case ObTenantCloneStatus::Status::CLONE_SYS_RELEASE_RESOURCE:
      next_status = ObTenantCloneStatus::Status::CLONE_SYS_SUCCESS;
      break;
    default :
      //do nothing
      break;
  }
  return next_status;
}

share::ObTenantCloneStatus ObCloneScheduler::get_sys_next_status_in_failed_(const share::ObTenantCloneStatus current_status)
{
  ObTenantCloneStatus next_status;

  switch (current_status) {
    case ObTenantCloneStatus::Status::CLONE_SYS_LOCK:
      next_status = ObTenantCloneStatus::Status::CLONE_SYS_LOCK_FAIL;
      break;
    case ObTenantCloneStatus::Status::CLONE_SYS_CREATE_INNER_RESOURCE_POOL:
      next_status = ObTenantCloneStatus::Status::CLONE_SYS_CREATE_INNER_RESOURCE_POOL_FAIL;
      break;
    case ObTenantCloneStatus::Status::CLONE_SYS_CREATE_SNAPSHOT:
      next_status = ObTenantCloneStatus::Status::CLONE_SYS_CREATE_SNAPSHOT_FAIL;
      break;
    case ObTenantCloneStatus::Status::CLONE_SYS_WAIT_CREATE_SNAPSHOT:
      next_status = ObTenantCloneStatus::Status::CLONE_SYS_WAIT_CREATE_SNAPSHOT_FAIL;
      break;
    case ObTenantCloneStatus::Status::CLONE_SYS_CREATE_TENANT:
      next_status = ObTenantCloneStatus::Status::CLONE_SYS_CREATE_TENANT_FAIL;
      break;
    case ObTenantCloneStatus::Status::CLONE_SYS_WAIT_TENANT_RESTORE_FINISH:
      next_status = ObTenantCloneStatus::Status::CLONE_SYS_WAIT_TENANT_RESTORE_FINISH_FAIL;
      break;
    case ObTenantCloneStatus::Status::CLONE_SYS_RELEASE_RESOURCE:
      next_status = ObTenantCloneStatus::Status::CLONE_SYS_RELEASE_RESOURCE_FAIL;
      break;
    default :
      //do nothing
      break;
  }
  return next_status;
}

ObTenantCloneStatus ObCloneScheduler::get_user_next_status_(
                    const int return_ret,
                    const ObTenantCloneStatus current_status)
{
  ObTenantCloneStatus next_status;
  if (OB_SUCCESS != return_ret) {
    next_status = ObTenantCloneStatus::Status::CLONE_USER_FAIL;
  } else {
    switch (current_status) {
      case ObTenantCloneStatus::Status::CLONE_USER_PREPARE:
        next_status = ObTenantCloneStatus::Status::CLONE_USER_CREATE_INIT_LS;
        break;
      case ObTenantCloneStatus::Status::CLONE_USER_CREATE_INIT_LS:
        next_status = ObTenantCloneStatus::Status::CLONE_USER_WAIT_LS;
        break;
      case ObTenantCloneStatus::Status::CLONE_USER_WAIT_LS:
        next_status = ObTenantCloneStatus::Status::CLONE_USER_POST_CHECK;
        break;
      case ObTenantCloneStatus::Status::CLONE_USER_POST_CHECK:
        next_status = ObTenantCloneStatus::Status::CLONE_USER_SUCCESS;
        break;
      default :
        //do nothing
        break;
    }
  }
  return next_status;
}

int ObCloneScheduler::check_sys_tenant_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObSimpleTenantSchema *tenant_schema = NULL;
  uint64_t data_version = 0;

  if (!is_sys_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected tenant id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null schema service", KR(ret), KP(GCTX.schema_service_));
  } else if (GCTX.is_standby_cluster() || GCONF.in_upgrade_mode()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("clone tenant while in standby cluster or in upgrade mode is not allowed", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("failed to get tenant info", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist", KR(ret), K(tenant_id));
  } else if (!tenant_schema->is_normal()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("sys tenant is not normal", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, data_version))) {
    LOG_WARN("fail to get sys tenant data version", KR(ret));
  } else if (DATA_VERSION_4_3_0_0 > data_version) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("sys tenant data version is below 4.3", KR(ret), K(tenant_id));
  }

  return ret;
}

//non-cloning tenant need stop this thread to save resources
int ObCloneScheduler::check_meta_tenant_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  bool compatibility_satisfied = false;
  uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
  bool is_clone_tenant = true;
  ObAllTenantInfo all_tenant_info;

  if (!is_meta_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected tenant id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null schema service", KR(ret), KP(GCTX.schema_service_));
  } else if (GCONF.in_upgrade_mode()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("clone tenant while in upgrade mode is not allowed", KR(ret));
  } else if (OB_FAIL(ObShareUtil::check_compat_version_for_clone_tenant(tenant_id, compatibility_satisfied))) {
    LOG_WARN("check tenant compatibility failed", KR(ret), K(tenant_id));
  } else if (!compatibility_satisfied) {
    ret = OB_OP_NOT_ALLOW;
    LOG_INFO("tenant data version is below 4.3", KR(ret), K(tenant_id), K(compatibility_satisfied));
  } else {
    share::schema::ObSchemaGetterGuard schema_guard;
    const share::schema::ObSimpleTenantSchema *meta_tenant_schema = NULL;
    const share::schema::ObTenantSchema *user_tenant_schema = NULL;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                  OB_SYS_TENANT_ID, schema_guard))) {
      LOG_WARN("fail to get schema guard", KR(ret));
    } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, meta_tenant_schema))) {
      LOG_WARN("failed to get tenant info", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(meta_tenant_schema)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant not exist", KR(ret), K(tenant_id));
    } else if (!meta_tenant_schema->is_normal()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_INFO("meta tenant is not normal", KR(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_tenant_info(user_tenant_id, user_tenant_schema))) {
      LOG_WARN("failed to get tenant info", KR(ret), K(user_tenant_id));
    } else if (OB_ISNULL(user_tenant_schema)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant not exist", KR(ret), K(user_tenant_id));
    } else if (!user_tenant_schema->is_restore_tenant_status()) {
      //we only stop this thread in clone tenant when the status of the tenant is normal
      is_clone_tenant = false;
      LOG_INFO("tenant is not in restore status", KR(ret), KPC(user_tenant_schema));
    } else { /*restore_tenant_status*/
      if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(user_tenant_id, GCTX.sql_proxy_,
                            false/*for_update*/, all_tenant_info))) {
        LOG_WARN("failed to load tenant info", KR(ret), K(user_tenant_id));
      } else if (all_tenant_info.is_restore()) {
        is_clone_tenant = false;
        LOG_INFO("tenant is in restore role", KR(ret), K(all_tenant_info));
      }
    }
  }

  if (OB_SUCC(ret) && !is_clone_tenant) {
    stop();
  }

  return ret;
}

int ObCloneScheduler::fill_clone_resource_pool_arg_(
    const share::ObCloneJob &job,
    const uint64_t resource_pool_id,
    obrpc::ObCloneResourcePoolArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job));
  } else if (OB_FAIL(arg.init(job.get_resource_pool_name(),
                              job.get_unit_config_name(),
                              job.get_source_tenant_id(),
                              resource_pool_id))) {
    LOG_WARN("fail to init arg", KR(ret), K(job));
  } else {
    arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
  }
  return ret;
}

int ObCloneScheduler::fill_create_tenant_arg_(
    const share::ObCloneJob &job,
    const uint64_t clone_tenant_id,
    obrpc::ObCreateTenantArg &arg)
{
  int ret = OB_SUCCESS;
  arg.reset();
  ObTenantSnapshotTableOperator snap_op;
  const uint64_t source_tenant_id = job.get_source_tenant_id();
  const ObTenantSnapshotID &snapshot_id = job.get_tenant_snapshot_id();
  ObTenantSnapItem snap_item;
  ObTenantSchema source_tenant_schema;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (has_set_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("clone scheduler stopped", KR(ret));
  } else if (OB_UNLIKELY(!job.is_valid()
                         || !snapshot_id.is_valid()
                         || !job.get_restore_scn().is_valid()
                         || OB_INVALID_TENANT_ID == clone_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job), K(clone_tenant_id));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null schema service", KR(ret), KP(schema_service_));
  } else if (OB_FAIL(ObTenantThreadHelper::get_tenant_schema(source_tenant_id, source_tenant_schema))) {
    LOG_WARN("failed to get tenant schema", KR(ret), K(source_tenant_id));
  } else if (OB_FAIL(snap_op.init(source_tenant_id, sql_proxy_))) {
    LOG_WARN("failed to init table op", KR(ret), K(source_tenant_id));
  } else if (OB_FAIL(snap_op.get_tenant_snap_item(job.get_tenant_snapshot_id(), false, snap_item))) {
    LOG_WARN("failed to get snap item", KR(ret), K(source_tenant_id), K(job.get_tenant_snapshot_id()));
  } else if (OB_FAIL(arg.pool_list_.push_back(job.get_resource_pool_name()))) {
    LOG_WARN("failed to push back", KR(ret), K(job.get_resource_pool_name()));
  } else if (OB_FAIL(arg.tenant_schema_.set_tenant_name(job.get_clone_tenant_name()))) {
    LOG_WARN("fail to set tenant name", KR(ret), K(job.get_clone_tenant_name()));
  } else if (OB_FAIL(arg.tenant_schema_.set_primary_zone(source_tenant_schema.get_primary_zone()))) {
    LOG_WARN("fail to set primary zone", KR(ret), K(source_tenant_schema.get_primary_zone()));
  } else if (OB_FAIL(arg.tenant_schema_.set_locality(source_tenant_schema.get_locality()))) {
    LOG_WARN("fail to set locality", KR(ret), K(source_tenant_schema.get_locality()));
  } else {
    HEAP_VAR(ObLSMetaPackage, sys_ls_meta_package) {
      if (OB_FAIL(snap_op.get_proper_ls_meta_package(snapshot_id, SYS_LS, sys_ls_meta_package))) {
        LOG_WARN("failed to get sys ls meta package", KR(ret), K(source_tenant_id), K(snapshot_id));
      } else {
        arg.tenant_schema_.set_compatibility_mode(source_tenant_schema.get_compatibility_mode());
        arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
        arg.tenant_schema_.set_tenant_id(clone_tenant_id);
        arg.if_not_exist_ = false;
        arg.palf_base_info_ = sys_ls_meta_package.palf_meta_;
        arg.recovery_until_scn_ = job.get_restore_scn();
        arg.compatible_version_ = snap_item.get_data_version();
        arg.source_tenant_id_ = source_tenant_id;
      }
    }
  }

  return ret;
}

int ObCloneScheduler::clone_root_key_(const share::ObCloneJob &job)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_TDE_SECURITY
  const uint64_t user_tenant_id = job.get_tenant_id();
  const uint64_t source_tenant_id = job.get_source_tenant_id();
  ObRootKey root_key;

  if (OB_UNLIKELY(!is_user_tenant(user_tenant_id)
                  || !is_user_tenant(source_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(user_tenant_id), K(source_tenant_id));
  } else if (OB_FAIL(ObMasterKeyGetter::instance().get_root_key(source_tenant_id, root_key, true))) {
    LOG_WARN("failed to get root key from source tenant", KR(ret), K(source_tenant_id));
  } else if (obrpc::RootKeyType::INVALID == root_key.key_type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid root key", KR(ret));
  } else if (OB_FAIL(ObMasterKeyGetter::instance().set_root_key(user_tenant_id, root_key.key_type_,
                                                                  root_key.key_, true))) {
    LOG_WARN("failed to set root key for clone tenant", KR(ret));
  } else if (OB_FAIL(ObRestoreCommonUtil::notify_root_key(srv_rpc_proxy_, sql_proxy_,
                                          user_tenant_id, root_key))) {
    LOG_WARN("failed to notify root key", KR(ret), K(user_tenant_id));
  }
#endif
  return ret;
}

int ObCloneScheduler::clone_keystore_(const share::ObCloneJob &job)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_TDE_SECURITY
  const int64_t timeout = max(GCONF.rpc_timeout, DEFAULT_TIMEOUT);
  ObString tde_method;
  const uint64_t user_tenant_id = job.get_tenant_id();
  const uint64_t source_tenant_id = job.get_source_tenant_id();
  ObUnitTableOperator unit_operator;
  common::ObArray<ObUnit> units;
  ObArray<int> return_code_array;
  obrpc::ObCloneKeyArg arg;
  if (OB_FAIL(ObEncryptionUtil::get_tde_method(source_tenant_id, tde_method))) {
    LOG_WARN("failed to get tde_method", KR(ret), K(source_tenant_id));
  } else if (OB_UNLIKELY(tde_method.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tde_method is empty", KR(ret));
  } else if (!ObTdeMethodUtil::is_valid(tde_method)) {
    //source tenant does not enable encryption.
    //do nothing
  } else if (OB_ISNULL(srv_rpc_proxy_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null svr rpc proxy or sql proxy", KR(ret));
  } else if (OB_FAIL(unit_operator.init(*sql_proxy_))) {
    LOG_WARN("failed to init unit operator", KR(ret));
  } else if (OB_FAIL(unit_operator.get_units_by_tenant(user_tenant_id, units))) {
    LOG_WARN("failed to get tenant unit", KR(ret), K(user_tenant_id));
  } else {
    ObCloneKeyProxy proxy(*srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::clone_key);
    arg.set_tenant_id(user_tenant_id);
    arg.set_source_tenant_id(source_tenant_id);
    for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); i++) {
      const ObUnit &unit = units.at(i);
      if (OB_FAIL(proxy.call(unit.server_, timeout, arg))) {
        LOG_WARN("failed to send rpc", KR(ret), K(unit.server_), K(arg));
      }
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
      LOG_WARN("wait batch result failed", KR(tmp_ret), KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
    if (OB_SUCC(ret)) {
      if (return_code_array.count() != proxy.get_dests().count()
          || return_code_array.count() != proxy.get_args().count()
          || return_code_array.count() != proxy.get_results().count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("count not match", KR(ret), K(return_code_array.count()), K(proxy.get_dests().count()),
                                  K(proxy.get_args().count()), K(proxy.get_results().count()));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); i++) {
          int res_ret = return_code_array.at(i);
          const ObAddr &addr = proxy.get_dests().at(i);
          if (OB_SUCCESS != res_ret) {
            ret = res_ret;
            LOG_WARN("rpc execute failed", KR(ret), K(addr));
          }
        }
      }
    }
  }
#endif
  return ret;
}

int ObCloneScheduler::get_ls_attrs_from_source_(
    const share::ObCloneJob &job,
    ObSArray<ObLSAttr> &ls_attr_array)
{
  int ret = OB_SUCCESS;
  ls_attr_array.reset();
  ObTenantSnapshotTableOperator table_op;
  ObArray<ObTenantSnapLSItem> snap_ls_items;

  if (OB_UNLIKELY(!job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job));
  } else {
    const uint64_t source_tenant_id = job.get_source_tenant_id();
    const ObTenantSnapshotID &snapshot_id = job.get_tenant_snapshot_id();
    MTL_SWITCH(OB_SYS_TENANT_ID) {
      if (OB_FAIL(table_op.init(source_tenant_id, sql_proxy_))) {
        LOG_WARN("failed to init table op", KR(ret));
      } else if (OB_FAIL(table_op.get_tenant_snap_ls_items(snapshot_id, snap_ls_items))) {
        LOG_WARN("failed to get tenant snapshot ls items", KR(ret), K(source_tenant_id), K(snapshot_id));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ARRAY_FOREACH_N(snap_ls_items, i, cnt) {
      const ObTenantSnapLSItem &snap_ls_item = snap_ls_items.at(i);
      if (OB_FAIL(ls_attr_array.push_back(snap_ls_item.get_ls_attr()))) {
        LOG_WARN("failed to push back ls attr", KR(ret), K(snap_ls_item));
      }
    }
  }
  return ret;
}

int ObCloneScheduler::create_all_ls_(
  const uint64_t user_tenant_id,
  const share::schema::ObTenantSchema &tenant_schema,
  const common::ObIArray<share::ObLSAttr> &ls_attr_array,
  const uint64_t source_tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (has_set_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("clone scheduler stopped", KR(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(sql_proxy_));
  } else if (OB_FAIL(ObRestoreCommonUtil::create_all_ls(sql_proxy_, user_tenant_id,
                            tenant_schema, ls_attr_array, source_tenant_id))) {
    LOG_WARN("failed to create all ls", KR(ret), K(user_tenant_id), K(tenant_schema),
                                    K(ls_attr_array), K(source_tenant_id));
  }
  return ret;
}

int ObCloneScheduler::wait_all_ls_created_(
    const share::schema::ObTenantSchema &tenant_schema,
    const share::ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  const uint64_t source_tenant_id = job.get_source_tenant_id();
  const ObTenantSnapshotID &tenant_snapshot_id = job.get_tenant_snapshot_id();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (has_set_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("clone scheduler stopped", KR(ret));
  } else if (OB_UNLIKELY(!tenant_schema.is_valid()
                         || !job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_schema), K(job));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(sql_proxy_));
  } else {
    const uint64_t tenant_id = tenant_schema.get_tenant_id();
    ObLSStatusOperator status_op;
    ObLSStatusInfoArray ls_array;
    ObLSRecoveryStat recovery_stat;
    ObLSRecoveryStatOperator ls_recovery_operator;

    if (OB_FAIL(status_op.get_all_ls_status_by_order(tenant_id, ls_array,
                                                     *sql_proxy_))) {
      LOG_WARN("failed to get all ls status", KR(ret), K(tenant_id));
    } else {
      HEAP_VAR(ObLSMetaPackage, ls_meta_package) {
        for (int64_t i = 0; OB_SUCC(ret) && i < ls_array.count(); ++i) {
          const ObLSStatusInfo &info = ls_array.at(i);
          if (info.ls_is_creating()) {
            recovery_stat.reset();
            ls_meta_package.reset();
            if (OB_FAIL(ls_recovery_operator.get_ls_recovery_stat(tenant_id, info.ls_id_,
                  false/*for_update*/, recovery_stat, *sql_proxy_))) {
              LOG_WARN("failed to get ls recovery stat", KR(ret), K(tenant_id), K(info));
            } else {
              MTL_SWITCH(OB_SYS_TENANT_ID) {
                ObTenantSnapshotTableOperator snap_op;
                if (OB_FAIL(snap_op.init(source_tenant_id, sql_proxy_))) {
                  LOG_WARN("failed to init snap op", KR(ret), K(source_tenant_id));
                } else if (OB_FAIL(snap_op.get_proper_ls_meta_package(tenant_snapshot_id, info.ls_id_, ls_meta_package))) {
                  LOG_WARN("failed to get ls meta package", KR(ret), K(tenant_snapshot_id), K(info.ls_id_));
                }
              }
            }
            if (FAILEDx(ObCommonLSService::do_create_user_ls(
                            tenant_schema, info, recovery_stat.get_create_scn(),
                            true, /*create with palf*/
                            ls_meta_package.palf_meta_, source_tenant_id))) {
              LOG_WARN("failed to create ls with palf", KR(ret), K(info), K(tenant_schema),
                          K(recovery_stat.get_create_scn()), K(ls_meta_package.palf_meta_), K(source_tenant_id));
            }
          }
        }
      }
    }
    LOG_INFO("[RESTORE] wait ls created", KR(ret), K(tenant_id), K(ls_array));
  }
  return ret;
}

int ObCloneScheduler::finish_create_ls_(
    const share::schema::ObTenantSchema &tenant_schema,
    const common::ObIArray<share::ObLSAttr> &ls_attr_array)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (has_set_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("clone scheduler stopped", KR(ret));
  } else if (OB_UNLIKELY(!tenant_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_schema));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(sql_proxy_));
  } else if (OB_FAIL(ObRestoreCommonUtil::finish_create_ls(sql_proxy_, tenant_schema, ls_attr_array))) {
    LOG_WARN("failed to finish create ls", KR(ret), K(tenant_schema));
  }
  return ret;
}

int ObCloneScheduler::get_source_tenant_archive_log_path_(
    const uint64_t source_tenant_id,
    share::ObBackupPathString& path)
{
  int ret = OB_SUCCESS;
  ObArchivePersistHelper op;
  common::ObArray<std::pair<int64_t, ObBackupPathString>> dest_arr;

  if (OB_UNLIKELY(!is_user_tenant(source_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(source_tenant_id));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(sql_proxy_));
  } else {
    MTL_SWITCH(OB_SYS_TENANT_ID) {
      if (OB_FAIL(op.init(source_tenant_id))) {
        LOG_WARN("fail to init ObArchivePersistHelper", KR(ret), K(source_tenant_id));
      } else if (OB_FAIL(op.get_valid_dest_pairs(*sql_proxy_, dest_arr))) {
        LOG_WARN("fail to get_valid_dest_pairs", KR(ret), K(source_tenant_id));
      } else if (dest_arr.empty()) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("dest arr is empty", KR(ret), K(source_tenant_id));
      } else {
        path = dest_arr.at(0).second;
        LOG_INFO("get source tenant archive log dest path succ", K(path), K(source_tenant_id));
      }
    }
  }
  return ret;
}

int ObCloneScheduler::convert_parameters_(
    const uint64_t user_tenant_id,
    const uint64_t source_tenant_id)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_TDE_SECURITY
  ObString tde_method;
  ObString kms_info;
  ObSqlString sql;
  int64_t affected_row = 0;
  uint64_t latest_master_key_id = 0;
  bool source_has_encrypt_info = true;
  bool clone_has_encrypt_info = true;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (has_set_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("clone scheduler stopped", KR(ret));
  } else if (OB_UNLIKELY(!is_user_tenant(user_tenant_id)
                         || !is_user_tenant(source_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(user_tenant_id), K(source_tenant_id));
  } else if (OB_FAIL(ObEncryptionUtil::get_tde_method(source_tenant_id, tde_method))) {
    LOG_WARN("failed to get tde_method", KR(ret), K(source_tenant_id));
  } else if (OB_UNLIKELY(tde_method.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tde_method is empty", KR(ret));
  } else if (!ObTdeMethodUtil::is_valid(tde_method)) {
    source_has_encrypt_info = false;
  } else if (OB_FAIL(get_latest_key_id_(user_tenant_id, latest_master_key_id))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      clone_has_encrypt_info = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get latest key id", KR(ret), K(user_tenant_id));
    }
  } else if (ObTdeMethodUtil::is_kms(tde_method)
             && OB_FAIL(ObEncryptionUtil::get_tde_kms_info(source_tenant_id, kms_info))) {
    LOG_WARN("failed to get tde kms info", KR(ret), K(tde_method), K(source_tenant_id));
    //TODO: TDE method can change | kms_info may change
  }

  /* If the source tenant has encrypt_info,
    (under current regulations)
    its tde_method cannot be changed, and encryption cannot be turned off.
    So if false == source_has_encrypt_info,
    we can assure clone tenant does not enable encryption. */
  if (OB_SUCC(ret) && source_has_encrypt_info) {
    if (OB_FAIL(trim_master_key_map_(user_tenant_id, latest_master_key_id))) {
      LOG_WARN("fail to trim master key map", KR(ret), K(user_tenant_id), K(latest_master_key_id));
    } else if (!clone_has_encrypt_info) {
      //do nothing
    } else if (OB_FAIL(ObRestoreCommonUtil::set_tde_parameters(sql_proxy_, rpc_proxy_,
                                    user_tenant_id, tde_method, kms_info))) {
      LOG_WARN("failed to set_tde_parameters", KR(ret), K(user_tenant_id), K(tde_method));
    }
  }
#endif
  return ret;
}

int ObCloneScheduler::trim_master_key_map_(
    const uint64_t user_tenant_id,
    const uint64_t latest_key_id)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_TDE_SECURITY
  const int64_t timeout = max(GCONF.rpc_timeout, DEFAULT_TIMEOUT);
  ObUnitTableOperator unit_operator;
  common::ObArray<ObUnit> units;
  ObArray<int> return_code_array;
  obrpc::ObTrimKeyListArg arg;
  if (OB_UNLIKELY(!is_user_tenant(user_tenant_id)
                  || OB_INVALID_ID == latest_key_id
                  || OB_ISNULL(sql_proxy_)
                  || OB_ISNULL(srv_rpc_proxy_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(user_tenant_id), K(latest_key_id),
                                              KP(sql_proxy_), KP(srv_rpc_proxy_));
  } else if (OB_FAIL(unit_operator.init(*sql_proxy_))) {
    LOG_WARN("failed to init unit operator", KR(ret));
  } else if (OB_FAIL(unit_operator.get_units_by_tenant(user_tenant_id, units))) {
    LOG_WARN("failed to get tenant unit", KR(ret), K(user_tenant_id));
  } else {
    ObTrimKeyListProxy proxy(*srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::trim_key_list);
    arg.set_tenant_id(user_tenant_id);
    arg.set_latest_master_key_id(latest_key_id);
    ARRAY_FOREACH_N(units, i, cnt) {
      const ObUnit &unit = units.at(i);
      if (OB_FAIL(proxy.call(unit.server_, timeout, arg))) {
        LOG_WARN("failed to send rpc", KR(ret));
      }
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
      LOG_WARN("wait batch result failed", KR(tmp_ret), KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
    if (OB_SUCC(ret)) {
      if (return_code_array.count() != proxy.get_dests().count()
          || return_code_array.count() != proxy.get_args().count()
          || return_code_array.count() != proxy.get_results().count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("count not match", KR(ret), K(return_code_array.count()), K(proxy.get_dests().count()),
                                  K(proxy.get_args().count()), K(proxy.get_results().count()));
      } else {
        ARRAY_FOREACH_N(return_code_array, i, cnt) {
          int res_ret = return_code_array.at(i);
          const ObAddr &addr = proxy.get_dests().at(i);
          if (OB_SUCCESS != res_ret) {
            ret = res_ret;
            LOG_WARN("rpc execute failed", KR(ret), K(addr));
          }
        }
      }
    }
  }
#endif
  return ret;
}

int ObCloneScheduler::get_latest_key_id_(
    const uint64_t user_tenant_id,
    uint64_t &latest_key_id)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_TDE_SECURITY
  ObSqlString sql;
  if (OB_UNLIKELY(!is_user_tenant(user_tenant_id)
                  || OB_ISNULL(sql_proxy_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(user_tenant_id), KP(sql_proxy_));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("SELECT master_key_id FROM %s ORDER BY MASTER_KEY_ID DESC LIMIT 1",
                                 OB_ALL_TENANT_KEYSTORE_HISTORY_TNAME))) {
        LOG_WARN("assign sql failed", KR(ret));
      //TODO: exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id)
      } else if (OB_FAIL(sql_proxy_->read(res, user_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql));
      } else if (OB_UNLIKELY(NULL == (result = res.get_result()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get result", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        } else {
          LOG_WARN("next failed", KR(ret));
        }
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "master_key_id", latest_key_id, uint64_t);
      }
    }
  }
#endif
  return ret;
}

int ObCloneScheduler::check_data_version_before_finish_clone_(
    const uint64_t source_tenant_id)
{
  int ret = OB_SUCCESS;
  ObTenantCloneTableOperator clone_op;
  ObCloneJob clone_job;
  uint64_t current_min_cluster_version = GET_MIN_CLUSTER_VERSION();
  uint64_t current_data_version = 0;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == source_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(source_tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(clone_op.init(OB_SYS_TENANT_ID, GCTX.sql_proxy_))) {
    LOG_WARN("fail to init clone table operator", KR(ret), K(source_tenant_id));
  } else if (OB_FAIL(clone_op.get_clone_job_by_source_tenant_id(
                         source_tenant_id, clone_job))) {
    LOG_WARN("fail to get job", KR(ret), K(source_tenant_id));
  } else if (clone_job.get_min_cluster_version() != current_min_cluster_version) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("min cluster version has changed, clone tenant should fail", KR(ret),
             K(source_tenant_id), K(clone_job), K(current_min_cluster_version));
  } else if (OB_FAIL(ObTenantSnapshotUtil::check_current_and_target_data_version(
                         source_tenant_id, current_data_version))) {
    LOG_WARN("fail to check and get current data version", KR(ret), K(source_tenant_id));
  } else if (clone_job.get_data_version() != current_data_version) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("data version has changed, clone tenant should fail", KR(ret),
             K(source_tenant_id), K(clone_job), K(current_data_version));
  }
  return ret;
}

}
}
