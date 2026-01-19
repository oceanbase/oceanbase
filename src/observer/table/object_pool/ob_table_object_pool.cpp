/**
 * Copyright (c) 2025 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX SERVER

#include "ob_table_object_pool.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_tenant.h"

using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

namespace oceanbase
{
namespace table
{

/*
  init object pool manager when create tenant
  - we just obly init the metadata when mtl_init.
*/
int ObTableObjectPoolMgr::mtl_init(ObTableObjectPoolMgr *&mgr)
{
  return mgr->init();
}

/*
  start tableapi retired object task
  - 5 second interval
  - repeated
*/
int ObTableObjectPoolMgr::start()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("table api object pool mgr isn't inited", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer*)->get_tg_id(),
                                 sess_elimination_task_,
                                 ELIMINATE_SESSION_DELAY/* 5s */,
                                 true/* repeat */))) {
    LOG_WARN("failed to schedule tableapi retired session task", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer*)->get_tg_id(),
                                 sys_var_update_task_,
                                 SYS_VAR_REFRESH_DELAY/* 5s */,
                                 true/* repeat */))) {
    LOG_WARN("failed to schedule tableapi update sys var task", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer*)->get_tg_id(),
                                 user_lock_status_refresh_task_,
                                 USER_LOCK_STATUS_REFRESH_DELAY/* 5s */,
                                 true/* repeat */))) {
    LOG_WARN("failed to schedule tableapi user lock status refresh task", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer*)->get_tg_id(),
                                 req_res_elimination_task_,
                                 ELIMINATE_RES_RESULT_DELAY/* 5s */,
                                 true/* repeat */))) {
    LOG_WARN("failed to schedule tableapi retired request result task", K(ret));
  } else {
    sess_elimination_task_.is_inited_ = true;
    sys_var_update_task_.is_inited_ = true;
    user_lock_status_refresh_task_.is_inited_ = true;
    req_res_elimination_task_.is_inited_ = true;
  }

  return ret;
}

// stop tableapi retired object task
void ObTableObjectPoolMgr::stop()
{
  if (OB_LIKELY(sess_elimination_task_.is_inited_)) {
    TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), sess_elimination_task_);
  }
  if (OB_LIKELY(sys_var_update_task_.is_inited_)) {
    TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), sys_var_update_task_);
  }
  if (OB_LIKELY(user_lock_status_refresh_task_.is_inited_)) {  // 添加这个新的任务清理
    TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), user_lock_status_refresh_task_);
  }
  if (OB_LIKELY(req_res_elimination_task_.is_inited_)) {
    TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), req_res_elimination_task_);
  }
}

// tableapi retired object task wait
void ObTableObjectPoolMgr::wait()
{
  if (OB_LIKELY(sess_elimination_task_.is_inited_)) {
    TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), sess_elimination_task_);
  }
  if (OB_LIKELY(sys_var_update_task_.is_inited_)) {
    TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), sys_var_update_task_);
  }
  if (OB_LIKELY(req_res_elimination_task_.is_inited_)) {
    TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), req_res_elimination_task_);
  }
}

/*
  destroy object pool manager.
  - cancel timer task.
  - destroy object pool.
*/
void ObTableObjectPoolMgr::destroy()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    // 1. cancel timer task
    if (sess_elimination_task_.is_inited_) {
      bool is_exist = true;
      if (OB_SUCC(TG_TASK_EXIST(MTL(omt::ObSharedTimer*)->get_tg_id(), sess_elimination_task_, is_exist))) {
        if (is_exist) {
          TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), sess_elimination_task_);
          TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), sess_elimination_task_);
          sess_elimination_task_.is_inited_ = false;
        }
      }
    }
    if (sys_var_update_task_.is_inited_) {
      bool is_exist = true;
      if (OB_SUCC(TG_TASK_EXIST(MTL(omt::ObSharedTimer*)->get_tg_id(), sys_var_update_task_, is_exist))) {
        if (is_exist) {
          TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), sys_var_update_task_);
          TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), sys_var_update_task_);
          sys_var_update_task_.is_inited_ = false;
        }
      }
    }
    if (user_lock_status_refresh_task_.is_inited_) {  // 添加这个新的任务清理
      bool is_exist = true;
      if (OB_SUCC(TG_TASK_EXIST(MTL(omt::ObSharedTimer*)->get_tg_id(), user_lock_status_refresh_task_, is_exist))) {
        if (is_exist) {
          TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), user_lock_status_refresh_task_);
          TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), user_lock_status_refresh_task_);
          user_lock_status_refresh_task_.is_inited_ = false;
        }
      }
    }
    if (req_res_elimination_task_.is_inited_) {
      bool is_exist = true;
      if (OB_SUCC(TG_TASK_EXIST(MTL(omt::ObSharedTimer*)->get_tg_id(), req_res_elimination_task_, is_exist))) {
        if (is_exist) {
          TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), req_res_elimination_task_);
          TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), req_res_elimination_task_);
          req_res_elimination_task_.is_inited_ = false;
        }
      }
    }

    // 2. destroy session pool
    if (OB_NOT_NULL(sess_pool_)) {
      sess_pool_->destroy();
      sess_pool_ = nullptr;
    }
    ls_op_pool_.destroy();
    ls_res_pool_.destroy();
    allocator_.reset(); // when mtl_destroy, all worker thread has beed existed, no need to lock allocator
    is_inited_ = false;
    LOG_INFO("ObTableObjectPoolMgr destroy successfully");
  }
}

// init object pool manager.
int ObTableObjectPoolMgr::init()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    sess_elimination_task_.obj_pool_mgr_ = this;
    sys_var_update_task_.obj_pool_mgr_ = this;
    req_res_elimination_task_.obj_pool_mgr_ = this;
    user_lock_status_refresh_task_.obj_pool_mgr_ = this;
    is_inited_ = true;
  }

  return ret;
}

/*
  get a session or create a new one if it doesn't exist
  - 1. the user should access the current tenant, so we check tenant id.
  - 2. ObTableApiSessGuard holds the reference count of session.
  - 3. sess_pool_ have been created when login normally,
    But some inner operation did not login, such as ttl operation, so we create a new pool for ttl.
    In the upgrade scenario, the odp does not login again. so we init system vars.
*/
int ObTableObjectPoolMgr::get_sess_info(ObTableApiCredential &credential, ObTableApiSessGuard &guard)
{
  int ret = OB_SUCCESS;

  if (credential.tenant_id_ != MTL_ID()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "access wrong tenant");
    LOG_WARN("access wrong tenant", K(ret), K(credential.tenant_id_), K(MTL_ID()));
  } else if (OB_UNLIKELY(OB_ISNULL(sess_pool_)) && OB_FAIL(create_session_pool_safe())) {
    LOG_WARN("fail to create session pool", K(ret), K(credential));
  } else if (OB_ISNULL(sess_pool_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session pool is null", K(ret), K(credential));
  } else if (OB_FAIL(sess_pool_->get_sess_info(credential, guard))) {
    LOG_WARN("fail to get session info", K(ret), K(credential));
  } else if (!sys_vars_.is_inited_ && OB_FAIL(sys_vars_.init())) {
    LOG_WARN("fail to init sys vars", K(ret));
  }

  return ret;
}

/*
  create session pool safely.
  - lock for allocator concurrency.
*/
int ObTableObjectPoolMgr::create_session_pool_safe()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(sess_pool_)) {
    ObLockGuard<ObSpinLock> guard(lock_);
    if (OB_ISNULL(sess_pool_)) { // double check
      if (OB_FAIL(create_session_pool_unsafe())) {
        LOG_WARN("fail to create session pool", K(ret));
      }
    }
  }

  return ret;
}

int ObTableObjectPoolMgr::create_session_pool_unsafe()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObTableApiSessPool *tmp_pool = nullptr;

  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObTableApiSessPool)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem for ObTableApiSessPool", K(ret));
  } else if (FALSE_IT(tmp_pool = new (buf) ObTableApiSessPool())) {
  } else if (OB_FAIL(tmp_pool->init())) {
    LOG_WARN("fail to init sess pool", K(ret));
    allocator_.free(tmp_pool);
    tmp_pool = nullptr;
  } else {
    sess_pool_ = tmp_pool;
  }

  return ret;
}

/*
  update session when login.
  - 1. because tableapi is not aware of changes to system variables,
    users need to log in again to get the latest system variables.
  - 2. we will create a new session node which has the latest system variables
    to replace the old session node.
  - 3. login is handled by sys tenant.
  - 4. login has concurrency, many thread will login together.
*/
int ObTableObjectPoolMgr::update_sess(ObTableApiCredential &credential)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_ISNULL(sess_pool_)) && OB_FAIL(create_session_pool_safe())) {
    LOG_WARN("fail to create session pool", K(ret), K(credential));
  } else if (OB_FAIL(sess_pool_->update_sess(credential))) {
    LOG_WARN("fail to update sess pool", K(ret), K(credential));
  } else if (!sys_vars_.is_inited_ && OB_FAIL(sys_vars_.init())) {
    LOG_WARN("fail to init sys vars", K(ret));
  }

  return ret;
}

/*
  The background timer tasks to delete session node.
  - retire session node that have not been accessed for more than 3 minutes.
  - recycle session node in retired node list.
*/
void ObTableObjectPoolMgr::ObTableSessEliminationTask::runTimerTask()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(run_retire_sess_task())) {
    LOG_WARN("fail to run retire sess task", K(ret));
  } else if (OB_FAIL(run_recycle_retired_sess_task())) {
    LOG_WARN("fail to run recycle retired sess task", K(ret));
  }
}

void ObTableObjectPoolMgr::ObTableSessSysVarUpdateTask::runTimerTask()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(run_update_sys_var_task())) {
    LOG_WARN("fail to run update sys var task", K(ret));
  }
}

void ObTableObjectPoolMgr::ObTableReqResEliminationTask::runTimerTask()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(run_recycle_retired_request_result_task())) {
    LOG_WARN("fail to run retire requst result task", K(ret));
  }
}

int ObTableObjectPoolMgr::ObTableReqResEliminationTask::run_recycle_retired_request_result_task()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(obj_pool_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("obj_pool_mgr_ is null", K(ret));
  } else if (OB_FAIL(obj_pool_mgr_->ls_op_pool_.recycle_retired_object())) {
    LOG_WARN("fail to recycle ls op pool", K(ret));
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(obj_pool_mgr_->ls_res_pool_.recycle_retired_object())) {
    LOG_WARN("fail to recycle ls result pool", K(tmp_ret));
  }
  ret = ret == OB_SUCCESS ? tmp_ret : ret;
  return ret;
}

/*
  retire session node that have not been accessed for more than 3 minutes.
  - move session node which have not been accessed for more than 3 minutes to retired node list.
*/
int ObTableObjectPoolMgr::ObTableSessEliminationTask::run_retire_sess_task()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(obj_pool_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("obj_pool_mgr_ is null", K(ret));
  } else if (OB_NOT_NULL(obj_pool_mgr_->sess_pool_) && OB_FAIL(obj_pool_mgr_->sess_pool_->retire_session_node())) {
    LOG_WARN("fail to retire session node", K(ret));
  }

  return ret;
}

/*
  evict retired session node from retired node list.
*/
int ObTableObjectPoolMgr::ObTableSessEliminationTask::run_recycle_retired_sess_task()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(obj_pool_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("obj_pool_mgr_ is null", K(ret));
  } else if (OB_NOT_NULL(obj_pool_mgr_->sess_pool_) && OB_FAIL(obj_pool_mgr_->sess_pool_->evict_retired_sess())) {
    LOG_WARN("fail to evict retired sess", K(ret));
  }

  return ret;
}

int ObTableObjectPoolMgr::ObTableSessSysVarUpdateTask::run_update_sys_var_task()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(obj_pool_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("obj_pool_mgr_ is null", K(ret));
  } else if (OB_FAIL(obj_pool_mgr_->update_sys_vars(true/*only_update_dynamic_vars*/))) {
    LOG_WARN("fail to update sys var", K(ret));
  }

  return ret;
}

}  // namespace table
}  // namespace oceanbase

// Add implementations for ObTableUserLockStatusRefreshTask
namespace oceanbase
{
namespace table
{

void ObTableObjectPoolMgr::ObTableUserLockStatusRefreshTask::runTimerTask()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(run_refresh_user_lock_status_task())) {
    LOG_WARN("fail to run refresh user lock status task", K(ret));
  }
}

int ObTableObjectPoolMgr::ObTableUserLockStatusRefreshTask::run_refresh_user_lock_status_task()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(obj_pool_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("obj_pool_mgr_ is null", K(ret));
  } else if (OB_NOT_NULL(obj_pool_mgr_->sess_pool_) && OB_FAIL(obj_pool_mgr_->sess_pool_->refresh_all_user_locked_status())) {
    LOG_WARN("fail to refresh user lock status", K(ret));
  }

  return ret;
}

}  // namespace table
}  // namespace oceanbase
