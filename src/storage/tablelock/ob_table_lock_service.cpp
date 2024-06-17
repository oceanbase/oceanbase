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

#define USING_LOG_PREFIX TABLELOCK
#include "common/ob_tablet_id.h"
#include "storage/tablelock/ob_table_lock_service.h"

#include "observer/ob_server_struct.h"
#include "observer/ob_srv_network_frame.h"
#include "share/ob_define.h"
#include "share/ob_errno.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "storage/ob_common_id_utils.h"
#include "storage/tx/ob_trans_deadlock_adapter.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tablelock/ob_lock_utils.h" // ObInnerTableLockUtil
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablelock/ob_table_lock_live_detector.h"

namespace oceanbase
{

using namespace common;
using namespace share;
using namespace share::schema;

namespace transaction
{

namespace tablelock
{
ObTableLockService::ObTableLockCtx::ObTableLockCtx(const ObTableLockTaskType task_type,
                                                   const int64_t origin_timeout_us,
                                                   const int64_t timeout_us)
  : task_type_(task_type),
    is_in_trans_(false),
    table_id_(OB_INVALID_ID),
    origin_timeout_us_(origin_timeout_us),
    timeout_us_(timeout_us),
    abs_timeout_ts_(),
    trans_state_(),
    tx_desc_(nullptr),
    current_savepoint_(),
    tablet_list_(),
    schema_version_(-1),
    tx_is_killed_(false),
    is_from_sql_(false),
    ret_code_before_end_stmt_or_tx_(OB_SUCCESS),
    stmt_savepoint_()
{
  abs_timeout_ts_ = (0 == timeout_us)
    ? ObTimeUtility::current_time() + DEFAULT_TIMEOUT_US
    : ObTimeUtility::current_time() + timeout_us;
}

ObTableLockService::ObTableLockCtx::ObTableLockCtx(const ObTableLockTaskType task_type,
                                                   const uint64_t table_id,
                                                   const int64_t origin_timeout_us,
                                                   const int64_t timeout_us)
  : ObTableLockCtx(task_type, origin_timeout_us, timeout_us)
{
  table_id_ = table_id;
}

ObTableLockService::ObTableLockCtx::ObTableLockCtx(const ObTableLockTaskType task_type,
                                                   const uint64_t table_id,
                                                   const uint64_t partition_id,
                                                   const int64_t origin_timeout_us,
                                                   const int64_t timeout_us)
  : ObTableLockCtx(task_type, table_id, origin_timeout_us, timeout_us)
{
  partition_id_ = partition_id;
}

ObTableLockService::ObTableLockCtx::ObTableLockCtx(const ObTableLockTaskType task_type,
                                                   const uint64_t table_id,
                                                   const share::ObLSID &ls_id,
                                                   const int64_t origin_timeout_us,
                                                   const int64_t timeout_us)
 : ObTableLockCtx(task_type, table_id, origin_timeout_us, timeout_us)
{
  ls_id_ = ls_id;
}

void ObTableLockService::ObRetryCtx::reuse()
{
  need_retry_ = false;
  send_rpc_count_ = 0;
  rpc_ls_array_.reuse();
  retry_lock_ids_.reuse();
}

int64_t ObTableLockService::ObOBJLockGarbageCollector::GARBAGE_COLLECT_PRECISION = 100_ms;
int64_t ObTableLockService::ObOBJLockGarbageCollector::GARBAGE_COLLECT_EXEC_INTERVAL = 10_s;
int64_t ObTableLockService::ObOBJLockGarbageCollector::GARBAGE_COLLECT_TIMEOUT = 10_min;

ObTableLockService::ObOBJLockGarbageCollector::ObOBJLockGarbageCollector()
  : timer_(),
    timer_handle_(),
    last_success_timestamp_(0) {}
ObTableLockService::ObOBJLockGarbageCollector::~ObOBJLockGarbageCollector() {}

int ObTableLockService::ObOBJLockGarbageCollector::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(obj_lock_gc_thread_pool_.init_and_start(
                 OBJ_LOCK_GC_THREAD_NUM))) {
    LOG_WARN(
        "fail to init and start gc thread pool for ObTableLockService::ObOBJLockGarbageCollector",
        KR(ret));
  } else if (OB_FAIL(timer_.init_and_start(obj_lock_gc_thread_pool_,
                                           GARBAGE_COLLECT_PRECISION,
                                           "OBJLockGC"))) {
    LOG_WARN("fail to init and start timer for ObTableLockService::ObOBJLockGarbageCollector",
              K(ret), KPC(this));
  } else if (OB_FAIL(timer_.schedule_task_repeat(
                 timer_handle_, GARBAGE_COLLECT_EXEC_INTERVAL,
                 [this]() mutable {
                   int ret = OB_SUCCESS;
                   if (OB_FAIL(garbage_collect_for_all_ls_())) {
                     check_and_report_timeout_();
                     LOG_WARN(
                         "check and clear obj lock failed, will retry later",
                         K(ret), K(last_success_timestamp_), KPC(this));
                   } else {
                     last_success_timestamp_ = ObClockGenerator::getClock();
                     LOG_DEBUG("check and clear obj lock successfully", K(ret),
                               K(last_success_timestamp_), KPC(this));
                   }
                   return false;
                 }))) {
    LOG_ERROR("ObTableLockService::ObOBJLockGarbageCollector schedules repeat task failed",
              K(ret), KPC(this));
  } else {
    LOG_INFO("ObTableLockService::ObOBJLockGarbageCollector starts successfully", K(ret),
             KPC(this));
  }
  return ret;
}

void ObTableLockService::ObOBJLockGarbageCollector::stop()
{
  timer_handle_.stop();
  LOG_INFO("ObTableLockService::ObOBJLockGarbageCollector stops successfully", KPC(this));
}

void ObTableLockService::ObOBJLockGarbageCollector::wait()
{
  timer_handle_.wait();
  LOG_INFO("ObTableLockService::ObOBJLockGarbageCollector waits successfully", KPC(this));
}

void ObTableLockService::ObOBJLockGarbageCollector::destroy()
{
  timer_.destroy();
  LOG_INFO("ObTableLockService::ObOBJLockGarbageCollector destroys successfully", KPC(this));
}

int ObTableLockService::ObOBJLockGarbageCollector::garbage_collect_right_now()
{
  int ret = OB_SUCCESS;
  if (!timer_.is_running()) {
    ret = OB_NOT_INIT;
    LOG_WARN("timer of ObTableLockService::ObOBJLockGarbageCollector is not running", K(ret));
  } else if (!timer_handle_.is_running()) {
    ret = OB_NOT_INIT;
    LOG_WARN("timer_handle of ObTableLockService::ObOBJLockGarbageCollector is not running", K(ret));
  } else if (OB_FAIL(timer_handle_.reschedule_after(10))) {
    LOG_WARN("reschedule task for ObTableLockService::ObOBJLockGarbageCollector failed", K(ret));
  }
  return ret;
}

int ObTableLockService::ObOBJLockGarbageCollector::garbage_collect_for_all_ls_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSharedGuard<ObLSIterator> ls_iter_guard;
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;
  bool is_leader = false;
  ObAddr leader_addr;

  if (!timer_.is_running()) {
    ret = OB_NOT_INIT;
    LOG_WARN("timer of ObTableLockService::ObOBJLockGarbageCollector is not running", K(ret));
  } else if (!timer_handle_.is_running()) {
    ret = OB_NOT_INIT;
    LOG_WARN("timer_handle of ObTableLockService::ObOBJLockGarbageCollector is not running", K(ret));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mtl ObLSService should not be null", K(ret));
  } else if (OB_FAIL(ls_service->get_ls_iter(ls_iter_guard, ObLSGetMod::TABLELOCK_MOD))) {
    LOG_WARN("fail to get ls iterator", K(ret));
  } else {
    do {
      if (OB_FAIL(ls_iter_guard->get_next(ls))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next iter failed", K(ret));
        }
      } else if (OB_TMP_FAIL(ls->check_and_clear_obj_lock(false))) {
        LOG_WARN("check and clear obj lock failed", K(ret), K(tmp_ret), K(ls->get_ls_id()));
      } else if (ls->is_sys_ls()) {
        if (OB_TMP_FAIL(check_is_leader_(ls, is_leader))) {
          LOG_WARN("can not check whether this ls is leader", K(ret), K(tmp_ret), K(ls->get_ls_id()));
        } else if (is_leader && OB_TMP_FAIL(ObTableLockDetector::do_detect_and_clear())) {
          LOG_WARN("do_detect_and_clear failed", K(ret), K(tmp_ret), K(ls->get_ls_id()));
        }
      } else {
        LOG_INFO("finish check and clear obj lock", K(ls->get_ls_id()));
      }
    } while (OB_SUCC(ret));
  }
  ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
  return ret;
}

void ObTableLockService::ObOBJLockGarbageCollector::check_and_report_timeout_()
{
  int ret = OB_SUCCESS;
  int64_t current_timestamp = ObClockGenerator::getClock();
  if (last_success_timestamp_ > current_timestamp) {
    LOG_ERROR("last success timestamp is not correct", K(current_timestamp),
              K(last_success_timestamp_), KPC(this));
  } else if (current_timestamp - last_success_timestamp_ >
                 GARBAGE_COLLECT_TIMEOUT &&
             last_success_timestamp_ != 0) {
    LOG_ERROR("task failed too many times", K(current_timestamp),
              K(last_success_timestamp_), KPC(this));
  }
}

int ObTableLockService::ObOBJLockGarbageCollector::check_is_leader_(ObLS *ls, bool &is_leader)
{
  int ret = OB_SUCCESS;
  ObRole role;
  int64_t proposal_id = 0;
  is_leader = false;

  if (OB_FAIL(ls->get_log_handler()->get_role(role, proposal_id))) {
    STORAGE_LOG(WARN, "failed to get role", K(ret), K(ls->get_ls_id()));
  } else {
    is_leader = is_strong_leader(role);
  }
  return ret;
}

int ObTableLockService::ObTableLockCtx::set_tablet_id(const common::ObIArray<common::ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  tablet_list_.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
    if (OB_FAIL(tablet_list_.push_back(tablet_ids.at(i)))) {
      LOG_WARN("set tablet id failed", K(ret), K(i), K(tablet_ids));
    }
  }
  return ret;
}

int ObTableLockService::ObTableLockCtx::set_tablet_id(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  tablet_list_.reuse();
  if (OB_FAIL(tablet_list_.push_back(tablet_id))) {
    LOG_WARN("set tablet id failed", K(ret), K(tablet_id));
  }
  return ret;
}

int ObTableLockService::ObTableLockCtx::set_lock_id(const common::ObIArray<ObLockID> &lock_ids)
{
  int ret = OB_SUCCESS;
  obj_list_.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < lock_ids.count(); i++) {
    if (OB_FAIL(obj_list_.push_back(lock_ids.at(i)))) {
      LOG_WARN("set lock id failed", K(ret), K(i), K(lock_ids));
    }
  }
  return ret;
}

int ObTableLockService::ObTableLockCtx::set_lock_id(const ObLockID &lock_id)
{
  int ret = OB_SUCCESS;
  obj_list_.reuse();
  if (OB_FAIL(obj_list_.push_back(lock_id))) {
    LOG_WARN("set lock id failed", K(ret), K(lock_id));
  }
  return ret;
}

int ObTableLockService::ObTableLockCtx::set_lock_id(const ObLockOBJType &obj_type, const uint64_t obj_id)
{
  int ret = OB_SUCCESS;
  ObLockID lock_id;
  obj_list_.reuse();
  if (OB_FAIL(lock_id.set(obj_type, obj_id))) {
    LOG_WARN("set lock id failed", K(ret), K(obj_type), K(obj_id));
  } else if (OB_FAIL(obj_list_.push_back(lock_id))) {
    LOG_WARN("set lock id failed", K(ret), K(lock_id));
  }
  return ret;
}

bool ObTableLockService::ObTableLockCtx::is_timeout() const
{
  return ObTimeUtility::current_time() >= abs_timeout_ts_;
}

int64_t ObTableLockService::ObTableLockCtx::remain_timeoutus() const
{
  int64_t remain_us = abs_timeout_ts_ - ObTimeUtility::current_time();
  return remain_us > 0 ? remain_us : 0;
}

int64_t ObTableLockService::ObTableLockCtx::get_rpc_timeoutus() const
{
  // rpc timeout should larger than stmt remain timeout us.
  // we add 2 second now.
  return (remain_timeoutus() + DEFAULT_RPC_TIMEOUT_US);
}

int64_t ObTableLockService::ObTableLockCtx::get_tablet_cnt() const
{
  return tablet_list_.count();
}

const ObTabletID &ObTableLockService::ObTableLockCtx::get_tablet_id(const int64_t index) const
{
  return tablet_list_.at(index);
}

int ObTableLockService::ObTableLockCtx::add_touched_ls(const ObLSID &lsid)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  // check if the touched ls exist.
  for (int64_t i = 0; OB_SUCC(ret) && i < need_rollback_ls_.count(); i++) {
    ObLSID &curr = need_rollback_ls_.at(i);
    if (curr == lsid) {
      exist = true;
      break;
    }
  }
  // add if the touched ls not exist.
  if (!exist && OB_FAIL(need_rollback_ls_.push_back(lsid))) {
    LOG_ERROR("add touche ls failed", K(ret), K(lsid));
  }
  return ret;
}

void ObTableLockService::ObTableLockCtx::clean_touched_ls()
{
  need_rollback_ls_.reuse();
}

bool ObTableLockService::ObTableLockCtx::is_deadlock_avoid_enabled() const
{
  return tablelock::is_deadlock_avoid_enabled(is_from_sql_, origin_timeout_us_);
}

int ObTableLockService::mtl_init(ObTableLockService* &lock_service)
{
  return lock_service->init();
}

int ObTableLockService::init()
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("lock service init twice.", K(ret));
  } else if (OB_UNLIKELY(!GCTX.self_addr().is_valid()) ||
             OB_ISNULL(GCTX.net_frame_) ||
             OB_ISNULL(GCTX.location_service_) ||
             OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(GCTX.self_addr()),
             KP(GCTX.net_frame_), KP(GCTX.location_service_), KP(GCTX.sql_proxy_));
  } else {
    location_service_ = GCTX.location_service_;
    sql_proxy_ = GCTX.sql_proxy_;
    is_inited_ = true;
  }

  if (OB_FAIL(ret)) {
    destroy();
  }

  return ret;
}

int ObTableLockService::start()
{
  obj_lock_garbage_collector_.start();
  return OB_SUCCESS;
}

void ObTableLockService::stop()
{
  obj_lock_garbage_collector_.stop();
}

void ObTableLockService::wait()
{
  obj_lock_garbage_collector_.wait();
}

void ObTableLockService::destroy()
{
  obj_lock_garbage_collector_.destroy();
  location_service_ = nullptr;
  sql_proxy_ = nullptr;
  is_inited_ = false;
}

static inline
bool need_retry_partitions(const int64_t ret)
{
  // TODO: check return code
  UNUSED(ret);
  return false;
}

int ObTableLockService::generate_owner_id(ObTableLockOwnerID &owner_id)
{
  int ret = OB_SUCCESS;
  ObCommonID id;
  if (OB_FAIL(ObCommonIDUtils::gen_unique_id(MTL_ID(), id))) {
    LOG_WARN("get unique id failed", K(ret));
  } else if (OB_FAIL(owner_id.convert_from_value(ObLockOwnerType::DEFAULT_OWNER_TYPE,
                                                 id.id()))) {
    LOG_WARN("get owner id failed", K(ret), K(id));
  }
  return ret;
}

int ObTableLockService::lock_table(const uint64_t table_id,
                                   const ObTableLockMode lock_mode,
                                   const ObTableLockOwnerID lock_owner,
                                   const int64_t timeout_us)
{
  LOG_INFO("ObTableLockService::lock_table",
            K(table_id), K(lock_mode), K(lock_owner), K(timeout_us));
  int ret = OB_SUCCESS;
  int ret_code_before_end_stmt_or_tx = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret), K(table_id), K(lock_mode),
             K(lock_owner));
  } else if (OB_UNLIKELY(!is_valid_id(table_id)) ||
             OB_UNLIKELY(!is_lock_mode_valid(lock_mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id), K(lock_mode), K(lock_owner));
  } else {
    // avoid deadlock when ddl conflict with dml
    // by restart ddl table lock trans
    int64_t retry_timeout_us = timeout_us;
    bool need_retry = false;
    int64_t abs_timeout_ts = (0 == timeout_us)
      ? ObTimeUtility::current_time() + DEFAULT_TIMEOUT_US
      : ObTimeUtility::current_time() + timeout_us;
    Thread::WaitGuard guard(Thread::WAIT);
    do {
      if (timeout_us != 0) {
        retry_timeout_us = abs_timeout_ts - ObTimeUtility::current_time();
      }
      ObTableLockCtx ctx(LOCK_TABLE, table_id, timeout_us, retry_timeout_us);
      ctx.lock_op_type_ = OUT_TRANS_LOCK;
      ret = process_lock_task_(ctx, lock_mode, lock_owner);
      need_retry = need_retry_trans_(ctx, ret);
      ret_code_before_end_stmt_or_tx = ctx.ret_code_before_end_stmt_or_tx_;
    } while (need_retry);
  }
  ret = rewrite_return_code_(ret, ret_code_before_end_stmt_or_tx, false /*is_from_sql*/);
  return ret;
}

int ObTableLockService::unlock_table(const uint64_t table_id,
                                     const ObTableLockMode lock_mode,
                                     const ObTableLockOwnerID lock_owner,
                                     const int64_t timeout_us)
{
  LOG_INFO("ObTableLockService::unlock_table",
            K(table_id), K(lock_mode), K(lock_owner), K(timeout_us));
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_LOCK_SERVICE_UNLOCK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret), K(table_id), K(lock_mode),
             K(lock_owner));
  } else if (OB_UNLIKELY(!is_valid_id(table_id)) ||
             OB_UNLIKELY(!is_lock_mode_valid(lock_mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id), K(lock_mode), K(lock_owner));
  } else {
    int64_t retry_timeout_us = timeout_us;
    bool need_retry = false;
    int64_t abs_timeout_ts = (0 == timeout_us)
      ? ObTimeUtility::current_time() + DEFAULT_TIMEOUT_US
      : ObTimeUtility::current_time() + timeout_us;
    Thread::WaitGuard guard(Thread::WAIT);
    do {
      if (timeout_us != 0) {
        retry_timeout_us = abs_timeout_ts - ObTimeUtility::current_time();
      }
      ObTableLockCtx ctx(UNLOCK_TABLE, table_id, timeout_us, retry_timeout_us);
      ctx.lock_op_type_ = OUT_TRANS_UNLOCK;
      ret = process_lock_task_(ctx, lock_mode, lock_owner);
      need_retry = need_retry_trans_(ctx, ret);
    } while (need_retry);
  }
  ret = rewrite_return_code_(ret);
  return ret;
}

int ObTableLockService::lock_tablet(const uint64_t table_id,
                                    const ObTabletID &tablet_id,
                                    const ObTableLockMode lock_mode,
                                    const ObTableLockOwnerID lock_owner,
                                    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int ret_code_before_end_stmt_or_tx = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret), K(table_id), K(tablet_id),
             K(lock_mode), K(lock_owner));
  } else if (OB_UNLIKELY(!is_valid_id(table_id)) ||
             OB_UNLIKELY(!tablet_id.is_valid()) ||
             OB_UNLIKELY(!is_lock_mode_valid(lock_mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id), K(tablet_id), K(lock_mode),
             K(lock_owner));
  } else {
    // avoid deadlock when ddl conflict with dml
    // by restart ddl table lock trans
    int64_t retry_timeout_us = timeout_us;
    bool need_retry = false;
    int64_t abs_timeout_ts = (0 == timeout_us)
      ? ObTimeUtility::current_time() + DEFAULT_TIMEOUT_US
      : ObTimeUtility::current_time() + timeout_us;
    Thread::WaitGuard guard(Thread::WAIT);
    do {
      if (timeout_us != 0) {
        retry_timeout_us = abs_timeout_ts - ObTimeUtility::current_time();
      }
      ObTableLockCtx ctx(LOCK_TABLET, table_id, timeout_us, retry_timeout_us);
      ctx.lock_op_type_ = OUT_TRANS_LOCK;
      if (OB_FAIL(ctx.set_tablet_id(tablet_id))) {
        LOG_WARN("set tablet id failed", K(ret), K(tablet_id));
      } else if (OB_FAIL(process_lock_task_(ctx, lock_mode, lock_owner))) {
        LOG_WARN("process lock task failed", K(ret), K(tablet_id));
      }
      need_retry = need_retry_trans_(ctx, ret);
      ret_code_before_end_stmt_or_tx = ctx.ret_code_before_end_stmt_or_tx_;
    } while (need_retry);
  }
  ret = rewrite_return_code_(ret, ret_code_before_end_stmt_or_tx, false /*is_from_sql*/);
  return ret;
}

int ObTableLockService::unlock_tablet(const uint64_t table_id,
                                      const ObTabletID &tablet_id,
                                      const ObTableLockMode lock_mode,
                                      const ObTableLockOwnerID lock_owner,
                                      const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_LOCK_SERVICE_UNLOCK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret), K(table_id), K(tablet_id),
             K(lock_mode), K(lock_owner));
  } else if (OB_UNLIKELY(!is_valid_id(table_id)) ||
             OB_UNLIKELY(!tablet_id.is_valid()) ||
             OB_UNLIKELY(!is_lock_mode_valid(lock_mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id), K(tablet_id), K(lock_mode),
             K(lock_owner));
  } else {
    int64_t retry_timeout_us = timeout_us;
    bool need_retry = false;
    int64_t abs_timeout_ts = (0 == timeout_us)
      ? ObTimeUtility::current_time() + DEFAULT_TIMEOUT_US
      : ObTimeUtility::current_time() + timeout_us;
    Thread::WaitGuard guard(Thread::WAIT);
    do {
      if (timeout_us != 0) {
        retry_timeout_us = abs_timeout_ts - ObTimeUtility::current_time();
      }
      ObTableLockCtx ctx(UNLOCK_TABLET, table_id, timeout_us, retry_timeout_us);
      ctx.lock_op_type_ = OUT_TRANS_UNLOCK;
      if (OB_FAIL(ctx.set_tablet_id(tablet_id))) {
        LOG_WARN("set tablet id failed", K(ret), K(tablet_id));
      } else if (OB_FAIL(process_lock_task_(ctx, lock_mode, lock_owner))) {
        LOG_WARN("process lock task failed", K(ret), K(tablet_id));
      }
      need_retry = need_retry_trans_(ctx, ret);
    } while (need_retry);
  }
  ret = rewrite_return_code_(ret);

  return ret;
}

int ObTableLockService::lock_table(ObTxDesc &tx_desc,
                                   const ObTxParam &tx_param,
                                   const ObLockTableRequest &arg)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret));
  } else if (OB_UNLIKELY(!tx_desc.is_valid()) ||
             OB_UNLIKELY(!tx_param.is_valid()) ||
             OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tx_desc), K(arg), K(tx_desc.is_valid()),
             K(tx_param.is_valid()), K(arg.is_valid()));
  } else {
    // origin_timeout_us_ and timeout_us_ are both set as timeout_us_, which
    // is set by user in the 'WAIT n' option.
    // Furthermore, if timeout_us_ is 0, this lock will be judged as a try
    // lock semantics. It meets the actual semantics of 'NOWAIT' option.
    Thread::WaitGuard guard(Thread::WAIT);
    ObTableLockCtx ctx(LOCK_TABLE, arg.table_id_, arg.timeout_us_, arg.timeout_us_);
    ctx.is_in_trans_ = true;
    ctx.tx_desc_ = &tx_desc;
    ctx.tx_param_ = tx_param;
    ctx.lock_op_type_ = arg.op_type_;
    ctx.is_from_sql_ = arg.is_from_sql_;
    ret = process_lock_task_(ctx, arg.lock_mode_, arg.owner_id_);
    ret = rewrite_return_code_(ret, ctx.ret_code_before_end_stmt_or_tx_, ctx.is_from_sql_);
  }
  return ret;
}

int ObTableLockService::unlock_table(ObTxDesc &tx_desc,
                                     const ObTxParam &tx_param,
                                     const ObUnLockTableRequest &arg)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret));
  } else if (OB_UNLIKELY(!tx_desc.is_valid()) ||
             OB_UNLIKELY(!tx_param.is_valid()) ||
             OB_UNLIKELY(!arg.is_valid()) ||
             OB_UNLIKELY(ObTableLockOpType::OUT_TRANS_UNLOCK != arg.op_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tx_desc), K(arg), K(tx_desc.is_valid()),
             K(tx_param.is_valid()), K(arg.is_valid()));
  } else {
    Thread::WaitGuard guard(Thread::WAIT);
    ObTableLockCtx ctx(UNLOCK_TABLE, arg.table_id_, arg.timeout_us_, arg.timeout_us_);
    ctx.is_in_trans_ = true;
    ctx.tx_desc_ = &tx_desc;
    ctx.tx_param_ = tx_param;
    ctx.lock_op_type_ = arg.op_type_;
    ctx.is_from_sql_ = arg.is_from_sql_;
    ret = process_lock_task_(ctx, arg.lock_mode_, arg.owner_id_);
    ret = rewrite_return_code_(ret, ctx.is_from_sql_);
  }
  return ret;
}

int ObTableLockService::lock_tablet(ObTxDesc &tx_desc,
                                    const ObTxParam &tx_param,
                                    const ObLockTabletRequest &arg)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret));
  } else if (OB_UNLIKELY(!tx_desc.is_valid()) ||
             OB_UNLIKELY(!tx_param.is_valid()) ||
             OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tx_desc), K(arg), K(tx_desc.is_valid()),
             K(tx_param.is_valid()), K(arg.is_valid()));
  } else {
    Thread::WaitGuard guard(Thread::WAIT);
    ObTableLockCtx ctx(LOCK_TABLET, arg.table_id_,
                       arg.timeout_us_, arg.timeout_us_);
    ctx.is_in_trans_ = true;
    ctx.tx_desc_ = &tx_desc;
    ctx.tx_param_ = tx_param;
    ctx.lock_op_type_ = arg.op_type_;
    ctx.is_from_sql_ = arg.is_from_sql_;
    if (OB_FAIL(ctx.set_tablet_id(arg.tablet_id_))) {
      LOG_WARN("set tablet id failed", K(ret), K(arg));
    } else if (OB_FAIL(process_lock_task_(ctx, arg.lock_mode_, arg.owner_id_))) {
      LOG_WARN("process lock task failed", K(ret), K(arg));
    }
    ret = rewrite_return_code_(ret, ctx.ret_code_before_end_stmt_or_tx_, ctx.is_from_sql_);
  }
  return ret;
}

int ObTableLockService::lock_tablet(ObTxDesc &tx_desc,
                                    const ObTxParam &tx_param,
                                    const ObLockTabletsRequest &arg)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret));
  } else if (OB_UNLIKELY(!tx_desc.is_valid()) ||
             OB_UNLIKELY(!tx_param.is_valid()) ||
             OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tx_desc), K(arg), K(tx_desc.is_valid()),
             K(tx_param.is_valid()), K(arg.is_valid()));
  } else if (OB_FAIL(check_cluster_version_after_(CLUSTER_VERSION_4_2_0_0))) {
    LOG_WARN("data version check failed", K(ret), K(arg));
  } else {
    Thread::WaitGuard guard(Thread::WAIT);
    ObTableLockCtx ctx(LOCK_TABLET, arg.table_id_,
                       arg.timeout_us_, arg.timeout_us_);
    ctx.is_in_trans_ = true;
    ctx.tx_desc_ = &tx_desc;
    ctx.tx_param_ = tx_param;
    ctx.lock_op_type_ = arg.op_type_;
    ctx.is_from_sql_ = arg.is_from_sql_;
    if (OB_FAIL(ctx.set_tablet_id(arg.tablet_ids_))) {
      LOG_WARN("set tablet id failed", K(ret), K(arg));
    } else if (OB_FAIL(process_lock_task_(ctx, arg.lock_mode_, arg.owner_id_))) {
      LOG_WARN("process lock task failed", K(ret), K(arg));
    }
    ret = rewrite_return_code_(ret, ctx.ret_code_before_end_stmt_or_tx_, ctx.is_from_sql_);
  }
  return ret;
}

int ObTableLockService::unlock_tablet(ObTxDesc &tx_desc,
                                      const ObTxParam &tx_param,
                                      const ObUnLockTabletRequest &arg)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret));
  } else if (OB_UNLIKELY(!tx_desc.is_valid()) ||
             OB_UNLIKELY(!tx_param.is_valid()) ||
             OB_UNLIKELY(!arg.is_valid()) ||
             OB_UNLIKELY(ObTableLockOpType::OUT_TRANS_UNLOCK != arg.op_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tx_desc), K(arg), K(tx_desc.is_valid()),
             K(tx_param.is_valid()), K(arg.is_valid()));
  } else {
    Thread::WaitGuard guard(Thread::WAIT);
    ObTableLockCtx ctx(UNLOCK_TABLET, arg.table_id_,
                       arg.timeout_us_, arg.timeout_us_);
    ctx.is_in_trans_ = true;
    ctx.tx_desc_ = &tx_desc;
    ctx.tx_param_ = tx_param;
    ctx.lock_op_type_ = arg.op_type_;
    ctx.is_from_sql_ = arg.is_from_sql_;
    if (OB_FAIL(ctx.set_tablet_id(arg.tablet_id_))) {
      LOG_WARN("set tablet id failed", K(ret), K(arg));
    } else if (OB_FAIL(process_lock_task_(ctx, arg.lock_mode_, arg.owner_id_))) {
      LOG_WARN("process lock task failed", K(ret), K(arg));
    }
    ret = rewrite_return_code_(ret, ctx.is_from_sql_);
  }
  return ret;
}

int ObTableLockService::unlock_tablet(ObTxDesc &tx_desc,
                                      const ObTxParam &tx_param,
                                      const ObUnLockTabletsRequest &arg)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret));
  } else if (OB_UNLIKELY(!tx_desc.is_valid()) ||
             OB_UNLIKELY(!tx_param.is_valid()) ||
             OB_UNLIKELY(!arg.is_valid()) ||
             OB_UNLIKELY(ObTableLockOpType::OUT_TRANS_UNLOCK != arg.op_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tx_desc), K(arg), K(tx_desc.is_valid()),
             K(tx_param.is_valid()), K(arg.is_valid()));
  } else if (OB_FAIL(check_cluster_version_after_(CLUSTER_VERSION_4_2_0_0))) {
    LOG_WARN("data version check failed", K(ret), K(arg));
  } else {
    Thread::WaitGuard guard(Thread::WAIT);
    ObTableLockCtx ctx(UNLOCK_TABLET, arg.table_id_, arg.timeout_us_, arg.timeout_us_);
    ctx.is_in_trans_ = true;
    ctx.tx_desc_ = &tx_desc;
    ctx.tx_param_ = tx_param;
    ctx.lock_op_type_ = arg.op_type_;
    ctx.is_from_sql_ = arg.is_from_sql_;
    if (OB_FAIL(ctx.set_tablet_id(arg.tablet_ids_))) {
      LOG_WARN("set tablet id failed", K(ret), K(arg));
    } else if (OB_FAIL(process_lock_task_(ctx, arg.lock_mode_, arg.owner_id_))) {
      LOG_WARN("process lock task failed", K(ret), K(arg));
    }
    ret = rewrite_return_code_(ret, ctx.is_from_sql_);
  }
  return ret;
}

int ObTableLockService::lock_tablet(ObTxDesc &tx_desc,
                                    const ObTxParam &tx_param,
                                    const ObLockAloneTabletRequest &arg)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret));
  } else if (OB_UNLIKELY(!tx_desc.is_valid()) ||
             OB_UNLIKELY(!tx_param.is_valid()) ||
             OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tx_desc), K(arg), K(tx_desc.is_valid()),
             K(tx_param.is_valid()), K(arg.is_valid()));
  } else if (OB_FAIL(check_cluster_version_after_(CLUSTER_VERSION_4_2_0_0))) {
    LOG_WARN("cluster version check failed", K(ret), K(arg));
  } else {
    Thread::WaitGuard guard(Thread::WAIT);
    ObTableLockCtx ctx(LOCK_ALONE_TABLET, arg.table_id_,
                       arg.ls_id_, arg.timeout_us_, arg.timeout_us_);
    ctx.is_in_trans_ = true;
    ctx.tx_desc_ = &tx_desc;
    ctx.tx_param_ = tx_param;
    ctx.lock_op_type_ = arg.op_type_;
    ctx.is_from_sql_ = arg.is_from_sql_;
    if (OB_FAIL(ctx.set_tablet_id(arg.tablet_ids_))) {
      LOG_WARN("set tablet id failed", K(ret), K(arg));
    } else if (OB_FAIL(process_lock_task_(ctx, arg.lock_mode_, arg.owner_id_))) {
      LOG_WARN("process lock task failed", K(ret), K(arg));
    }
    ret = rewrite_return_code_(ret, ctx.ret_code_before_end_stmt_or_tx_, ctx.is_from_sql_);
  }
  return ret;
}

int ObTableLockService::unlock_tablet(ObTxDesc &tx_desc,
                                      const ObTxParam &tx_param,
                                      const ObUnLockAloneTabletRequest &arg)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret));
  } else if (OB_UNLIKELY(!tx_desc.is_valid()) ||
             OB_UNLIKELY(!tx_param.is_valid()) ||
             OB_UNLIKELY(!arg.is_valid()) ||
             OB_UNLIKELY(ObTableLockOpType::OUT_TRANS_UNLOCK != arg.op_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tx_desc), K(arg), K(tx_desc.is_valid()),
             K(tx_param.is_valid()), K(arg.is_valid()));
  } else if (OB_FAIL(check_cluster_version_after_(CLUSTER_VERSION_4_2_0_0))) {
    LOG_WARN("cluster version check failed", K(ret), K(arg));
  } else {
    Thread::WaitGuard guard(Thread::WAIT);
    ObTableLockCtx ctx(UNLOCK_ALONE_TABLET, arg.table_id_,
                       arg.ls_id_, arg.timeout_us_, arg.timeout_us_);
    ctx.is_in_trans_ = true;
    ctx.tx_desc_ = &tx_desc;
    ctx.tx_param_ = tx_param;
    ctx.lock_op_type_ = arg.op_type_;
    ctx.is_from_sql_ = arg.is_from_sql_;
    if (OB_FAIL(ctx.set_tablet_id(arg.tablet_ids_))) {
      LOG_WARN("set tablet id failed", K(ret), K(arg));
    } else if (OB_FAIL(process_lock_task_(ctx, arg.lock_mode_, arg.owner_id_))) {
      LOG_WARN("process lock task failed", K(ret), K(arg));
    }
    ret = rewrite_return_code_(ret, ctx.is_from_sql_);
  }
  return ret;
}

int ObTableLockService::lock_partition_or_subpartition(ObTxDesc &tx_desc,
                                                       const ObTxParam &tx_param,
                                                       const ObLockPartitionRequest &arg)
{
  int ret = OB_SUCCESS;
  ObPartitionLevel part_level = PARTITION_LEVEL_MAX;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret));
  } else if (OB_FAIL(get_table_partition_level_(arg.table_id_, part_level))) {
    LOG_WARN("can not get table partition level", K(ret), K(arg));
  } else {
    Thread::WaitGuard guard(Thread::WAIT);
    switch (part_level) {
    case PARTITION_LEVEL_ONE: {
      if (OB_FAIL(lock_partition(tx_desc, tx_param, arg))) {
          LOG_WARN("lock partition failed", K(ret), K(arg));
      }
      break;
    }
    case PARTITION_LEVEL_TWO: {
      if (OB_FAIL(lock_subpartition(tx_desc, tx_param, arg))) {
          LOG_WARN("lock subpartition failed", K(ret), K(arg));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected partition level", K(ret), K(arg), K(part_level));
    }
    }
  }
  return ret;
}

int ObTableLockService::lock_partition(ObTxDesc &tx_desc,
                                       const ObTxParam &tx_param,
                                       const ObLockPartitionRequest &arg)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret));
  } else if (OB_UNLIKELY(!tx_desc.is_valid()) ||
             OB_UNLIKELY(!tx_param.is_valid()) ||
             OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tx_desc.is_valid()),
             K(tx_param.is_valid()), K(arg.is_valid()), K(tx_desc), K(tx_param), K(arg));
  } else if (OB_FAIL(check_cluster_version_after_(CLUSTER_VERSION_4_1_0_0))) {
    LOG_WARN("cluster version check failed", K(ret), K(arg));
  } else {
    Thread::WaitGuard guard(Thread::WAIT);
    ObTableLockCtx ctx(LOCK_PARTITION, arg.table_id_, arg.part_object_id_,
                       arg.timeout_us_, arg.timeout_us_);
    ctx.is_in_trans_ = true;
    ctx.tx_desc_ = &tx_desc;
    ctx.tx_param_ = tx_param;
    ctx.lock_op_type_ = arg.op_type_;
    ctx.is_from_sql_ = arg.is_from_sql_;
    ret = process_lock_task_(ctx, arg.lock_mode_, arg.owner_id_);
    ret = rewrite_return_code_(ret, ctx.ret_code_before_end_stmt_or_tx_, ctx.is_from_sql_);
  }
  return ret;
}

int ObTableLockService::unlock_partition(ObTxDesc &tx_desc,
                                         const ObTxParam &tx_param,
                                         const ObUnLockPartitionRequest &arg)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret));
  } else if (OB_UNLIKELY(!tx_desc.is_valid()) ||
             OB_UNLIKELY(!tx_param.is_valid()) ||
             OB_UNLIKELY(!arg.is_valid()) ||
             OB_UNLIKELY(ObTableLockOpType::OUT_TRANS_UNLOCK != arg.op_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tx_desc), K(arg), K(tx_desc.is_valid()),
             K(tx_param.is_valid()), K(arg.is_valid()));
  } else if (OB_FAIL(check_cluster_version_after_(CLUSTER_VERSION_4_1_0_0))) {
    LOG_WARN("cluster version check failed", K(ret), K(arg));
  } else {
    Thread::WaitGuard guard(Thread::WAIT);
    ObTableLockCtx ctx(UNLOCK_PARTITION, arg.table_id_, arg.part_object_id_,
                       arg.timeout_us_, arg.timeout_us_);
    ctx.is_in_trans_ = true;
    ctx.tx_desc_ = &tx_desc;
    ctx.tx_param_ = tx_param;
    ctx.lock_op_type_ = arg.op_type_;
    ctx.is_from_sql_ = arg.is_from_sql_;
    ret = process_lock_task_(ctx, arg.lock_mode_, arg.owner_id_);
    ret = rewrite_return_code_(ret, ctx.is_from_sql_);
  }
  return ret;
}

int ObTableLockService::lock_subpartition(ObTxDesc &tx_desc,
                                          const ObTxParam &tx_param,
                                          const ObLockPartitionRequest &arg)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret));
  } else if (OB_UNLIKELY(!tx_desc.is_valid()) ||
             OB_UNLIKELY(!tx_param.is_valid()) ||
             OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tx_desc), K(arg), K(tx_desc.is_valid()),
             K(tx_param.is_valid()), K(arg.is_valid()));
  } else if (OB_FAIL(check_cluster_version_after_(CLUSTER_VERSION_4_1_0_0))) {
    LOG_WARN("cluster version check failed", K(ret), K(arg));
  } else {
    Thread::WaitGuard guard(Thread::WAIT);
    ObTableLockCtx ctx(LOCK_SUBPARTITION, arg.table_id_, arg.part_object_id_,
                       arg.timeout_us_, arg.timeout_us_);
    ctx.is_in_trans_ = true;
    ctx.tx_desc_ = &tx_desc;
    ctx.tx_param_ = tx_param;
    ctx.lock_op_type_ = arg.op_type_;
    ctx.is_from_sql_ = arg.is_from_sql_;
    ret = process_lock_task_(ctx, arg.lock_mode_, arg.owner_id_);
    ret = rewrite_return_code_(ret, ctx.ret_code_before_end_stmt_or_tx_, ctx.is_from_sql_);
  }
  return ret;
}

int ObTableLockService::unlock_subpartition(ObTxDesc &tx_desc,
                                            const ObTxParam &tx_param,
                                            const ObUnLockPartitionRequest &arg)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret));
  } else if (OB_UNLIKELY(!tx_desc.is_valid()) ||
             OB_UNLIKELY(!tx_param.is_valid()) ||
             OB_UNLIKELY(!arg.is_valid()) ||
             OB_UNLIKELY(ObTableLockOpType::OUT_TRANS_UNLOCK != arg.op_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tx_desc), K(arg), K(tx_desc.is_valid()),
             K(tx_param.is_valid()), K(arg.is_valid()));
  } else if (OB_FAIL(check_cluster_version_after_(CLUSTER_VERSION_4_1_0_0))) {
    LOG_WARN("cluster version check failed", K(ret), K(arg));
  } else {
    Thread::WaitGuard guard(Thread::WAIT);
    ObTableLockCtx ctx(UNLOCK_SUBPARTITION, arg.table_id_, arg.part_object_id_,
                       arg.timeout_us_, arg.timeout_us_);
    ctx.is_in_trans_ = true;
    ctx.tx_desc_ = &tx_desc;
    ctx.tx_param_ = tx_param;
    ctx.lock_op_type_ = arg.op_type_;
    ctx.is_from_sql_ = arg.is_from_sql_;
    ret = process_lock_task_(ctx, arg.lock_mode_, arg.owner_id_);
    ret = rewrite_return_code_(ret, ctx.is_from_sql_);
  }
  return ret;
}

int ObTableLockService::lock_obj(ObTxDesc &tx_desc,
                                 const ObTxParam &tx_param,
                                 const ObLockObjRequest &arg)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret));
  } else if (OB_UNLIKELY(!tx_desc.is_valid()) ||
             OB_UNLIKELY(!tx_param.is_valid()) ||
             OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tx_desc), K(arg), K(tx_desc.is_valid()),
             K(tx_param.is_valid()), K(arg.is_valid()));
  } else if (OB_FAIL(check_data_version_after_(DATA_VERSION_4_1_0_0))) {
    LOG_WARN("data version check failed", K(ret), K(arg));
  } else {
    Thread::WaitGuard guard(Thread::WAIT);
    ObTableLockCtx ctx(LOCK_OBJECT, arg.timeout_us_, arg.timeout_us_);
    ctx.is_in_trans_ = true;
    ctx.tx_desc_ = &tx_desc;
    ctx.tx_param_ = tx_param;
    ctx.lock_op_type_ = arg.op_type_;
    ctx.is_from_sql_ = arg.is_from_sql_;
    if (OB_FAIL(ctx.set_lock_id(arg.obj_type_, arg.obj_id_))) {
      LOG_WARN("set lock id failed", K(ret), K(arg));
    } else {
      ret = process_lock_task_(ctx, arg.lock_mode_, arg.owner_id_);
    }
    ret = rewrite_return_code_(ret, ctx.ret_code_before_end_stmt_or_tx_, ctx.is_from_sql_);
  }
  return ret;
}

int ObTableLockService::unlock_obj(ObTxDesc &tx_desc,
                                   const ObTxParam &tx_param,
                                   const ObUnLockObjRequest &arg)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret));
  } else if (OB_UNLIKELY(!tx_desc.is_valid()) ||
             OB_UNLIKELY(!tx_param.is_valid()) ||
             OB_UNLIKELY(!arg.is_valid()) ||
             OB_UNLIKELY(ObTableLockOpType::OUT_TRANS_UNLOCK != arg.op_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tx_desc), K(arg), K(tx_desc.is_valid()),
             K(tx_param.is_valid()), K(arg.is_valid()));
  } else if (OB_FAIL(check_data_version_after_(DATA_VERSION_4_1_0_0))) {
    LOG_WARN("data version check failed", K(ret), K(arg));
  } else {
    Thread::WaitGuard guard(Thread::WAIT);
    ObTableLockCtx ctx(UNLOCK_OBJECT, arg.timeout_us_, arg.timeout_us_);
    ctx.is_in_trans_ = true;
    ctx.tx_desc_ = &tx_desc;
    ctx.tx_param_ = tx_param;
    ctx.lock_op_type_ = arg.op_type_;
    ctx.is_from_sql_ = arg.is_from_sql_;
    if (OB_FAIL(ctx.set_lock_id(arg.obj_type_, arg.obj_id_))) {
      LOG_WARN("set lock id failed", K(ret), K(arg));
    } else {
      ret = process_lock_task_(ctx, arg.lock_mode_, arg.owner_id_);
    }
    ret = rewrite_return_code_(ret, ctx.is_from_sql_);
  }
  return ret;
}

int ObTableLockService::lock_obj(ObTxDesc &tx_desc,
                                 const ObTxParam &tx_param,
                                 const ObLockObjsRequest &arg)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret));
  } else if (OB_UNLIKELY(!tx_desc.is_valid()) ||
             OB_UNLIKELY(!tx_param.is_valid()) ||
             OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tx_desc), K(arg), K(tx_desc.is_valid()),
             K(tx_param.is_valid()), K(arg.is_valid()));
  } else if (OB_FAIL(check_data_version_after_(DATA_VERSION_4_1_0_0))) {
    LOG_WARN("data version check failed", K(ret), K(arg));
  } else if (OB_FAIL(check_cluster_version_after_(CLUSTER_VERSION_4_2_0_0))) {
    LOG_WARN("cluster version check failed", K(ret), K(arg));
  } else {
    Thread::WaitGuard guard(Thread::WAIT);
    ObTableLockCtx ctx(LOCK_OBJECT, arg.timeout_us_, arg.timeout_us_);
    ctx.is_in_trans_ = true;
    ctx.tx_desc_ = &tx_desc;
    ctx.tx_param_ = tx_param;
    ctx.lock_op_type_ = arg.op_type_;
    ctx.is_from_sql_ = arg.is_from_sql_;
    if (OB_FAIL(ctx.set_lock_id(arg.objs_))) {
      LOG_WARN("set lock id failed", K(ret), K(arg));
    } else {
      ret = process_lock_task_(ctx, arg.lock_mode_, arg.owner_id_);
    }
    ret = rewrite_return_code_(ret, ctx.ret_code_before_end_stmt_or_tx_, ctx.is_from_sql_);
  }
  return ret;
}

int ObTableLockService::unlock_obj(ObTxDesc &tx_desc,
                                   const ObTxParam &tx_param,
                                   const ObUnLockObjsRequest &arg)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret));
  } else if (OB_UNLIKELY(!tx_desc.is_valid()) ||
             OB_UNLIKELY(!tx_param.is_valid()) ||
             OB_UNLIKELY(!arg.is_valid()) ||
             OB_UNLIKELY(ObTableLockOpType::OUT_TRANS_UNLOCK != arg.op_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tx_desc), K(arg), K(tx_desc.is_valid()),
             K(tx_param.is_valid()), K(arg.is_valid()));
  } else if (OB_FAIL(check_data_version_after_(DATA_VERSION_4_1_0_0))) {
    LOG_WARN("data version check failed", K(ret), K(arg));
  } else if (OB_FAIL(check_cluster_version_after_(CLUSTER_VERSION_4_2_0_0))) {
    LOG_WARN("cluster version check failed", K(ret), K(arg));
  } else {
    Thread::WaitGuard guard(Thread::WAIT);
    ObTableLockCtx ctx(UNLOCK_OBJECT, arg.timeout_us_, arg.timeout_us_);
    ctx.is_in_trans_ = true;
    ctx.tx_desc_ = &tx_desc;
    ctx.tx_param_ = tx_param;
    ctx.lock_op_type_ = arg.op_type_;
    ctx.is_from_sql_ = arg.is_from_sql_;
    if (OB_FAIL(ctx.set_lock_id(arg.objs_))) {
      LOG_WARN("set lock id failed", K(ret), K(arg));
    } else {
      ret = process_lock_task_(ctx, arg.lock_mode_, arg.owner_id_);
    }
    ret = rewrite_return_code_(ret, ctx.is_from_sql_);
  }
  return ret;
}

int ObTableLockService::garbage_collect_right_now()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLockService is not be inited", K(ret));
  } else if (OB_FAIL(obj_lock_garbage_collector_.garbage_collect_right_now())) {
    LOG_WARN("garbage collect right now failed", K(ret));
  } else {
    LOG_DEBUG("garbage collect right now");
  }
  return ret;
}

int ObTableLockService::get_obj_lock_garbage_collector(ObOBJLockGarbageCollector *&obj_lock_garbage_collector)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLockService is not be inited", K(ret));
  } else {
    obj_lock_garbage_collector = &obj_lock_garbage_collector_;
  }
  return ret;
}
int ObTableLockService::process_lock_task_(ObTableLockCtx &ctx,
                                           const ObTableLockMode lock_mode,
                                           const ObTableLockOwnerID lock_owner)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (!ctx.is_in_trans_ && OB_FAIL(start_tx_(ctx))) {
    LOG_WARN("failed to start trans", K(ret));
  } else if (ctx.is_in_trans_ && OB_FAIL(start_stmt_(ctx))) {
    LOG_WARN("start stmt failed", K(ret), K(ctx));
  } else if (LOCK_OBJECT == ctx.task_type_ || UNLOCK_OBJECT == ctx.task_type_) {
    if (OB_FAIL(process_obj_lock_task_(ctx, lock_mode, lock_owner))) {
      LOG_WARN("lock obj failed", K(ret), K(ctx), K(lock_mode), K(lock_owner));
    }
  } else if (LOCK_ALONE_TABLET == ctx.task_type_ || UNLOCK_ALONE_TABLET == ctx.task_type_) {
    // only alone tablet should do like this.
    if (OB_FAIL(process_tablet_lock_task_(ctx, lock_mode, lock_owner, nullptr/* schema ptr */))) {
      LOG_WARN("process tablet lock task failed", K(ret), K(ctx), K(lock_mode), K(lock_owner));
    }
  } else {
    if (OB_FAIL(process_table_lock_task_(ctx, lock_mode, lock_owner))) {
      LOG_WARN("process table lock task failed", K(ret), K(ctx), K(lock_mode), K(lock_owner));
    }
  }
  ctx.ret_code_before_end_stmt_or_tx_ = ret;
  if (ctx.is_in_trans_ && OB_UNLIKELY(OB_SUCCESS != (tmp_ret = end_stmt_(ctx, OB_SUCCESS != ret)))) {
    LOG_WARN("failed to end stmt", K(ret), K(tmp_ret), K(ctx));
    // end stmt failed need rollback the whole trans.
    ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  } else if (!ctx.is_in_trans_ && OB_UNLIKELY(OB_SUCCESS != (tmp_ret = end_tx_(ctx, OB_SUCCESS != ret)))) {
    LOG_WARN("failed to end trans", K(ret), K(tmp_ret), K(ctx));
    ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
  }
  if (ctx.is_in_trans_ && ctx.tx_is_killed_) {
    // kill the in trans lock trans.
    // kill the whole trans if it is mysql mode.
    // kill the current stmt if it is oracle mode.
    if (OB_SUCCESS != (tmp_ret = deal_with_deadlock_(ctx))) {
      LOG_WARN("deal with deadlock failed.", K(tmp_ret), K(ctx));
    }
  }

  LOG_INFO("[table lock] lock_table", K(ret), K(ctx), K(lock_mode), K(lock_owner));

  return ret;
}

int ObTableLockService::process_obj_lock_task_(ObTableLockCtx &ctx,
                                               const ObTableLockMode lock_mode,
                                               const ObTableLockOwnerID lock_owner)
{
  int ret = OB_SUCCESS;
  if (ctx.obj_list_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("obj list is empty when lock obj", K(ret), K(ctx), K(lock_mode), K(lock_owner));
  } else {
    if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_0_0) {
      ObLockID lock_id;
      for (int64_t i = 0; OB_SUCC(ret) && i < ctx.obj_list_.count(); ++i) {
        lock_id = ctx.obj_list_.at(i);
        if (OB_FAIL(process_obj_lock_(ctx, LOCK_SERVICE_LS, lock_id, lock_mode, lock_owner))) {
            LOG_WARN("lock obj failed", K(ret), K(ctx), K(LOCK_SERVICE_LS), K(lock_id), K(ctx.task_type_), K(lock_mode));
        }
      }
    } else {
      if (OB_FAIL(process_obj_lock_(ctx, LOCK_SERVICE_LS, ctx.obj_list_, lock_mode, lock_owner))) {
        LOG_WARN("lock obj failed", K(ret), K(ctx), K(LOCK_SERVICE_LS), K(ctx.obj_list_), K(ctx.task_type_), K(lock_mode));
      }
    }
  }
  return ret;
}

int ObTableLockService::process_table_lock_task_(ObTableLockCtx &ctx,
                                                 const ObTableLockMode lock_mode,
                                                 const ObTableLockOwnerID lock_owner)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObSimpleTableSchemaV2 *table_schema = nullptr;
  bool is_allowed = false;
  ObArenaAllocator allocator("TableSchema");

  ctx.schema_version_ = 0;

  if (OB_FAIL(process_table_lock_(ctx, lock_mode, lock_owner))) {
    LOG_WARN("lock table failed", K(ret), K(ctx), K(lock_mode), K(lock_owner));
  } else if (OB_FAIL(ObSchemaUtils::get_latest_table_schema(
      *sql_proxy_,
      allocator,
      tenant_id,
      ctx.table_id_,
      table_schema))) {
    if (OB_TABLE_NOT_EXIST == ret) {
      LOG_INFO("table not exist, check whether it meets expectations", K(ret), K(ctx));
    } else {
      LOG_WARN("get table schema failed", K(ret), K(ctx));
    }
  } else if (OB_FAIL(check_op_allowed_(ctx.table_id_, table_schema, is_allowed))) {
    LOG_WARN("failed to check op allowed", K(ret), K(ctx));
  } else if (!is_allowed) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("lock table not allowed now", K(ret), K(ctx));
  } else if (OB_FAIL(process_tablet_lock_task_(ctx,
                                               lock_mode,
                                               lock_owner,
                                               table_schema))) {
    LOG_WARN("failed to lock table tablet", K(ret), K(ctx));
  }
  return ret;
}

int ObTableLockService::process_tablet_lock_task_(ObTableLockCtx &ctx,
                                                  const ObTableLockMode lock_mode,
                                                  const ObTableLockOwnerID lock_owner,
                                                  const ObSimpleTableSchemaV2 *table_schema)
{
  int ret = OB_SUCCESS;
  LockMap lock_map;
  ObLSLockMap ls_lock_map;

  // TODO: yanyuan.cxf we may need the right schema_version while lock/unlock alone tablet.
  if (OB_ISNULL(table_schema)) {
    ctx.schema_version_ = 0;
  } else {
    ctx.schema_version_ = table_schema->get_schema_version();
  }

  if (OB_FAIL(get_process_tablets_(lock_mode, table_schema, ctx))) {
    LOG_WARN("failed to get parts", K(ret), K(ctx));
  // lock_map and ls_lock_map are the map of lock which is generated with tablet_id
  } else if (OB_FAIL(get_ls_lock_map_(ctx, ctx.tablet_list_, lock_map, ls_lock_map))) {
    LOG_WARN("fail to get ls lock map", K(ret), K(ctx.get_tablet_cnt()));
  } else if (OB_FAIL(pre_check_lock_(ctx,
                                     lock_mode,
                                     lock_owner,
                                     ls_lock_map))) {
    LOG_WARN("failed to pre_check_lock_", K(ret), K(ctx), K(lock_mode), K(lock_owner));
  } else if (OB_FAIL(process_table_tablet_lock_(ctx,
                                                lock_mode,
                                                lock_owner,
                                                lock_map,
                                                ls_lock_map))) {
    LOG_WARN("failed to lock table tablet", K(ret), K(ctx));
  }
  return ret;
}

bool ObTableLockService::is_part_table_lock_(const ObTableLockTaskType task_type)
{
  return (LOCK_TABLET == task_type || UNLOCK_TABLET == task_type ||
          LOCK_PARTITION == task_type || UNLOCK_PARTITION == task_type ||
          LOCK_SUBPARTITION == task_type || UNLOCK_SUBPARTITION == task_type ||
          LOCK_ALONE_TABLET == task_type || UNLOCK_ALONE_TABLET == task_type);
}

int ObTableLockService::get_table_lock_mode_(const ObTableLockTaskType task_type,
                                             const ObTableLockMode part_lock_mode,
                                             ObTableLockMode &table_lock_mode)
{
  int ret = OB_SUCCESS;
  if (is_part_table_lock_(task_type)) {
    // lock tablet.
    if (EXCLUSIVE == part_lock_mode ||
        ROW_EXCLUSIVE == part_lock_mode) {
      table_lock_mode = ROW_EXCLUSIVE;
    } else if (SHARE == part_lock_mode ||
               ROW_SHARE == part_lock_mode) {
      table_lock_mode = ROW_SHARE;
    } else if (SHARE_ROW_EXCLUSIVE == part_lock_mode) {
      // TODO: cxf lock all the tablet of this table.
      // our srx is not the same as oracle now.
      table_lock_mode = SHARE_ROW_EXCLUSIVE;
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObTableLockService::get_retry_lock_ids_(const ObLockIDArray &lock_ids,
                                            const int64_t start_pos,
                                            ObLockIDArray &retry_lock_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = start_pos; i < lock_ids.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(retry_lock_ids.push_back(lock_ids.at(i)))) {
      LOG_WARN("get retry tablet failed", K(ret), K(lock_ids.at(i)));
    }
  }
  return ret;
}

int ObTableLockService::get_retry_lock_ids_(const ObLSID &ls_id,
                                            const ObLSLockMap &ls_lock_map,
                                            const int64_t start_pos,
                                            ObLockIDArray &retry_lock_ids)
{
  int ret = OB_SUCCESS;
  const ObLockIDArray *lock_ids = nullptr;
  // get the retry tablet list
  if (OB_ISNULL(lock_ids = ls_lock_map.get(ls_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the ls not exist at tablet map", K(ret), K(ls_id));
  } else if (OB_FAIL(get_retry_lock_ids_(*lock_ids, start_pos, retry_lock_ids))) {
    // get the lock ids
    LOG_WARN("get retry lock id list failed", K(ret));
  }
  return ret;
}

int ObTableLockService::collect_rollback_info_(const share::ObLSID &ls_id,
                                               ObTableLockCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ctx.add_touched_ls(ls_id))) {
    LOG_ERROR("add touched ls failed.", K(ret), K(ls_id));
  }
  return ret;
}

int ObTableLockService::collect_rollback_info_(const ObArray<share::ObLSID> &ls_array,
                                               ObTableLockCtx &ctx)
{
  int ret = OB_SUCCESS;
  // all rpcs treated as failed
  for (int i = 0; i < ls_array.count() && OB_SUCC(ret); i++) {
    if (OB_FAIL(ctx.add_touched_ls(ls_array.at(i)))) {
      LOG_ERROR("add touched ls failed.", K(ret), K(ls_array.at(i)));
    }
  }
  return ret;
}

template<class RpcProxy>
int ObTableLockService::collect_rollback_info_(const ObArray<share::ObLSID> &ls_array,
                                               RpcProxy &proxy_batch,
                                               ObTableLockCtx &ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  // 1. need wait rpcs that sent finish
  //    otherwise proxy reused or destructored will cause flying rpc core
  // 2. don't use arg/dest here because call() may has failure.
  // 3. return_array/result can be used only when wait_all() is success.
  ObArray<int> return_code_array;
  if (OB_TMP_FAIL(proxy_batch.wait_all(return_code_array))) {
    LOG_WARN("wait rpc failed", K(tmp_ret));
  }

  if (OB_FAIL(collect_rollback_info_(ls_array, ctx))) {
    LOG_WARN("collect rollback info failed", K(ret));
  }
  return ret;
}

template<class RpcProxy>
int ObTableLockService::handle_parallel_rpc_response_(RpcProxy &proxy_batch,
                                                      ObTableLockCtx &ctx,
                                                      const ObLSLockMap &ls_lock_map,
                                                      bool &can_retry,
                                                      ObRetryCtx &retry_ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTransService *txs = MTL(ObTransService*);
  ObLSID ls_id;

  can_retry = true;
  retry_ctx.need_retry_ = true;
  // handle result
  ObArray<int> return_code_array;
  if (OB_TMP_FAIL(proxy_batch.wait_all(return_code_array))
      || OB_TMP_FAIL(proxy_batch.check_return_cnt(return_code_array.count()))
      || retry_ctx.send_rpc_count_ != return_code_array.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc failed", KR(ret), KR(tmp_ret), K(retry_ctx.send_rpc_count_), K(return_code_array.count()));
    // we need add the ls into touched to make rollback.
    can_retry = false;
    retry_ctx.need_retry_ = false;
    (void) collect_rollback_info_(retry_ctx.rpc_ls_array_, ctx);
  } else {
    //check each ret of every rpc
    const ObTableLockTaskResult *result = nullptr;
    for (int64_t i = 0; i < return_code_array.count(); ++i) {
      result = nullptr;
      tmp_ret = return_code_array.at(i);
      ls_id = retry_ctx.rpc_ls_array_.at(i);
      if (need_retry_whole_rpc_task_(tmp_ret)) {
        // rpc failed, but we need retry the whole rpc task.
        LOG_WARN("lock rpc failed, but we need retry", KR(tmp_ret), K(i), K(ls_id));
        if (OB_TMP_FAIL(get_retry_lock_ids_(ls_id,
                                            ls_lock_map,
                                            0,
                                            retry_ctx.retry_lock_ids_))) {
          can_retry = false;
          retry_ctx.need_retry_ = false;
          ret = tmp_ret;
          LOG_WARN("get retry tablet list failed", KR(ret), K(ls_id));
        }
      } else {
        if (OB_TMP_FAIL(tmp_ret)) {
          LOG_WARN("lock rpc failed", KR(tmp_ret), K(i), K(ls_id));
        } else if (OB_ISNULL(result = proxy_batch.get_results().at(i))) {
          tmp_ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", KR(tmp_ret), K(i), K(ls_id));
        } else if (OB_TMP_FAIL(result->get_tx_result_code())) {
          LOG_WARN("get tx exec result failed", KR(tmp_ret), K(i), K(ls_id));
        } else if (OB_TMP_FAIL(txs->add_tx_exec_result(*ctx.tx_desc_,
                                                       result->tx_result_))) {
          LOG_WARN("failed to add exec result", K(tmp_ret), K(ctx), K(result->tx_result_));
        }

        // rpc failed or we get tx exec result failed,
        // we need add the ls into touched to make rollback.
        if (OB_TMP_FAIL(tmp_ret)) {
          ret = tmp_ret;
          can_retry = false;
          retry_ctx.need_retry_ = false;
          (void) collect_rollback_info_(ls_id, ctx);
        } else {
          // if error codes are only OB_TRY_LOCK_ROW_CONFLICT, will retry
          tmp_ret = result->get_ret_code();
          if (need_retry_part_rpc_task_(tmp_ret, result)) {
            LOG_WARN("lock rpc failed, but we need retry", KR(tmp_ret), K(i), K(ls_id));
            if (OB_TMP_FAIL(get_retry_lock_ids_(ls_id,
                                                ls_lock_map,
                                                result->get_success_pos() + 1,
                                                retry_ctx.retry_lock_ids_))) {
              can_retry = false;
              retry_ctx.need_retry_ = false;
              ret = tmp_ret;
              LOG_WARN("get retry tablet list failed", KR(ret), K(ls_id));
            }
          } else if (OB_TRANS_KILLED == tmp_ret) {
            // the trans need kill.
            ctx.tx_is_killed_ = true;
            can_retry = false;
          } else if (OB_TMP_FAIL(tmp_ret)) {
            can_retry = false;
            retry_ctx.need_retry_ = false;
          }
          if (OB_TMP_FAIL(tmp_ret)) {
            LOG_WARN("lock rpc wrong", K(tmp_ret), K(ls_id));
            if (OB_SUCC(ret) || ret == OB_TRY_LOCK_ROW_CONFLICT) {
              ret = tmp_ret;
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObTableLockService::pre_check_lock_(ObTableLockCtx &ctx,
                                        const ObTableLockMode lock_mode,
                                        const ObTableLockOwnerID lock_owner,
                                        const ObLSLockMap &ls_lock_map)
{
  int ret = OB_SUCCESS;
  if (GET_MIN_CLUSTER_VERSION() > CLUSTER_VERSION_4_0_0_0) {
    ret = batch_pre_check_lock_(ctx, lock_mode, lock_owner, ls_lock_map);
  } else {
    ret = pre_check_lock_old_version_(ctx, lock_mode, lock_owner, ls_lock_map);
  }
  return ret;
}

// for 4.1
template<class RpcProxy>
int ObTableLockService::parallel_batch_rpc_handle_(RpcProxy &proxy_batch,
                                                   ObTableLockCtx &ctx,
                                                   const ObTableLockTaskType lock_task_type,
                                                   const ObLSLockMap &ls_lock_map,
                                                   const ObTableLockMode lock_mode,
                                                   const ObTableLockOwnerID lock_owner)
{
  int ret = OB_SUCCESS;
  constexpr static int64_t MAP_NUM = 2;
  ObLSLockMap maps[MAP_NUM];
  const ObLSLockMap *in_map = nullptr;
  ObLSLockMap *retry_map = nullptr;
  bool can_retry = true;       // whether the whole rpc task can retry.
  int64_t retry_times = 1;
  for (int64_t i = 0; i < MAP_NUM && OB_SUCC(ret); i++) {
    if (OB_FAIL(maps[i].create(10, lib::ObLabel("LSLockMap")))) {
      LOG_WARN("ls lock map create failed", KR(ret), K(i));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    retry_map = const_cast<ObLSLockMap *>(&ls_lock_map);
    do {
      in_map = retry_map;
      retry_map = &maps[retry_times % MAP_NUM];
      if (OB_FAIL(retry_map->reuse())) {
        LOG_WARN("reuse retry map failed", K(ret));
      } else if (OB_FAIL(parallel_batch_rpc_handle_(proxy_batch,
                                                    ctx,
                                                    lock_task_type,
                                                    *in_map,
                                                    lock_mode,
                                                    lock_owner,
                                                    can_retry,
                                                    *retry_map))) {
        LOG_WARN("process rpc failed", KR(ret), K(can_retry), K(ctx), K(retry_times));
      }
      if (can_retry && !retry_map->empty()) {
        retry_times++;
      }
      if (retry_times % 10 == 0) {
        LOG_WARN("retry too many times", K(retry_times), K(can_retry), K(ctx));
        FOREACH(data, ls_lock_map) {
          const share::ObLSID &ls_id = data->first;
          const ObLockIDArray &lock_ids = data->second;
          LOG_WARN("retry data", K(ls_id), K(lock_ids));
        }
      }
    } while (can_retry && !retry_map->empty());
  }
  return ret;
}

template<class RpcProxy>
int ObTableLockService::parallel_send_rpc_task_(RpcProxy &proxy_batch,
                                                ObTableLockCtx &ctx,
                                                const ObTableLockTaskType lock_task_type,
                                                const ObLSLockMap &ls_lock_map,
                                                const ObTableLockMode lock_mode,
                                                const ObTableLockOwnerID lock_owner,
                                                ObRetryCtx &retry_ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool has_retry = false;

  retry_ctx.send_rpc_count_ = 0;
  // send async rpc parallel
  FOREACH_X(data, ls_lock_map, OB_SUCC(ret)) {
    const share::ObLSID &ls_id = data->first;
    const ObLockIDArray &lock_ids = data->second;
    if (!has_retry) {
      if (OB_FAIL(send_rpc_task_(proxy_batch,
                                 ctx,
                                 ls_id,
                                 lock_ids,
                                 lock_mode,
                                 lock_owner,
                                 retry_ctx))) {
        LOG_WARN("send rpc task failed", K(ret));
      }
    }
    if (retry_ctx.need_retry_ || has_retry) {
      has_retry = true;
      if (OB_TMP_FAIL(get_retry_lock_ids_(lock_ids,
                                          0,
                                          retry_ctx.retry_lock_ids_))) {
        LOG_WARN("get retry tablet failed", KR(ret));
        ret = tmp_ret;
      };
    }
  }
  return ret;
}

template<class RpcProxy>
int ObTableLockService::parallel_batch_rpc_handle_(RpcProxy &proxy_batch,
                                                   ObTableLockCtx &ctx,
                                                   const ObTableLockTaskType lock_task_type,
                                                   const ObLSLockMap &ls_lock_map,
                                                   const ObTableLockMode lock_mode,
                                                   const ObTableLockOwnerID lock_owner,
                                                   bool &can_retry,
                                                   ObLSLockMap &retry_ls_lock_map)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObRetryCtx retry_ctx;
  proxy_batch.reuse();

  // send async rpc parallel
  if (OB_FAIL(parallel_send_rpc_task_(proxy_batch,
                                      ctx,
                                      lock_task_type,
                                      ls_lock_map,
                                      lock_mode,
                                      lock_owner,
                                      retry_ctx))) {
    can_retry = false;
    (void)collect_rollback_info_(retry_ctx.rpc_ls_array_, proxy_batch, ctx);
    LOG_WARN("send rpc task failed", KR(ret));
  } else {
    // process rpc response
    ret = handle_parallel_rpc_response_(proxy_batch,
                                        ctx,
                                        ls_lock_map,
                                        can_retry,
                                        retry_ctx);
  }

  // get the retry map
  if (can_retry && retry_ctx.retry_lock_ids_.count() != 0) {
    LOG_WARN("lock rpc failed, but we need retry", K(ret), K(can_retry), K(retry_ctx));
    if (OB_FAIL(fill_ls_lock_map_(ctx,
                                  retry_ctx.retry_lock_ids_,
                                  retry_ls_lock_map,
                                  true /* force refresh location */))) {
      LOG_WARN("refill ls lock map failed", KP(ret), K(ctx));
      can_retry = false;
    }
  }
  return ret;
}

int ObTableLockService::batch_pre_check_lock_(ObTableLockCtx &ctx,
                                              const ObTableLockMode lock_mode,
                                              const ObTableLockOwnerID lock_owner,
                                              const ObLSLockMap &ls_lock_map)
{
  int ret = OB_SUCCESS;
  int last_ret = OB_SUCCESS;
  int64_t USLEEP_TIME = 100; // 0.1 ms
  bool need_retry = false;
  ObBatchLockProxy proxy_batch(*GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::batch_lock_obj);
  // only used in LOCK_TABLE/LOCK_PARTITION
  if (LOCK_TABLE == ctx.task_type_ ||
      LOCK_PARTITION == ctx.task_type_) {
    do {
      need_retry = false;
      if (ctx.is_timeout()) {
        ret = (last_ret == OB_TRY_LOCK_ROW_CONFLICT) ?
          OB_ERR_EXCLUSIVE_LOCK_CONFLICT : OB_TIMEOUT;
        LOG_WARN("process obj lock timeout", K(ret), K(ctx));
      } else {
        ret = parallel_batch_rpc_handle_(proxy_batch,
                                         ctx,
                                         PRE_CHECK_TABLET,
                                         ls_lock_map,
                                         lock_mode,
                                         lock_owner);
        // the process process may be timeout because left time not enough,
        // just rewrite it to OB_ERR_EXCLUSIVE_LOCK_CONFLICT
        if (is_timeout_ret_code_(ret)) {
          ret = (last_ret == OB_TRY_LOCK_ROW_CONFLICT) ?
            OB_ERR_EXCLUSIVE_LOCK_CONFLICT : OB_TIMEOUT;
          LOG_WARN("process obj lock timeout", K(ret), K(ctx));
        }
      }

      if (!ctx.is_try_lock() &&
          ctx.is_deadlock_avoid_enabled() &&
          OB_TRY_LOCK_ROW_CONFLICT == ret) {
        ret = OB_TRANS_KILLED;
        ctx.tx_is_killed_ = true;
      }
      if (ret == OB_TRY_LOCK_ROW_CONFLICT) {
        if (ctx.is_try_lock()) {
          ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
          LOG_INFO("try lock and meet conflict", K(ret), K(ctx));
        } else if (OB_UNLIKELY(ctx.is_timeout())) {
          ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
          LOG_WARN("lock table timeout", K(ret), K(ctx));
        } else {
          need_retry = true;
          last_ret = ret;
          ret = OB_SUCCESS;
          ob_usleep(USLEEP_TIME);
        }
      }
    } while (need_retry);  // retry task level
    LOG_DEBUG("ObTableLockService::pre_check_lock_", K(ret), K(ctx),
              K(ctx.task_type_), K(lock_mode), K(lock_owner));
  }
  return ret;
}

template<class RpcProxy, class LockRequest>
int ObTableLockService::rpc_call_(RpcProxy &proxy_batch,
                                  const ObAddr &addr,
                                  const int64_t timeout_us,
                                  const LockRequest &request)
{
  int ret = OB_SUCCESS;
  int32_t group_id = 0;
  const int64_t min_cluster_version = GET_MIN_CLUSTER_VERSION();
  if ((min_cluster_version >= MOCK_CLUSTER_VERSION_4_2_1_4 && min_cluster_version < CLUSTER_VERSION_4_2_2_0)
      || (min_cluster_version >= MOCK_CLUSTER_VERSION_4_2_3_0 && min_cluster_version < CLUSTER_VERSION_4_3_0_0)
      || (min_cluster_version >= CLUSTER_VERSION_4_3_0_0)) {
    group_id = share::OBCG_LOCK;
    if (request.is_unlock_request()) {
      group_id = share::OBCG_UNLOCK;
    }
    if (OB_FAIL(proxy_batch.call(addr,
                                 timeout_us,
                                 GCONF.cluster_id,
                                 MTL_ID(),
                                 group_id,
                                 request))) {
      LOG_WARN("failed to all async rpc", KR(ret), K(addr), K(timeout_us), K(request));
    }
  } else {
    if (OB_FAIL(proxy_batch.call(addr,
                                 timeout_us,
                                 MTL_ID(),
                                 request))) {
      LOG_WARN("failed to all async rpc", KR(ret), K(addr), K(timeout_us), K(request));
    }
  }
  return ret;
}

int ObTableLockService::pre_check_lock_old_version_(ObTableLockCtx &ctx,
                                                    const ObTableLockMode lock_mode,
                                                    const ObTableLockOwnerID lock_owner,
                                                    const ObLSLockMap &ls_lock_map)
{
  int ret = OB_SUCCESS;
  int last_ret = OB_SUCCESS;
  int64_t USLEEP_TIME = 100; // 0.1 ms
  bool need_retry = false;
  int64_t timeout_us = 0;
  bool unused = false;
  share::ObLSID ls_id;
  ObLockID lock_id;
  ObRetryCtx retry_ctx;
  ObAddr addr;
  ObTableLockTaskRequest request;
  ObTableLockProxy proxy_batch(*GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::lock_table);
  // only used in LOCK_TABLE/LOCK_PARTITION
  if (LOCK_TABLE == ctx.task_type_ ||
      LOCK_PARTITION == ctx.task_type_) {
    do {
      need_retry = false;
      timeout_us = 0;
      proxy_batch.reuse();
      retry_ctx.reuse();
      // send async rpc parallel
      for (int64_t i = 0; i < ctx.get_tablet_cnt() && OB_SUCC(ret); ++i) {
        const ObTabletID &tablet_id = ctx.tablet_list_.at(i);
        if (OB_FAIL(get_tablet_ls_(ctx, tablet_id, ls_id))) {
          LOG_WARN("failed to get tablet ls", K(ret), K(tablet_id));
        } else if (OB_FAIL(get_lock_id(tablet_id, lock_id))) {
          LOG_WARN("get lock id failed", K(ret), K(ctx));
        } else {
          addr.reset();
          request.reset();
          // can not reused because of allocator reset
          ObTableLockTaskResult result;

          if (OB_FAIL(retry_ctx.rpc_ls_array_.push_back(ls_id))) {
            LOG_WARN("push_back lsid failed", K(ret), K(ls_id));
          } else if (OB_FAIL(pack_request_(ctx, PRE_CHECK_TABLET, lock_mode, lock_owner,
                                           lock_id, ls_id, addr, request))) {
            LOG_WARN("pack_request_ failed", K(ret), K(ls_id), K(lock_id));
            // the rpc timeout must larger than stmt timeout.
          } else if (FALSE_IT(timeout_us = ctx.get_rpc_timeoutus())) {
          } else if (ctx.is_timeout()) {
            ret = (last_ret == OB_TRY_LOCK_ROW_CONFLICT) ? OB_ERR_EXCLUSIVE_LOCK_CONFLICT : OB_TIMEOUT;
            LOG_WARN("process obj lock timeout", K(ret), K(ctx));
          } else if (OB_FAIL(rpc_call_(proxy_batch,
                                       addr,
                                       timeout_us,
                                       request))) {
            LOG_WARN("failed to all async rpc", KR(ret), K(addr), K(request));
          } else {
            retry_ctx.send_rpc_count_++;
            ALLOW_NEXT_LOG();
            LOG_INFO("send table pre_check rpc", KR(ret), K(addr), "request", request);
          }
        }
      }
      if (OB_FAIL(ret)) {
        (void)collect_rollback_info_(retry_ctx.rpc_ls_array_, proxy_batch, ctx);
      } else {
        ret = handle_parallel_rpc_response_(proxy_batch, ctx, ls_lock_map, unused, retry_ctx);
      }

      if (is_timeout_ret_code_(ret)) {
        ret = (last_ret == OB_TRY_LOCK_ROW_CONFLICT) ? OB_ERR_EXCLUSIVE_LOCK_CONFLICT : OB_TIMEOUT;
        LOG_WARN("process obj lock timeout", K(ret), K(ctx));
      }

      if (!ctx.is_try_lock() &&
          ctx.is_deadlock_avoid_enabled() &&
          OB_TRY_LOCK_ROW_CONFLICT == ret) {
        ret = OB_TRANS_KILLED;
        ctx.tx_is_killed_ = true;
      }
      if (ret == OB_TRY_LOCK_ROW_CONFLICT) {
        if (ctx.is_try_lock()) {
          // do nothing
        } else if (OB_UNLIKELY(ctx.is_timeout())) {
          ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
          LOG_WARN("lock table timeout", K(ret), K(ctx));
        } else {
          need_retry = true;
          last_ret = ret;
          ret = OB_SUCCESS;
          ob_usleep(USLEEP_TIME);
        }
      }
    } while (need_retry);
    LOG_DEBUG("ObTableLockService::pre_check_lock_", K(ret), K(ctx),
              K(ctx.task_type_), K(lock_mode), K(lock_owner));
  }
  return ret;
}

int ObTableLockService::deal_with_deadlock_(ObTableLockCtx &ctx)
{
  int ret = OB_SUCCESS;
  SessionGuard session_guard;
  const uint32_t sess_id = ctx.tx_desc_->get_session_id();
  if (OB_FAIL(ObTransDeadlockDetectorAdapter::get_session_info(sess_id, session_guard))) {
    LOG_WARN("get session info failed", K(ret), K(sess_id));
  } else if (!session_guard.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session guard invalid", K(ret), K(sess_id));
  } else if (ObCompatibilityMode::MYSQL_MODE == session_guard->get_compatibility_mode()) {
    ret = ObTransDeadlockDetectorAdapter::kill_tx(sess_id);
  } else if (ObCompatibilityMode::ORACLE_MODE == session_guard->get_compatibility_mode()) {
    ret = ObTransDeadlockDetectorAdapter::kill_stmt(sess_id);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unknown mode", K(ret), K(session_guard->get_compatibility_mode()));
  }
  if (!OB_SUCC(ret)) {
    LOG_WARN("kill trans or stmt failed", K(ret), K(sess_id));
  }
  LOG_DEBUG("ObTableLockService::deal_with_deadlock_", K(ret), K(sess_id));
  return ret;
}

int ObTableLockService::get_table_partition_level_(const ObTableID table_id,
                                                  ObPartitionLevel &part_level)
{
  int ret = OB_SUCCESS;
  ObSimpleTableSchemaV2 *table_schema = nullptr;
  ObArenaAllocator allocator("TableSchema");

  if (OB_FAIL(ObSchemaUtils::get_latest_table_schema(
      *sql_proxy_,
      allocator,
      MTL_ID(),
      table_id,
      table_schema))) {
    LOG_WARN("can not get table schema", K(ret), K(table_id));
  } else {
    part_level = table_schema->get_part_level();
  }
  return ret;
}

int ObTableLockService::pack_batch_request_(ObTableLockCtx &ctx,
                                            const ObTableLockTaskType task_type,
                                            const ObTableLockMode &lock_mode,
                                            const ObTableLockOwnerID &lock_owner,
                                            const share::ObLSID &ls_id,
                                            const ObLockIDArray &lock_ids,
                                            ObLockTaskBatchRequest &request)
{
  int ret = OB_SUCCESS;
  ObLockParam lock_param;
  if (OB_FAIL(request.init(task_type, ls_id, ctx.tx_desc_))) {
    LOG_WARN("request init failed", K(ret), K(task_type), K(ls_id), KP(ctx.tx_desc_));
  } else {
    for (int i = 0; i < lock_ids.count() && OB_SUCC(ret); ++i) {
      lock_param.reset();
      if (OB_FAIL(lock_param.set(lock_ids[i],
                                 lock_mode,
                                 lock_owner,
                                 ctx.lock_op_type_,
                                 ctx.schema_version_,
                                 ctx.is_deadlock_avoid_enabled(),
                                 ctx.is_try_lock(),
                                 ctx.abs_timeout_ts_))) {
        LOG_WARN("get lock param failed", K(ret));
      } else if (OB_FAIL(request.params_.push_back(lock_param))) {
        LOG_WARN("get lock request failed", K(ret), K(lock_param));
      }
    }
  }
  return ret;
}

int ObTableLockService::pack_request_(ObTableLockCtx &ctx,
                                      const ObTableLockTaskType task_type,
                                      const ObTableLockMode &lock_mode,
                                      const ObTableLockOwnerID &lock_owner,
                                      const ObLockID &lock_id,
                                      const share::ObLSID &ls_id,
                                      ObAddr &addr,
                                      ObTableLockTaskRequest &request)
{
  int ret = OB_SUCCESS;
  ObLockParam lock_param;
  if (OB_FAIL(lock_param.set(lock_id,
                             lock_mode,
                             lock_owner,
                             ctx.lock_op_type_,
                             ctx.schema_version_,
                             ctx.is_deadlock_avoid_enabled(),
                             ctx.is_try_lock(),
                             ctx.abs_timeout_ts_))) {
    LOG_WARN("get lock param failed", K(ret));
  } else if (OB_FAIL(request.set(task_type,
                                 ls_id,
                                 lock_param,
                                 ctx.tx_desc_))) {
    LOG_WARN("get lock request failed", K(ret));
  } else if (OB_FAIL(get_ls_leader_(ctx.tx_desc_->get_cluster_id(),
                                    ctx.tx_desc_->get_tenant_id(),
                                    ls_id,
                                    ctx.abs_timeout_ts_,
                                    addr))) {
    LOG_WARN("failed to get ls leader", K(ret), K(ctx), K(ls_id));
  }
  return ret;
}

template<class RpcProxy>
int ObTableLockService::batch_rpc_handle_(RpcProxy &proxy_batch,
                                          ObTableLockCtx &ctx,
                                          const ObLSLockMap &ls_lock_map,
                                          const ObTableLockMode lock_mode,
                                          const ObTableLockOwnerID lock_owner)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  constexpr static int64_t MAP_NUM = 2;
  ObLSLockMap maps[MAP_NUM];
  const ObLSLockMap *in_map = nullptr;
  ObLSLockMap *retry_map = nullptr;
  bool can_retry = true;       // whether the whole rpc task can retry.
  int64_t retry_times = 1;
  for (int64_t i = 0; i < MAP_NUM && OB_SUCC(ret); i++) {
    if (OB_FAIL(maps[i].create(10, lib::ObLabel("LSLockMap")))) {
      LOG_WARN("ls lock map create failed", KR(ret), K(i));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    retry_map = const_cast<ObLSLockMap *>(&ls_lock_map);
    do {
      in_map = retry_map;
      retry_map = &maps[retry_times % MAP_NUM];
      if (OB_FAIL(retry_map->reuse())) {
        LOG_WARN("reuse retry map failed", K(ret));
      } else if (OB_FAIL(batch_rpc_handle_(proxy_batch,
                                           ctx,
                                           *in_map,
                                           lock_mode,
                                           lock_owner,
                                           can_retry,
                                           *retry_map))) {
        LOG_WARN("process rpc failed", KR(ret), K(ctx), K(retry_times));
      }
      if (can_retry && !retry_map->empty()) {
        retry_times++;
      }
      if (retry_times % 10 == 0) {
        LOG_WARN("retry too many times", K(retry_times), K(ctx), K(retry_map->size()));
      }
    } while (can_retry && !retry_map->empty());
  }
  return ret;
}

template<class RpcProxy>
int ObTableLockService::send_rpc_task_(RpcProxy &proxy_batch,
                                       ObTableLockCtx &ctx,
                                       const share::ObLSID &ls_id,
                                       const ObLockIDArray &lock_ids,
                                       const ObTableLockMode lock_mode,
                                       const ObTableLockOwnerID lock_owner,
                                       ObRetryCtx &retry_ctx)
{
  int ret = OB_SUCCESS;
  ObLockTaskBatchRequest request;
  ObAddr addr;

  retry_ctx.need_retry_ = false;
  if (OB_FAIL(retry_ctx.rpc_ls_array_.push_back(ls_id))) {
    LOG_WARN("push_back lsid failed", K(ret), K(ls_id));
  } else if (OB_FAIL(pack_batch_request_(ctx,
                                         ctx.task_type_,
                                         lock_mode,
                                         lock_owner,
                                         ls_id,
                                         lock_ids,
                                         request))) {
    LOG_WARN("pack_request_ failed", K(ret), K(ls_id));
  } else if (OB_FAIL(get_ls_leader_(ctx.tx_desc_->get_cluster_id(),
                                    ctx.tx_desc_->get_tenant_id(),
                                    ls_id,
                                    ctx.abs_timeout_ts_,
                                    addr))) {
    if (need_renew_location_(ret)) {
      retry_ctx.need_retry_ = true;
      ret = OB_SUCCESS;
    }
    LOG_WARN("failed to get ls leader", K(ret), K(ctx), K(ls_id));
  } else if (ctx.is_timeout()) {
    ret = OB_TIMEOUT;
    LOG_WARN("process obj lock timeout", K(ret), K(ctx));
  } else if (OB_FAIL(rpc_call_(proxy_batch,
                               addr,
                               ctx.get_rpc_timeoutus(),
                               request))) {
    LOG_WARN("failed to call async rpc", KR(ret), K(addr),
                                        K(ctx.abs_timeout_ts_), K(request));
  } else {
    retry_ctx.send_rpc_count_++;
    ALLOW_NEXT_LOG();
    LOG_INFO("send table lock rpc", KR(ret), K(retry_ctx.send_rpc_count_), K(addr), "request", request);
  }
  if (OB_FAIL(ret)) {
    retry_ctx.need_retry_ = false;
  }
  return ret;
}

template<class RpcProxy>
int ObTableLockService::send_one_rpc_task_(RpcProxy &proxy_batch,
                                           ObTableLockCtx &ctx,
                                           const share::ObLSID &ls_id,
                                           const ObLockIDArray &lock_ids,
                                           const ObTableLockMode lock_mode,
                                           const ObTableLockOwnerID lock_owner,
                                           ObRetryCtx &retry_ctx)
{
  int ret = OB_SUCCESS;

  retry_ctx.send_rpc_count_ = 0;
  if (OB_FAIL(send_rpc_task_(proxy_batch,
                             ctx,
                             ls_id,
                             lock_ids,
                             lock_mode,
                             lock_owner,
                             retry_ctx))) {
    LOG_WARN("send rpc task failed", K(ret), K(ls_id));
  } else if (retry_ctx.need_retry_) {
    if (OB_FAIL(get_retry_lock_ids_(lock_ids,
                                    0,
                                    retry_ctx.retry_lock_ids_))) {
      retry_ctx.need_retry_ = false;
      LOG_WARN("get retry tablet failed", KR(ret));
    };
  }
  return ret;
}

template<class RpcProxy>
int ObTableLockService::batch_rpc_handle_(RpcProxy &proxy_batch,
                                          ObTableLockCtx &ctx,
                                          const ObLSLockMap &ls_lock_map,
                                          const ObTableLockMode lock_mode,
                                          const ObTableLockOwnerID lock_owner,
                                          bool &can_retry,
                                          ObLSLockMap &retry_ls_lock_map)
{
  int ret = OB_SUCCESS;
  ObLockIDArray retry_lock_ids;
  ObRetryCtx retry_ctx;
  FOREACH_X(data, ls_lock_map, OB_SUCC(ret) || can_retry) {
    proxy_batch.reuse();
    retry_ctx.reuse();
    const share::ObLSID &ls_id = data->first;
    const ObLockIDArray &lock_ids = data->second;

    if (OB_FAIL(send_one_rpc_task_(proxy_batch,
                                   ctx,
                                   ls_id,
                                   lock_ids,
                                   lock_mode,
                                   lock_owner,
                                   retry_ctx))) {
      can_retry = false;
      (void)collect_rollback_info_(retry_ctx.rpc_ls_array_, proxy_batch, ctx);
      LOG_WARN("send rpc task failed", KR(ret));
    } else {
      ret = handle_parallel_rpc_response_(proxy_batch,
                                          ctx,
                                          ls_lock_map,
                                          can_retry,
                                          retry_ctx);
    }
    // collect one rpc's retry tablets.
    if (can_retry) {
      if (OB_FAIL(get_retry_lock_ids_(retry_ctx.retry_lock_ids_,
                                      0,
                                      retry_lock_ids))) {
        can_retry = false;
        LOG_WARN("get retry tablet list failed", K(ret));
      }
    }
  }
  // get the retry map
  if (can_retry && retry_lock_ids.count() != 0) {
    if (OB_FAIL(fill_ls_lock_map_(ctx,
                                  retry_lock_ids,
                                  retry_ls_lock_map,
                                  true /* force refresh location */))) {
      LOG_WARN("refill ls lock map failed", KP(ret), K(ctx));
      can_retry = false;
    }
  }
  return ret;
}

template<class RpcProxy>
int ObTableLockService::parallel_rpc_handle_(RpcProxy &proxy_batch,
                                             ObTableLockCtx &ctx,
                                             const LockMap &lock_map,
                                             const ObLSLockMap &ls_lock_map,
                                             const ObTableLockMode lock_mode,
                                             const ObTableLockOwnerID lock_owner)
{
  int ret = OB_SUCCESS;
  int64_t timeout_us = 0;
  bool unused = false;
  ObRetryCtx retry_ctx;
  ObAddr addr;
  ObTableLockTaskRequest request;
  FOREACH_X(lock, lock_map, OB_SUCC(ret)) {
    proxy_batch.reuse();
    retry_ctx.reuse();
    addr.reset();
    request.reset();
    const ObLockID &lock_id = lock->first;
    const share::ObLSID &ls_id = lock->second;

    if (OB_FAIL(retry_ctx.rpc_ls_array_.push_back(ls_id))) {
      LOG_WARN("push_back lsid failed", K(ret), K(ls_id));
    } else if (OB_FAIL(pack_request_(ctx, ctx.task_type_, lock_mode, lock_owner,
                                     lock_id, ls_id, addr, request))) {
      LOG_WARN("pack_request_ failed", K(ret), K(ls_id), K(lock_id));
    } else if (FALSE_IT(timeout_us = ctx.get_rpc_timeoutus())) {
    } else if (ctx.is_timeout()) {
      ret = OB_TIMEOUT;
      LOG_WARN("process obj lock timeout", K(ret), K(ctx));
    } else if (OB_FAIL(rpc_call_(proxy_batch,
                                 addr,
                                 timeout_us,
                                 request))) {
      LOG_WARN("failed to all async rpc", KR(ret), K(addr), K(timeout_us), K(request));
    } else {
      retry_ctx.send_rpc_count_++;
      ALLOW_NEXT_LOG();
      LOG_INFO("send table lock rpc", KR(ret), K(retry_ctx.send_rpc_count_),
               K(addr), "request", request);
    }
    if (OB_FAIL(ret)) {
      (void)collect_rollback_info_(retry_ctx.rpc_ls_array_, proxy_batch, ctx);
    } else {
      ret = handle_parallel_rpc_response_(proxy_batch, ctx, ls_lock_map, unused, retry_ctx);
    }
  }
  return ret;
}

int ObTableLockService::inner_process_obj_lock_batch_(ObTableLockCtx &ctx,
                                                      const ObLSLockMap &lock_map,
                                                      const ObTableLockMode lock_mode,
                                                      const ObTableLockOwnerID lock_owner)
{
  int ret = OB_SUCCESS;
  if (ctx.is_unlock_task()) {
    ObHighPriorityBatchLockProxy proxy_batch(*GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::batch_unlock_obj);
    ret = batch_rpc_handle_(proxy_batch, ctx, lock_map, lock_mode, lock_owner);
  } else {
    ObBatchLockProxy proxy_batch(*GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::batch_lock_obj);
    ret = batch_rpc_handle_(proxy_batch, ctx, lock_map, lock_mode, lock_owner);
  }

  return ret;
}

int ObTableLockService::inner_process_obj_lock_old_version_(ObTableLockCtx &ctx,
                                                            const LockMap &lock_map,
                                                            const ObLSLockMap &ls_lock_map,
                                                            const ObTableLockMode lock_mode,
                                                            const ObTableLockOwnerID lock_owner)
{
  int ret = OB_SUCCESS;
  // TODO: yanyuan.cxf we process the rpc one by one and do parallel later.
  if (ctx.is_unlock_task()) {
    ObHighPriorityTableLockProxy proxy_batch(*GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::unlock_table);
    ret = parallel_rpc_handle_(proxy_batch, ctx, lock_map, ls_lock_map, lock_mode, lock_owner);
  } else {
    ObTableLockProxy proxy_batch(*GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::lock_table);
    ret = parallel_rpc_handle_(proxy_batch, ctx, lock_map, ls_lock_map, lock_mode, lock_owner);
  }

  return ret;
}

int ObTableLockService::inner_process_obj_lock_(ObTableLockCtx &ctx,
                                                const LockMap &lock_map,
                                                const ObLSLockMap &ls_lock_map,
                                                const ObTableLockMode lock_mode,
                                                const ObTableLockOwnerID lock_owner)
{
  int ret = OB_SUCCESS;
  if (GET_MIN_CLUSTER_VERSION() > CLUSTER_VERSION_4_0_0_0) {
    ret = inner_process_obj_lock_batch_(ctx, ls_lock_map, lock_mode, lock_owner);
  } else {
    ret = inner_process_obj_lock_old_version_(ctx, lock_map, ls_lock_map, lock_mode, lock_owner);
  }

  return ret;
}

int ObTableLockService::process_obj_lock_(ObTableLockCtx &ctx,
                                          const ObLSID &ls_id,
                                          const ObLockID &lock_id,
                                          const ObTableLockMode lock_mode,
                                          const ObTableLockOwnerID lock_owner)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_retry = false;
  LockMap lock_map;
  ObLSLockMap ls_lock_map;
  ObLockIDArray lock_array;
  ObLockIDArray *p = nullptr;
  if (OB_FAIL(lock_map.create(1, lib::ObLabel("TableLockMap")))) {
    LOG_WARN("lock_map create failed");
  } else if (OB_FAIL(lock_map.set_refactored(lock_id, ls_id))) {
    LOG_WARN("fail to set lock_map", K(ret), K(lock_id), K(ls_id));
  } else if (OB_FAIL(ls_lock_map.create(10, lib::ObLabel("LSLockMap")))) {
    LOG_WARN("ls_lock_map create failed");
  } else if (OB_FAIL(ls_lock_map.set_refactored(ls_id, lock_array)) &&
             OB_ENTRY_EXIST != ret && OB_HASH_EXIST != ret) {
    LOG_WARN("fail to set tablet_map", K(ret), K(ls_id));
  } else if (OB_ISNULL(p = const_cast<ObLockIDArray *>(ls_lock_map.get(ls_id)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the ls not exist at tablet map", K(ret), K(ls_id));
  } else if (OB_FAIL(p->push_back(lock_id))) {
    LOG_WARN("push_back tablet_id failed", K(ret), K(ls_id), K(lock_id));
  } else {
    do {
      need_retry = false;

      if (ctx.is_timeout()) {
        ret = OB_TIMEOUT;
        LOG_WARN("lock table timeout", K(ret), K(ctx));
      } else if (OB_FAIL(start_sub_tx_(ctx))) {
        LOG_WARN("failed to start sub tx", K(ret), K(ctx));
      } else {
        if (OB_FAIL(inner_process_obj_lock_(ctx,
                                            lock_map,
                                            ls_lock_map,
                                            lock_mode,
                                            lock_owner))) {
          LOG_WARN("process object lock failed", K(ret), K(lock_id), K(ls_id));
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(end_sub_tx_(ctx, false/*not rollback*/))) {
            LOG_WARN("failed to end sub tx", K(ret), K(ctx));
          }
        } else {
          // rollback the sub tx.
          need_retry = need_retry_single_task_(ctx, ret);
          // overwrite the ret code.
          if (need_retry &&
              OB_FAIL(end_sub_tx_(ctx, true/*rollback*/))) {
            LOG_WARN("failed to rollback sub tx", K(ret), K(ctx));
          }
        }
      }
    } while (need_retry && OB_SUCC(ret));
  }
  LOG_DEBUG("ObTableLockService::process_obj_lock_", K(ret), K(ctx), K(ls_id),
            K(lock_id), K(ctx.task_type_), K(lock_mode), K(lock_owner));

  return ret;
}

int ObTableLockService::process_obj_lock_(ObTableLockCtx &ctx,
                                          const ObLSID &ls_id,
                                          const common::ObIArray<ObLockID> &lock_ids,
                                          const ObTableLockMode lock_mode,
                                          const ObTableLockOwnerID lock_owner)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_retry = false;
  ObLSLockMap ls_lock_map;
  ObLockIDArray lock_array;
  ObLockIDArray *p = nullptr;
  if (OB_FAIL(ls_lock_map.create(10, lib::ObLabel("LSLockMap")))) {
    LOG_WARN("ls_lock_map create failed");
  } else if (OB_FAIL(ls_lock_map.set_refactored(ls_id, lock_array)) &&
             OB_ENTRY_EXIST != ret && OB_HASH_EXIST != ret) {
    LOG_WARN("fail to set tablet_map", K(ret), K(ls_id));
  } else if (OB_ISNULL(p = const_cast<ObLockIDArray *>(ls_lock_map.get(ls_id)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the ls not exist at tablet map", K(ret), K(ls_id));
  } else if (OB_FAIL(p->assign(lock_ids))) {
    LOG_WARN("push_back tablet_id failed", K(ret), K(ls_id), K(lock_ids));
  } else {
    do {
      need_retry = false;

      if (ctx.is_timeout()) {
        ret = OB_TIMEOUT;
        LOG_WARN("lock table timeout", K(ret), K(ctx));
      } else if (OB_FAIL(start_sub_tx_(ctx))) {
        LOG_WARN("failed to start sub tx", K(ret), K(ctx));
      } else {
        if (OB_FAIL(inner_process_obj_lock_batch_(ctx, ls_lock_map, lock_mode, lock_owner))) {
          LOG_WARN("process object locks failed", K(ret), K(lock_ids), K(ls_id));
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(end_sub_tx_(ctx, false/*not rollback*/))) {
            LOG_WARN("failed to end sub tx", K(ret), K(ctx));
          }
        } else {
          // rollback the sub tx.
          need_retry = need_retry_single_task_(ctx, ret);
          // overwrite the ret code.
          if (need_retry &&
              OB_FAIL(end_sub_tx_(ctx, true/*rollback*/))) {
            LOG_WARN("failed to rollback sub tx", K(ret), K(ctx));
          }
        }
      }
    } while (need_retry && OB_SUCC(ret));
  }
  LOG_DEBUG("ObTableLockService::process_obj_lock_", K(ret), K(ctx), K(ls_id),
            K(lock_ids), K(ctx.task_type_), K(lock_mode), K(lock_owner));

  return ret;
}

int ObTableLockService::process_table_lock_(ObTableLockCtx &ctx,
                                            const ObTableLockMode lock_mode,
                                            const ObTableLockOwnerID lock_owner)
{
  int ret = OB_SUCCESS;
  ObLockID lock_id;
  ObTableLockMode table_lock_mode = lock_mode;
  if (OB_FAIL(get_lock_id(ctx.table_id_,
                          lock_id))) {
    LOG_WARN("get lock id failed", K(ret), K(ctx));
  } else if (is_part_table_lock_(ctx.task_type_) &&
             OB_FAIL(get_table_lock_mode_(ctx.task_type_,
                                          lock_mode,
                                          table_lock_mode))) {
    LOG_WARN("get table lock mode failed", K(ret), K(ctx), K(ctx.task_type_), K(lock_mode));
  } else if (OB_FAIL(process_obj_lock_(ctx,
                                       LOCK_SERVICE_LS,
                                       lock_id,
                                       table_lock_mode,
                                       lock_owner))) {
    LOG_WARN("lock obj failed", K(ret), K(ctx), K(LOCK_SERVICE_LS), K(lock_id), K(ctx.task_type_),
             K(lock_mode));
  }

  LOG_DEBUG("ObTableLockService::process_table_lock_", K(ret), K(ctx), K(ctx.task_type_), K(lock_mode), K(lock_owner));
  return ret;
}

int ObTableLockService::process_table_tablet_lock_(ObTableLockCtx &ctx,
                                                   const ObTableLockMode lock_mode,
                                                   const ObTableLockOwnerID lock_owner,
                                                   const LockMap &lock_map,
                                                   const ObLSLockMap &ls_lock_map)
{
  int ret = OB_SUCCESS;
  bool need_retry = false;
  do {
    need_retry = false;

    if (ctx.is_timeout()) {
      ret = OB_TIMEOUT;
      LOG_WARN("lock table timeout", K(ret), K(ctx));
    } else if (OB_FAIL(start_sub_tx_(ctx))) {
      LOG_WARN("failed to start sub tx", K(ret), K(ctx));
    } else if (OB_FAIL(inner_process_obj_lock_(ctx,
                                               lock_map,
                                               ls_lock_map,
                                               lock_mode,
                                               lock_owner))) {
      LOG_WARN("fail to lock tablets", K(ret));
      // rollback the sub tx.
      need_retry = need_retry_single_task_(ctx, ret);
      // overwrite the ret code.
      if (need_retry &&
          OB_FAIL(end_sub_tx_(ctx, true/*rollback*/))) {
        LOG_WARN("failed to rollback sub tx", K(ret), K(ctx));
      }
    } else if (OB_FAIL(end_sub_tx_(ctx, false/*not rollback*/))) {
      LOG_WARN("failed to end sub tx", K(ret), K(ctx));
    }
  } while (need_retry && OB_SUCC(ret));
  LOG_DEBUG("ObTableLockService::process_table_tablet_lock_", K(ret), K(ctx),
            K(ctx.task_type_), K(lock_mode), K(lock_owner));

  return ret;
}

int ObTableLockService::check_op_allowed_(const uint64_t table_id,
                                          const ObSimpleTableSchemaV2 *table_schema,
                                          bool &is_allowed)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();

  is_allowed = true;

  if (!table_schema->is_user_table()
      && !table_schema->is_tmp_table()
      && !ObInnerTableLockUtil::in_inner_table_lock_white_list(table_id)
      && !table_schema->is_external_table()) {
    // all the tmp table is a normal table now, deal it as a normal user table
    // table lock not support virtual table/sys table(not in white list) etc.
    is_allowed = false;
  } else if (GCTX.is_standby_cluster() && OB_SYS_TENANT_ID != tenant_id) {
    is_allowed = false;
  } else if (!GCTX.is_standby_cluster()) {
    bool is_restore = false;
    ObMultiVersionSchemaService *schema_service = MTL(ObTenantSchemaService*)->get_schema_service();
    if (OB_FAIL(schema_service->check_tenant_is_restore(NULL,
                                                        tenant_id,
                                                        is_restore))) {
      LOG_WARN("failed to check tenant restore", K(ret), K(table_id));
    } else if (is_restore) {
      is_allowed = false;
    }
  }

  return ret;
}

int ObTableLockService::get_lock_id_ls_(
    const ObTableLockCtx &ctx,
    const ObLockID &lock_id,
    ObLSID &ls_id,
    bool force_refresh)
{
  int ret = OB_SUCCESS;
  if (lock_id.is_tablet_lock()) {
    ObTabletID tablet_id;
    if (OB_FAIL(lock_id.convert_to(tablet_id))) {
      LOG_WARN("convert tablet id failed", K(ret), K(lock_id));
    } else if (OB_FAIL(get_tablet_ls_(ctx, tablet_id, ls_id, force_refresh))) {
      LOG_WARN("get tablet ls failed", K(ret), K(tablet_id), K(force_refresh));
    }
  } else {
    ls_id = LOCK_SERVICE_LS;
  }
  return ret;
}

int ObTableLockService::get_tablet_ls_(
    const ObTableLockCtx &ctx,
    const ObTabletID &tablet_id,
    ObLSID &ls_id,
    bool force_refresh)
{
  int ret = OB_SUCCESS;
  bool unused_cache_hit;
  const uint64_t tenant_id = MTL_ID();
  if (LOCK_ALONE_TABLET == ctx.task_type_ || UNLOCK_ALONE_TABLET == ctx.task_type_) {
    // we have specified the ls, just do lock and unlock at the specified ls
    ls_id = ctx.ls_id_;
  } else if (force_refresh) {
    if (OB_FAIL(location_service_->get(tenant_id,
                                       tablet_id,
                                       INT64_MAX,
                                       unused_cache_hit,
                                       ls_id))) {
      LOG_WARN("failed to sync get ls by tablet failed.",
               K(ret), K(tenant_id), K(tablet_id));
    }
  } else if (OB_FAIL(location_service_->nonblock_get(tenant_id,
                                                     tablet_id,
                                                     ls_id))) {
    if (OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST == ret &&
        OB_FAIL(location_service_->get(tenant_id,
                                       tablet_id,
                                       INT64_MAX,
                                       unused_cache_hit,
                                       ls_id))) {
      LOG_WARN("failed to sync get ls by tablet failed.",
               K(ret), K(tenant_id), K(tablet_id));
    } else if (OB_FAIL(ret)) {
      LOG_WARN("failed to get ls by tablet", K(ret), K(tenant_id),
               K(tablet_id));
    }
  }
  LOG_DEBUG("get tablet ls", K(ret), K(tenant_id), K(tablet_id), K(ls_id));

  return ret;
}

int ObTableLockService::get_process_tablets_(const ObTableLockMode lock_mode,
                                             const ObSimpleTableSchemaV2 *table_schema,
                                             ObTableLockCtx &ctx)
{
  int ret = OB_SUCCESS;

  if (ctx.is_tablet_lock_task()) {
    // case 1: lock/unlock tablet
    // just push the specified tablet into the tablet list.
    // if (OB_FAIL(ctx.tablet_list_.push_back(ctx.tablet_id_))) {
    //   LOG_WARN("failed to push back tablet id", K(ret));
    // }
  } else {
    ctx.tablet_list_.reuse();
    if (LOCK_PARTITION == ctx.task_type_ || UNLOCK_PARTITION == ctx.task_type_) {
      // case 2: lock/unlock partition
      // get all the tablet of this partition.
      ObObjectID part_id(ctx.partition_id_);
      if (OB_FAIL(table_schema->get_tablet_ids_by_part_object_id(part_id,
                                                                 ctx.tablet_list_))) {
        LOG_WARN("failed to get tablet ids", K(ret), K(part_id));
      }
    } else if (LOCK_SUBPARTITION == ctx.task_type_ || UNLOCK_SUBPARTITION == ctx.task_type_) {
      // case 3: lock/unlock subpartition
      // get the tablet of subpartition
      ObObjectID part_id(ctx.partition_id_);
      ObTabletID tablet_id;
      if (OB_FAIL(table_schema->get_tablet_id_by_object_id(part_id,
                                                           tablet_id))) {
        LOG_WARN("failed to get tablet id", K(ret), K(part_id));
      } else if (OB_FAIL(ctx.tablet_list_.push_back(tablet_id))) {
        LOG_WARN("failed to push back tablet id", K(ret));
      }
    } else if ((LOCK_TABLE == ctx.task_type_ || UNLOCK_TABLE == ctx.task_type_) &&
               (SHARE == lock_mode || SHARE_ROW_EXCLUSIVE == lock_mode || EXCLUSIVE == lock_mode)) {
      // case 4: lock/unlock table
      // get all the tablet of this table.
      if (OB_FAIL(table_schema->get_tablet_ids(ctx.tablet_list_))) {
        LOG_WARN("failed to get tablet ids", K(ret));
      }
    } else {
      // do nothing
    }
  }
  LOG_DEBUG("ObTableLockService::get_process_tablets_", K(ret), K(ctx.task_type_), K(ctx));

  return ret;
}

int ObTableLockService::fill_ls_lock_map_(ObTableLockCtx &ctx,
                                          const ObLockIDArray &lock_ids,
                                          ObLSLockMap &ls_lock_map,
                                          bool force_refresh_location)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id;
  ObLockIDArray lock_array;
  ObLockIDArray *p = nullptr;
  if (OB_FAIL(ls_lock_map.reuse())) {
    LOG_WARN("fail to reuse ls_lock_map", KR(ret));
  } else {
    for (int64_t i = 0; i < lock_ids.count() && OB_SUCC(ret); ++i) {
      ls_id.reset();
      lock_array.reuse();
      p = nullptr;
      const ObLockID &lock_id = lock_ids.at(i);
      if (OB_FAIL(get_lock_id_ls_(ctx, lock_id, ls_id, force_refresh_location))) {
        LOG_WARN("failed to get lock ls", K(ret), K(lock_id));
      } else if (OB_FAIL(ls_lock_map.set_refactored(ls_id, lock_array)) &&
                 OB_ENTRY_EXIST != ret && OB_HASH_EXIST != ret) {
        LOG_WARN("fail to set ls_lock_map", K(ret), K(ls_id));
      } else if (OB_ISNULL(p = ls_lock_map.get(ls_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the ls not exist at ls lock map", K(ret), K(ls_id));
      } else if (OB_FAIL(p->push_back(lock_id))) {
        LOG_WARN("push_back lock_id failed", K(ret), K(ls_id), K(lock_id));
      }
      LOG_DEBUG("lock add to lock map", K(lock_id), K(i));
    }
  }

  return ret;
}

int ObTableLockService::fill_ls_lock_map_(ObTableLockCtx &ctx,
                                         const common::ObTabletIDArray &tablets,
                                         LockMap &lock_map,
                                         ObLSLockMap &ls_lock_map)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id;
  ObLockIDArray lock_array;
  ObLockIDArray *p = nullptr;
  ObLockID lock_id;
  if (OB_FAIL(lock_map.reuse())) {
    LOG_WARN("fail to reuse lock_map", KR(ret));
  } else if (OB_FAIL(ls_lock_map.reuse())) {
    LOG_WARN("fail to reuse ls_lock_map", KR(ret));
  } else {
    for (int64_t i = 0; i < tablets.count() && OB_SUCC(ret); ++i) {
      ls_id.reset();
      lock_array.reuse();
      p = nullptr;
      lock_id.reset();
      const ObTabletID &tablet_id = tablets.at(i);
      if (OB_FAIL(get_tablet_ls_(ctx, tablet_id, ls_id))) {
        LOG_WARN("failed to get tablet ls", K(ret), K(tablet_id));
      } else if (OB_FAIL(get_lock_id(tablet_id,
                                     lock_id))) {
        LOG_WARN("get lock id failed", K(ret), K(ctx));
      } else if (OB_FAIL(lock_map.set_refactored(lock_id, ls_id))) {
        LOG_WARN("fail to set lock_map", K(ret), K(lock_id),
                 K(ls_id), K(tablet_id));
      } else if (OB_FAIL(ls_lock_map.set_refactored(ls_id, lock_array)) &&
                 OB_ENTRY_EXIST != ret && OB_HASH_EXIST != ret) {
        LOG_WARN("fail to set ls_lock_map", K(ret), K(ls_id));
      } else if (OB_ISNULL(p = ls_lock_map.get(ls_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the ls not exist at ls lock map", K(ret), K(ls_id));
      } else if (OB_FAIL(p->push_back(lock_id))) {
        LOG_WARN("push_back lock_id failed", K(ret), K(ls_id), K(lock_id));
      }
      LOG_DEBUG("tablet add to lock map", K(lock_id), K(tablet_id), K(i));
    }
  }

  return ret;
}

int ObTableLockService::get_ls_lock_map_(ObTableLockCtx &ctx,
                                         const common::ObTabletIDArray &tablets,
                                         LockMap &lock_map,
                                         ObLSLockMap &ls_lock_map)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(lock_map.create(tablets.count() > 0 ? tablets.count() : 1,
                              lib::ObLabel("TableLockMap")))) {
    LOG_WARN("fail to create lock_map", K(ret), K(tablets.count()));
  } else if (OB_FAIL(ls_lock_map.create(10, lib::ObLabel("LSLockMap")))) {
    LOG_WARN("ls_lock_map create failed", KR(ret));
  } else if (OB_FAIL(fill_ls_lock_map_(ctx, tablets, lock_map, ls_lock_map))) {
    LOG_WARN("fill ls lock map failed", KR(ret));
  }

  return ret;
}

int ObTableLockService::check_cluster_version_after_(const uint64_t version)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = GET_MIN_CLUSTER_VERSION();
  if (compat_version < version) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cluster version check failed, not supported now", K(ret), K(compat_version), K(version));
  }
  return ret;
}

int ObTableLockService::check_data_version_after_(const uint64_t version)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), compat_version))) {
    LOG_WARN("fail to get data version", K(ret));
  } else if (compat_version < version) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version check failed, not supported now", K(ret), K(compat_version), K(version));
  }
  return ret;
}

bool ObTableLockService::need_retry_trans_(const ObTableLockCtx &ctx,
                                           const int64_t ret) const
{
  bool need_retry = false;
  if (ctx.is_in_trans_) {
  } else {
    // only anonymous can retry
    // retry condition 1
    need_retry = (ctx.tx_is_killed_ &&
                  !ctx.is_try_lock() &&
                  !ctx.is_timeout());
    // retry condition 2
    need_retry = need_retry || ((OB_NOT_MASTER == ret ||
                                 OB_LS_NOT_EXIST == ret ||
                                 OB_TABLET_NOT_EXIST == ret) && !ctx.is_timeout());
  }
  return need_retry;
}

bool ObTableLockService::need_retry_single_task_(const ObTableLockCtx &ctx,
                                                 const int64_t ret) const
{
  bool need_retry = false;
  if (ctx.is_in_trans_) {
    need_retry = (OB_NOT_MASTER == ret ||
                  OB_LS_NOT_EXIST == ret ||
                  OB_TABLET_NOT_EXIST == ret);
  } else {
    // TODO: yanyuan.cxf multi data source can not rollback, so we can not retry.
  }
  return need_retry;
}

bool ObTableLockService::need_retry_whole_rpc_task_(const int ret)
{
  return (OB_TENANT_NOT_IN_SERVER == ret);
}

bool ObTableLockService::need_retry_part_rpc_task_(const int ret,
                                                   const ObTableLockTaskResult *result) const
{
  bool need_retry = false;
  need_retry = (OB_LS_NOT_EXIST == ret ||
                OB_TABLET_NOT_EXIST == ret);
  need_retry = need_retry && result->can_retry();
  // retry if OB_LS_NOT_EXIST/OB_TABLET_NOT_EXIST and lock task result is can retry.
  return need_retry;
}

bool ObTableLockService::need_renew_location_(const int64_t ret) const
{
  return (OB_LS_LOCATION_NOT_EXIST == ret || OB_LS_LOCATION_LEADER_NOT_EXIST == ret);
}

int ObTableLockService::rewrite_return_code_(const int ret, const int ret_code_before_end_stmt_or_tx, const bool is_from_sql) const
{
  int rewrite_rcode = ret;
  if (is_from_sql) {
    if (is_lock_conflict_ret_code_(ret_code_before_end_stmt_or_tx) && is_timeout_ret_code_(ret)) {
      rewrite_rcode = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
    }
  } else if (is_can_retry_err_(ret)) {
    // rewrite to OB_EAGAIN, to make sure the ddl process will retry again.
    rewrite_rcode = OB_EAGAIN;
  }
  return rewrite_rcode;
}

bool ObTableLockService::is_lock_conflict_ret_code_(const int ret) const
{
  return (OB_TRY_LOCK_ROW_CONFLICT == ret || OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret);
}

bool ObTableLockService::is_timeout_ret_code_(const int ret) const
{
  return (OB_TIMEOUT == ret || OB_TRANS_TIMEOUT == ret ||
          OB_TRANS_STMT_TIMEOUT == ret || OB_GET_LOCATION_TIME_OUT == ret);
}

bool ObTableLockService::is_can_retry_err_(const int ret) const
{
  return (OB_TRANS_KILLED == ret || OB_OBJ_UNLOCK_CONFLICT == ret || OB_OBJ_LOCK_NOT_COMPLETED == ret
          || OB_TRY_LOCK_ROW_CONFLICT == ret || OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret || OB_NOT_MASTER == ret
          || OB_TIMEOUT == ret || OB_LS_LOCATION_LEADER_NOT_EXIST == ret || OB_TRANS_CTX_NOT_EXIST == ret);
}

int ObTableLockService::get_ls_leader_(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const int64_t abs_timeout_ts,
    ObAddr &addr)
{
  int ret = OB_SUCCESS;
  // priority to set timeout_ctx: ctx > worker > default_timeout
  ObTimeoutCtx ctx;
  if (OB_FAIL(ctx.set_abs_timeout(abs_timeout_ts))) {
    LOG_WARN("set abs timeout ts failed", KR(ret));
  } else if (OB_ISNULL(location_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("table lock service not inited", K(ret));
  } else if (OB_FAIL(location_service_->get_leader(cluster_id,
                                                   tenant_id,
                                                   ls_id,
                                                   true, /* force renew */
                                                   addr))) {
    LOG_WARN("failed to get ls leader with retry until timeout",
             K(ret), K(cluster_id), K(tenant_id), K(ls_id), K(addr));
  } else {
    LOG_DEBUG("get ls leader from location_service",
              K(ret), K(cluster_id), K(tenant_id), K(ls_id), K(addr));
  }
  LOG_DEBUG("ObTableLockService::process_obj_lock_", K(ret), K(tenant_id), K(ls_id), K(addr));

  return ret;
}

int ObTableLockService::start_tx_(ObTableLockCtx &ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTxParam &tx_param = ctx.tx_param_;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.timeout_us_ = common::max(0l, ctx.abs_timeout_ts_ - ObTimeUtility::current_time());
  tx_param.lock_timeout_us_ = -1; // use abs_timeout_ts as lock wait timeout
  tx_param.cluster_id_ = GCONF.cluster_id;
  // no session id here

  ObTransService *txs = MTL(ObTransService*);
  if (ctx.trans_state_.is_start_trans_executed()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("start_trans is executed", K(ret));
  } else if (OB_FAIL(txs->acquire_tx(ctx.tx_desc_))) {
    LOG_WARN("fail acquire txDesc", K(ret), K(tx_param));
  } else {
    if (OB_FAIL(txs->start_tx(*ctx.tx_desc_, tx_param))) {
      LOG_WARN("fail start trans", K(ret), K(tx_param));
    } else {
      ctx.trans_state_.set_start_trans_executed(true);
    }
    // start tx failed, release the txDesc I just created.
    if (OB_FAIL(ret)) {
      if (OB_TMP_FAIL(txs->release_tx(*ctx.tx_desc_))) {
        LOG_ERROR("release tx failed", K(tmp_ret), KPC(ctx.tx_desc_));
      }
    }
  }

  LOG_DEBUG("ObTableLockService::start_tx_", K(ret), K(ctx), K(tx_param));
  return ret;
}

int ObTableLockService::end_tx_(ObTableLockCtx &ctx, const bool is_rollback)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (!ctx.trans_state_.is_start_trans_executed()
      || !ctx.trans_state_.is_start_trans_success()) {
    LOG_INFO("end_trans skip", K(ret), K(ctx));
  } else {
    ObTransService *txs = MTL(ObTransService*);
    const int64_t stmt_timeout_ts = ctx.abs_timeout_ts_;
    if (is_rollback) {
      if (OB_FAIL(txs->rollback_tx(*ctx.tx_desc_))) {
        LOG_WARN("fail rollback tx when session terminate",
                 K(ret), KPC(ctx.tx_desc_), K(stmt_timeout_ts));
      }
    } else {
      ACTIVE_SESSION_FLAG_SETTER_GUARD(in_committing);
      if (OB_FAIL(txs->commit_tx(*ctx.tx_desc_, stmt_timeout_ts))) {
        LOG_WARN("fail end trans when session terminate",
                K(ret), KPC(ctx.tx_desc_), K(stmt_timeout_ts));
      }
    }
    if (OB_TMP_FAIL(txs->release_tx(*ctx.tx_desc_))) {
      LOG_ERROR("release tx failed", K(ret), K(tmp_ret), KPC(ctx.tx_desc_));
    }
    ctx.tx_desc_ = NULL;
    ctx.trans_state_.clear_start_trans_executed();
  }

  ctx.trans_state_.reset();
  LOG_DEBUG("ObTableLockService::end_tx_", K(ret), K(tmp_ret), K(ctx), K(is_rollback));

  return ret;
}

int ObTableLockService::start_sub_tx_(ObTableLockCtx &ctx)
{
  int ret = OB_SUCCESS;

  if (ctx.is_savepoint_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("start_sub_tx is executed", K(ret));
  } else {
    ObTransService *txs = MTL(ObTransService*);
    const ObTxParam &tx_param = ctx.tx_param_;
    const ObTxIsolationLevel &isolation_level = tx_param.isolation_;
    const int64_t expire_ts = ctx.abs_timeout_ts_;
    auto &savepoint = ctx.current_savepoint_;
    if (OB_FAIL(txs->create_implicit_savepoint(*ctx.tx_desc_,
                                               tx_param,
                                               savepoint))) {
      ctx.reset_savepoint();
      LOG_WARN("create implicit savepoint failed", K(ret), KPC(ctx.tx_desc_), K(tx_param));
    }
  }
  LOG_DEBUG("ObTableLockService::start_sub_tx_", K(ret), K(ctx));

  return ret;
}

int ObTableLockService::end_sub_tx_(ObTableLockCtx &ctx, const bool is_rollback)
{
  int ret = OB_SUCCESS;

  if (!ctx.is_savepoint_valid()) {
    LOG_INFO("end_sub_tx_ skip", K(ret), K(ctx));
  } else {
    const auto &savepoint = ctx.current_savepoint_;
    const int64_t expire_ts = OB_MAX(ctx.abs_timeout_ts_, DEFAULT_TIMEOUT_US + ObTimeUtility::current_time());
    ObTransService *txs = MTL(ObTransService*);
    if (is_rollback &&
        OB_FAIL(txs->rollback_to_implicit_savepoint(*ctx.tx_desc_,
                                                    savepoint,
                                                    expire_ts,
                                                    &ctx.need_rollback_ls_))) {
      LOG_WARN("fail to rollback sub tx", K(ret), K(ctx.tx_desc_),
               K(ctx.need_rollback_ls_));
    }

    ctx.clean_touched_ls();
    ctx.reset_savepoint();
  }
  LOG_DEBUG("ObTableLockService::end_sub_tx_", K(ret), K(ctx));

  return ret;
}

int ObTableLockService::start_stmt_(ObTableLockCtx &ctx)
{
  int ret = OB_SUCCESS;

  if (ctx.is_stmt_savepoint_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("start_stmt_ is executed", K(ret));
  } else {
    ObTransService *txs = MTL(ObTransService*);
    const ObTxParam &tx_param = ctx.tx_param_;
    const ObTxIsolationLevel &isolation_level = tx_param.isolation_;
    const int64_t expire_ts = ctx.abs_timeout_ts_;
    auto &savepoint = ctx.stmt_savepoint_;
    if (OB_FAIL(txs->create_implicit_savepoint(*ctx.tx_desc_,
                                               tx_param,
                                               savepoint))) {
      ctx.reset_stmt_savepoint();
      LOG_WARN("create implicit savepoint failed", K(ret), KPC(ctx.tx_desc_), K(tx_param));
    }
  }
  LOG_DEBUG("ObTableLockService::start_stmt_", K(ret), K(ctx));

  return ret;
}

int ObTableLockService::end_stmt_(ObTableLockCtx &ctx, const bool is_rollback)
{
  int ret = OB_SUCCESS;

  if (!ctx.is_stmt_savepoint_valid()) {
    LOG_INFO("end_stmt_ skip", K(ret), K(ctx));
  } else {
    const auto &savepoint = ctx.stmt_savepoint_;
    const int64_t expire_ts = OB_MAX(ctx.abs_timeout_ts_, DEFAULT_TIMEOUT_US + ObTimeUtility::current_time());
    ObTransService *txs = MTL(ObTransService*);
    // just rollback the whole stmt, if it is needed.
    if (is_rollback &&
        OB_FAIL(txs->rollback_to_implicit_savepoint(*ctx.tx_desc_,
                                                    savepoint,
                                                    expire_ts,
                                                    &ctx.need_rollback_ls_))) {
      LOG_WARN("fail to rollback stmt", K(ret), K(ctx.tx_desc_),
               K(ctx.need_rollback_ls_));
    }

    ctx.clean_touched_ls();
    ctx.reset_stmt_savepoint();
  }
  LOG_DEBUG("ObTableLockService::end_stmt_", K(ret), K(ctx));

  return ret;
}

} // tablelock
} // transaction
} // oceanbase
