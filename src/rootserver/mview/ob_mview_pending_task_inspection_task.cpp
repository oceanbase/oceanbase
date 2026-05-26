/**
 * Copyright (c) 2024 OceanBase
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

#include "rootserver/mview/ob_mview_pending_task_inspection_task.h"
#include "lib/container/ob_se_array.h"
#include "lib/ob_errno.h"
#include "lib/time/ob_time_utility.h"
#include "observer/ob_server_struct.h"
#include "rootserver/mview/ob_mview_pending_task_manager.h"
#include "rootserver/mview/ob_mview_utils.h"
#include "share/schema/ob_mview_info.h"
#include "storage/mview/ob_mview_refresh_helper.h"
#include "storage/mview/ob_mview_transaction.h"

namespace oceanbase
{
namespace rootserver
{
using namespace common;
using namespace share::schema;
using namespace storage;

ObMViewPendingTaskInspectionTask::ObMViewPendingTaskInspectionTask()
  : manager_(NULL),
    is_inited_(false),
    in_sched_(false),
    is_stop_(true),
    list_lock_(common::ObLatchIds::MVIEW_PENDING_TASK_INSPECTION_LOCK),
    recovery_list_(),
    retry_list_()
{
}

ObMViewPendingTaskInspectionTask::~ObMViewPendingTaskInspectionTask()
{
  destroy();
}

int ObMViewPendingTaskInspectionTask::init(ObMViewPendingTaskManager &manager, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("pending task inspection task init twice", KR(ret), KP(this));
  } else {
    const ObMemAttr attr(tenant_id, "MVPendTaskList");
    recovery_list_.set_attr(attr);
    retry_list_.set_attr(attr);
    manager_ = &manager;
    is_inited_ = true;
  }
  return ret;
}

int ObMViewPendingTaskInspectionTask::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task inspection task not init", KR(ret), KP(this));
  } else if (OB_ISNULL(manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("manager is null", KR(ret), KP(this));
  } else {
    ATOMIC_STORE(&is_stop_, false);
    // Register a repeating task so the timer service itself reschedules after
    // each tick. CAS guards against double-register on repeated leader-switch
    // start() calls.
    if (ATOMIC_BCAS(&in_sched_, false, true)) {
      if (OB_FAIL(schedule_task(INSPECTION_INTERVAL, true /*repeate*/))) {
        LOG_WARN("fail to schedule inspection task", KR(ret));
        ATOMIC_STORE(&in_sched_, false);
      }
    }
  }
  return ret;
}

int ObMViewPendingTaskInspectionTask::stop()
{
  ATOMIC_STORE(&is_stop_, true);
  cancel_task();
  ATOMIC_STORE(&in_sched_, false);
  return OB_SUCCESS;
}

int ObMViewPendingTaskInspectionTask::wait()
{
  int ret = OB_SUCCESS;
  wait_task();
  clear_lists();
  return ret;
}

void ObMViewPendingTaskInspectionTask::clear_lists()
{
  ObSpinLockGuard guard(list_lock_);
  recovery_list_.reuse();
  retry_list_.reuse();
}

int ObMViewPendingTaskInspectionTask::destroy()
{
  ATOMIC_STORE(&is_stop_, true);
  cancel_task();
  wait_task();
  ATOMIC_STORE(&in_sched_, false);
  {
    ObSpinLockGuard guard(list_lock_);
    recovery_list_.reset();
    retry_list_.reset();
  }
  manager_ = NULL;
  is_inited_ = false;
  return OB_SUCCESS;
}

int ObMViewPendingTaskInspectionTask::register_for_recovery(uint64_t tenant_id,
                                                             int64_t refresh_id,
                                                             uint64_t mview_id,
                                                             uint64_t target_data_sync_scn)
{
  int ret = OB_SUCCESS;
  RecoveryEntry entry;
  entry.tenant_id_            = tenant_id;
  entry.refresh_id_           = refresh_id;
  entry.mview_id_             = mview_id;
  entry.target_data_sync_scn_ = target_data_sync_scn;
  ObSpinLockGuard guard(list_lock_);
  if (OB_FAIL(recovery_list_.push_back(entry))) {
    LOG_WARN("fail to push recovery entry", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
  }
  return ret;
}

int ObMViewPendingTaskInspectionTask::register_for_retry(uint64_t tenant_id,
                                                          int64_t refresh_id,
                                                          uint64_t mview_id,
                                                          int64_t next_retry_ts)
{
  int ret = OB_SUCCESS;
  RetryEntry entry;
  entry.tenant_id_     = tenant_id;
  entry.refresh_id_    = refresh_id;
  entry.mview_id_      = mview_id;
  entry.next_retry_ts_ = next_retry_ts;
  ObSpinLockGuard guard(list_lock_);
  if (OB_FAIL(retry_list_.push_back(entry))) {
    LOG_WARN("fail to push retry entry", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
  }
  return ret;
}

void ObMViewPendingTaskInspectionTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task inspection task not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stop_))) {
  } else if (OB_ISNULL(manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("manager is null", KR(ret), KP(this));
  } else {
    if (OB_FAIL(process_recovery_list())) {
      LOG_WARN("process recovery list failed", KR(ret));
      ret = OB_SUCCESS;
    }
    if (OB_FAIL(process_retry_list())) {
      LOG_WARN("process retry list failed", KR(ret));
      ret = OB_SUCCESS;
    }
  }
}

int ObMViewPendingTaskInspectionTask::drain_recovery_list(ObIArray<RecoveryEntry> &snapshot)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(list_lock_);
  for (int64_t i = 0; OB_SUCC(ret) && i < recovery_list_.count(); ++i) {
    if (OB_FAIL(snapshot.push_back(recovery_list_.at(i)))) {
      LOG_WARN("fail to snapshot recovery entry", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    recovery_list_.reuse();
  }
  return ret;
}

int ObMViewPendingTaskInspectionTask::recover_single_entry(
    const RecoveryEntry &entry, ObIArray<RecoveryEntry> &requeue)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = entry.tenant_id_;
  share::schema::ObSchemaGetterGuard schema_guard;
  sql::ObFreeSessionCtx free_session_ctx;
  sql::ObSQLSessionInfo *session_info = NULL;
  ObMViewTransaction trans;
  ObMViewInfo mview_info;
  bool lock_succ = false;

  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObMViewUtils::create_inner_session(
                 tenant_id, schema_guard, free_session_ctx, session_info))) {
    LOG_WARN("fail to create inner session", KR(ret), K(tenant_id));
  } else if (OB_FAIL(trans.start(session_info, GCTX.sql_proxy_,
                                  session_info->get_database_id(),
                                  session_info->get_database_name()))) {
    LOG_WARN("fail to start trans", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObMViewRefreshHelper::lock_mview(
                 trans, tenant_id, entry.mview_id_, true /*try_lock*/))) {
    if (OB_TRY_LOCK_ROW_CONFLICT == ret || OB_EAGAIN == ret) {
      LOG_INFO("mview locked by executor during recovery, requeue", K(entry.mview_id_), KR(ret));
      ret = OB_SUCCESS;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(requeue.push_back(entry))) {
        LOG_WARN("fail to requeue recovery entry", KR(tmp_ret));
      }
    } else {
      LOG_WARN("fail to lock mview for recovery", KR(ret), K(entry.mview_id_));
    }
  } else {
    lock_succ = true;
  }

  if (OB_SUCC(ret) && lock_succ) {
    if (OB_FAIL(ObMViewInfo::fetch_mview_info(
                           *GCTX.sql_proxy_, tenant_id, entry.mview_id_, mview_info))) {
      LOG_WARN("fail to fetch mview info", KR(ret), K(entry.mview_id_));
    } else if (mview_info.get_data_sync_scn() >= entry.target_data_sync_scn_) {
      if (OB_FAIL(manager_->finalize_task(
                      entry.tenant_id_, entry.refresh_id_, entry.mview_id_, OB_SUCCESS))) {
        LOG_WARN("fail to finalize recovered task as success", KR(ret),
                 K(entry.refresh_id_), K(entry.mview_id_));
      } else {
        LOG_INFO("recovered RUNNING task as SUCCESS", K(entry.mview_id_),
                 "data_sync_scn", mview_info.get_data_sync_scn(),
                 "target", entry.target_data_sync_scn_);
      }
    } else if (OB_FAIL(manager_->mark_task_retry_wait(
                           entry.tenant_id_, entry.refresh_id_, entry.mview_id_))) {
      LOG_WARN("fail to mark recovered task retry_wait", KR(ret),
               K(entry.refresh_id_), K(entry.mview_id_));
    } else {
      LOG_INFO("recovered RUNNING task as RETRY_WAIT", K(entry.mview_id_),
               "data_sync_scn", mview_info.get_data_sync_scn(),
               "target", entry.target_data_sync_scn_);
    }
  }

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCCESS == ret))) {
      LOG_WARN("fail to end trans", KR(tmp_ret));
    }
  }
  ObMViewUtils::release_inner_session(free_session_ctx, session_info);

  return ret;
}

int ObMViewPendingTaskInspectionTask::requeue_recovery_entries(
    const ObIArray<RecoveryEntry> &requeue)
{
  int ret = OB_SUCCESS;
  if (requeue.count() > 0) {
    ObSpinLockGuard guard(list_lock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < requeue.count(); ++i) {
      if (OB_FAIL(recovery_list_.push_back(requeue.at(i)))) {
        LOG_WARN("fail to requeue recovery entry", KR(ret));
      }
    }
  }
  return ret;
}

int ObMViewPendingTaskInspectionTask::process_recovery_list()
{
  int ret = OB_SUCCESS;
  ObSEArray<RecoveryEntry, 16> snapshot;
  ObSEArray<RecoveryEntry, 16> requeue;

  if (OB_FAIL(drain_recovery_list(snapshot))) {
    LOG_WARN("fail to drain recovery list", KR(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; i < snapshot.count(); ++i) {
      const RecoveryEntry &entry = snapshot.at(i);
      if (OB_TMP_FAIL(recover_single_entry(entry, requeue))) {
        LOG_WARN("recover single RUNNING task failed, force mark task failed",
                 KR(tmp_ret), K(entry.refresh_id_), K(entry.mview_id_));
      }
      if (OB_FAIL(tmp_ret)) {
        int recover_ret = tmp_ret;
        if (OB_TMP_FAIL(manager_->finalize_task(entry.tenant_id_, entry.refresh_id_,
                                               entry.mview_id_, recover_ret))) {
          LOG_WARN("fail to finalize task as failed after recovery error, fallback to recycle and history",
            KR(tmp_ret), K(entry.refresh_id_), K(entry.mview_id_));
          // 连环失败时丢弃 entry，避免 recovery_list_ 被坏数据反复打爆；
          // 回收与历史归档并行执行，互不短路。
          if (OB_TMP_FAIL(manager_->recycle_refresh(entry.tenant_id_, entry.refresh_id_))) {
            LOG_WARN("fail to recycle refresh after finalize_task error",
              KR(tmp_ret), K(entry.tenant_id_), K(entry.refresh_id_), K(entry.mview_id_));
          }
          // TODO: 异常处理需要写入历史表，避免数据丢失
        }
      }
    }
    if (OB_FAIL(requeue_recovery_entries(requeue))) {
      LOG_WARN("fail to requeue recovery entries", KR(ret));
    }
  }

  return ret;
}

int ObMViewPendingTaskInspectionTask::drain_retry_list(ObIArray<RetryEntry> &snapshot)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(list_lock_);
  for (int64_t i = 0; OB_SUCC(ret) && i < retry_list_.count(); ++i) {
    if (OB_FAIL(snapshot.push_back(retry_list_.at(i)))) {
      LOG_WARN("fail to snapshot retry entry", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    retry_list_.reuse();
  }
  return ret;
}

int ObMViewPendingTaskInspectionTask::process_single_retry_entry(
    const RetryEntry &entry, int64_t now,
    ObIArray<RetryEntry> &remaining, bool &need_wakeup)
{
  int ret = OB_SUCCESS;
  if (entry.next_retry_ts_ > 0 && entry.next_retry_ts_ > now) {
    if (OB_FAIL(remaining.push_back(entry))) {
      LOG_WARN("fail to keep retry entry", KR(ret));
    }
  } else {
    if (OB_FAIL(manager_->mark_task_pending(
                    entry.tenant_id_, entry.refresh_id_, entry.mview_id_))) {
      LOG_WARN("fail to advance retry task to pending", KR(ret),
               K(entry.refresh_id_), K(entry.mview_id_));
    } else {
      need_wakeup = true;
      LOG_INFO("advanced RETRY_WAIT task to PENDING",
               K(entry.refresh_id_), K(entry.mview_id_));
    }
  }
  return ret;
}

int ObMViewPendingTaskInspectionTask::restore_retry_list(
    const ObIArray<RetryEntry> &remaining)
{
  int ret = OB_SUCCESS;
  if (remaining.count() > 0) {
    ObSpinLockGuard guard(list_lock_);
    if (OB_FAIL(append(retry_list_, remaining))) {
      LOG_WARN("fail to restore retry entry", KR(ret));
    }
  }
  return ret;
}

int ObMViewPendingTaskInspectionTask::process_retry_list()
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  ObSEArray<RetryEntry, 16> snapshot;
  ObSEArray<RetryEntry, 16> remaining;
  bool need_wakeup = false;

  if (OB_FAIL(drain_retry_list(snapshot))) {
    LOG_WARN("fail to drain retry list", KR(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; i < snapshot.count(); ++i) {
      if (OB_TMP_FAIL(process_single_retry_entry(snapshot.at(i), now, remaining, need_wakeup))) {
        LOG_WARN("fail to process single retry entry, skip", KR(tmp_ret),
                 K(snapshot.at(i).refresh_id_), K(snapshot.at(i).mview_id_));
      }
    }
    if (OB_FAIL(restore_retry_list(remaining))) {
      LOG_WARN("fail to restore retry list", KR(ret));
    }
  }

  if (need_wakeup) {
    manager_->wakeup();
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
