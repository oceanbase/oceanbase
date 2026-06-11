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

#include "rootserver/mview/ob_mview_pending_task_manager.h"
#include "rootserver/mview/ob_mview_utils.h"
#include "storage/mview/ob_mview_transaction.h"
#include "lib/worker.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/container/ob_bit_set.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "observer/ob_server_struct.h"
#include "rootserver/mview/ob_mview_pending_task_rpc_processor.h"
#include "share/location_cache/ob_location_service.h"
#include "share/ob_ddl_common.h"
#include "share/ob_rpc_struct.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "sql/resolver/mv/ob_mv_dep_utils.h"
#include "storage/mview/cmd/ob_mview_executor_util.h"
#include "storage/mview/ob_mview_refresh_helper.h"
#include "storage/mview/ob_mview_refresh_stats_utils.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/location_cache/ob_location_service.h"
#include "share/config/ob_server_config.h"
#include "share/ob_rpc_struct.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_all_server_tracer.h"
#include "share/ob_unit_table_operator.h"
#include "sql/engine/cmd/ob_kill_executor.h"

namespace oceanbase
{
namespace rootserver
{
using namespace common;
using namespace share;
using namespace sql;
using namespace storage;

void ObMViewPendingTaskManager::TaskResultEntry::set_err_msg(const char *msg)
{
  if (OB_NOT_NULL(msg)) {
    const int64_t len = MIN(static_cast<int64_t>(STRLEN(msg)), static_cast<int64_t>(sizeof(err_msg_buf_) - 1));
    if (len > 0) {
      MEMCPY(err_msg_buf_, msg, len);
    }
    err_msg_buf_[len] = '\0';
  } else {
    err_msg_buf_[0] = '\0';
  }
}

ObMViewPendingTaskManager::ObMViewPendingTaskManager()
  : queue_(),
    table_operator_(),
    scheduler_(),
    inspection_task_(),
    result_queue_(),
    result_alloc_(),
    reload_cond_(),
    reload_state_(RS_NOT_READY),
    mview_context_map_(),
    mview_ctx_alloc_(),
    is_inited_(false)
{
}

ObMViewPendingTaskManager::~ObMViewPendingTaskManager()
{
}

int ObMViewPendingTaskManager::init()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("pending task manager init twice", KR(ret));
  } else if (OB_FAIL(queue_.init(tenant_id))) {
    LOG_WARN("fail to init pending task queue", KR(ret), K(tenant_id));
  } else if (OB_FAIL(result_alloc_.init(lib::ObMallocAllocator::get_instance(),
                                        OB_MALLOC_NORMAL_BLOCK_SIZE,
                                        ObMemAttr(tenant_id, "MVTaskResult")))) {
    LOG_WARN("fail to init result allocator", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_operator_.init(GCTX.sql_proxy_))) {
    LOG_WARN("fail to init pending task table operator", KR(ret));
  } else if (OB_FAIL(scheduler_.init(*this))) {
    LOG_WARN("fail to init pending task scheduler", KR(ret));
  } else if (OB_FAIL(inspection_task_.init(*this, tenant_id))) {
    LOG_WARN("fail to init inspection task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(reload_cond_.init(ObWaitEventIds::THREAD_IDLING_COND_WAIT))) {
    LOG_WARN("fail to init reload cond", KR(ret));
  } else if (OB_FAIL(mview_context_map_.create(10000, "MVCtxMap", "MVCtxMap", tenant_id))) {
    LOG_WARN("fail to create mview context map", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mview_ctx_alloc_.init(lib::ObMallocAllocator::get_instance(),
                                           OB_MALLOC_NORMAL_BLOCK_SIZE,
                                           ObMemAttr(tenant_id, "MVCtx")))) {
    LOG_WARN("fail to init mview context allocator", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObMViewPendingTaskManager::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task manager not init", KR(ret));
  } else if (OB_FAIL(publish_reload_state(RS_RELOADING))) {
    LOG_WARN("fail to publish reload state", KR(ret));
  } else if (OB_FAIL(inspection_task_.start())) {
    LOG_WARN("fail to start inspection task", KR(ret));
  } else if (OB_FAIL(scheduler_.start())) {
    LOG_WARN("fail to start pending task scheduler", KR(ret));
  }
  return ret;
}

int ObMViewPendingTaskManager::drain_result_queue_and_clear_pending_queue()
{
  int ret = OB_SUCCESS;
  common::ObLink *link = NULL;
  bool drain_done = false;
  while (OB_SUCC(ret) && !drain_done) {
    int pop_ret = result_queue_.pop(link);
    if (OB_EAGAIN == pop_ret) {
      drain_done = true;
    } else if (OB_SUCCESS != pop_ret) {
      ret = pop_ret;
      LOG_WARN("fail to pop result queue during drain", KR(ret));
    } else if (OB_ISNULL(link)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("popped null link from result queue", KR(ret));
    } else {
      TaskResultEntry *entry = static_cast<TaskResultEntry *>(link);
      entry->~TaskResultEntry();
      result_alloc_.free(entry);
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(queue_.clear())) {
    LOG_WARN("fail to clear pending task queue", KR(ret));
  }
  if (OB_SUCC(ret) && OB_FAIL(reset_mview_contexts())) {
    // The queue (source of in-memory task state) was just cleared and the
    // leader-only block markers no longer apply; drop all contexts so a later
    // reload rebuilds them from scratch.
    LOG_WARN("fail to reset mview contexts", KR(ret));
  }
  return ret;
}

int ObMViewPendingTaskManager::publish_reload_state(const ReloadState new_state)
{
  int ret = OB_SUCCESS;
  ObThreadCondGuard guard(reload_cond_);
  ATOMIC_STORE(&reload_state_, static_cast<int64_t>(new_state));
  if (OB_FAIL(reload_cond_.broadcast())) {
    LOG_WARN("fail to broadcast reload cond", KR(ret), K(new_state));
  }
  return ret;
}

void ObMViewPendingTaskManager::stop()
{
  int tmp_ret = OB_SUCCESS;
  if (OB_LIKELY(is_inited_)) {
    // Flip state to NOT_READY *before* tearing down subcomponents: new
    // writers arriving during the shutdown window observe NOT_READY and
    // bail out with OB_NOT_MASTER rather than racing a half-stopped
    // scheduler. ATOMIC_STORE publishes the transition to fast-path readers;
    // broadcast wakes any slow-path waiter.
    if (OB_TMP_FAIL(publish_reload_state(RS_NOT_READY))) {
      LOG_WARN_RET(tmp_ret, "fail to publish reload state on stop");
    }
    inspection_task_.stop();
    scheduler_.stop();
  }
}

void ObMViewPendingTaskManager::on_reload_done()
{
  int tmp_ret = OB_SUCCESS;
  if (OB_LIKELY(is_inited_)) {
    if (OB_TMP_FAIL(publish_reload_state(RS_READY))) {
      LOG_WARN_RET(tmp_ret, "fail to publish reload state on reload done");
    }
  }
}

int ObMViewPendingTaskManager::wait_reload_ready(int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  // Fast path: state is already READY. Steady state (no leader switching in
  // progress) this is by far the common case, so we avoid taking the cond
  // mutex entirely. ATOMIC_LOAD pairs with writers' ATOMIC_STORE to publish
  // the transition. If the load races with a concurrent NOT_READY transition
  // we might see a stale READY for a brief window — that is acceptable: the
  // caller's subsequent SQL / queue operation will either succeed (state was
  // still effectively READY) or fail with a transient error (e.g. leader
  // already switched away). No persistent state inconsistency.
  if (static_cast<int64_t>(RS_READY) == ATOMIC_LOAD(&reload_state_)) {
    // nothing to wait for
  } else {
    // Slow path: RELOADING or NOT_READY. Take the cond mutex and use the
    // standard POSIX condition-wait loop (predicate under mutex, guard
    // against spurious wakeups), bounding total wait by an absolute deadline.
    // Mirrors ObThreadIdling::idle.
    ObThreadCondGuard guard(reload_cond_);
    const int64_t deadline = ObTimeUtility::current_time() + timeout_ms * 1000;
    while (OB_SUCC(ret) && static_cast<int64_t>(RS_RELOADING) == reload_state_) {
      const int64_t now = ObTimeUtility::current_time();
      if (now >= deadline) {
        ret = OB_TIMEOUT;
      } else {
        const int64_t wait_ms = (deadline - now + 999) / 1000;
        // ObThreadCond::wait may return non-SUCCESS on timeout; the outer
        // while re-evaluates state, so we intentionally drop the return value.
        (void)reload_cond_.wait(static_cast<int>(wait_ms));
      }
    }
    if (OB_SUCC(ret) && static_cast<int64_t>(RS_READY) != reload_state_) {
      ret = OB_NOT_MASTER;  // state == RS_NOT_READY
    }
  }
  return ret;
}

void ObMViewPendingTaskManager::wait()
{
  if (OB_LIKELY(is_inited_)) {
    scheduler_.wait();
    inspection_task_.wait();
  }
}

void ObMViewPendingTaskManager::destroy()
{
  int tmp_ret = OB_SUCCESS;
  if (OB_LIKELY(is_inited_)) {
    // Wake any straggler waiter before tearing down so they don't hang on a
    // condvar that's about to be destroyed.
    if (OB_TMP_FAIL(publish_reload_state(RS_NOT_READY))) {
      LOG_WARN_RET(tmp_ret, "fail to publish reload state on destroy");
    }
    inspection_task_.destroy();
    if (OB_TMP_FAIL(drain_result_queue_and_clear_pending_queue())) {
      LOG_WARN_RET(tmp_ret, "fail to drain result queue during destroy");
    }
    scheduler_.destroy();
    table_operator_.destroy();
    queue_.destroy();
    // Free any remaining context nodes before tearing down the map / allocator.
    if (OB_TMP_FAIL(reset_mview_contexts())) {
      LOG_WARN_RET(tmp_ret, "fail to reset mview contexts during destroy");
    }
    mview_context_map_.destroy();
    mview_ctx_alloc_.reset();
    result_alloc_.reset();
    reload_cond_.destroy();
    is_inited_ = false;
  }
}

void ObMViewPendingTaskManager::wakeup()
{
  if (OB_LIKELY(is_inited_)) {
    scheduler_.wakeup();
  }
}

int ObMViewPendingTaskManager::enqueue_reload_task(const ObIArray<ObMViewPendingTask *> &group,
                                                   bool &queue_full)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_valid = false;
  bool group_expired = false;
  int64_t inc_count = 0;
  bool need_end = false;
  int task_ret = OB_SUCCESS;
  ObString err_msg;
  uint64_t tenant_id = OB_INVALID_ID;
  int64_t refresh_id = OB_INVALID_ID;
  if (OB_UNLIKELY(group.empty()) || OB_ISNULL(group.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty reload group or null head task", KR(ret), K(group));
  } else if (OB_FALSE_IT(tenant_id = group.at(0)->tenant_id_)) {
  } else if (OB_FALSE_IT(refresh_id = group.at(0)->refresh_id_)) {
  } else if (OB_FALSE_IT(group_expired = (group.at(0)->expire_ts_ > 0
                                       && group.at(0)->expire_ts_ <= ObTimeUtility::current_time()))) {
  } else if (group_expired) {
    // Deadline already crossed before this leader could re-pick up the group.
    // Drop the persisted rows so subsequent reload scans don't keep replaying
    // a refresh whose original caller has long since timed out.
    LOG_WARN("reload group already expired, discard",
             K(group.count()),
             "tenant_id", group.at(0)->tenant_id_,
             "refresh_id", group.at(0)->refresh_id_,
             "expire_ts", group.at(0)->expire_ts_);
    need_end = true;
    task_ret = OB_TIMEOUT;
    err_msg = "reload group already expired, discard";
  } else if (OB_FAIL(check_reload_tasks_reachable(group, is_valid))) {
    LOG_WARN("validate reload group failed", KR(ret), K(group.count()));
  } else if (OB_UNLIKELY(!is_valid)) {
    LOG_WARN("this mview has unreachable tasks, maybe deleted",
      KR(ret), K(group.count()),
      "tenant_id", group.at(0)->tenant_id_,
      "refresh_id", group.at(0)->refresh_id_);
    need_end = true;
    task_ret = OB_ERR_UNEXPECTED;
    err_msg = "this mview has unreachable tasks, maybe deleted";
  } else if (OB_FAIL(check_pending_tasks_schedule_block(group,
                                                        group.at(0)->mview_id_,
                                                        false,
                                                        false,
                                                        inc_count))) {
    LOG_WARN("pending tasks blocked by concurrent drop/force", KR(ret),
             "tenant_id", group.at(0)->tenant_id_,
             "refresh_id", group.at(0)->refresh_id_);
    need_end = true;
    task_ret = ret;
    if (OB_EAGAIN == ret) {
      ret = OB_SUCCESS;
      err_msg = "pending tasks blocked by concurrent drop/force";
    }
  } else if (OB_FAIL(queue_.push_tasks(group))) {
    need_end = true;
    task_ret = ret;
    if (OB_ALLOCATE_MEMORY_FAILED == ret || OB_SIZE_OVERFLOW == ret) {
      queue_full = true;
      LOG_WARN("reload queue full, discard remaining tasks in this batch",
               KR(ret), K(group.count()),
               "tenant_id", group.at(0)->tenant_id_,
               "refresh_id", group.at(0)->refresh_id_);
      if (OB_ALLOCATE_MEMORY_FAILED == ret) {
        err_msg = "pending refresh failed with memory allocation error";
      } else if (OB_SIZE_OVERFLOW == ret) {
        err_msg = "pending refresh failed with global task queue limit exceeded";
      }
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to push tasks", KR(ret), K(group.count()));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < group.count(); ++i) {
      const ObMViewPendingTask *task = group.at(i);
      if (OB_ISNULL(task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null task", KR(ret), K(i));
      } else if (MV_TASK_RUNNING == task->status_) {
        if (OB_TMP_FAIL(inspection_task_.register_for_recovery(
                           task->tenant_id_, task->refresh_id_, task->mview_id_,
                           task->target_data_sync_scn_))) {
          LOG_WARN("fail to register for recovery", KR(tmp_ret), KPC(task));
        }
      } else if (MV_TASK_RETRY_WAIT == task->status_) {
        if (OB_TMP_FAIL(inspection_task_.register_for_retry(
                           task->tenant_id_, task->refresh_id_, task->mview_id_,
                           task->next_retry_ts_))) {
          LOG_WARN("fail to register for retry", KR(tmp_ret), KPC(task));
        }
      }
    }
  }
  if (need_end) {
    if (OB_TMP_FAIL(on_schedule_task_failed(tenant_id, refresh_id, true, task_ret, err_msg))) {
      LOG_WARN("fail to schedule task failed", KR(ret));
    }
  }
  if (OB_FAIL(ret) && inc_count > 0) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(dec_tasks_queue_cnt(group, inc_count))) {
      LOG_WARN("dec mview queue cnt failed", KR(tmp_ret), K(inc_count));
    }
  }
  return ret;
}

int ObMViewPendingTaskManager::reload_tasks()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  uint64_t last_scn = 0;
  int64_t last_rid = -1;
  int64_t total_loaded = 0;
  bool queue_full = false;
  bool has_more = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task manager not init", KR(ret));
  } else if (OB_FAIL(drain_result_queue_and_clear_pending_queue())) {
    LOG_WARN("fail to drain result queue and clear pending queue", KR(ret));
  } else if (OB_FAIL(table_operator_.delete_terminal_tasks(tenant_id))) {
    LOG_WARN("fail to delete terminal tasks", KR(ret), K(tenant_id));
  } else {
    // Tasks and their dep arrays land directly in this arena; queue_.push_tasks
    // deep-copies into the queue's own allocator, so we reuse the arena each batch.
    common::ObArenaAllocator batch_alloc("MVReloadBatch",
                                         OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
    common::ObSEArray<ObMViewPendingTask *, 64> batch;
    while (OB_SUCC(ret) && !queue_full && has_more) {
      batch_alloc.reuse();
      batch.reuse();
      if (OB_FAIL(table_operator_.load_tasks_batch(batch_alloc, batch,
                                                    last_scn, last_rid,
                                                    RELOAD_PENDING_BATCH_SIZE))) {
        LOG_WARN("fail to load tasks batch", KR(ret), K(last_scn), K(last_rid));
      } else if (batch.empty()) {
        has_more = false;
      } else if (OB_FAIL(process_reload_batch(batch, last_scn, last_rid,
                                               total_loaded, queue_full, has_more))) {
        LOG_WARN("fail to process reload batch", KR(ret), K(last_scn), K(last_rid));
      }
    }
  }
  LOG_INFO("reload tasks done", KR(ret), K(tenant_id), K(total_loaded), K(queue_full));
  return ret;
}

int ObMViewPendingTaskManager::process_reload_batch(const ObIArray<ObMViewPendingTask *> &batch,
                                                     uint64_t &last_scn,
                                                     int64_t &last_rid,
                                                     int64_t &total_loaded,
                                                     bool &queue_full,
                                                     bool &has_more)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObMViewPendingTask *, 4> current_group;
  int64_t group_refresh_id = -1;
  uint64_t group_scn = 0;
  int64_t loaded_refresh_cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && !queue_full && i < batch.count(); ++i) {
    ObMViewPendingTask *task = batch.at(i);
    if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null task in batch", KR(ret), K(i));
    } else {
      const bool key_changed = !current_group.empty()
          && (task->refresh_id_ != group_refresh_id
              || task->target_data_sync_scn_ != group_scn);
      if (key_changed) {
        if (OB_FAIL(enqueue_reload_task(current_group, queue_full))) {
          LOG_WARN("fail to enqueue reload group", KR(ret), K(group_refresh_id), K(group_scn));
        } else {
          current_group.reuse();
        }
      }
      if (OB_SUCC(ret) && !queue_full) {
        if (OB_FAIL(current_group.push_back(task))) {
          LOG_WARN("fail to push back task pointer", KR(ret));
        } else {
          group_refresh_id = task->refresh_id_;
          group_scn = task->target_data_sync_scn_;
          if (task->refresh_id_ != last_rid
              || task->target_data_sync_scn_ != last_scn) {
            last_scn = task->target_data_sync_scn_;
            last_rid = task->refresh_id_;
            ++loaded_refresh_cnt;
          }
          ++total_loaded;
        }
      }
    }
  }
  // A (refresh_id, scn) group never spans batches (load_tasks_batch's inner SQL
  // is DISTINCT (scn, refresh_id) LIMIT N, outer JOIN pulls all tasks for each),
  // so the trailing group is flushed here before the caller reuses batch memory.
  if (OB_SUCC(ret) && !queue_full && !current_group.empty()) {
    if (OB_FAIL(enqueue_reload_task(current_group, queue_full))) {
      LOG_WARN("fail to enqueue final reload group in batch", KR(ret),
               K(group_refresh_id), K(group_scn));
    }
  }
  if (OB_SUCC(ret)) {
    has_more = loaded_refresh_cnt >= RELOAD_PENDING_BATCH_SIZE;
  }
  return ret;
}

int ObMViewPendingTaskManager::check_reload_tasks_reachable(
    const ObIArray<ObMViewPendingTask *> &group,
    bool &is_valid) const
{
  int ret = OB_SUCCESS;
  is_valid = true;
  const int64_t n = group.count();
  typedef common::hash::ObHashMap<uint64_t, int64_t, common::hash::NoPthreadDefendMode> MViewIdxMap;
  MViewIdxMap mview_idx_map;
  common::ObBitSet<> reached;
  ObSEArray<int64_t, 32> pending_idxs;
  int64_t reached_cnt = 0;
  if (OB_UNLIKELY(n <= 0)) {
    is_valid = false;
    LOG_WARN("reload group is empty");
  } else if (OB_FAIL(mview_idx_map.create(n * 2 + 1,
                                          "MVReloadIdx",
                                          "MVReloadIdx",
                                          MTL_ID()))) {
    LOG_WARN("create reload group mview idx map failed", KR(ret), K(n));
  } else if (OB_FAIL(reached.prepare_allocate(n))) {
    LOG_WARN("prepare reached bitmap failed", KR(ret), K(n));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < n; ++i) {
      const ObMViewPendingTask *task = group.at(i);
      if (OB_ISNULL(task)) {
        is_valid = false;
        LOG_WARN("reload group has null task slot", K(i), K(n));
      } else if (task->dep_mview_id_cnt_ < 0
                 || (task->dep_mview_id_cnt_ > 0 && OB_ISNULL(task->dep_mview_ids_))) {
        is_valid = false;
        LOG_WARN("reload group dep array invalid", K(i), K(n), KPC(task));
      } else if (OB_FAIL(mview_idx_map.set_refactored(task->mview_id_, i, 1))) {
        LOG_WARN("set reload group mview idx failed", KR(ret), K(i), K(n), KPC(task));
      }
    }
    if (OB_SUCC(ret) && is_valid) {
      if (OB_FAIL(reached.add_member(n - 1))) {
        LOG_WARN("add root idx to reached bitmap failed", KR(ret), K(n));
      } else if (OB_FAIL(pending_idxs.push_back(n - 1))) {
        LOG_WARN("push root idx failed", KR(ret), K(n));
      } else {
        reached_cnt = 1;
      }
    }
    while (OB_SUCC(ret) && is_valid && pending_idxs.count() > 0) {
      const int64_t idx = pending_idxs.at(pending_idxs.count() - 1);
      pending_idxs.pop_back();
      const ObMViewPendingTask *task = group.at(idx);
      for (int64_t d = 0; OB_SUCC(ret) && is_valid && d < task->dep_mview_id_cnt_; ++d) {
        const uint64_t dep_id = task->dep_mview_ids_[d];
        int64_t dep_idx = -1;
        const int hash_ret = mview_idx_map.get_refactored(dep_id, dep_idx);
        if (OB_HASH_NOT_EXIST == hash_ret) {
          is_valid = false;
          LOG_WARN("reload group dep mview not in group", K(idx), K(dep_id), KPC(task));
        } else if (OB_FAIL(hash_ret)) {
          ret = hash_ret;
          LOG_WARN("get reload group dep idx failed", KR(ret), K(idx), K(dep_id), KPC(task));
        } else if (!reached.has_member(dep_idx)) {
          if (OB_FAIL(reached.add_member(dep_idx))) {
            LOG_WARN("add dep idx to reached bitmap failed", KR(ret), K(idx), K(dep_idx), K(dep_id));
          } else {
            ++reached_cnt;
            if (OB_FAIL(pending_idxs.push_back(dep_idx))) {
              LOG_WARN("push dep idx failed", KR(ret), K(idx), K(dep_idx), K(dep_id));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && is_valid && reached_cnt != n) {
      is_valid = false;
      LOG_WARN("reload group has unreachable tasks", K(reached_cnt), K(n));
    }
  }
  mview_idx_map.destroy();
  return ret;
}

int ObMViewPendingTaskManager::peek_task(ObMViewPendingTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task manager not init", KR(ret));
  } else if (OB_FAIL(queue_.peek_task(task))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("peek task failed", KR(ret));
    }
  }
  return ret;
}

int ObMViewPendingTaskManager::generate_refresh_id(int64_t &refresh_id)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObMViewExecutorUtil::generate_refresh_id(tenant_id, refresh_id))) {
    LOG_WARN("generate refresh id failed", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObMViewPendingTaskManager::build_pending_tasks(ObIAllocator &alloc,
                                                   const obrpc::ObScheduleMViewRefreshArg &arg,
                                                   const int64_t refresh_id,
                                                   const int64_t start_time,
                                                   const share::SCN &target_data_sync_scn,
                                                   ObIArray<ObMViewPendingTask*> &pending_tasks)
{
  int ret = OB_SUCCESS;
  pending_tasks.reuse();
  ObMViewPendingTask task;
  task.tenant_id_ = arg.tenant_id_;
  task.refresh_id_ = refresh_id;
  task.target_data_sync_scn_ = target_data_sync_scn.get_val_for_gts();
  task.refresh_method_ = arg.refresh_method_;
  task.refresh_parallel_ = arg.refresh_parallel_;
  task.status_ = MV_TASK_PENDING;
  task.gmt_create_ = start_time;
  task.gmt_modified_ = task.gmt_create_;
  task.expire_ts_ = arg.expire_ts_;
  ObSEArray<uint64_t, 4> topo_ordered_mview_ids;
  ObSEArray<ObSEArray<uint64_t, 4>, 4> topo_ordered_dep_mview_ids;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null for nested mview refresh", KR(ret));
  } else if (!arg.is_nested_ &&
             (OB_FAIL(topo_ordered_mview_ids.push_back(arg.mview_id_)) ||
              OB_FAIL(topo_ordered_dep_mview_ids.push_back(ObSEArray<uint64_t, 4>())))) {
    LOG_WARN("fail to push back target mview id", KR(ret), K(arg.mview_id_));
  } else if (arg.is_nested_ &&
             OB_FAIL(ObMVDepUtils::get_mview_ids_in_topo_refresh_order(*GCTX.sql_proxy_,
                                                                       arg.tenant_id_,
                                                                       arg.mview_id_,
                                                                       topo_ordered_mview_ids,
                                                                       topo_ordered_dep_mview_ids))) {
    LOG_WARN("fail to get nested mview ids in topo refresh order", KR(ret), K(arg.tenant_id_), K(arg.mview_id_));
  } else if (OB_UNLIKELY(topo_ordered_mview_ids.empty() || topo_ordered_mview_ids.count() != topo_ordered_dep_mview_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected array", KR(ret), K(topo_ordered_mview_ids), K(topo_ordered_dep_mview_ids));
  } else if (OB_FAIL(pending_tasks.prepare_allocate(topo_ordered_mview_ids.count()))) {
    LOG_WARN("prepare allocate pending tasks failed", KR(ret), K(topo_ordered_mview_ids));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < topo_ordered_mview_ids.count(); i++) {
      ObIArray<uint64_t> &dep_ids = topo_ordered_dep_mview_ids.at(i);
      task.mview_id_ = topo_ordered_mview_ids.at(i);
      task.seq_ = i;
      task.dep_mview_id_cnt_ = dep_ids.count();
      task.dep_mview_ids_ = dep_ids.count() > 0 ? &dep_ids.at(0) : NULL;
      task.flags_ = topo_ordered_mview_ids.count() == i + 1 ? ObMViewPendingTask::ROOT_TASK_FLAG : 0;
      if (arg.is_nested_) {
        task.flags_ |= ObMViewPendingTask::NESTED_REFRESH_FLAG;
      }
      if (OB_FAIL(ObMViewPendingTask::deep_copy(alloc, task, pending_tasks.at(i)))) {
        LOG_WARN("deep copy pending task failed", KR(ret), K(task));
      }
    }
  }
  return ret;
}

int ObMViewPendingTaskManager::write_run_start(const obrpc::ObScheduleMViewRefreshArg &arg,
                                               const int64_t refresh_id,
                                               const int64_t start_time,
                                               const share::SCN &target_data_sync_scn)
{
  int ret = OB_SUCCESS;
  ObMViewRefreshRunStatsParam run_params;
  run_params.tenant_id_       = arg.tenant_id_;
  run_params.run_user_id_     = arg.run_user_id_;
  run_params.mview_id_        = arg.mview_id_;
  run_params.refresh_id_      = refresh_id;
  run_params.start_time_      = start_time;
  run_params.data_target_scn_ = target_data_sync_scn.get_val_for_gts();
  run_params.parallelism_     = arg.refresh_parallel_;
  run_params.nested_          = arg.is_nested_;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(ObMViewExecutorUtil::to_refresh_method(arg.refresh_method_, run_params.method_))) {
    LOG_WARN("fail to convert refresh method to string", KR(ret), K(arg.refresh_method_));
  } else if (OB_FAIL(ObMViewRefreshStatsUtils::write_run_start(GCTX.sql_proxy_, run_params))) {
    LOG_WARN("failed to write_run_start", KR(ret), K(run_params));
  }
  return ret;
}

// Re-check the schedule block for *every* mview that will actually get a task,
// not just the root arg.mview_id_. For a nested refresh the dependency mviews
// resolved by build_pending_tasks are the ones that get inserted/pushed, so a
// concurrent drop/force on any of them must reject this schedule too -- otherwise
// a task for a being-dropped mview would slip past the kill enumeration. Only the
// root (root_mview_id) can be the force owner; every dependency is checked as a
// non-owner so another session's FORCE/DROP marker still blocks it.
int ObMViewPendingTaskManager::check_pending_tasks_schedule_block(
    const ObIArray<ObMViewPendingTask *> &pending_tasks,
    const uint64_t root_mview_id,
    const bool is_force_owner,
    const bool is_check_limit,
    int64_t &inc_count)
{
  int ret = OB_SUCCESS;
  inc_count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < pending_tasks.count(); ++i) {
    if (OB_ISNULL(pending_tasks.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null pending task", KR(ret), K(i));
    } else {
      const uint64_t cur_mview_id = pending_tasks.at(i)->mview_id_;
      const bool cur_is_force_owner = (cur_mview_id == root_mview_id) && is_force_owner;
      if (OB_FAIL(check_mview_schedule_block(cur_mview_id,
                                             cur_is_force_owner,
                                             is_check_limit,
                                             MAX_PENDING_REFRESH_CNT_PER_MVIEW))) {
        LOG_WARN("mview schedule blocked by concurrent drop/force", KR(ret),
                 K(cur_mview_id), K(root_mview_id), K(cur_is_force_owner));
      } else {
        ++inc_count;
      }
    }
  }
  return ret;
}

// All callbacks below run inside the hashmap bucket read lock (read_atomic).
// The read lock is shared, so concurrent callbacks may touch the same node;
// every field access therefore uses ATOMIC_* primitives. The node itself is
// only freed under the bucket write lock (erase_refactored), so it stays valid
// for the whole callback.
// Read block_flags_ atomically and decide whether the schedule is blocked.
struct CheckBlockFn
{
  CheckBlockFn(bool is_force_owner, bool check_limit,
               int64_t limit, int &ret)
    : is_force_owner_(is_force_owner), limit_(limit),
      check_limit_(check_limit), ret_(ret) {}
  void operator()(common::hash::HashMapPair<uint64_t, ObMViewContext *> &kv)
  {
    const int64_t flags = ATOMIC_LOAD(&kv.second->block_flags_);
    bool blocked = (0 != (flags & ObMViewContext::BLOCK_FLAG_DROP)) ||
                   (0 != (flags & ObMViewContext::BLOCK_FLAG_FORCE) && !is_force_owner_);
    if (OB_LIKELY(!blocked)) {
      bool done = false;
      while (!done) {
        const int64_t old_cnt = ATOMIC_LOAD(&kv.second->queue_refresh_cnt_);
        if (check_limit_ && old_cnt >= limit_) {
          ret_ = OB_SIZE_OVERFLOW;
          done = true;
        } else if (ATOMIC_BCAS(&kv.second->queue_refresh_cnt_, old_cnt, old_cnt + 1)) {
          done = true;
        }
        // else: CAS lost the race, retry with the fresh value.
      }
    } else {
      ret_ = OB_EAGAIN;
    }
  }
  bool is_force_owner_;
  int64_t limit_;
  bool check_limit_;
  int &ret_;
};

// OR a flag bit into block_flags_. For FORCE, the set must be atomic with a
// "no existing DROP/FORCE" check, otherwise two forced schedules could both
// observe an empty flag and both proceed. DROP just ORs in unconditionally.
struct AcquireBlockFn
{
  AcquireBlockFn(int64_t flag, int &ret) : flag_(flag), ret_(ret) {}
  void operator()(common::hash::HashMapPair<uint64_t, ObMViewContext *> &kv)
  {
    const bool need_conflict_check = (0 != (flag_ & ObMViewContext::BLOCK_FLAG_FORCE));
    bool done = false;
    while (!done) {
      const int64_t old_flags = ATOMIC_LOAD(&kv.second->block_flags_);
      if (need_conflict_check &&
          0 != (old_flags & (ObMViewContext::BLOCK_FLAG_DROP |
                             ObMViewContext::BLOCK_FLAG_FORCE))) {
        // Another DROP/FORCE marker already present: FORCE must stay exclusive.
        ret_ = OB_EAGAIN;
        done = true;
      } else {
        const int64_t new_flags = old_flags | flag_;
        if (old_flags == new_flags) {
          done = true; // already set, nothing to do
        } else if (ATOMIC_BCAS(&kv.second->block_flags_, old_flags, new_flags)) {
          done = true;
        }
        // else: CAS lost the race, retry with the fresh value.
      }
    }
  }
  int64_t flag_;
  int &ret_;
};

// Clear the specified flag bit(s) from block_flags_ (BCAS loop AND with ~flag).
struct ClearBlockFlagFn
{
  explicit ClearBlockFlagFn(int64_t flag) : flag_(flag) {}
  void operator()(common::hash::HashMapPair<uint64_t, ObMViewContext *> &kv)
  {
    bool done = false;
    while (!done) {
      const int64_t old_flags = ATOMIC_LOAD(&kv.second->block_flags_);
      const int64_t new_flags = old_flags & ~flag_;
      if (old_flags == new_flags) {
        done = true;
      } else if (ATOMIC_BCAS(&kv.second->block_flags_, old_flags, new_flags)) {
        done = true;
      }
    }
  }
  int64_t flag_;
};


struct DecQueueCntFn
{
  DecQueueCntFn() {}
  void operator()(common::hash::HashMapPair<uint64_t, ObMViewContext *> &kv)
  {
    bool done = false;
    while (!done) {
      const int64_t old_cnt = ATOMIC_LOAD(&kv.second->queue_refresh_cnt_);
      if (old_cnt <= 0) {
        done = true; // already at floor, nothing to release
      } else if (ATOMIC_BCAS(&kv.second->queue_refresh_cnt_, old_cnt, old_cnt - 1)) {
        done = true;
      }
      // else: CAS lost the race, retry with the fresh value.
    }
  }
};

// Insert a fresh ObMViewContext for mview_id. Only called on the miss path of
// acquire_mview_block (the node is absent), so it does not pre-check existence;
// set_refactored(flag=0) still guards the race where a concurrent inserter wins.
int ObMViewPendingTaskManager::ensure_mview_ctx(const uint64_t mview_id)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObMViewContext *ctx = nullptr;
  if (OB_ISNULL(buf = mview_ctx_alloc_.alloc(sizeof(ObMViewContext)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc mview context failed", KR(ret), K(mview_id));
  } else {
    ctx = new (buf) ObMViewContext();
    // flag=0 => do not overwrite an existing node; a loser gets OB_HASH_EXIST.
    if (OB_FAIL(mview_context_map_.set_refactored(mview_id, ctx, 0))) {
      if (OB_HASH_EXIST == ret) {
        // Lost the insert race: the first object stays in the map, free ours.
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("set mview context failed", KR(ret), K(mview_id));
      }
      ctx->~ObMViewContext();
      mview_ctx_alloc_.free(ctx);
      ctx = nullptr;
    }
  }
  return ret;
}

struct CollectAllFn
{
  explicit CollectAllFn(ObIArray<uint64_t> &ids) : mview_ids_(ids), ret_(OB_SUCCESS) {}
  int operator()(common::hash::HashMapPair<uint64_t, ObMViewContext *> &kv)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(mview_ids_.push_back(kv.first))) {
      RS_LOG(WARN, "push back mview id failed", KR(ret), "mview_id", kv.first);
      ret_ = ret;
    }
    return ret;
  }
  ObIArray<uint64_t> &mview_ids_;
  int ret_;
};

struct DecMViewQueueCntCb {
  explicit DecMViewQueueCntCb(MViewContextMap &mview_context_map)
    : mview_context_map_(mview_context_map) {}

  int operator()(const ObMViewPendingTask *task) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task is null", KR(ret));
    } else if (OB_FAIL(mview_context_map_.read_atomic(task->mview_id_, fn_))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("dec mview queue cnt failed", KR(ret), KPC(task));
      }
    }
    return ret;
  }
  MViewContextMap &mview_context_map_;
  DecQueueCntFn fn_;
};

int ObMViewPendingTaskManager::dec_tasks_queue_cnt(
    const common::ObIArray<ObMViewPendingTask *> &tasks,
    const int64_t dec_count)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t cnt = MIN(dec_count, tasks.count());
  DecMViewQueueCntCb dec_fn(mview_context_map_);
  for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
    if (OB_TMP_FAIL(dec_fn(tasks.at(i)))) {
      LOG_WARN("dec mview queue cnt failed", KR(tmp_ret), KPC(tasks.at(i)));
    }
  }
  return ret;
}

int ObMViewPendingTaskManager::check_mview_schedule_block(const uint64_t mview_id,
                                                          const bool is_force_owner,
                                                          const bool check_limit,
                                                          const int64_t limit)
{
  int ret = OB_SUCCESS;
  int cb_ret = OB_SUCCESS;
  CheckBlockFn fn(is_force_owner, check_limit, limit, cb_ret);
  if (OB_FAIL(mview_context_map_.read_atomic(mview_id, fn))) {
    if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(ensure_mview_ctx(mview_id))) {
        LOG_WARN("ensure mview context failed", KR(ret), K(mview_id));
      } else if (OB_FAIL(mview_context_map_.read_atomic(mview_id, fn))) {
        LOG_WARN("read mview context failed", KR(ret), K(mview_id));
      }
    } else {
      LOG_WARN("read mview context failed", KR(ret), K(mview_id));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_EAGAIN == cb_ret) {
      ret = OB_EAGAIN;
      LOG_WARN("mview schedule blocked by concurrent drop/force", KR(ret),
                K(mview_id), K(is_force_owner));
    } else if (OB_SIZE_OVERFLOW == cb_ret) {
      ret = OB_SIZE_OVERFLOW;
      LOG_USER_ERROR(OB_SIZE_OVERFLOW, "mview pending refresh count limit exceeded", K(mview_id));
    }
  }
  return ret;
}

// Acquire a block marker for mview_id. DROP just ORs its bit in unconditionally
// (it never conflicts). FORCE must be exclusive: AcquireBlockFn returns OB_EAGAIN
// when another DROP/FORCE marker is already present, so only one forced refresh
// can own the marker at a time. Rather than bouncing OB_EAGAIN back to the user,
// a contender *waits* here until the current owner releases the marker
// (release_mview_block) and then takes it.
//
// The wait loop lives in acquire_mview_block (not in AcquireBlockFn) on purpose:
// AcquireBlockFn runs under the hashmap bucket read lock, and the release path
// (ClearForceFn) needs the same bucket lock to clear the FORCE bit -- sleeping
// inside the callback would hold the bucket lock and could starve the very
// release we are waiting for. AcquireBlockFn returns OB_EAGAIN with no side
// effect, so re-invoking read_atomic each iteration is safe.
//
// The wait is bounded so a wedged owner cannot pin the RPC worker forever:
//   - by THIS_WORKER's timeout_ts when valid (respects query_timeout / the RPC
//     deadline, and lets the worker bail promptly if the request is killed);
//   - otherwise by an ACQUIRE_FORCE_WAIT_TIMEOUT_US fallback.
// Exceeding either bound (or a worker that is already timed out) returns
// OB_TIMEOUT.
int ObMViewPendingTaskManager::acquire_mview_block(const uint64_t mview_id, const int64_t flag)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  const int64_t deadline = start_ts + ACQUIRE_FORCE_WAIT_TIMEOUT_US;
  bool acquired = false;
  while (OB_SUCC(ret) && !acquired) {
    int cb_ret = OB_SUCCESS;
    AcquireBlockFn fn(flag, cb_ret);
    // Try to set the flag first. read_atomic only fires the callback when the
    // node exists, so the steady state (node already present, insert-once) costs
    // a single hash lookup with no allocation. Only on a miss do we create the
    // node and retry -- this is the first acquire for an mview, or a recreate
    // after a committed drop's cleanup erased it.
    bool node_missing = false;
    if (OB_FAIL(mview_context_map_.read_atomic(mview_id, fn))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        node_missing = true;
        if (OB_FAIL(ensure_mview_ctx(mview_id))) {
          LOG_WARN("ensure mview context failed", KR(ret), K(mview_id), K(flag));
        }
      } else {
        LOG_WARN("read mview context failed", KR(ret), K(mview_id), K(flag));
      }
    } else if (OB_SUCC(cb_ret)) {
      acquired = true;
    } else if (OB_EAGAIN != cb_ret) {
      // Either a non-retryable error, or the non-waiting (DROP) path: report it.
      ret = cb_ret;
      LOG_WARN("mview schedule blocked by concurrent drop/force", KR(ret), K(mview_id), K(flag));
    }
    if (OB_SUCC(ret) && !acquired) {
      // Bound every retry (miss-then-create or FORCE contention) by the worker
      // status and the wait deadline so a wedged owner -- or a pathological
      // create/erase race -- cannot pin the RPC worker forever.
      const int64_t now = ObTimeUtility::current_time();
      if (THIS_WORKER.is_timeout() || now >= deadline) {
        ret = OB_TIMEOUT;
        LOG_WARN("timeout waiting for mview force block to be released", KR(ret),
                 K(mview_id), K(flag), K(now), K(deadline), K(start_ts));
      } else if (!node_missing) {
        // FORCE contender: another DROP/FORCE marker is present. Back off before
        // retrying. A miss-then-create retries immediately, no need to sleep.
        ob_usleep(ACQUIRE_FORCE_RETRY_INTERVAL_US);
      }
    }
  }
  return ret;
}

int ObMViewPendingTaskManager::release_mview_block(const uint64_t mview_id, const int64_t flag)
{
  int ret = OB_SUCCESS;
  ClearBlockFlagFn fn(flag);
  if (OB_FAIL(mview_context_map_.read_atomic(mview_id, fn))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // node already gone (e.g. cleared by drop cleanup)
    } else {
      LOG_WARN("clear block flag failed", KR(ret), K(mview_id), K(flag));
    }
  }
  return ret;
}

int ObMViewPendingTaskManager::erase_and_free_ctx(const uint64_t mview_id)
{
  int ret = OB_SUCCESS;
  ObMViewContext *ctx = nullptr;
  // erase_refactored摘除节点在bucket写锁内完成，与所有read_atomic回调互斥，
  // 故返回后free该指针不会与正在deref的回调发生UAF。
  if (OB_FAIL(mview_context_map_.erase_refactored(mview_id, &ctx))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("erase mview context failed", KR(ret), K(mview_id));
    }
  } else if (OB_NOT_NULL(ctx)) {
    ctx->~ObMViewContext();
    mview_ctx_alloc_.free(ctx);
  }
  return ret;
}

// Collect all mview_ids under the bucket read lock (no free inside the
// iteration), then erase+free each one under the bucket write lock. Freeing
// inside foreach_refactored would race a concurrent read_atomic reader.
int ObMViewPendingTaskManager::reset_mview_contexts()
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 16> mview_ids;
  CollectAllFn collect_fn(mview_ids);
  if (OB_FAIL(mview_context_map_.foreach_refactored(collect_fn))) {
    LOG_WARN("foreach mview context map failed", KR(ret));
  } else if (OB_FAIL(collect_fn.ret_)) {
    LOG_WARN("collect mview ids failed", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < mview_ids.count(); ++i) {
    if (OB_FAIL(erase_and_free_ctx(mview_ids.at(i)))) {
      LOG_WARN("erase and free mview context failed", KR(ret), "mview_id", mview_ids.at(i));
    }
  }
  return ret;
}

int ObMViewPendingTaskManager::schedule_task(const obrpc::ObScheduleMViewRefreshArg &arg,
                                             int64_t &refresh_id,
                                             bool is_force_owner)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  refresh_id = OB_INVALID_ID;
  ObArenaAllocator allocator("MVPendTaskMgr", OB_MALLOC_NORMAL_BLOCK_SIZE, arg.tenant_id_);
  ObSEArray<ObMViewPendingTask*, 4> pending_tasks;
  int64_t inc_count = 0;
  const int64_t start_time = ObTimeUtility::current_time();
  SCN target_data_sync_scn;
  ObString err_msg = "pending refresh failed with unknown error";
  bool need_write_run_end = false;
  bool need_rollback = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task manager not init", KR(ret));
  } else if (OB_FAIL(wait_reload_ready(RELOAD_WAIT_TIMEOUT_MS))) {
    LOG_WARN("wait reload ready failed", KR(ret));
  } else if (OB_UNLIKELY(arg.mview_id_ == OB_INVALID_ID)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid mview id", KR(ret), K(arg.mview_id_));
  } else if (OB_FAIL(generate_refresh_id(refresh_id))) {
    LOG_WARN("generate refresh id failed", KR(ret), K(arg.mview_id_));
  } else if (OB_FAIL(ObMViewRefreshHelper::get_current_scn(target_data_sync_scn))) {
    LOG_WARN("fail to get current scn", K(ret));
  } else if (OB_FAIL(write_run_start(arg, refresh_id, start_time, target_data_sync_scn))) {
    LOG_WARN("fail to write_run_start", KR(ret), K(arg), K(refresh_id));
  } else if (OB_FALSE_IT(need_write_run_end = true)) {
  } else if (OB_FAIL(build_pending_tasks(allocator, arg, refresh_id, start_time, target_data_sync_scn, pending_tasks))) {
    LOG_WARN("build pending tasks failed", KR(ret), K(arg));
  } else if (OB_FAIL(check_pending_tasks_schedule_block(pending_tasks, arg.mview_id_, is_force_owner, true, inc_count))) {
    LOG_WARN("pending tasks blocked by concurrent drop/force", KR(ret), K(arg.mview_id_));
    if (ret == OB_SIZE_OVERFLOW) {
      err_msg = "per-mview pending refresh task count limit exceeded";
    } else if (ret == OB_EAGAIN) {
      err_msg = "pending refresh blocked by concurrent drop or force refresh";
    }
  } else if (OB_FAIL(table_operator_.insert_tasks(pending_tasks))) {
    LOG_WARN("insert pending tasks failed", KR(ret), K(pending_tasks.count()));
  } else if (OB_FALSE_IT(need_rollback = true)) {
  } else if (OB_FAIL(queue_.push_tasks(pending_tasks, common::ObCurTraceId::get_trace_id()))) {
    LOG_WARN("push pending tasks failed", KR(ret), K(pending_tasks.count()));
    if (ret == OB_SIZE_OVERFLOW) {
      err_msg = "pending refresh global task queue limit exceeded";
    }
  }
  if (OB_FAIL(ret) && need_write_run_end) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(on_schedule_task_failed(arg.tenant_id_, refresh_id, need_rollback, ret, err_msg))) {
      LOG_WARN("fail to rollback after push failed", KR(tmp_ret),
               K(arg.tenant_id_), K(refresh_id));
    }
  }
  if (OB_FAIL(ret) && inc_count > 0) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(dec_tasks_queue_cnt(pending_tasks, inc_count))) {
      LOG_WARN("dec mview queue cnt failed", KR(tmp_ret), K(inc_count));
    }
  }
  return ret;
}

// Called in two paths:
//   1) schedule_task: the user just pushed a refresh but queue_.push_tasks failed;
//   2) enqueue_reload_task: on leader startup reload_tasks() tries to rehydrate
//      persisted tasks into memory but this batch can no longer fit (queue
//      overflow / OOM).
// In both paths we discard the tasks instead of re-queueing them:
//   - schedule_task path: re-queueing would loop on the same failure;
//   - reload path: reload_tasks() only runs once per leader term (see run1() —
//     on success it breaks out of the reload loop), so leaving the rows in
//     __all_mview_refresh_task would permanently pollute subsequent queries,
//     sync-wait lookups and delete_terminal_tasks scans.
// TODO: once the run/mview history-write path is solid (needs an
// ObExecContext-free variant of ObMViewRefreshStatsUtils::write_run_start /
// write_mv_start, plus a retry_id probing loop against
// __all_mview_refresh_stats' PK), emit one aborted row per refresh_id and one
// per (refresh_id, mview_id, retry_id) here before the delete, so DBAs can see
// why the refresh was dropped. For now we only remove the rows from
// __all_mview_refresh_task; the caller already logged the reason at WARN.
int ObMViewPendingTaskManager::on_schedule_task_failed(uint64_t tenant_id,
                                                       int64_t refresh_id,
                                                       const bool need_rollback,
                                                       const int task_ret,
                                                       const common::ObString &err_msg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (need_rollback && OB_FAIL(table_operator_.delete_tasks_by_refresh_id(tenant_id, refresh_id))) {
    LOG_WARN("fail to delete aborted tasks by refresh id",
              KR(ret), K(tenant_id), K(refresh_id));
  }
  if (OB_TMP_FAIL(ObMViewRefreshStatsUtils::write_run_end(GCTX.sql_proxy_,
                                                          tenant_id,
                                                          refresh_id,
                                                          ObTimeUtility::current_time(),
                                                          0,
                                                          task_ret,
                                                          err_msg,
                                                          1))) {
    LOG_WARN("fail to write run end", KR(ret), K(tenant_id), K(refresh_id), K(task_ret), K(err_msg));
  }
  return ret;
}

int ObMViewPendingTaskManager::get_refresh_status(uint64_t tenant_id,
                                                   int64_t refresh_id,
                                                   ObMViewTaskStatus &status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task manager not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(queue_.get_refresh_status(tenant_id, refresh_id, status))) {
    LOG_WARN("get refresh status failed", KR(ret), K(tenant_id), K(refresh_id));
  }
  return ret;
}

int ObMViewPendingTaskManager::mark_task_running(uint64_t tenant_id,
                                                 int64_t refresh_id,
                                                 uint64_t mview_id,
                                                 const ObAddr &leader_addr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task manager not init", KR(ret));
  } else if (OB_FAIL(wait_reload_ready(RELOAD_WAIT_TIMEOUT_MS))) {
    LOG_WARN("wait reload ready failed", KR(ret));
  } else if (OB_FAIL(table_operator_.update_task_to_running(tenant_id, refresh_id, mview_id,
                                                            leader_addr))) {
    // OB_EAGAIN: table CAS PENDING→RUNNING failed (task may have been concurrently cancelled).
    LOG_WARN("update task to running failed", KR(ret), K(tenant_id), K(refresh_id), K(mview_id),
             K(leader_addr));
  } else if (OB_FAIL(queue_.set_task_running(tenant_id, refresh_id, mview_id, &leader_addr))) {
    // Memory CAS failed — most likely a concurrent cancel already changed the in-memory state.
    // The cancel will also update the table (NOT IN terminal guard), so table self-heals.
    LOG_WARN("set task running failed", KR(ret), K(tenant_id), K(refresh_id), K(mview_id),
             K(leader_addr));
  }
  return ret;
}

int ObMViewPendingTaskManager::mark_task_success(uint64_t tenant_id,
                                                 int64_t refresh_id,
                                                 uint64_t mview_id)
{
  int ret = OB_SUCCESS;
  bool refresh_finished = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task manager not init", KR(ret));
  } else if (OB_FAIL(wait_reload_ready(RELOAD_WAIT_TIMEOUT_MS))) {
    LOG_WARN("wait reload ready failed", KR(ret));
  } else if (OB_FAIL(table_operator_.update_task_to_success(tenant_id, refresh_id, mview_id))) {
    LOG_WARN("update task to success failed", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
  } else if (OB_FAIL(queue_.set_task_success(tenant_id, refresh_id, mview_id, refresh_finished))) {
    LOG_WARN("set task success failed", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
  } else if (refresh_finished && OB_FAIL(inner_recycle_refresh(tenant_id, refresh_id))) {
    LOG_WARN("recycle refresh failed", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
  }
  return ret;
}

bool ObMViewPendingTaskManager::need_delay_before_retry(int task_ret)
{
  bool bret = false;
  if (is_master_changed_error(task_ret)) {
    bret = true;
  }
  return bret;
}

int ObMViewPendingTaskManager::mark_task_retry_wait(uint64_t tenant_id,
                                                    int64_t refresh_id,
                                                    uint64_t mview_id,
                                                    int task_ret,
                                                    const ObString &err_msg)
{
  int ret = OB_SUCCESS;
  bool refresh_finished = false;
  int64_t retry_count = 0;
  int64_t next_retry_ts = 0;
  bool refresh_cancelled = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task manager not init", KR(ret));
  } else if (OB_FAIL(wait_reload_ready(RELOAD_WAIT_TIMEOUT_MS))) {
    LOG_WARN("wait reload ready failed", KR(ret));
  } else if (OB_FAIL(queue_.get_task_retry_count(tenant_id, refresh_id,
                                                  mview_id, retry_count))) {
    LOG_WARN("get task retry count failed", KR(ret),
             K(tenant_id), K(refresh_id), K(mview_id));
  } else if (OB_FAIL(queue_.is_refresh_cancelled(tenant_id, refresh_id, refresh_cancelled))) {
    LOG_WARN("check refresh cancelled failed", KR(ret),
             K(tenant_id), K(refresh_id), K(mview_id));
  } else if (retry_count >= MAX_RETRY_COUNT || refresh_cancelled) {
    LOG_INFO("task retry count exhausted, marking failed directly",
             K(tenant_id), K(refresh_id), K(mview_id), K(retry_count), K(task_ret), K(refresh_cancelled));
    if (OB_FAIL(finalize_task(tenant_id, refresh_id, mview_id, task_ret, err_msg))) {
      LOG_WARN("finalize task after retry exhaustion failed", KR(ret),
               K(tenant_id), K(refresh_id), K(mview_id), K(task_ret));
    }
  } else if (!need_delay_before_retry(task_ret)) {
    if (OB_FAIL(table_operator_.update_task_running_to_pending(tenant_id, refresh_id,
                                                               mview_id))) {
      LOG_WARN("update task running to pending failed", KR(ret),
               K(tenant_id), K(refresh_id), K(mview_id));
    } else if (OB_FAIL(queue_.set_task_running_to_pending(tenant_id, refresh_id, mview_id,
                                                          refresh_finished))) {
      LOG_WARN("set task running to pending failed", KR(ret),
               K(tenant_id), K(refresh_id), K(mview_id));
    }
  } else {
    next_retry_ts = ObTimeUtility::current_time() + 1_s;
    if (OB_FAIL(table_operator_.update_task_to_retry_wait(tenant_id, refresh_id,
                                                           mview_id, next_retry_ts))) {
      LOG_WARN("update task to retry wait failed", KR(ret), K(tenant_id), K(refresh_id),
               K(mview_id), K(next_retry_ts));
    } else if (OB_FAIL(queue_.set_task_retry_wait(tenant_id, refresh_id, mview_id,
                                                  refresh_finished))) {
      LOG_WARN("set task retry wait failed", KR(ret),
               K(tenant_id), K(refresh_id), K(mview_id));
    } else if (OB_FAIL(inspection_task_.register_for_retry(tenant_id, refresh_id, mview_id, next_retry_ts))) {
      LOG_WARN("fail to register for retry, task may be delayed until next reload",
               KR(ret), K(tenant_id), K(refresh_id), K(mview_id), K(next_retry_ts));
    }
  }
  return ret;
}

int ObMViewPendingTaskManager::mark_task_pending(uint64_t tenant_id,
                                                 int64_t refresh_id,
                                                 uint64_t mview_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task manager not init", KR(ret));
  } else if (OB_FAIL(wait_reload_ready(RELOAD_WAIT_TIMEOUT_MS))) {
    LOG_WARN("wait reload ready failed", KR(ret));
  } else if (OB_FAIL(table_operator_.update_task_to_pending(tenant_id, refresh_id, mview_id))) {
    LOG_WARN("update task to pending failed", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
  } else if (OB_FAIL(queue_.set_task_pending(tenant_id, refresh_id, mview_id))) {
    LOG_WARN("set task pending failed", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
  }
  return ret;
}

int ObMViewPendingTaskManager::mark_task_failed(uint64_t tenant_id,
                                                int64_t refresh_id,
                                                uint64_t mview_id,
                                                int task_ret)
{
  int ret = OB_SUCCESS;
  bool refresh_finished = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task manager not init", KR(ret));
  } else if (OB_FAIL(wait_reload_ready(RELOAD_WAIT_TIMEOUT_MS))) {
    LOG_WARN("wait reload ready failed", KR(ret));
  } else if (OB_FAIL(table_operator_.update_task_to_failed(tenant_id, refresh_id, mview_id))) {
    LOG_WARN("update task to failed failed", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
  } else if (OB_FAIL(queue_.set_task_failed(tenant_id, refresh_id, mview_id, task_ret, refresh_finished))) {
    // Memory CAS failed — concurrent cancel already moved state. Table self-heals.
    LOG_WARN("set task failed failed", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
  } else if (refresh_finished && OB_FAIL(inner_recycle_refresh(tenant_id, refresh_id))) {
    LOG_WARN("recycle refresh failed", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
  }
  return ret;
}

int ObMViewPendingTaskManager::finalize_task(uint64_t tenant_id,
                                             int64_t refresh_id,
                                             uint64_t mview_id,
                                             int task_ret,
                                             const common::ObString &err_msg)
{
  int ret = OB_SUCCESS;
  int64_t retry_count = 0;
  if (OB_FAIL(queue_.get_task_retry_count(tenant_id, refresh_id, mview_id, retry_count))) {
    LOG_WARN("get task retry count failed", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
  } else {
    int64_t per_mv_failures = retry_count + (OB_SUCCESS == task_ret ? 0 : 1);
    if (OB_FAIL(ObMViewRefreshStatsUtils::write_run_end(GCTX.sql_proxy_,
                                                        tenant_id,
                                                        refresh_id,
                                                        ObTimeUtility::current_time(),
                                                        0,
                                                        task_ret,
                                                        err_msg,
                                                        per_mv_failures))) {
      LOG_WARN("write run end failed", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_SUCCESS == task_ret) {
      if (OB_FAIL(mark_task_success(tenant_id, refresh_id, mview_id))) {
        LOG_WARN("mark task success failed", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
      }
    } else {
      if (OB_FAIL(mark_task_failed(tenant_id, refresh_id, mview_id, task_ret))) {
        LOG_WARN("mark task failed failed", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
      } else if (OB_FAIL(mark_all_tasks_canceled(tenant_id, refresh_id))) {
        LOG_WARN("mark all tasks canceled failed", KR(ret), K(tenant_id), K(refresh_id));
      }
    }
  }
  return ret;
}

// resync_task_from_disk treats disk as the authority and aligns memory to it.
//
// Two phases:
//   1. Snapshot disk state (row presence + status + retry/recovery hints).
//   2. Align in-memory state via queue_.align_task_to_status, which handles
//      every (memory_state, disk_state) combination uniformly. The caller
//      only owns the disk-state-specific side effects:
//        - ROW_GONE:    align mem to CANCELLED, cancel deps, recycle if finished.
//        - PENDING:     align (no side effect); scheduler will pick it up.
//        - RUNNING:     align + register_for_recovery.
//        - RETRY_WAIT:  align + register_for_retry.
//        - SUCCESS:     align + recycle if finished.
//        - FAILED:      align + cancel deps + recycle if finished.
//        - CANCELLED:   align + cancel deps + recycle if finished.
//
// Memory anomalies (task absent, memory advanced past disk into terminal)
// are logged as WARN inside align_task_to_status and surfaced to the caller
// via memory_absent; this function does not try to recreate missing tasks
// or revive terminal memory.
int ObMViewPendingTaskManager::resync_task_from_disk(uint64_t tenant_id,
                                                     int64_t refresh_id,
                                                     uint64_t mview_id)
{
  int ret = OB_SUCCESS;
  ObMViewTaskStatus disk_status = MV_TASK_PENDING;
  int64_t disk_next_retry_ts = 0;
  uint64_t target_data_sync_scn = 0;
  bool row_gone = false;
  bool memory_absent = false;
  bool refresh_finished = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task manager not init", KR(ret));
  } else if (OB_FAIL(wait_reload_ready(RELOAD_WAIT_TIMEOUT_MS))) {
    LOG_WARN("wait reload ready failed", KR(ret));
  } else if (OB_FAIL(table_operator_.get_task_sync_info(tenant_id, refresh_id, mview_id,
                                                         disk_status, disk_next_retry_ts,
                                                         target_data_sync_scn))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      row_gone = true;
    } else {
      LOG_WARN("get task sync info failed", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
    }
  }
  if (OB_FAIL(ret)) {
    // disk read failed, nothing to do
  } else {
    // Phase 1: align memory to disk under queue lock.
    const int64_t target_status = row_gone ? static_cast<int64_t>(MV_TASK_CANCELLED)
                                           : static_cast<int64_t>(disk_status);
    LOG_INFO("resync task memory from disk", K(tenant_id), K(refresh_id), K(mview_id),
             K(row_gone), K(disk_status), K(disk_next_retry_ts));
    if (OB_FAIL(queue_.align_task_to_status(tenant_id, refresh_id, mview_id,
                                            target_status, memory_absent, refresh_finished))) {
      LOG_WARN("align task to disk status failed", KR(ret),
               K(tenant_id), K(refresh_id), K(mview_id), K(target_status));
    } else if (memory_absent) {
      LOG_WARN("resync task: memory entry absent, will be picked up by next reload",
               K(tenant_id), K(refresh_id), K(mview_id), K(target_status));
    } else {
      // Phase 2: disk-state-specific side effects.
      const ObMViewTaskStatus effective_status = row_gone ? MV_TASK_CANCELLED : disk_status;
      switch (effective_status) {
        case MV_TASK_PENDING: {
          // No-op. Scheduler will retry mark_task_running on the next tick.
          break;
        }
        case MV_TASK_RUNNING: {
          if (OB_FAIL(inspection_task_.register_for_recovery(
                  tenant_id, refresh_id, mview_id, target_data_sync_scn))) {
            LOG_WARN("register for recovery failed after resync", KR(ret),
                     K(tenant_id), K(refresh_id), K(mview_id));
          }
          break;
        }
        case MV_TASK_RETRY_WAIT: {
          if (OB_FAIL(inspection_task_.register_for_retry(
                  tenant_id, refresh_id, mview_id, disk_next_retry_ts))) {
            LOG_WARN("register for retry failed after resync", KR(ret),
                     K(tenant_id), K(refresh_id), K(mview_id), K(disk_next_retry_ts));
          }
          break;
        }
        case MV_TASK_SUCCESS: {
          if (refresh_finished
              && OB_FAIL(inner_recycle_refresh(tenant_id, refresh_id))) {
            LOG_WARN("recycle refresh failed after success resync", KR(ret),
                     K(tenant_id), K(refresh_id), K(mview_id));
          }
          break;
        }
        case MV_TASK_FAILED:
        case MV_TASK_CANCELLED: {
          if (!refresh_finished
              && OB_FAIL(queue_.cancel_all_pending_tasks(tenant_id, refresh_id, refresh_finished))) {
            LOG_WARN("cancel all pending tasks after terminal resync failed", KR(ret),
                     K(tenant_id), K(refresh_id), K(mview_id), K(effective_status));
          } else if (refresh_finished
                     && OB_FAIL(inner_recycle_refresh(tenant_id, refresh_id))) {
            LOG_WARN("recycle refresh failed after terminal resync", KR(ret),
                     K(tenant_id), K(refresh_id), K(mview_id), K(effective_status));
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unknown effective status after resync", KR(ret),
                   K(tenant_id), K(refresh_id), K(mview_id), K(effective_status));
          break;
        }
      }
    }
  }
  return ret;
}

int ObMViewPendingTaskManager::recycle_refresh(uint64_t tenant_id,
                                               int64_t refresh_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task manager not init", KR(ret));
  } else if (OB_FAIL(wait_reload_ready(RELOAD_WAIT_TIMEOUT_MS))) {
    LOG_WARN("wait reload ready failed", KR(ret));
  } else if (OB_FAIL(inner_recycle_refresh(tenant_id, refresh_id))) {
    LOG_WARN("inner recycle refresh failed", KR(ret), K(tenant_id), K(refresh_id));
  }
  return ret;
}

int ObMViewPendingTaskManager::inner_recycle_refresh(uint64_t tenant_id,
                                                     int64_t refresh_id)
{
  int ret = OB_SUCCESS;
  DecMViewQueueCntCb cb(mview_context_map_);
  ObMViewPendingTaskQueue::RecycleCb release_cb(cb);
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || refresh_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(refresh_id));
  } else if (OB_FAIL(table_operator_.delete_tasks_by_refresh_id(tenant_id, refresh_id))) {
    LOG_WARN("delete tasks by refresh id failed", KR(ret), K(tenant_id), K(refresh_id));
  } else if (OB_FAIL(queue_.recycle_refresh(tenant_id, refresh_id, release_cb))) {
    LOG_WARN("queue recycle refresh failed", KR(ret), K(tenant_id), K(refresh_id));
  }
  return ret;
}

int ObMViewPendingTaskManager::schedule_mview_refresh_local(
    const obrpc::ObScheduleMViewRefreshArg &arg,
    obrpc::ObScheduleMViewRefreshResult &result)
{
  int ret = OB_SUCCESS;
  bool need_release_force_block = false;
  result.refresh_id_ = OB_INVALID_ID;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task manager not init", KR(ret));
  } else if (arg.force_ && OB_FAIL(acquire_mview_block(arg.mview_id_, ObMViewContext::BLOCK_FLAG_FORCE))) {
    LOG_WARN("fail to acquire mview force block", KR(ret), K(arg.tenant_id_), K(arg.mview_id_));
  } else if (OB_FALSE_IT(need_release_force_block = arg.force_)) {
  } else if (arg.force_ && OB_FAIL(kill_refreshes_by_mview_local(arg.tenant_id_, arg.mview_id_))) {
    LOG_WARN("fail to kill existing refreshes before forced schedule",
             KR(ret), K(arg.tenant_id_), K(arg.mview_id_));
  } else if (OB_FAIL(schedule_task(arg, result.refresh_id_, arg.force_))) {
    LOG_WARN("fail to schedule mview refresh task", KR(ret), K(arg));
  } else {
    wakeup();
  }
  if (need_release_force_block) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(release_mview_block(arg.mview_id_, ObMViewContext::BLOCK_FLAG_FORCE))) {
      LOG_WARN("release force block failed", KR(tmp_ret), K(arg.tenant_id_), K(arg.mview_id_));
    }
  }
  return ret;
}

int ObMViewPendingTaskManager::schedule_mview_refresh(
    const obrpc::ObScheduleMViewRefreshArg &arg,
    obrpc::ObScheduleMViewRefreshResult &result)
{
  int ret = OB_SUCCESS;
  result.refresh_id_ = OB_INVALID_ID;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task manager not init", KR(ret));
  } else if (OB_ISNULL(GCTX.location_service_) || OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location_service or srv_rpc_proxy is null", KR(ret),
             KP(GCTX.location_service_), KP(GCTX.srv_rpc_proxy_));
  } else {
    const int64_t RETRY_CNT_LIMIT = 3;
    int64_t retry_cnt = 0;
    const uint64_t tenant_id = arg.tenant_id_;
    common::ObAddr leader_addr;
    do {
      if (OB_NOT_MASTER == ret) {
        ret = OB_SUCCESS;
        ob_usleep(1_s);
      }
      if (FAILEDx(GCTX.location_service_->get_leader_with_retry_until_timeout(
              GCONF.cluster_id, tenant_id, share::SYS_LS, leader_addr))) {
        LOG_WARN("fail to get sys ls leader addr", KR(ret), K(tenant_id));
      } else if (leader_addr == GCONF.self_addr_) {
        if (OB_FAIL(schedule_mview_refresh_local(arg, result))) {
          LOG_WARN("fail to schedule mview refresh local", KR(ret), K(arg));
        }
      } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(leader_addr).by(tenant_id)
                        .timeout(static_cast<int64_t>(GCONF.rpc_timeout))
                        .schedule_mview_refresh(arg, result))) {
        LOG_WARN("fail to send schedule mview refresh rpc", KR(ret), K(tenant_id),
                  K(arg), K(leader_addr));
      } else if (OB_FAIL(result.ret_)) {
        LOG_WARN("schedule mview refresh on leader failed", KR(ret), K(tenant_id),
                  K(arg), K(leader_addr));
      }
    } while (OB_NOT_MASTER == ret && ++retry_cnt <= RETRY_CNT_LIMIT);
  }
  return ret;
}

int ObMViewPendingTaskManager::get_mview_tablet_id(
    const share::schema::ObTableSchema &table_schema,
    common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  const share::schema::ObPartitionLevel part_level = table_schema.get_part_level();
  if (share::schema::PARTITION_LEVEL_ZERO == part_level) {
    tablet_id = table_schema.get_tablet_id();
  } else if (share::schema::PARTITION_LEVEL_ONE == part_level) {
    const share::schema::ObPartition *const *part_array = table_schema.get_part_array();
    if (OB_ISNULL(part_array) || OB_ISNULL(part_array[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition array is null", KR(ret), "mview_id", table_schema.get_table_id());
    } else {
      tablet_id = part_array[0]->get_tablet_id();
    }
  } else if (share::schema::PARTITION_LEVEL_TWO == part_level) {
    const share::schema::ObPartition *const *part_array = table_schema.get_part_array();
    if (OB_ISNULL(part_array) || OB_ISNULL(part_array[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition array is null", KR(ret), "mview_id", table_schema.get_table_id());
    } else {
      const share::schema::ObSubPartition *const *subpart_array = part_array[0]->get_subpart_array();
      if (OB_ISNULL(subpart_array) || OB_ISNULL(subpart_array[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subpartition array is null", KR(ret), "mview_id", table_schema.get_table_id());
      } else {
        tablet_id = subpart_array[0]->get_tablet_id();
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected partition level", KR(ret), K(part_level),
             "mview_id", table_schema.get_table_id());
  }
  return ret;
}

int ObMViewPendingTaskManager::get_mview_leader_addr(uint64_t tenant_id,
                                                      uint64_t mview_id,
                                                      ObAddr &leader_addr)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *table_schema = nullptr;
  const share::schema::ObTableSchema *container_table_schema = nullptr;
  common::ObTabletID tablet_id;
  share::ObLSID ls_id;
  const int64_t timeout_us = 10 * 1000 * 1000L;

  if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service or location_service is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, mview_id, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(mview_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("mview table schema not found", KR(ret), K(tenant_id), K(mview_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                    table_schema->get_data_table_id(),
                                                    container_table_schema))) {
    LOG_WARN("fail to get container table schema", KR(ret), K(tenant_id), K(mview_id),
             "container_table_id", table_schema->get_data_table_id());
  } else if (OB_ISNULL(container_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("mview container table schema not found", KR(ret), K(tenant_id), K(mview_id),
             "container_table_id", table_schema->get_data_table_id());
  } else if (OB_FAIL(get_mview_tablet_id(*container_table_schema, tablet_id))) {
    LOG_WARN("fail to get mview tablet id", KR(ret), K(mview_id));
  } else if (OB_FAIL(share::ObDDLUtil::get_tablet_leader_addr(
                 GCTX.location_service_, tenant_id, tablet_id, timeout_us, ls_id, leader_addr))) {
    LOG_WARN("fail to get tablet leader addr", KR(ret), K(tenant_id), K(mview_id), K(tablet_id));
  }
  if (OB_UNLIKELY(ret == OB_TABLE_NOT_EXIST)) {
    if (OB_FAIL(server_random_pick_for_tenant(tenant_id, leader_addr))) {
      LOG_WARN("schema-based leader addr and random fallback both failed",
                K(tenant_id), K(mview_id));
    }
  }
  return ret;
}

int ObMViewPendingTaskManager::server_random_pick_for_tenant(uint64_t tenant_id,
                                                              ObAddr &server)
{
  int ret = OB_SUCCESS;
  share::ObUnitTableOperator unit_op;
  common::ObSEArray<share::ObUnit, 4> units;
  common::ObSEArray<common::ObAddr, 8> total_server;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else if (OB_FAIL(unit_op.init(*GCTX.sql_proxy_))) {
    LOG_WARN("fail to init unit op", KR(ret));
  } else if (OB_FAIL(unit_op.get_units_by_tenant(tenant_id, units))) {
    LOG_WARN("fail to get units by tenant", KR(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); ++i) {
      ObAddr addr = units.at(i).server_;
      bool is_active = false;
      bool is_service = false;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(SVR_TRACER.check_server_active(addr, is_active))) {
        LOG_WARN("fail to check server active", KR(tmp_ret), K(addr));
      } else if (OB_TMP_FAIL(SVR_TRACER.check_in_service(addr, is_service))) {
        LOG_WARN("fail to check in service", KR(tmp_ret), K(addr));
      } else if (is_active && is_service) {
        if (OB_FAIL(total_server.push_back(addr))) {
          LOG_WARN("fail to push server", KR(ret), K(addr));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (total_server.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("no alive in-service server found", KR(ret), K(tenant_id));
      } else {
        int64_t pos = common::ObRandom::rand(0, 65536) % total_server.count();
        server = total_server.at(pos);
      }
    }
  }
  return ret;
}

int ObMViewPendingTaskManager::get_rpc_timeout_us(int64_t expire_ts, int64_t &rpc_timeout_us)
{
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_RPC_TIMEOUT_US = 24LL * 60 * 60 * 1000 * 1000;
  const int64_t MIN_RPC_TIMEOUT_US = 1LL * 1000 * 1000;
  rpc_timeout_us = DEFAULT_RPC_TIMEOUT_US;
  if (expire_ts > 0) {
    const int64_t now = ObTimeUtility::current_time();
    const int64_t remaining = expire_ts - now;
    if (remaining < MIN_RPC_TIMEOUT_US) {
      rpc_timeout_us = MIN_RPC_TIMEOUT_US;
    } else if (remaining > DEFAULT_RPC_TIMEOUT_US) {
      rpc_timeout_us = DEFAULT_RPC_TIMEOUT_US;
    } else {
      rpc_timeout_us = remaining;
    }
  }
  return ret;
}

int ObMViewPendingTaskManager::run_task(uint64_t tenant_id,
                                        int64_t refresh_id,
                                        uint64_t mview_id,
                                        uint64_t target_data_sync_scn,
                                        const share::schema::ObMVRefreshMethod refresh_method,
                                        const int64_t refresh_parallel,
                                        const int64_t retry_count,
                                        const bool is_consistent_refresh,
                                        const int64_t expire_ts,
                                        const ObAddr &leader_addr)
{
  int ret = OB_SUCCESS;
  common::ObCurTraceId::TraceId task_trace_id;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task manager not init", KR(ret));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc proxy is null", KR(ret));
  } else if (OB_UNLIKELY(!leader_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid leader addr", KR(ret), K(tenant_id), K(refresh_id), K(mview_id),
             K(leader_addr));
  } else if (expire_ts > 0 && expire_ts <= ObTimeUtility::current_time()) {
    // The deadline has already been crossed between peek_task and here (concurrent
    // limit wait / get_mview_leader_addr / mark_task_running all take time). Skip the
    // RPC entirely and let the caller finalize the task as failed, instead of dispatching
    // a refresh that the leader would only discard once it re-checks expire_ts.
    ret = OB_TIMEOUT;
    LOG_WARN("mview pending task already expired before dispatch, skip rpc",
             KR(ret), K(tenant_id), K(refresh_id), K(mview_id),
             K(expire_ts), "now", ObTimeUtility::current_time());
  } else if (OB_FAIL(queue_.get_refresh_trace_id(tenant_id, refresh_id, task_trace_id))) {
    LOG_WARN("get refresh trace id failed, generate new one", KR(ret), K(tenant_id), K(refresh_id));
  } else {
    ObTraceIdGuard trace_id_guard(task_trace_id);
    obrpc::ObRunMViewPendingTaskArg arg(tenant_id,
                                        refresh_id,
                                        mview_id,
                                        target_data_sync_scn,
                                        refresh_method,
                                        refresh_parallel,
                                        retry_count,
                                        is_consistent_refresh,
                                        expire_ts);
    obrpc::ObRpcRunMViewPendingTaskCB cb;
    int64_t rpc_timeout_us = 0;
    if (OB_FAIL(get_rpc_timeout_us(expire_ts, rpc_timeout_us))) {
      LOG_WARN("get rpc timeout us failed", KR(ret), K(tenant_id), K(refresh_id),
               K(mview_id), K(expire_ts));
    } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(leader_addr).by(tenant_id)
                                    .group_id(share::OBCG_OLAP_ASYNC_JOB)
                                    .timeout(rpc_timeout_us)
                                    .run_mview_pending_task(arg, &cb))) {
      LOG_WARN("run mview pending task rpc failed", KR(ret), K(tenant_id), K(refresh_id),
               K(mview_id), K(leader_addr), K(expire_ts), K(rpc_timeout_us));
    }
    // TODO: remove this log
    LOG_INFO("run mview pending task rpc done", KR(ret), K(tenant_id), K(refresh_id),
             K(mview_id), K(leader_addr), K(expire_ts), K(rpc_timeout_us));
  }
  return ret;
}


int ObMViewPendingTaskManager::register_running_task_for_recovery(
    uint64_t tenant_id, int64_t refresh_id,
    uint64_t mview_id, uint64_t target_data_sync_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task manager not init", KR(ret));
  } else if (OB_FAIL(wait_reload_ready(RELOAD_WAIT_TIMEOUT_MS))) {
    LOG_WARN("wait reload ready failed", KR(ret));
  } else if (OB_FAIL(inspection_task_.register_for_recovery(
                 tenant_id, refresh_id, mview_id, target_data_sync_scn))) {
    LOG_WARN("register for recovery failed", KR(ret),
             K(tenant_id), K(refresh_id), K(mview_id));
  }
  return ret;
}

int ObMViewPendingTaskManager::push_task_result(uint64_t tenant_id,
                                                int64_t refresh_id,
                                                uint64_t mview_id,
                                                uint64_t target_data_sync_scn,
                                                int task_ret,
                                                const char *err_msg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  void *buf = NULL;
  bool need_fallback = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task manager not init", KR(ret));
  } else if (OB_FAIL(wait_reload_ready(RELOAD_WAIT_TIMEOUT_MS))) {
    LOG_WARN("wait reload ready failed", KR(ret));
  } else if (OB_ISNULL(buf = result_alloc_.alloc(sizeof(TaskResultEntry)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc task result entry failed, fallback to recovery list", KR(ret), K(tenant_id), K(refresh_id), K(mview_id), K(task_ret));
  } else {
    TaskResultEntry *entry = new (buf) TaskResultEntry();
    entry->tenant_id_ = tenant_id;
    entry->refresh_id_ = refresh_id;
    entry->mview_id_ = mview_id;
    entry->task_ret_ = task_ret;
    entry->set_err_msg(err_msg);
    if (OB_SUCCESS != (tmp_ret = result_queue_.push(entry))) {
      LOG_WARN("push task result to queue failed, fallback to recovery list",
               KR(tmp_ret), K(tenant_id), K(refresh_id), K(mview_id));
      entry->~TaskResultEntry();
      result_alloc_.free(entry);
      need_fallback = true;
    } else {
      wakeup();
    }
  }
  if (OB_ALLOCATE_MEMORY_FAILED == ret) {
    ret = OB_SUCCESS;
    need_fallback = true;
  }
  if (OB_SUCC(ret) && need_fallback) {
    if (OB_FAIL(inspection_task_.register_for_recovery(
                    tenant_id, refresh_id, mview_id, target_data_sync_scn))) {
      LOG_WARN("fallback register for recovery failed, task result may be lost",
               KR(ret), K(tenant_id), K(refresh_id), K(mview_id), K(task_ret));
    } else {
      wakeup();
    }
  }
  return ret;
}

bool ObMViewPendingTaskManager::has_pending_results() const
{
  return result_queue_.size() > 0;
}

int ObMViewPendingTaskManager::process_task_results(int64_t max_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  common::ObLink *link = NULL;
  bool queue_empty = false;
  for (int64_t i = 0; OB_SUCC(ret) && !queue_empty && i < max_cnt; ++i) {
    if (OB_FAIL(result_queue_.pop(link))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
        queue_empty = true;
      } else {
        LOG_WARN("fail to pop from result queue", KR(ret));
      }
    } else {
      TaskResultEntry *entry = static_cast<TaskResultEntry *>(link);
      if (OB_ISNULL(entry)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task result entry is null", KR(ret));
      } else {
        if (OB_TMP_FAIL(process_single_task_result(*entry))) {
          LOG_WARN("process single task result failed", KR(tmp_ret), KPC(entry));
        }
        entry->~TaskResultEntry();
        result_alloc_.free(entry);
      }
    }
  }
  return ret;
}

int ObMViewPendingTaskManager::process_single_task_result(const TaskResultEntry &entry)
{
  int ret = OB_SUCCESS;
  if (ATOMIC_LOAD(&reload_state_) != RS_READY) {
    LOG_INFO("reload state is not ready, skip process task result", KR(ret), K(entry));
  } else if (OB_INVALID_QUERY_TIMESTAMP != entry.task_ret_ &&
             ObMViewExecutorUtil::is_mview_refresh_retry_ret_code(entry.task_ret_)) {
    if (OB_FAIL(mark_task_retry_wait(entry.tenant_id_,
                                     entry.refresh_id_,
                                     entry.mview_id_,
                                     entry.task_ret_,
                                     entry.err_msg()))) {
      LOG_WARN("mark task retry wait failed", KR(ret),
               K(entry.tenant_id_), K(entry.refresh_id_), K(entry.mview_id_),
               K(entry.task_ret_));
    }
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      ObString err_msg;
      if (OB_TMP_FAIL(finalize_task(entry.tenant_id_,
                                    entry.refresh_id_,
                                    entry.mview_id_,
                                    entry.task_ret_,
                                    err_msg))) {
        LOG_WARN("finalize task failed", KR(tmp_ret), K(entry));
      }
    }
  } else {
    if (OB_FAIL(finalize_task(entry.tenant_id_,
                              entry.refresh_id_,
                              entry.mview_id_,
                              entry.task_ret_,
                              entry.err_msg()))) {
      LOG_WARN("finalize task failed", KR(ret), K(entry));
    }
  }
  return ret;
}

int ObMViewPendingTaskManager::get_min_pending_task_snapshot(share::SCN &scn)
{
  int ret = OB_SUCCESS;
  scn.set_invalid();
  if (OB_UNLIKELY(!is_inited_)) {
    // not inited yet, do not contribute to GC lower bound; callsite guards by is_valid()
  } else if (OB_FAIL(table_operator_.get_min_pending_task_snapshot(scn))) {
    LOG_WARN("fail to get min pending task snapshot", KR(ret));
  }
  return ret;
}

int ObMViewPendingTaskManager::load_missing_session_ids_for_running(
    ObIArray<ObMViewPendingRunningJobInfo> &infos)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObMViewPendingTaskSessionIdEntry, 32> missing_session_id_entries;
  for (int64_t i = 0; OB_SUCC(ret) && i < infos.count(); ++i) {
    if (0 == infos.at(i).session_id_) {
      ObMViewPendingTaskSessionIdEntry *entry = NULL;
      if (OB_ISNULL(entry = missing_session_id_entries.alloc_place_holder())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc missing session id entry failed", KR(ret), K(i));
      } else {
        entry->key_.tenant_id_ = infos.at(i).tenant_id_;
        entry->key_.refresh_id_ = infos.at(i).refresh_id_;
        entry->key_.mview_id_ = infos.at(i).mview_id_;
        entry->info_idx_ = i;
        entry->session_id_ = 0;
      }
    }
  }
  if (OB_SUCC(ret) && !missing_session_id_entries.empty()) {
    // Queue is MTL-scoped so every snapshot entry shares the same tenant.
    const uint64_t tenant_id = missing_session_id_entries.at(0).key_.tenant_id_;
    if (OB_FAIL(table_operator_.batch_get_running_session_ids(
                    tenant_id, missing_session_id_entries))) {
      LOG_WARN("batch get running session ids failed",
               KR(ret), K(tenant_id), K(missing_session_id_entries.count()));
    } else {
      for (int64_t k = 0; OB_SUCC(ret) && k < missing_session_id_entries.count(); ++k) {
        const uint32_t sid = missing_session_id_entries.at(k).session_id_;
        if (0 != sid) {
          const int64_t info_idx = missing_session_id_entries.at(k).info_idx_;
          if (OB_UNLIKELY(info_idx < 0 || info_idx >= infos.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid running job info index",
                     KR(ret), K(k), K(info_idx), K(infos.count()), K(missing_session_id_entries.at(k)));
          } else {
            infos.at(info_idx).session_id_ = sid;
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(queue_.patch_session_ids_for_running_tasks(missing_session_id_entries))) {
          LOG_WARN("patch session_ids into queue failed", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObMViewPendingTaskManager::query_running_session_infos(uint64_t tenant_id,
                                                           int64_t refresh_id,
                                                           ObIArray<uint32_t> &out_session_ids,
                                                           ObIArray<ObAddr> &out_addrs,
                                                           bool &need_retry)
{
  int ret = OB_SUCCESS;
  out_session_ids.reset();
  out_addrs.reset();
  need_retry = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task manager not init", KR(ret));
  } else if (OB_FAIL(table_operator_.get_running_session_infos(
                 tenant_id, refresh_id, out_session_ids, out_addrs, need_retry))) {
    LOG_WARN("get running session infos failed", KR(ret), K(tenant_id), K(refresh_id));
  }
  return ret;
}

int ObMViewPendingTaskManager::kill_refresh_local(uint64_t tenant_id, int64_t refresh_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task manager not init", KR(ret));
  } else if (OB_FAIL(mark_all_tasks_canceled(tenant_id, refresh_id))) {
    LOG_WARN("mark all tasks canceled failed", KR(ret), K(tenant_id), K(refresh_id));
  } else {
    static const int64_t KILL_RETRY_MAX = 3;
    static const int64_t KILL_RETRY_INTERVAL_US = 10 * 1000;
    bool kill_done = false;
    for (int64_t retry = 0; !kill_done && retry < KILL_RETRY_MAX; ++retry) {
      if (retry > 0) {
        ob_usleep(KILL_RETRY_INTERVAL_US);
      }
      ObSEArray<uint32_t, 4> session_ids;
      ObSEArray<ObAddr, 4> addrs;
      bool need_retry = false;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(query_running_session_infos(tenant_id, refresh_id, session_ids, addrs, need_retry))) {
        LOG_WARN("query running session infos failed, will retry", KR(tmp_ret), K(tenant_id), K(refresh_id), K(retry));
        continue;
      } else {
        bool all_killed = true;
        for (int64_t i = 0; i < session_ids.count(); ++i) {
          if (OB_TMP_FAIL(sql::ObKillSession::kill_one_session(session_ids.at(i), addrs.at(i), OB_SYS_TENANT_ID))) {
            if (OB_ENTRY_NOT_EXIST == tmp_ret) {
              LOG_INFO("session already gone, treat as killed", K(session_ids.at(i)), K(addrs.at(i)), K(retry));
            } else {
              all_killed = false;
              LOG_WARN("kill session failed, will retry", KR(tmp_ret), K(session_ids.at(i)), K(addrs.at(i)), K(retry));
            }
          } else {
            LOG_INFO("kill session issued", K(session_ids.at(i)), K(addrs.at(i)), K(retry));
          }
        }
        if (all_killed && !need_retry) {
          kill_done = true;
        }
      }
    }
    if (OB_SUCC(ret) && !kill_done) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("kill refresh local did not converge after retries", KR(ret),
               K(tenant_id), K(refresh_id), K(KILL_RETRY_MAX));
    }
  }
  LOG_INFO("kill refresh local done", KR(ret), K(tenant_id), K(refresh_id));
  return ret;
}

int ObMViewPendingTaskManager::mark_all_tasks_canceled(uint64_t tenant_id, int64_t refresh_id)
{
  int ret = OB_SUCCESS;
  bool refresh_finished = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task manager not init", KR(ret));
  } else if (OB_FAIL(wait_reload_ready(RELOAD_WAIT_TIMEOUT_MS))) {
    LOG_WARN("wait reload ready failed", KR(ret));
  // Step 1: DB — batch UPDATE PENDING/RETRY_WAIT → CANCELLED.
  //
  // Race condition:
  //   1) Scheduler updates a PENDING task (task1) to RUNNING in the DB
  //      (pending_task internal table), but has not yet touched memory.
  //   2) mark_all_tasks_canceled runs:
  //      a) DB batch UPDATE PENDING/RETRY_WAIT → CANCELLED. Since task1
  //         is already RUNNING in the DB, it is NOT affected.
  //      b) Queue-side cancel flips all in-memory PENDING/RETRY_WAIT to
  //         CANCELLED. Since memory still shows task1 as PENDING, task1
  //         is incorrectly set to CANCELLED in memory.
  //   3) Scheduler tries to CAS task1 memory PENDING→RUNNING, but finds
  //      it is already CANCELLED — status mismatch.
  //
  // The scheduler's response to the mismatch is to re-sync memory from
  // the DB on its next reload tick. Since the DB shows task1 as RUNNING,
  // memory is corrected back to RUNNING. Because we no longer write stats
  // rows for cancel, there is no persistent side effect from the transient
  // memory inconsistency. No task is lost or double-run.
  } else if (OB_FAIL(table_operator_.batch_cancel_pending_tasks(tenant_id, refresh_id))) {
    LOG_WARN("batch cancel pending tasks in DB failed", KR(ret), K(tenant_id), K(refresh_id));
    // Step 2: Queue — flip in-memory PENDING/RETRY_WAIT to CANCELLED and
    // report whether the refresh has reached terminal state.
  } else if (OB_FAIL(queue_.cancel_all_pending_tasks(tenant_id,
                                                     refresh_id,
                                                     refresh_finished))) {
    LOG_WARN("cancel all pending tasks in queue failed", KR(ret), K(tenant_id), K(refresh_id));
    // Step 3: Terminal — finalize run_stats first, then recycle.
    // success/failed paths write run_stats via finalize_task; cancel paths
    // never reach finalize_task, so we write it here. For the failure path
    // (finalize_task already wrote run_stats), the SQL guard
    // "AND (result IN (0,1))" makes this write a harmless no-op so the
    // first-wins terminal error/message is preserved.
  } else if (refresh_finished
             && OB_FAIL(ObMViewRefreshStatsUtils::write_run_end(GCTX.sql_proxy_,
                                                                tenant_id,
                                                                refresh_id,
                                                                ObTimeUtility::current_time(),
                                                                0 /*log_purge_time*/,
                                                                OB_CANCELED,
                                                                ObString::make_string("mview refresh was canceled"),
                                                                0 /*num_failures*/))) {
    LOG_WARN("write run end before recycle failed", KR(ret), K(tenant_id), K(refresh_id));
  } else if (refresh_finished && OB_FAIL(inner_recycle_refresh(tenant_id, refresh_id))) {
    LOG_WARN("recycle refresh after mark_all_tasks_canceled failed", KR(ret), K(tenant_id), K(refresh_id));
  }
  return ret;
}

int ObMViewPendingTaskManager::kill_refresh(const obrpc::ObKillMViewRefreshArg &arg, bool force_rpc)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.tenant_id_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task manager not init", KR(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (OB_ISNULL(GCTX.location_service_) || OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location_service or srv_rpc_proxy is null", KR(ret),
             KP(GCTX.location_service_), KP(GCTX.srv_rpc_proxy_));
  } else {
    const int64_t RETRY_CNT_LIMIT = 3;
    int64_t retry_cnt = 0;
    common::ObAddr leader_addr;
    do {
      if (OB_NOT_MASTER == ret) {
        ret = OB_SUCCESS;
        ob_usleep(1_s);
      }
      if (FAILEDx(GCTX.location_service_->get_leader_with_retry_until_timeout(GCONF.cluster_id,
                                                                              tenant_id,
                                                                              share::SYS_LS,
                                                                              leader_addr))) {
        LOG_WARN("fail to get sys ls leader addr", KR(ret), K(tenant_id));
      } else if (!force_rpc && leader_addr == GCONF.self_addr_) {
        if (arg.is_kill_by_mview_id_) {
          if (arg.is_drop_ && OB_FAIL(acquire_mview_block(arg.mview_id_, ObMViewContext::BLOCK_FLAG_DROP))) {
            LOG_WARN("acquire drop block failed", KR(ret), K(arg));
          } else if (OB_FAIL(kill_refreshes_by_mview_local(tenant_id, arg.mview_id_))) {
            LOG_WARN("kill refreshes by mview local failed", KR(ret), K(arg));
          }
        } else {
          if (OB_FAIL(kill_refresh_local(tenant_id, arg.refresh_id_))) {
            LOG_WARN("kill refresh local failed", KR(ret), K(arg));
          }
        }
      } else {
        obrpc::ObKillMViewRefreshResult result;
        if (OB_FAIL(GCTX.srv_rpc_proxy_->to(leader_addr)
                    .by(tenant_id)
                    .timeout(GCONF.rpc_timeout)
                    .kill_mview_refresh(arg, result))) {
          LOG_WARN("kill mview refresh rpc failed", KR(ret), K(arg), K(leader_addr));
        } else if (OB_FAIL(result.ret_)) {
          LOG_WARN("kill mview refresh on leader failed", KR(ret), K(arg), K(leader_addr));
        }
      }
    } while (OB_NOT_MASTER == ret && ++retry_cnt <= RETRY_CNT_LIMIT);
  }
  return ret;
}

int ObMViewPendingTaskManager::kill_refreshes_by_mview_local(uint64_t tenant_id, uint64_t mview_id)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> refresh_ids;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task manager not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || OB_INVALID_ID == mview_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(mview_id));
  } else if (OB_FAIL(table_operator_.get_active_refresh_ids_by_mview(tenant_id, mview_id, refresh_ids))) {
    LOG_WARN("get active refresh ids by mview failed", KR(ret), K(tenant_id), K(mview_id));
  } else {
    // Best-effort: keep going across refresh_ids so one transient failure
    // doesn't leave other refreshes alive holding the mview lock.
    int first_err = OB_SUCCESS;
    for (int64_t i = 0; i < refresh_ids.count(); ++i) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(kill_refresh_local(tenant_id, refresh_ids.at(i)))) {
        LOG_WARN("kill refresh local failed", KR(tmp_ret), K(tenant_id),
                 "refresh_id", refresh_ids.at(i), K(mview_id));
        if (OB_SUCCESS == first_err) {
          first_err = tmp_ret;
        }
      }
    }
    if (OB_SUCC(ret) && OB_SUCCESS != first_err) {
      ret = first_err;
    }
    LOG_INFO("kill refreshes by mview local done", KR(ret), K(tenant_id), K(mview_id),
             "kill_count", refresh_ids.count());
  }
  return ret;
}

// Snapshot every DROP block marker's mview_id under the hashmap bucket lock,
// doing no schema lookup inside the callback. The actual schema check and erase
// happen afterwards in cleanup_stale_drop_blocks, outside the iteration.
struct CollectDropBlockFunctor
{
  explicit CollectDropBlockFunctor(common::ObIArray<uint64_t> &mview_ids)
    : mview_ids_(mview_ids), ret_(common::OB_SUCCESS) {}
  int operator()(common::hash::HashMapPair<uint64_t, ObMViewContext *> &kv)
  {
    int ret = common::OB_SUCCESS;
    if (OB_NOT_NULL(kv.second) &&
        0 != (ATOMIC_LOAD(&kv.second->block_flags_) & ObMViewContext::BLOCK_FLAG_DROP)) {
      if (OB_FAIL(mview_ids_.push_back(kv.first))) {
        RS_LOG(WARN, "push back drop block mview id failed", KR(ret), "mview_id", kv.first);
        ret_ = ret;
      }
    }
    return ret;
  }
  common::ObIArray<uint64_t> &mview_ids_;
  int ret_;
};

int ObMViewPendingTaskManager::try_clear_drop_block_by_lock(
    const uint64_t tenant_id,
    const uint64_t mview_id,
    share::schema::ObSchemaGetterGuard &schema_guard,
    bool &lock_succ)
{
  int ret = OB_SUCCESS;
  lock_succ = false;
  sql::ObFreeSessionCtx free_session_ctx;
  sql::ObSQLSessionInfo *session_info = nullptr;
  ObMViewTransaction trans;
  if (OB_FAIL(ObMViewUtils::create_inner_session(
                  tenant_id, schema_guard, free_session_ctx, session_info))) {
    LOG_WARN("create inner session failed", KR(ret), K(tenant_id), K(mview_id));
  } else if (OB_FAIL(trans.start(session_info, GCTX.sql_proxy_,
                                  session_info->get_database_id(),
                                  session_info->get_database_name()))) {
    LOG_WARN("start trans failed", KR(ret), K(tenant_id), K(mview_id));
  } else if (OB_FAIL(ObMViewRefreshHelper::lock_mview(
                  trans, tenant_id, mview_id, true /*try_lock*/))) {
    if (OB_TRY_LOCK_ROW_CONFLICT == ret || OB_EAGAIN == ret) {
      // DROP DDL transaction is still active; keep the block flag in place.
      LOG_INFO("mview drop DDL still in progress, skip stale block cleanup",
               K(tenant_id), K(mview_id));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("lock mview failed during drop block cleanup",
               KR(ret), K(tenant_id), K(mview_id));
    }
  } else {
    // Lock acquired: no DDL transaction holds it → DROP must have rolled back.
    lock_succ = true;
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    // Always rollback to release the IN_TRANS lock we may have acquired.
    if (OB_TMP_FAIL(trans.end(false /*commit*/))) {
      LOG_WARN("end trans failed", KR(tmp_ret), K(tenant_id), K(mview_id));
    }
  }
  ObMViewUtils::release_inner_session(free_session_ctx, session_info);
  return ret;
}

int ObMViewPendingTaskManager::cleanup_stale_drop_blocks()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObSEArray<uint64_t, 16> mview_ids;
  share::schema::ObSchemaGetterGuard schema_guard;
  CollectDropBlockFunctor collect_fn(mview_ids);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task manager not init", KR(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mview_context_map_.foreach_refactored(collect_fn))) {
    LOG_WARN("foreach mview context map failed", KR(ret));
  } else if (OB_FAIL(collect_fn.ret_)) {
    LOG_WARN("collect drop block mview ids failed", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < mview_ids.count(); ++i) {
    const uint64_t mview_id = mview_ids.at(i);
    bool should_clear = false;
    const share::schema::ObTableSchema *table_schema = nullptr;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(schema_guard.get_table_schema(tenant_id, mview_id, table_schema))) {
      LOG_WARN("get table schema failed, skip this mview",
               KR(tmp_ret), K(tenant_id), K(mview_id));
    } else if (OB_ISNULL(table_schema)) {
      // schema gone: the drop committed, safe to erase the whole context.
      should_clear = true;
    } else {
      bool lock_succ = false;
      if (OB_TMP_FAIL(try_clear_drop_block_by_lock(
                          tenant_id, mview_id, schema_guard, lock_succ))) {
        LOG_WARN("try clear drop block by lock failed",
                 KR(tmp_ret), K(tenant_id), K(mview_id));
      } else if (lock_succ) {
        if (OB_TMP_FAIL(release_mview_block(mview_id, ObMViewContext::BLOCK_FLAG_DROP))) {
          LOG_WARN("release drop block failed", KR(tmp_ret), K(tenant_id), K(mview_id));
        } else {
          LOG_INFO("cleared stale mview drop block (drop rolled back)",
                   K(tenant_id), K(mview_id));
        }
      }
    }
    if (OB_SUCC(ret) && should_clear) {
      if (OB_TMP_FAIL(erase_and_free_ctx(mview_id))) {
        LOG_WARN("erase stale drop block failed", KR(tmp_ret), K(tenant_id), K(mview_id));
      } else {
        LOG_INFO("cleared stale mview drop block (drop committed)",
                 K(tenant_id), K(mview_id));
      }
    }
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
