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

#include "rootserver/mview/ob_mview_pending_task_queue.h"
#include "lib/ob_errno.h"
#include "share/ob_errno.h"
#include "common/ob_smart_call.h"
#include "lib/container/ob_se_array.h"
#include "lib/thread/thread.h"
#include "share/config/ob_server_config.h"
namespace oceanbase
{
namespace rootserver
{
using namespace common;

static const int64_t PENDING_TASK_BUCKET_NUM = 1024;
static const int64_t MAX_PENDING_TASK_CNT = 100000;

ObMViewPendingTaskQueue::ObMViewPendingTaskQueue()
  : pending_map_(),
    refresh_map_(),
    task_list_(),
    rw_lock_(),
    allocator_(),
    total_running_cnt_(0),
    task_count_(0),
    is_inited_(false)
{
}

ObMViewPendingTaskQueue::~ObMViewPendingTaskQueue()
{
  destroy();
}

int ObMViewPendingTaskQueue::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("pending task queue init twice", KR(ret));
  } else if (OB_FAIL(pending_map_.create(PENDING_TASK_BUCKET_NUM,
                                         "MVPendingMap",
                                         "MVPendingMap",
                                         tenant_id))) {
    LOG_WARN("create pending map failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(refresh_map_.create(PENDING_TASK_BUCKET_NUM,
                                         "MVRefreshMap",
                                         "MVRefreshMap",
                                         tenant_id))) {
    LOG_WARN("create refresh map failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(allocator_.init(lib::ObMallocAllocator::get_instance(),
                                     OB_MALLOC_NORMAL_BLOCK_SIZE,
                                     ObMemAttr(tenant_id, "MViewTask")))) {
    LOG_WARN("init allocator failed", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObMViewPendingTaskQueue::destroy()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(clear())) {
    LOG_WARN("clear failed", KR(tmp_ret));
    ret = COVER_SUCC(tmp_ret);
  }
  if (OB_TMP_FAIL(pending_map_.destroy())) {
    LOG_WARN("destroy pending map failed", KR(tmp_ret));
    ret = COVER_SUCC(tmp_ret);
  }
  if (OB_TMP_FAIL(refresh_map_.destroy())) {
    LOG_WARN("destroy refresh map failed", KR(tmp_ret));
    ret = COVER_SUCC(tmp_ret);
  }
  allocator_.reset();
  is_inited_ = false;
  return ret;
}

int ObMViewPendingTaskQueue::free_task(ObMViewPendingTask *task)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(NULL != task)) {
    task->~ObMViewPendingTask();
    allocator_.free(task);
  }
  return ret;
}

int ObMViewPendingTaskQueue::alloc_refresh_ctx(uint64_t tenant_id,
                                               int64_t refresh_id,
                                               const common::ObCurTraceId::TraceId *trace_id,
                                               ObMViewPendingRefreshCtx *&ctx)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  ctx = NULL;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMViewPendingRefreshCtx)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc refresh ctx failed", K(ret));
  } else {
    ctx = new (buf) ObMViewPendingRefreshCtx();
    ctx->tenant_id_ = tenant_id;
    ctx->refresh_id_ = refresh_id;
    if (OB_NOT_NULL(trace_id) && trace_id->is_valid()) {
      ctx->trace_id_ = *trace_id;
    } else {
      ctx->trace_id_.init(GCONF.self_addr_);
    }
  }
  return ret;
}

int ObMViewPendingTaskQueue::free_refresh_ctx(ObMViewPendingRefreshCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(ctx)) {
    ctx->~ObMViewPendingRefreshCtx();
    allocator_.free(ctx);
  }
  return ret;
}

int ObMViewPendingTaskQueue::push_tasks(const ObIArray<ObMViewPendingTask *> &tasks,
                                        const common::ObCurTraceId::TraceId *trace_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("queue not init", KR(ret));
  } else if (OB_UNLIKELY(tasks.empty()) || OB_ISNULL(tasks.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty tasks or first task is null", KR(ret), K(tasks));
  } else {
    ObLatchWGuard guard(rw_lock_, ObLatchIds::MVIEW_TASK_QUEUE_LOCK);
    ObMViewPendingTaskKey task_key(tasks.at(0)->tenant_id_, tasks.at(0)->refresh_id_, tasks.at(0)->mview_id_);
    const ObMViewRefreshKey refresh_key(tasks.at(0)->tenant_id_, tasks.at(0)->refresh_id_);
    ObMViewPendingTask *task_ptr = NULL;
    ObSEArray<ObMViewPendingTask*, 4> copied_tasks;
    ObMViewPendingRefreshCtx *refresh_ctx = NULL;
    ObMViewPendingRefreshCtx ctx_init;
    if (task_count_ + tasks.count() > MAX_PENDING_TASK_CNT) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("pending task queue is full", KR(ret),
               K_(task_count), K(tasks.count()), K(MAX_PENDING_TASK_CNT));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
      if (OB_ISNULL(tasks.at(i)) || OB_UNLIKELY(refresh_key.refresh_id_ != tasks.at(i)->refresh_id_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected params", KR(ret), K(i), K(refresh_key), K(tasks));
      } else if (OB_FAIL(ObMViewPendingTask::deep_copy(allocator_, *tasks.at(i), task_ptr))) {
        LOG_WARN("deep copy task failed", KR(ret), K(i));
      } else if (OB_FAIL(copied_tasks.push_back(task_ptr))) {
        LOG_WARN("push back copied task failed", KR(ret), K(i));
        if (OB_TMP_FAIL(free_task(task_ptr))) {
          LOG_WARN("free task failed", KR(tmp_ret));
        }
      } else if (OB_FALSE_IT(task_key.mview_id_ = tasks.at(i)->mview_id_)) {
      } else if (OB_FAIL(pending_map_.set_refactored(task_key, task_ptr))) {
        LOG_WARN("insert task failed", KR(ret), K(task_key));
      } else if (OB_UNLIKELY(!task_list_.add_last(task_ptr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("add task to task list failed", KR(ret), K(task_key));
      } else if (OB_FAIL(accum_ctx_stats_for_task(*task_ptr, ctx_init))) {
        LOG_WARN("accum ctx stats for task failed", KR(ret), KPC(task_ptr));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(OB_INVALID_ID == ctx_init.root_mview_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no root task found in batch", KR(ret), K(refresh_key), K(tasks.count()));
    } else if (OB_FAIL(alloc_refresh_ctx(refresh_key.tenant_id_, refresh_key.refresh_id_, trace_id, refresh_ctx))) {
      LOG_WARN("alloc refresh ctx failed", KR(ret));
    } else if (OB_FAIL(refresh_map_.set_refactored(refresh_key, refresh_ctx))) {
      LOG_WARN("set refresh ctx failed", KR(ret), K(refresh_key));
    }
    if (OB_SUCC(ret)) {
      refresh_ctx->unfinished_task_cnt_  = ctx_init.unfinished_task_cnt_;
      refresh_ctx->running_task_cnt_     = ctx_init.running_task_cnt_;
      refresh_ctx->root_mview_id_        = ctx_init.root_mview_id_;
      refresh_ctx->has_terminal_failure_ = ctx_init.has_terminal_failure_;
      refresh_ctx->root_task_succeeded_  = ctx_init.root_task_succeeded_;
      task_count_ += tasks.count();
      total_running_cnt_ += ctx_init.running_task_cnt_;
    }

    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(refresh_ctx) && OB_TMP_FAIL(refresh_map_.erase_refactored(refresh_key, &refresh_ctx))) {
        LOG_WARN("erase refresh ctx failed", K(ret), K(refresh_key));
      }
      if (OB_NOT_NULL(refresh_ctx) && OB_TMP_FAIL(free_refresh_ctx(refresh_ctx))) {
        LOG_WARN("free refresh ctx failed", KR(tmp_ret));
      }

      for (int i = 0; i < copied_tasks.count(); ++i) {
        if (OB_NOT_NULL(task_ptr = copied_tasks.at(i))) {
          task_key.mview_id_ = task_ptr->mview_id_;
          if (OB_TMP_FAIL(pending_map_.erase_refactored(task_key, &task_ptr))) {
            LOG_WARN("erase task from pending map failed", K(tmp_ret), K(task_key));
          }
          task_list_.remove(task_ptr);
          if (OB_TMP_FAIL(free_task(task_ptr))) {
            LOG_WARN("free copied task failed", KR(tmp_ret), K(i));
          }
        }
      }
    }
  }
  return ret;
}

int64_t ObMViewPendingTaskQueue::get_total_running_cnt() const
{
  ObLatchRGuard guard(rw_lock_, ObLatchIds::MVIEW_TASK_QUEUE_LOCK);
  return total_running_cnt_;
}

int ObMViewPendingTaskQueue::peek_task(ObMViewPendingTask &task)
{
  int ret = OB_SUCCESS;
  bool found = false;
  bool deps_satisfied = false;
  bool prev_satisfied = false;
  ObMViewPendingTask *cur = NULL;
  ObLatchRGuard guard(rw_lock_, ObLatchIds::MVIEW_TASK_QUEUE_LOCK);
  cur = task_list_.get_first();
  while (OB_SUCC(ret) && !found && cur != task_list_.get_header()) {
    deps_satisfied = false;
    if (MV_TASK_PENDING != cur->status_) {
      cur = cur->get_next();
    } else if (OB_FAIL(check_task_deps_satisfied(*cur, deps_satisfied))) {
      LOG_WARN("check task deps failed", KR(ret), KPC(cur));
    } else if (!deps_satisfied) {
      cur = cur->get_next();
    } else if (OB_FAIL(check_prev_task_satisfied(*cur, prev_satisfied))) {
      LOG_WARN("check prev task satisfied failed", KR(ret), KPC(cur));
    } else if (!prev_satisfied) {
      cur = cur->get_next();
    } else if (OB_FAIL(task.assign(*cur))) {
      LOG_WARN("assign task failed", KR(ret), KPC(cur));
    } else {
      found = true;
    }
  }
  if (OB_SUCC(ret) && !found) {
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObMViewPendingTaskQueue::get_refresh_ctx(uint64_t tenant_id,
                                             int64_t refresh_id,
                                             ObMViewPendingRefreshCtx *&ctx) const
{
  int ret = OB_SUCCESS;
  ObMViewRefreshKey refresh_key(tenant_id, refresh_id);
  ctx = NULL;
  if (OB_FAIL(refresh_map_.get_refactored(refresh_key, ctx))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      LOG_WARN("get refresh ctx failed", KR(ret), K(refresh_key));
    }
  }
  return ret;
}

int ObMViewPendingTaskQueue::get_refresh_trace_id(uint64_t tenant_id,
                                                  int64_t refresh_id,
                                                  common::ObCurTraceId::TraceId &trace_id) const
{
  int ret = OB_SUCCESS;
  ObMViewPendingRefreshCtx *ctx = nullptr;
  ObLatchRGuard guard(rw_lock_, ObLatchIds::MVIEW_TASK_QUEUE_LOCK);
  if (OB_FAIL(get_refresh_ctx(tenant_id, refresh_id, ctx))) {
    LOG_WARN("get refresh ctx failed", KR(ret), K(tenant_id), K(refresh_id));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("refresh ctx is null", KR(ret), K(tenant_id), K(refresh_id));
  } else {
    trace_id = ctx->trace_id_;
  }
  return ret;
}

int ObMViewPendingTaskQueue::refresh_finished_after_status_change(ObMViewPendingRefreshCtx &ctx,
                                                                  bool &refresh_finished) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ctx.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid refresh ctx", KR(ret), K(ctx));
  } else {
    refresh_finished = (ctx.unfinished_task_cnt_ <= 0);
  }
  return ret;
}

int ObMViewPendingTaskQueue::set_task_status(uint64_t tenant_id,
                                             int64_t refresh_id,
                                             uint64_t mview_id,
                                             const int64_t from_status,
                                             const int64_t to_status,
                                             int error_ret,
                                             bool &refresh_finished,
                                             bool ignore_status_check,
                                             const ObAddr *svr_addr)
{
  int ret = OB_SUCCESS;
  ObMViewPendingRefreshCtx *ctx = NULL;
  ObMViewRefreshKey refresh_key(tenant_id, refresh_id);
  refresh_finished = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("queue not init", KR(ret));
  } else {
    ObLatchWGuard guard(rw_lock_, ObLatchIds::MVIEW_TASK_QUEUE_LOCK);
    bool task_set_failed = false;
    if (OB_FAIL(refresh_map_.get_refactored(refresh_key, ctx))) {
      LOG_WARN("get refresh ctx failed", KR(ret), K(refresh_key));
    } else if (OB_ISNULL(ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("refresh ctx is null", KR(ret), K(refresh_key));
    } else if (OB_FAIL(inner_set_task_status(*ctx,
                                             tenant_id,
                                             refresh_id,
                                             mview_id,
                                             from_status,
                                             to_status,
                                             ignore_status_check,
                                             svr_addr))) {
      LOG_WARN("inner set task status failed", KR(ret), K(tenant_id),
               K(refresh_id), K(mview_id), K(from_status), K(to_status),
               K(ignore_status_check));
    } else if (OB_UNLIKELY(to_status == MV_TASK_FAILED)) {
      task_set_failed = true;
    }
    if (OB_SUCC(ret) && OB_FAIL(refresh_finished_after_status_change(*ctx, refresh_finished))) {
      LOG_WARN("check refresh finished after status change failed", KR(ret), KPC(ctx));
    }
  }
  return ret;
}

int ObMViewPendingTaskQueue::inner_set_task_status(ObMViewPendingRefreshCtx &ctx,
                                                   uint64_t tenant_id,
                                                   int64_t refresh_id,
                                                   uint64_t mview_id,
                                                   const int64_t from_status,
                                                   const int64_t to_status,
                                                   bool ignore_status_check,
                                                   const ObAddr *svr_addr)
{
  int ret = OB_SUCCESS;
  ObMViewPendingTaskKey task_key(tenant_id, refresh_id, mview_id);
  ObMViewPendingTask *task = NULL;
  if (OB_FAIL(pending_map_.get_refactored(task_key, task))) {
    LOG_WARN("get task failed", KR(ret), K(task_key));
  } else if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task is null", KR(ret), K(task_key));
  } else if (OB_UNLIKELY(!ignore_status_check && task->status_ != from_status)) {
    ret = OB_EAGAIN;
    LOG_WARN("task status not match, try again", KR(ret), K(task_key), K(task->status_), K(from_status), K(to_status));
  } else if (OB_UNLIKELY(MV_TASK_FAILED == task->status_ ||
                         MV_TASK_CANCELLED == task->status_ ||
                         MV_TASK_SUCCESS == task->status_)) {
    // do nothing
  } else {
    const int64_t old_status = task->status_;
    task->status_ = to_status;
    task->gmt_modified_ = ObTimeUtility::current_time();
    if (MV_TASK_RUNNING == to_status) {
      ++ctx.running_task_cnt_;
      ++total_running_cnt_;
      if (NULL != svr_addr && svr_addr->is_valid()) {
        task->svr_addr_ = *svr_addr;
      }
    } else if (MV_TASK_SUCCESS == to_status) {
      // running counter delta depends on the actual leaving state. normal
      // success path is RUNNING -> SUCCESS; align path may enter from
      // PENDING / RETRY_WAIT and must not touch running counters.
      if (MV_TASK_RUNNING == old_status) {
        --ctx.running_task_cnt_;
        --total_running_cnt_;
      }
      --ctx.unfinished_task_cnt_;
      if (task->mview_id_ == ctx.root_mview_id_) {
        ctx.root_task_succeeded_ = true;
      }
    } else if (MV_TASK_RETRY_WAIT == to_status) {
      if (MV_TASK_RUNNING == old_status) {
        --ctx.running_task_cnt_;
        --total_running_cnt_;
      }
      ++task->retry_count_;
    } else if (MV_TASK_FAILED == to_status) {
      if (MV_TASK_RUNNING == old_status) {
        --ctx.running_task_cnt_;
        --total_running_cnt_;
      }
      --ctx.unfinished_task_cnt_;
      ctx.has_terminal_failure_ = true;
    } else if (MV_TASK_PENDING == to_status) {
      if (MV_TASK_RUNNING == old_status) {
        --ctx.running_task_cnt_;
        --total_running_cnt_;
        ++task->retry_count_;
      }
    } else if (MV_TASK_CANCELLED == to_status) {
      if (MV_TASK_RUNNING == old_status) {
        --ctx.running_task_cnt_;
        --total_running_cnt_;
      }
      --ctx.unfinished_task_cnt_;
    }
  }
  return ret;
}

int ObMViewPendingTaskQueue::inner_recursive_cancel_dept_tasks(
                             ObMViewPendingRefreshCtx &ctx,
                             uint64_t tenant_id,
                             int64_t refresh_id,
                             uint64_t mview_id)
{
  int ret = OB_SUCCESS;
  ObMViewPendingTaskKey task_key(tenant_id, refresh_id, mview_id);
  ObMViewPendingTask *task = NULL;
  if (OB_FAIL(pending_map_.get_refactored(task_key, task))) {
    LOG_WARN("get task failed", KR(ret), K(task_key));
  } else if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task is null", KR(ret), K(task_key));
  } else {
    const int64_t old_status = task->status_;
    if (MV_TASK_PENDING == old_status || MV_TASK_RETRY_WAIT == old_status) {
      task->status_ = MV_TASK_CANCELLED;
      task->gmt_modified_ = ObTimeUtility::current_time();
      --ctx.unfinished_task_cnt_;
      LOG_INFO("cancel pending mview task", K(tenant_id), K(refresh_id), K(mview_id), K(old_status));
    }
    // always iterate dependent tasks so that pending/retry_wait deps can still be cancelled
    // even when the current task is already in a terminal or running state
    for (int64_t i = 0; OB_SUCC(ret) && i < task->dep_mview_id_cnt_; ++i) {
      if (OB_FAIL(SMART_CALL(inner_recursive_cancel_dept_tasks(ctx,
                                                               tenant_id,
                                                               refresh_id,
                                                               task->dep_mview_ids_[i])))) {
        LOG_WARN("inner recursive cancel dept tasks failed", KR(ret), K(tenant_id), K(refresh_id), K(task->dep_mview_ids_[i]));
      }
    }
  }
  return ret;
}

int ObMViewPendingTaskQueue::set_task_running(uint64_t tenant_id,
                                              int64_t refresh_id,
                                              uint64_t mview_id,
                                              const ObAddr *svr_addr)
{
  int ret = OB_SUCCESS;
  bool refresh_finished = false;
  if (OB_FAIL(set_task_status(tenant_id,
                               refresh_id,
                               mview_id,
                               MV_TASK_PENDING,
                               MV_TASK_RUNNING,
                               OB_SUCCESS,
                               refresh_finished,
                               false /*ignore_status_check*/,
                               svr_addr))) {
    LOG_WARN("set task running failed", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
  }
  return ret;
}

int ObMViewPendingTaskQueue::set_task_success(uint64_t tenant_id,
                                              int64_t refresh_id,
                                              uint64_t mview_id,
                                              bool &refresh_finished)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_task_status(tenant_id,
                               refresh_id,
                               mview_id,
                               MV_TASK_RUNNING,
                               MV_TASK_SUCCESS,
                               OB_SUCCESS,
                               refresh_finished))) {
    LOG_WARN("set task success failed", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
  }
  return ret;
}

int ObMViewPendingTaskQueue::set_task_retry_wait(uint64_t tenant_id,
                                                 int64_t refresh_id,
                                                 uint64_t mview_id,
                                                 bool &refresh_finished)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_task_status(tenant_id,
                               refresh_id,
                               mview_id,
                               MV_TASK_RUNNING,
                               MV_TASK_RETRY_WAIT,
                               OB_SUCCESS,
                               refresh_finished))) {
    LOG_WARN("set task retry wait failed", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
  }
  return ret;
}

int ObMViewPendingTaskQueue::set_task_failed(uint64_t tenant_id,
                                             int64_t refresh_id,
                                             uint64_t mview_id,
                                             int error_ret,
                                             bool &refresh_finished)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_task_status(tenant_id,
                               refresh_id,
                               mview_id,
                               MV_TASK_RUNNING,
                               MV_TASK_FAILED,
                               error_ret,
                               refresh_finished,
                               true /*ignore_status_check*/))) {
    LOG_WARN("set task failed failed", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
  }
  return ret;
}

int ObMViewPendingTaskQueue::set_task_cancelled(uint64_t tenant_id,
                                                int64_t refresh_id,
                                                uint64_t mview_id,
                                                bool &refresh_finished)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_task_status(tenant_id,
                               refresh_id,
                               mview_id,
                               MV_TASK_PENDING,
                               MV_TASK_CANCELLED,
                               OB_SUCCESS,
                               refresh_finished,
                               true))) {
    LOG_WARN("set task cancelled failed", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
  }
  return ret;
}

int ObMViewPendingTaskQueue::set_task_pending(uint64_t tenant_id,
                                              int64_t refresh_id,
                                              uint64_t mview_id)
{
  int ret = OB_SUCCESS;
  bool refresh_finished = false;
  if (OB_FAIL(set_task_status(tenant_id,
                              refresh_id,
                              mview_id,
                              MV_TASK_RETRY_WAIT,
                              MV_TASK_PENDING,
                              OB_SUCCESS,
                              refresh_finished))) {
    LOG_WARN("set task pending failed", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
  }
  return ret;
}

int ObMViewPendingTaskQueue::set_task_running_to_pending(uint64_t tenant_id,
                                                          int64_t refresh_id,
                                                          uint64_t mview_id,
                                                          bool &refresh_finished)
{
  int ret = OB_SUCCESS;
  refresh_finished = false;
  if (OB_FAIL(set_task_status(tenant_id,
                              refresh_id,
                              mview_id,
                              MV_TASK_RUNNING,
                              MV_TASK_PENDING,
                              OB_SUCCESS,
                              refresh_finished))) {
    LOG_WARN("set task running to pending failed", KR(ret),
             K(tenant_id), K(refresh_id), K(mview_id));
  }
  return ret;
}

int ObMViewPendingTaskQueue::cancel_all_pending_tasks(uint64_t tenant_id, int64_t refresh_id, bool &refresh_finished)
{
  int ret = OB_SUCCESS;
  refresh_finished = false;
  ObMViewRefreshKey refresh_key(tenant_id, refresh_id);
  ObMViewPendingRefreshCtx *ctx = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("queue not init", KR(ret));
  } else {
    ObLatchWGuard guard(rw_lock_, ObLatchIds::MVIEW_TASK_QUEUE_LOCK);
    if (OB_FAIL(refresh_map_.get_refactored(refresh_key, ctx))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("refresh ctx gone, nothing to cancel", K(tenant_id), K(refresh_id));
      } else {
        LOG_WARN("get refresh ctx failed", KR(ret), K(refresh_key));
      }
    } else if (OB_ISNULL(ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("refresh ctx is null", KR(ret), K(refresh_key));
    } else if (OB_FALSE_IT(ctx->cancelled_ = true)) {
    } else if (OB_FAIL(inner_recursive_cancel_dept_tasks(*ctx,
                                                         tenant_id,
                                                         refresh_id,
                                                         ctx->root_mview_id_))) {
      LOG_WARN("inner recursive cancel dept tasks failed", KR(ret), K(tenant_id), K(refresh_id));
    } else if (OB_FAIL(refresh_finished_after_status_change(*ctx, refresh_finished))) {
      LOG_WARN("check refresh finished failed", KR(ret), K(tenant_id), K(refresh_id));
    }
  }
  return ret;
}

int ObMViewPendingTaskQueue::get_task_status(uint64_t tenant_id,
                                              int64_t refresh_id,
                                              uint64_t mview_id,
                                              int64_t &status) const
{
  int ret = OB_SUCCESS;
  ObMViewPendingTaskKey task_key(tenant_id, refresh_id, mview_id);
  ObMViewPendingTask *task = NULL;
  ObLatchRGuard guard(rw_lock_, ObLatchIds::MVIEW_TASK_QUEUE_LOCK);
  if (OB_FAIL(pending_map_.get_refactored(task_key, task))) {
    LOG_WARN("get task failed", KR(ret), K(task_key));
  } else if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task is null", KR(ret), K(task_key));
  } else {
    status = task->status_;
  }
  return ret;
}

int ObMViewPendingTaskQueue::is_refresh_cancelled(uint64_t tenant_id,
                                                   int64_t refresh_id,
                                                   bool &cancelled) const
{
  int ret = OB_SUCCESS;
  cancelled = false;
  ObMViewRefreshKey refresh_key(tenant_id, refresh_id);
  ObMViewPendingRefreshCtx *ctx = NULL;
  ObLatchRGuard guard(rw_lock_, ObLatchIds::MVIEW_TASK_QUEUE_LOCK);
  if (OB_FAIL(refresh_map_.get_refactored(refresh_key, ctx))) {
    LOG_WARN("get refresh ctx failed", KR(ret), K(refresh_key));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("refresh ctx is null", KR(ret), K(refresh_key));
  } else {
    cancelled = ctx->cancelled_;
  }
  return ret;
}

int ObMViewPendingTaskQueue::get_task_retry_count(uint64_t tenant_id,
                                                  int64_t refresh_id,
                                                  uint64_t mview_id,
                                                  int64_t &retry_count) const
{
  int ret = OB_SUCCESS;
  ObMViewPendingTaskKey task_key(tenant_id, refresh_id, mview_id);
  ObMViewPendingTask *task = NULL;
  ObLatchRGuard guard(rw_lock_, ObLatchIds::MVIEW_TASK_QUEUE_LOCK);
  if (OB_FAIL(pending_map_.get_refactored(task_key, task))) {
    LOG_WARN("get task failed", KR(ret), K(task_key));
  } else if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task is null", KR(ret), K(task_key));
  } else {
    retry_count = task->retry_count_;
  }
  return ret;
}

int ObMViewPendingTaskQueue::recycle_refresh(uint64_t tenant_id,
                                             int64_t refresh_id)
{
  int ret = OB_SUCCESS;
  ObMViewRefreshKey refresh_key(tenant_id, refresh_id);
  ObMViewPendingRefreshCtx *refresh_ctx = NULL;
  ObSEArray<ObMViewPendingTask *, 16> erased_tasks;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("queue not init", KR(ret));
  } else {
    ObLatchWGuard guard(rw_lock_, ObLatchIds::MVIEW_TASK_QUEUE_LOCK);
    if (OB_FAIL(refresh_map_.get_refactored(refresh_key, refresh_ctx))) {
      LOG_WARN("get refresh ctx failed", KR(ret), K(refresh_key));
    } else if (OB_ISNULL(refresh_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("refresh ctx is null", KR(ret), K(refresh_key));
    } else if (OB_FAIL(inner_recursive_recycle_tasks(tenant_id, refresh_id,
                                                     refresh_ctx->root_mview_id_,
                                                     erased_tasks))) {
      LOG_WARN("inner recursive recycle tasks failed", KR(ret), K(refresh_key));
    } else {
      ObMViewPendingRefreshCtx *erase_ctx = NULL;
      if (OB_FAIL(refresh_map_.erase_refactored(refresh_key, &erase_ctx))) {
        LOG_WARN("erase refresh ctx failed", KR(ret), K(refresh_key));
      }
    }
  }
  {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; i < erased_tasks.count(); ++i) {
      LOG_TRACE("free task", KR(ret), KPC(erased_tasks.at(i)));
      if (OB_TMP_FAIL(free_task(erased_tasks.at(i)))) {
        LOG_WARN("free task failed", KR(tmp_ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(free_refresh_ctx(refresh_ctx))) {
      LOG_WARN("free refresh ctx failed", KR(tmp_ret), K(refresh_ctx));
    }
  }
  return ret;
}

int ObMViewPendingTaskQueue::inner_recursive_recycle_tasks(uint64_t tenant_id,
                                                           int64_t refresh_id,
                                                           uint64_t mview_id,
                                                           ObIArray<ObMViewPendingTask *> &erased_tasks)
{
  int ret = OB_SUCCESS;
  ObMViewPendingTaskKey task_key(tenant_id, refresh_id, mview_id);
  ObMViewPendingTask *task = NULL;
  if (OB_FAIL(pending_map_.get_refactored(task_key, task))) {
    if (OB_HASH_NOT_EXIST == ret) {
      // Already recycled by a previous (possibly partial) call — treat as success
      // so the caller can continue recycling the rest of the tree.
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("get task failed", KR(ret), K(task_key));
    }
  } else if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task is null", KR(ret), K(task_key));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task->dep_mview_id_cnt_; ++i) {
      if (OB_FAIL(SMART_CALL(inner_recursive_recycle_tasks(tenant_id, refresh_id, task->dep_mview_ids_[i], erased_tasks)))) {
        LOG_WARN("inner recursive recycle tasks failed", KR(ret),
                 K(tenant_id), K(refresh_id), K(task->dep_mview_ids_[i]));
      }
    }
    if (OB_SUCC(ret)) {
      ObMViewPendingTask *erase_task = NULL;
      if (OB_FAIL(pending_map_.erase_refactored(task_key, &erase_task))) {
        LOG_WARN("erase task failed", KR(ret), K(task_key));
      } else if (OB_FALSE_IT(task_list_.remove(erase_task))) {
      } else if (OB_FALSE_IT(--task_count_)) {
      } else if (OB_FAIL(erased_tasks.push_back(erase_task))) {
        LOG_WARN("push back erased task failed", KR(ret), K(task_key));
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(free_task(erase_task))) {
          LOG_WARN("free task after push back failed", KR(tmp_ret), K(task_key));
        }
      }
    }
  }
  return ret;
}

int ObMViewPendingTaskQueue::get_refresh_status(uint64_t tenant_id,
                                                int64_t refresh_id,
                                                ObMViewTaskStatus &status) const
{
  int ret = OB_SUCCESS;
  ObMViewPendingRefreshCtx *ctx = NULL;
  ObLatchRGuard guard(rw_lock_, ObLatchIds::MVIEW_TASK_QUEUE_LOCK);
  if (OB_FAIL(get_refresh_ctx(tenant_id, refresh_id, ctx))) {
    LOG_WARN("get refresh ctx failed", KR(ret), K(tenant_id), K(refresh_id));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("refresh ctx is null", KR(ret), K(tenant_id), K(refresh_id));
  } else if (ctx->running_task_cnt_ > 0) {
    status = MV_TASK_RUNNING;
  } else if (ctx->unfinished_task_cnt_ > 0) {
    status = MV_TASK_PENDING;
  } else if (ctx->has_terminal_failure_) {
    status = MV_TASK_FAILED;
  } else {
    status = MV_TASK_SUCCESS;
  }
  return ret;
}

int ObMViewPendingTaskQueue::accum_ctx_stats_for_task(const ObMViewPendingTask &task,
                                                      ObMViewPendingRefreshCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (task.is_root_task()) {
    ctx.root_mview_id_ = task.mview_id_;
    if (MV_TASK_SUCCESS == task.status_) {
      ctx.root_task_succeeded_ = true;
    }
  }
  switch (task.status_) {
    case MV_TASK_PENDING:
    case MV_TASK_RETRY_WAIT:
      ++ctx.unfinished_task_cnt_;
      break;
    case MV_TASK_RUNNING:
      ++ctx.unfinished_task_cnt_;
      ++ctx.running_task_cnt_;
      break;
    case MV_TASK_FAILED:
      ctx.has_terminal_failure_ = true;
      break;
    case MV_TASK_SUCCESS:
    case MV_TASK_CANCELLED:
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected task status on load", KR(ret), K(task));
      break;
  }
  return ret;
}

int ObMViewPendingTaskQueue::check_task_deps_satisfied(const ObMViewPendingTask &task,
                                                       bool &satisfied) const
{
  int ret = OB_SUCCESS;
  satisfied = true;
  for (int64_t i = 0; OB_SUCC(ret) && satisfied && i < task.dep_mview_id_cnt_; ++i) {
    ObMViewPendingTask *dep_task = NULL;
    ObMViewPendingTaskKey dep_key(task.tenant_id_, task.refresh_id_, task.dep_mview_ids_[i]);
    if (OB_FAIL(pending_map_.get_refactored(dep_key, dep_task))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get dep task failed", KR(ret), K(dep_key));
      }
    } else if (OB_ISNULL(dep_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dep task is null", KR(ret), K(dep_key));
    } else if (MV_TASK_SUCCESS != dep_task->status_) {
      satisfied = false;
    }
  }
  return ret;
}

int ObMViewPendingTaskQueue::check_prev_task_satisfied(const ObMViewPendingTask &task,
                                                       bool &satisfied) const
{
  int ret = OB_SUCCESS;
  satisfied = false;
  const ObMViewPendingTask *header = task_list_.get_header();
  const ObMViewPendingTask *prev_task = NULL;
  const ObMViewPendingTask *cur = NULL;
  for (cur = task.get_prev(); NULL == prev_task && cur != header && OB_SUCC(ret); cur = cur->get_prev()) {
    if (OB_ISNULL(cur)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur is null", KR(ret), K(task));
    } else if (task.mview_id_ == cur->mview_id_ &&
               (MV_TASK_CANCELLED != cur->status_ ||
                (MV_TASK_CANCELLED == cur->status_ && cur->retry_count_ > 0))) {
      prev_task = cur;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (NULL == prev_task
             || MV_TASK_FAILED == prev_task->status_
             || MV_TASK_CANCELLED == prev_task->status_) {
    satisfied = true;
  } else if (is_task_status_non_terminal(prev_task->status_)) {
    satisfied = false;
  } else if (OB_UNLIKELY(MV_TASK_SUCCESS != prev_task->status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected prev task status", KR(ret), KPC(prev_task));
  } else {
    satisfied = true;
    for (cur = prev_task->get_next(); satisfied && cur != &task && OB_SUCC(ret); cur = cur->get_next()) {
      if (OB_ISNULL(cur)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cur is null", KR(ret), KPC(prev_task));
      } else if (prev_task->refresh_id_ != cur->refresh_id_ || !is_task_status_non_terminal(cur->status_)) {
        // do nothing
      } else {
        for (int64_t i = 0; satisfied && i < cur->dep_mview_id_cnt_; ++i) {
          if (prev_task->mview_id_ == cur->dep_mview_ids_[i]) {
            satisfied = false;
          }
        }
      }
    }
  }
  return ret;
}

struct FreeRefreshCtxFn {
  explicit FreeRefreshCtxFn(common::ObFIFOAllocator &alloc) : alloc_(alloc) {}
  int operator()(common::hash::HashMapPair<ObMViewRefreshKey,
                                           ObMViewPendingRefreshCtx *> &pair) {
    if (OB_NOT_NULL(pair.second)) {
      pair.second->~ObMViewPendingRefreshCtx();
      alloc_.free(pair.second);
      pair.second = nullptr;
    }
    return OB_SUCCESS;
  }
  common::ObFIFOAllocator &alloc_;
};

int ObMViewPendingTaskQueue::clear()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_LIKELY(is_inited_)) {
    ObLatchWGuard guard(rw_lock_, ObLatchIds::MVIEW_TASK_QUEUE_LOCK);
    FreeRefreshCtxFn free_ctx_fn(allocator_);
    {
      ObMViewPendingTask *cur = task_list_.get_first();
      while (cur != task_list_.get_header()) {
        ObMViewPendingTask *next = cur->get_next();
        cur->~ObMViewPendingTask();
        allocator_.free(cur);
        cur = next;
      }
    }
    if (OB_TMP_FAIL(refresh_map_.foreach_refactored(free_ctx_fn))) {
      LOG_WARN("free refresh ctx failed", KR(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
    if (OB_TMP_FAIL(pending_map_.clear())) {
      LOG_WARN("clear pending map failed", KR(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
    if (OB_TMP_FAIL(refresh_map_.clear())) {
      LOG_WARN("clear refresh map failed", KR(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
    task_list_.clear();
    total_running_cnt_ = 0;
    task_count_ = 0;
    allocator_.reuse();
  }
  return ret;
}

int ObMViewPendingTaskQueue::get_refresh_ctx_for_test(uint64_t tenant_id,
                                                      int64_t refresh_id,
                                                      ObMViewPendingRefreshCtx &ctx) const
{
  int ret = OB_SUCCESS;
  ObLatchRGuard guard(rw_lock_, ObLatchIds::MVIEW_TASK_QUEUE_LOCK);
  ObMViewPendingRefreshCtx *ptr = nullptr;
  if (OB_FAIL(get_refresh_ctx(tenant_id, refresh_id, ptr))) {
    LOG_WARN("refresh ctx not found", KR(ret), K(tenant_id), K(refresh_id));
  } else {
    ctx = *ptr;
  }
  return ret;
}

int ObMViewPendingTaskQueue::collect_running_jobs(
    ObIArray<ObMViewPendingRunningJobInfo> &infos) const
{
  int ret = OB_SUCCESS;
  infos.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("queue not init", KR(ret));
  } else {
    ObLatchRGuard guard(rw_lock_, ObLatchIds::MVIEW_TASK_QUEUE_LOCK);
    const ObMViewPendingTask *cur = task_list_.get_first();
    for (; OB_SUCC(ret) && cur != task_list_.get_header(); cur = cur->get_next()) {
      if (MV_TASK_RUNNING == cur->status_) {
        ObMViewPendingRunningJobInfo info;
        info.tenant_id_ = cur->tenant_id_;
        info.refresh_id_ = cur->refresh_id_;
        info.mview_id_ = cur->mview_id_;
        info.target_data_sync_scn_ = cur->target_data_sync_scn_;
        info.refresh_method_ = cur->refresh_method_;
        info.refresh_parallel_ = cur->refresh_parallel_;
        info.gmt_create_ = cur->gmt_create_;
        info.gmt_modified_ = cur->gmt_modified_;
        info.session_id_ = cur->session_id_;
        if (OB_FAIL(infos.push_back(info))) {
          LOG_WARN("push back running job info failed", KR(ret), K(info));
        }
      }
    }
  }
  return ret;
}

int ObMViewPendingTaskQueue::patch_session_ids_for_running_tasks(
    const ObIArray<ObMViewPendingTaskSessionIdEntry> &session_id_entries)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("queue not init", KR(ret));
  } else if (session_id_entries.empty()) {
    // nothing to patch
  } else {
    for (int64_t i = 0; i < session_id_entries.count(); ++i) {
      const uint32_t sid = session_id_entries.at(i).session_id_;
      if (0 != sid) {
        ObLatchWGuard guard(rw_lock_, ObLatchIds::MVIEW_TASK_QUEUE_LOCK);
        ObMViewPendingTask *task = nullptr;
        const int tmp_ret = pending_map_.get_refactored(session_id_entries.at(i).key_, task);
        if (OB_SUCCESS == tmp_ret
            && OB_NOT_NULL(task)
            && MV_TASK_RUNNING == task->status_
            && 0 == task->session_id_) {
          task->session_id_ = sid;
        }
      }
    }
  }
  return ret;
}

// Align in-memory state to disk state. Disk is the authority; memory is
// rewritten to match it. Reuses inner_set_task_status with ignore_status_check
// for the actual transition + counter bookkeeping; only adds:
//   * memory absent surface-up                (task or ctx not in memory)
//   * same-state shortcut                     (mem == target; only refresh svr_addr on RUNNING)
//   * terminal-mem-vs-different-disk          (LOG_WARN, do not touch memory)
int ObMViewPendingTaskQueue::align_task_to_status(uint64_t tenant_id,
                                                  int64_t refresh_id,
                                                  uint64_t mview_id,
                                                  int64_t target_status,
                                                  bool &memory_absent,
                                                  bool &refresh_finished,
                                                  const ObAddr *svr_addr)
{
  int ret = OB_SUCCESS;
  memory_absent = false;
  refresh_finished = false;
  ObMViewRefreshKey refresh_key(tenant_id, refresh_id);
  ObMViewPendingTaskKey task_key(tenant_id, refresh_id, mview_id);
  ObMViewPendingRefreshCtx *ctx = NULL;
  ObMViewPendingTask *task = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("queue not init", KR(ret));
  } else {
    ObLatchWGuard guard(rw_lock_, ObLatchIds::MVIEW_TASK_QUEUE_LOCK);
    if (OB_FAIL(refresh_map_.get_refactored(refresh_key, ctx))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        memory_absent = true;
      } else {
        LOG_WARN("get refresh ctx failed", KR(ret), K(refresh_key));
      }
    } else if (OB_ISNULL(ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("refresh ctx is null", KR(ret), K(refresh_key));
    } else if (OB_FAIL(pending_map_.get_refactored(task_key, task))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        memory_absent = true;
      } else {
        LOG_WARN("get task failed", KR(ret), K(task_key));
      }
    } else if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task is null", KR(ret), K(task_key));
    } else {
      const int64_t mem_status = task->status_;
      const bool mem_is_terminal = (MV_TASK_SUCCESS == mem_status ||
                                    MV_TASK_FAILED == mem_status ||
                                    MV_TASK_CANCELLED == mem_status);
      if (mem_status == target_status) {
        // memory already matches disk. only refresh svr_addr on RUNNING.
        if (MV_TASK_RUNNING == target_status
            && NULL != svr_addr && svr_addr->is_valid()) {
          task->svr_addr_ = *svr_addr;
        }
      } else if (mem_is_terminal) {
        // memory is already terminal but disk reports a different state.
        // memory terminals are reached only after the DB row was updated first,
        // so this should not happen in practice. surface as anomaly without
        // touching memory.
        LOG_WARN("align task: memory terminal differs from disk, skip",
                 K(tenant_id), K(refresh_id), K(mview_id),
                 K(mem_status), K(target_status));
      } else if (OB_FAIL(inner_set_task_status(*ctx, tenant_id, refresh_id, mview_id,
                                               mem_status, target_status,
                                               true /*ignore_status_check*/,
                                               svr_addr))) {
        LOG_WARN("align task: inner set task status failed", KR(ret),
                 K(tenant_id), K(refresh_id), K(mview_id),
                 K(mem_status), K(target_status));
      } else {
        LOG_INFO("align task: realigned memory to disk",
                 K(tenant_id), K(refresh_id), K(mview_id),
                 K(mem_status), K(target_status));
      }
      if (OB_SUCC(ret) && OB_FAIL(refresh_finished_after_status_change(*ctx, refresh_finished))) {
        LOG_WARN("check refresh finished after align failed", KR(ret), KPC(ctx));
      }
    }
  }
  return ret;
}



} // namespace rootserver
} // namespace oceanbase
