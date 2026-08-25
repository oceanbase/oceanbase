/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "sql/table_format/common/utils/ob_lake_table_executor.h"

#include "lib/allocator/ob_malloc.h"
#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/thread/thread_mgr.h"
#include "lib/worker.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/ob_thread_mgr.h"
#include "share/rc/ob_tenant_base.h"

#include <new>

#define USING_LOG_PREFIX SQL

namespace oceanbase
{
namespace sql
{
namespace lake_table
{

// Thin wrapper so that a std::function<void()> can be passed through
// TG_PUSH_TASK (which transports a raw void*).
struct LakeTableTask
{
  explicit LakeTableTask(std::function<void()> &&f)
      : func_(std::move(f)),
        timeout_ts_(THIS_WORKER.get_timeout_ts())
  {
    trace_id_.set(*common::ObCurTraceId::get_trace_id());
  }
  std::function<void()> func_;
  common::ObCurTraceId::TraceId trace_id_;
  int64_t timeout_ts_;
};

struct LakeTableTaskGroupFinishTask
{
  LakeTableTaskGroupFinishTask(ObLakeTableTaskGroupHandle task_group,
                               std::function<int()> &&func)
      : task_group_(task_group),
        func_(std::move(func))
  {
  }

  void operator()() const
  {
    int task_ret = func_();
    task_group_->finish(task_ret);
  }

  ObLakeTableTaskGroupHandle task_group_;
  std::function<int()> func_;
};

// LakeTableTask is allocated on the producing thread and freed on the executor
// worker thread (cross-thread), so it MUST go through OB's thread-safe global
// allocator (ob_malloc / ob_free) — NOT a per-thread arena memctx (those are
// not thread-safe) and NOT global `new`/`delete` (OB does not override global
// operator new, so those bypass tenant memory accounting). ob_malloc also
// charges the allocation to the executor's tenant under a labeled attr.
// These helpers centralize the ob_malloc + placement-new / dtor + ob_free pair.
static LakeTableTask *new_lake_table_task(uint64_t tenant_id, std::function<void()> &&func)
{
  void *mem = ob_malloc(sizeof(LakeTableTask), ObMemAttr(tenant_id, "LakeTableTask"));
  return OB_ISNULL(mem) ? nullptr : new (mem) LakeTableTask(std::move(func));
}

static void delete_lake_table_task(LakeTableTask *task)
{
  if (OB_NOT_NULL(task)) {
    task->~LakeTableTask();
    ob_free(task);
  }
}

int ObLakeTableExecutor::mtl_init(ObLakeTableExecutor *&exec)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(exec->init())) {
    LOG_WARN("lake table executor init failed", K(ret));
  }
  return ret;
}

ObLakeTableExecutor::ObLakeTableExecutor()
    : is_inited_(false),
      tg_id_(-1),
      tenant_id_(MTL_ID())
{
}

ObLakeTableExecutor::~ObLakeTableExecutor()
{
  destroy();
}

int ObLakeTableExecutor::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("lake table executor already inited", K(ret));
  } else if (FALSE_IT(tenant_id_ = MTL_ID())) {
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::LakeTableExecutor, tg_id_))) {
    LOG_WARN("lake table executor TG_CREATE_TENANT failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObLakeTableExecutor::start()
{
  int ret = OB_SUCCESS;
  int64_t thread_cnt = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lake table executor not inited", K(ret));
  } else if (OB_FAIL(get_config_thread_cnt_(thread_cnt))) {
    LOG_WARN("failed to get lake table executor thread count", K(ret), K_(tenant_id));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    LOG_WARN("lake table executor TG_SET_HANDLER_AND_START failed", K(ret), K_(tg_id));
  } else if (OB_FAIL(TG_SET_THREAD_CNT(tg_id_, thread_cnt))) {
    LOG_WARN("lake table executor TG_SET_THREAD_CNT failed", K(ret), K_(tg_id), K(thread_cnt));
  }
  return ret;
}

int ObLakeTableExecutor::get_config_thread_cnt_(int64_t &thread_cnt) const
{
  int ret = OB_SUCCESS;
  thread_cnt = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (OB_UNLIKELY(!tenant_config.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant config is invalid", K(ret), K_(tenant_id));
  } else {
    const int64_t config_thread_cnt = tenant_config->lake_table_pruning_thread_count;
    if (OB_UNLIKELY(config_thread_cnt <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid lake table executor thread count", K(ret), K_(tenant_id), K(config_thread_cnt));
    } else {
      thread_cnt = config_thread_cnt;
    }
  }
  return ret;
}

int ObLakeTableExecutor::apply_thread_cnt_(const int64_t thread_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t old_thread_cnt = get_thread_cnt();
  if (OB_UNLIKELY(!is_inited_ || -1 == tg_id_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lake table executor not inited", K(ret), K_(tg_id));
  } else if (OB_UNLIKELY(thread_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid thread count", K(ret), K(thread_cnt), K_(tg_id));
  } else if (old_thread_cnt != thread_cnt && OB_FAIL(TG_SET_THREAD_CNT(tg_id_, thread_cnt))) {
    LOG_WARN("lake table executor set thread count failed", K(ret), K_(tg_id), K(old_thread_cnt), K(thread_cnt));
  } else {
    LOG_TRACE("lake table executor thread count updated", K_(tg_id), K(old_thread_cnt), K(thread_cnt), K(get_thread_cnt()));
  }
  return ret;
}

int ObLakeTableExecutor::reload_config()
{
  int ret = OB_SUCCESS;
  int64_t thread_cnt = 0;
  if (OB_FAIL(get_config_thread_cnt_(thread_cnt))) {
    LOG_WARN("failed to get lake table executor thread count", K(ret), K_(tenant_id));
  } else if (OB_FAIL(apply_thread_cnt_(thread_cnt))) {
    LOG_WARN("failed to reload lake table executor config", K(ret), K_(tenant_id), K(thread_cnt));
  }
  return ret;
}

void ObLakeTableExecutor::stop()
{
  if (-1 != tg_id_) {
    TG_STOP(tg_id_);
  }
}

void ObLakeTableExecutor::wait()
{
  if (-1 != tg_id_) {
    TG_WAIT(tg_id_);
  }
}

void ObLakeTableExecutor::destroy()
{
  if (-1 != tg_id_) {
    TG_STOP(tg_id_);
    TG_WAIT(tg_id_);
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
  }
  is_inited_ = false;
}

int64_t ObLakeTableExecutor::get_queue_num() const
{
  int64_t num = 0;
  if (is_inited_ && -1 != tg_id_) {
    TG_GET_QUEUE_NUM(tg_id_, num);
  }
  return num;
}

int ObLakeTableExecutor::get_thread_cnt() const
{
  int cnt = 0;
  if (is_inited_ && -1 != tg_id_) {
    cnt = TG_GET_THREAD_CNT(tg_id_);
  }
  return cnt;
}

double ObLakeTableExecutor::get_tenant_cpu_count()
{
  return MTL_CPU_COUNT();
}

int ObLakeTableExecutor::set_adaptive_thread(int64_t min_thread_cnt, int64_t max_thread_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lake table executor not inited", K(ret));
  } else if (OB_UNLIKELY(min_thread_cnt <= 0 || max_thread_cnt <= 0 || min_thread_cnt > max_thread_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid thread count range", K(ret), K(min_thread_cnt), K(max_thread_cnt));
  } else if (OB_FAIL(TG_SET_ADAPTIVE_THREAD(tg_id_, min_thread_cnt, max_thread_cnt))) {
    LOG_WARN("lake table executor set adaptive thread failed", K(ret), K_(tg_id), K(min_thread_cnt), K(max_thread_cnt));
  } else {
    LOG_INFO("lake table executor adaptive thread updated", K_(tg_id), K(min_thread_cnt), K(max_thread_cnt));
  }
  return ret;
}

int ObLakeTableExecutor::adjust_thread_count()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lake table executor not inited", K(ret));
  } else {
    const int64_t queue_num = get_queue_num();
    const int cur_thread_cnt = get_thread_cnt();
    const double tenant_cpu = get_tenant_cpu_count();
    const int64_t max_allowed = std::max(static_cast<int64_t>(tenant_cpu / 2), 1L);
    int64_t target = cur_thread_cnt;
    if (queue_num > cur_thread_cnt * 4) {
      // queue pressure high: double threads (at least 1), cap at tenant cpu / 2
      target = std::min(std::max(static_cast<int64_t>(cur_thread_cnt) * 2, 1L), max_allowed);
    } else if (queue_num == 0 && cur_thread_cnt > 1) {
      // idle: shrink by half, keep at least 1
      target = std::max(static_cast<int64_t>(cur_thread_cnt) / 2, 1L);
    }
    if (target != cur_thread_cnt) {
      if (OB_FAIL(TG_SET_THREAD_CNT(tg_id_, target))) {
        LOG_WARN("lake table executor adjust thread count failed", K(ret), K_(tg_id),
                 K(queue_num), K(cur_thread_cnt), K(tenant_cpu), K(target));
      } else {
        LOG_INFO("lake table executor adjust thread count", K_(tg_id),
                 K(queue_num), K(cur_thread_cnt), K(tenant_cpu), K(target));
      }
    }
  }
  return ret;
}

void ObLakeTableExecutor::Add(std::function<void()> func)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    LOG_WARN("lake table executor not inited, run task inline");
    func();
  } else {
    LakeTableTask *task = new_lake_table_task(tenant_id_, std::move(func));
    if (OB_ISNULL(task)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("lake table executor allocate task failed, run task inline");
      func();
    } else if (OB_FAIL(TG_PUSH_TASK(tg_id_, task))) {
      LOG_WARN("lake table executor TG_PUSH_TASK failed, run task inline", K(ret), K_(tg_id));
      // Execute synchronously so the caller never silently drops work.
      task->func_();
      delete_lake_table_task(task);
    }
  }
}

void ObLakeTableExecutor::handle(void *task)
{
  int ret = OB_SUCCESS;
  LakeTableTask *t = static_cast<LakeTableTask *>(task);
  if (OB_NOT_NULL(t)) {
    const int64_t original_timeout_ts = THIS_WORKER.get_timeout_ts();
    common::ObCurTraceId::set(t->trace_id_);
    THIS_WORKER.set_timeout_ts(t->timeout_ts_);
    LOG_TRACE("lake table executor task begin");
    MTL_SWITCH(tenant_id_) {
      t->func_();
    }
    LOG_TRACE("lake table executor task end");
    if (THIS_WORKER.get_timeout_ts() != original_timeout_ts) {
      THIS_WORKER.set_timeout_ts(original_timeout_ts);
    }
    common::ObCurTraceId::reset();
    delete_lake_table_task(t);
  }
}

ObLakeTableTaskGroup::ObLakeTableTaskGroup()
    : is_inited_(false),
      pending_cnt_(0),
      ret_code_(OB_SUCCESS)
{
}

int ObLakeTableTaskGroup::init(const int64_t task_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("lake table task group already inited", K(ret));
  } else if (OB_UNLIKELY(task_cnt < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lake table task count", K(ret), K(task_cnt));
  } else if (OB_FAIL(cond_.init(common::ObWaitEventIds::LAKE_TABLE_TASK_GROUP_COND_WAIT))) {
    LOG_WARN("init lake table task group cond failed", K(ret));
  } else {
    pending_cnt_ = task_cnt;
    is_inited_ = true;
  }
  return ret;
}

void ObLakeTableTaskGroup::finish(int task_ret)
{
  if (OB_UNLIKELY(!is_inited_)) {
  } else {
    common::ObThreadCondGuard guard(cond_);
    if (OB_SUCCESS == ret_code_ && OB_SUCCESS != task_ret) {
      ret_code_ = task_ret;
    }
    if (pending_cnt_ > 0 && 0 == --pending_cnt_) {
      IGNORE_RETURN cond_.broadcast();
    }
  }
}

int ObLakeTableTaskGroup::wait()
{
  int ret = OB_SUCCESS;
  common::ObThreadCondGuard guard(cond_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lake table task group not inited", K(ret));
  } else {
    while (pending_cnt_ > 0 && OB_SUCC(ret)) {
      if (OB_FAIL(cond_.wait())) {
        LOG_WARN("wait lake table task group failed", K(ret), K(pending_cnt_));
      }
    }
    if (OB_SUCC(ret)) {
      ret = ret_code_;
    }
  }
  return ret;
}

int ObLakeTableExecutor::create_task_group(common::ObIAllocator &allocator,
                                           const int64_t task_cnt,
                                           ObLakeTableTaskGroupHandle &task_group)
{
  int ret = OB_SUCCESS;
  task_group = OB_NEWx(ObLakeTableTaskGroup, &allocator);
  if (OB_UNLIKELY(!task_group)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate lake table task group failed", K(ret));
  } else if (OB_FAIL(task_group->init(task_cnt))) {
    LOG_WARN("init lake table task group failed", K(ret));
    destroy_task_group(allocator, task_group);
  }
  return ret;
}

void ObLakeTableExecutor::destroy_task_group(common::ObIAllocator &allocator,
                                             ObLakeTableTaskGroupHandle &task_group)
{
  if (OB_NOT_NULL(task_group)) {
    OB_DELETEx(ObLakeTableTaskGroup, &allocator, task_group);
    task_group = nullptr;
  }
}

int ObLakeTableExecutor::Add(ObLakeTableTaskGroupHandle task_group, std::function<int()> func)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!task_group)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lake table task group is null", K(ret));
  } else {
    Add(LakeTableTaskGroupFinishTask(task_group, std::move(func)));
  }
  return ret;
}

} // namespace lake_table
} // namespace sql
} // namespace oceanbase

#undef USING_LOG_PREFIX
