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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_task_scheduler.h"
#include "common/ob_timeout_ctx.h"
#include "lib/stat/ob_session_stat.h"
#include "observer/omt/ob_tenant.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "observer/table_load/ob_table_load_task.h"
#include "share/ob_share_util.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/direct_load/ob_direct_load_table_builder_allocator.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace share;
using namespace storage;

void ObTableLoadTaskThreadPoolScheduler::MyThreadPool::run1()
{
  OB_ASSERT(OB_NOT_NULL(scheduler_));
  ObTenantStatEstGuard stat_est_guard(MTL_ID());
  const int64_t thread_count = get_thread_count();
  // LOG_INFO("table load task thread start", KP(this), "pid", get_tid_cache(), "thread_idx", get_thread_idx());
  // 启动成功的线程数+1
  if (thread_count == ATOMIC_AAF(&running_thread_count_, 1)) {
    scheduler_->before_running();
  }
  while (!has_set_stop()) {
    // 等待所有线程起来
    if (!scheduler_->is_running()) {
      PAUSE();
    } else {
      scheduler_->run(get_thread_idx());
      break;
    }
  }
  // 启动成功的线程数-1
  if (0 == ATOMIC_AAF(&running_thread_count_, -1)) {
    scheduler_->after_running();
  }
}

ObTableLoadTaskThreadPoolScheduler::ObTableLoadTaskThreadPoolScheduler(int64_t thread_count,
                                                                       uint64_t table_id,
                                                                       const char *label,
                                                                       int64_t session_queue_size)
  : allocator_("TLD_ThreadPool"),
    thread_count_(thread_count),
    session_queue_size_(session_queue_size),
    timeout_ts_(INT64_MAX),
    thread_pool_(this),
    worker_ctx_array_(nullptr),
    state_(STATE_ZERO),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  snprintf(name_, OB_THREAD_NAME_BUF_LEN, "TLD_%03ld_%s", table_id % 1000, label);
}

ObTableLoadTaskThreadPoolScheduler::~ObTableLoadTaskThreadPoolScheduler()
{
  if (nullptr != worker_ctx_array_) {
    for (int64_t i = 0; i < thread_count_; ++i) {
      WorkerContext *worker_ctx = worker_ctx_array_ + i;
      worker_ctx->~WorkerContext();
    }
    allocator_.free(worker_ctx_array_);
    worker_ctx_array_ = nullptr;
  }
}

int ObTableLoadTaskThreadPoolScheduler::init_worker_ctx_array()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(WorkerContext) * thread_count_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret));
  } else {
    worker_ctx_array_ = new (buf) WorkerContext[thread_count_];
    for (int64_t i = 0; OB_SUCC(ret) && i < thread_count_; ++i) {
      WorkerContext *worker_ctx = worker_ctx_array_ + i;
      worker_ctx->worker_id_ = i;
      if (OB_FAIL(worker_ctx->cond_.init(1))) {
        LOG_WARN("fail to init thread cond", KR(ret));
      } else if (OB_FAIL(worker_ctx->task_queue_.init(session_queue_size_, "TLD_Queue", MTL_ID()))) {
        LOG_WARN("fail to init task queue", KR(ret), K(i));
      }
    }
  }
  return ret;
}

int ObTableLoadTaskThreadPoolScheduler::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadTaskThreadPoolScheduler init twice", KR(ret), KP(this));
  } else {
    thread_pool_.set_thread_count(thread_count_);
    thread_pool_.set_run_wrapper(MTL_CTX());
    ObCurTraceId::TraceId *cur_trace_id = ObCurTraceId::get_trace_id();
    if (nullptr != cur_trace_id) {
      trace_id_ = *cur_trace_id;
    } else {
      // generate trace id
      ObAddr zero_addr;
      trace_id_.init(zero_addr);
    }
    timeout_ts_ = THIS_WORKER.get_timeout_ts();
    if (OB_FAIL(init_worker_ctx_array())) {
      LOG_WARN("fail to init worker ctx array", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadTaskThreadPoolScheduler::start()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(state_mutex_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadTaskThreadPoolScheduler not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(STATE_ZERO != state_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("state must be zero", KR(ret), K_(state));
  } else {
    state_ = STATE_STARTING;
    if (OB_FAIL(thread_pool_.start())) {
      LOG_WARN("fail to start thread pool", KR(ret));
    } else {
      while (STATE_STARTING == state_) {
        PAUSE();
      }
      if (OB_UNLIKELY(STATE_RUNNING != state_)) {
        ret = OB_NOT_RUNNING;
        LOG_WARN("thread not running", KR(ret));
      }
    }
  }
  return ret;
}

void ObTableLoadTaskThreadPoolScheduler::stop()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(state_mutex_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadTaskThreadPoolScheduler not init", KR(ret), KP(this));
  } else {
    if (state_ == STATE_ZERO) {
      state_ = STATE_STOPPED_NO_WAIT;
    } else if (STATE_STARTING == state_) {
      ret = OB_ERR_UNEXPECTED; //因为加锁了，这种情况不能出现
      LOG_WARN("state cann't be starting here", KR(ret));
    } else if (STATE_RUNNING == state_) {
      thread_pool_.stop();
      state_ = STATE_STOPPING;
      // 唤醒所有线程
      for (int64_t i = 0; i < thread_count_; ++i) {
        WorkerContext &worker_ctx = worker_ctx_array_[i];
        ObThreadCondGuard guard(worker_ctx.cond_);
        worker_ctx.cond_.signal();
      }
    }
  }
}

//调用wait前，必须保证之前调用过stop
void ObTableLoadTaskThreadPoolScheduler::wait()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(state_mutex_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadTaskThreadPoolScheduler not init", KR(ret), KP(this));
  } else {
    if (state_ == STATE_STOPPING || state_ == STATE_STOPPED) {
      thread_pool_.wait();
    }
  }

  for (int i = 0; i < warning_buffer_.get_total_warning_count(); ++i) {
    const common::ObWarningBuffer::WarningItem &warning_item = *(warning_buffer_.get_warning_item(i));
    if (ObLogger::USER_WARN == warning_item.log_level_) {
      FORWARD_USER_WARN(warning_item.code_, warning_item.msg_);
    } else if (ObLogger::USER_NOTE == warning_item.log_level_) {
      FORWARD_USER_NOTE(warning_item.code_, warning_item.msg_);
    }
  }
}

void ObTableLoadTaskThreadPoolScheduler::before_running()
{
  OB_ASSERT(STATE_STARTING == state_);
  state_ = STATE_RUNNING;
}

void ObTableLoadTaskThreadPoolScheduler::after_running()
{
  state_ = STATE_STOPPED;
  clear_all_task();
  const ObWarningBuffer *wb = common::ob_get_tsi_warning_buffer();
  if (wb != nullptr) {
    if (wb->get_total_warning_count() > 0) {
      lib::ObMutexGuard guard(wb_mutex_);
      warning_buffer_ = *wb;
    }
  }
}

void ObTableLoadTaskThreadPoolScheduler::run(uint64_t thread_idx)
{
  int ret = OB_SUCCESS;
  // set thread name
  lib::set_thread_name(name_);
  // set trace id
  ObCurTraceId::set(trace_id_);
  // set worker timeout
  THIS_WORKER.set_timeout_ts(timeout_ts_);
  // set compatibility mode
  share::ObTenantBase *tenant_base = MTL_CTX();
  lib::Worker::CompatMode mode = ((omt::ObTenant *)tenant_base)->get_compat_mode();
  lib::Worker::set_compatibility_mode(mode);

  LOG_INFO("table load task thread run", KP(this), "pid", get_tid_cache(), K(thread_idx));

  WorkerContext &worker_ctx = worker_ctx_array_[thread_idx];
  while (OB_SUCC(ret) && OB_LIKELY(STATE_RUNNING == state_)) {
    if (OB_FAIL(execute_worker_tasks(worker_ctx))) {
      LOG_WARN("fail to execute worker tasks", KR(ret));
    } else {
      ObThreadCondGuard guard(worker_ctx.cond_);
      if (OB_LIKELY(STATE_RUNNING == state_) && 0 == worker_ctx.task_queue_.size()) {
        // LOG_WARN("table load task thread wait begin", KP(this), K(thread_idx), "size", worker_ctx.task_queue_.size());
        worker_ctx.need_signal_ = true;
        worker_ctx.cond_.wait();
        worker_ctx.need_signal_ = false;
        // LOG_WARN("table load task thread wait end", KP(this), K(thread_idx), "size", worker_ctx.task_queue_.size());
      }
    }
  }

  // 为了保证在abort场景, clean_up_task能在工作线程执行
  execute_worker_tasks(worker_ctx);

  // 线程非正常结束, 设置全局状态, 阻止其他线程继续执行
  if (STATE_RUNNING == state_) {
    state_ = STATE_STOPPING;
  }

  // clear thread local variables
  get_table_builder_allocator()->reset();

  LOG_INFO("table load task thread stopped", KP(this), "pid", get_tid_cache(), K(thread_idx));
}

int ObTableLoadTaskThreadPoolScheduler::add_task(int64_t thread_idx, ObTableLoadTask *task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadTaskThreadPoolScheduler not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(thread_idx >= thread_count_) || OB_ISNULL(task) ||
             OB_UNLIKELY(!task->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(thread_idx), K_(thread_count), KPC(task));
  } else if (OB_UNLIKELY(STATE_RUNNING != state_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("scheduler not running", KR(ret), K_(state));
  } else {
    ObTimeoutCtx ctx;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, DEFAULT_TIMEOUT_US))) {
      LOG_WARN("fail to set default timeout ctx", KR(ret));
    } else {
      OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, add_task_time_us);
      WorkerContext &worker_ctx = worker_ctx_array_[thread_idx];
      // LOG_WARN("table load add task begin", KP(this), K(thread_idx), "size", worker_ctx.task_queue_.size());
      if (OB_FAIL(worker_ctx.task_queue_.push(task, ctx.get_timeout()))) {
        if (OB_UNLIKELY(OB_TIMEOUT != ret)) {
          LOG_WARN("fail to push task into queue", KR(ret), KPC(task));
        }
      } else {
        // 唤醒线程
        ObThreadCondGuard guard(worker_ctx.cond_);
        // LOG_WARN("table load add task end", KP(this), K(thread_idx), "size", worker_ctx.task_queue_.size(), "need_signal", worker_ctx.need_signal_);
        if (worker_ctx.need_signal_) {
          worker_ctx.cond_.signal();
        }
      }
    }
  }
  return ret;
}

int ObTableLoadTaskThreadPoolScheduler::execute_worker_tasks(WorkerContext &worker_ctx)
{
  int ret = OB_SUCCESS;
  int64_t size = 0;
  while (OB_SUCC(ret) && (size = worker_ctx.task_queue_.size()) > 0) {
    while (OB_SUCC(ret) && size-- > 0) {
      void *tmp = nullptr;
      if (OB_FAIL(worker_ctx.task_queue_.pop(tmp))) {
        LOG_WARN("fail to pop queue", KR(ret), K(worker_ctx.worker_id_));
      } else {
        // 任务的运行结果不影响工作线程的运行
        ObTableLoadTask *task = (ObTableLoadTask *)tmp;
        ObTraceIdGuard trace_id_guard(task->get_trace_id());
        int task_ret = task->do_work();
        task->callback(task_ret);
      }
    }
  }
  return ret;
}

void ObTableLoadTaskThreadPoolScheduler::clear_all_task()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadTaskThreadPoolScheduler not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(STATE_STOPPED != state_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("scheduler not stopped", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < thread_count_; ++i) {
      WorkerContext &worker_ctx = worker_ctx_array_[i];
      while (worker_ctx.task_queue_.size() > 0) {
        void *tmp = nullptr;
        if (OB_FAIL(worker_ctx.task_queue_.pop(tmp))) {
          LOG_WARN("fail to pop queue", KR(ret), K(i));
        } else {
          // 触发task回调
          ObTableLoadTask *task = (ObTableLoadTask *)tmp;
          ObTraceIdGuard trace_id_guard(task->get_trace_id());
          task->callback(OB_CANCELED);
        }
      }
    }
  }
}

}  // namespace observer
}  // namespace oceanbase
