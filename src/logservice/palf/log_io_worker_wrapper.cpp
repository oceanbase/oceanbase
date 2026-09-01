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

#define USING_LOG_PREFIX PALF
#include "log_io_worker_wrapper.h"

namespace oceanbase
{
using namespace common;
namespace palf
{

LogIOWorkerWrapper::LogIOWorkerWrapper()
    : is_user_tenant_(false),
      enable_async_io_(false),
      worker_count_(-1),
      log_io_workers_(NULL),
      async_workers_(NULL),
      throttle_(),
      need_purging_throttling_func_(),
      purge_task_count_(0),
      round_robin_idx_(-1),
      palf_env_impl_(NULL),
      is_inited_(false) {}


LogIOWorkerWrapper::~LogIOWorkerWrapper()
{
  destroy();
}

void LogIOWorkerWrapper::destroy()
{
  const bool need_stop_wait = is_inited_;
  const bool need_log_destroy = is_inited_ || NULL != async_workers_
                                || NULL != log_io_workers_ || worker_count_ >= 0;
  is_inited_ = false;
  if (need_stop_wait) {
    stop_();
    wait_();
  }
  throttle_.reset();
  need_purging_throttling_func_.reset();
  purge_task_count_ = 0;
  // Only one pool should be alive. Destroy the expected pool by mode first,
  // then clean any unexpected residual pool defensively. The helpers are
  // internally guarded by NULL checks, so repeated cleanup after a partial init
  // failure is idempotent.
  if (enable_async_io_) {
    destroy_and_free_async_pool_();
  } else {
    destroy_and_free_log_io_workers_();
  }
  if (NULL != async_workers_) {
    PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED,
                 "unexpected async worker pool remains during wrapper destroy", KPC(this));
    destroy_and_free_async_pool_();
  }
  if (NULL != log_io_workers_) {
    PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED,
                 "unexpected legacy worker pool remains during wrapper destroy", KPC(this));
    destroy_and_free_log_io_workers_();
  }
  palf_async_index_map_.destroy();
  if (need_log_destroy) {
    PALF_LOG(INFO, "LogIOWorkerWrapper destroy success",
             K_(is_user_tenant), K_(enable_async_io),
             K_(worker_count), K_(round_robin_idx));
  }
  worker_count_ = -1;
  round_robin_idx_ = -1;
  enable_async_io_ = false;
  is_user_tenant_ = false;
  palf_env_impl_ = NULL;
}

int LogIOWorkerWrapper::init(const LogIOWorkerConfig &config,
                             const int64_t tenant_id,
                             int cb_thread_pool_tg_id,
                             ObIAllocator *allocator,
                             const bool enable_async_io,
                             IPalfEnvImpl *palf_env_impl)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("LogIOWorkerWrapper has inited twice", K(config), K(tenant_id));
  } else if (!config.is_valid() || OB_UNLIKELY(!is_valid_tenant_id(tenant_id))
             || 0 >= cb_thread_pool_tg_id || OB_ISNULL(allocator) || OB_ISNULL(palf_env_impl)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(config), K(tenant_id), K(cb_thread_pool_tg_id), KP(allocator),
             KP(palf_env_impl));
  } else {
    // Decide the pool mode once from enable_async_io and tenant type.
    // is_user_tenant_ / enable_async_io_ must be set before the pool builder
    // so submitter selection semantics match the layout.
    is_user_tenant_ = is_user_tenant(tenant_id);
    // User-tenant SYS and data LS take the async path when async is enabled.
    // Otherwise this wrapper owns the legacy LogIOWorker pool only.
    enable_async_io_ = (enable_async_io && is_user_tenant_);
    throttle_.reset();
    purge_task_count_ = 0;
    need_purging_throttling_func_ = [this]() {
      return 0 < ATOMIC_LOAD(&purge_task_count_);
    };
    if (!need_purging_throttling_func_.is_valid()) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PALF_LOG(WARN, "generate need_purging_throttling_func_ failed", KR(ret), K(tenant_id));
    }
    if (OB_FAIL(ret)) {
    } else if (enable_async_io_) {
      if (OB_FAIL(create_and_init_async_workers_(config, tenant_id, palf_env_impl))) {
        LOG_WARN("create_and_init_async_workers_ failed", K(config));
      } else if (OB_FAIL(palf_async_index_map_.create(64, "PalfAsyncIdx"))) {
        LOG_WARN("palf_async_index_map create failed", KR(ret));
      }
    } else if (OB_FAIL(create_and_init_log_io_workers_(config, tenant_id, cb_thread_pool_tg_id,
                                                       allocator, palf_env_impl))) {
      LOG_WARN("create_and_init_log_io_workers_ failed", K(config));
    }
    if (OB_SUCC(ret)) {
      round_robin_idx_ = 0;
      palf_env_impl_ = palf_env_impl;
      is_inited_ = true;
      LOG_INFO("success to init LogIOWorkerWrapper", K(config), K(tenant_id),
               K(enable_async_io), KPC(this));
    }
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

int LogIOWorkerWrapper::select_palf_io_submitter(const int64_t palf_id,
                                                 LogIOWorkerBase *&submitter)
{
  int ret = OB_SUCCESS;
  submitter = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogIOWorkerWrapper not inited", KR(ret), K(palf_id));
  } else if (OB_UNLIKELY(!is_valid_palf_id(palf_id))) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid palf_id", KR(ret), K(palf_id));
  } else if (OB_FAIL(build_palf_io_submitter_(palf_id, submitter))) {
    PALF_LOG(WARN, "build_palf_io_submitter_ failed", KR(ret), K(palf_id));
  } else if (OB_ISNULL(submitter)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "invalid palf io submitter", KR(ret), K(palf_id));
  } else {
    PALF_LOG(TRACE, "select_palf_io_submitter success", KPC(this), K(palf_id), KP(submitter));
  }
  return ret;
}

int LogIOWorkerWrapper::register_palf_async_index_(const int64_t palf_id,
                                                   const int64_t async_index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!enable_async_io_ || OB_ISNULL(async_workers_)
                  || !is_valid_palf_id(palf_id) || async_index < 0
                  || async_index >= worker_count_)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid async index registration", KR(ret), K(palf_id),
             K(async_index), K_(worker_count), KP(async_workers_), K_(enable_async_io));
  } else if (OB_FAIL(palf_async_index_map_.set_refactored(palf_id, async_index, 1/*overwrite*/))) {
    PALF_LOG(WARN, "palf_async_index_map set failed", KR(ret), K(palf_id), K(async_index));
  }
  return ret;
}

void LogIOWorkerWrapper::unregister_palf_async_index_(const int64_t palf_id)
{
  const int tmp_ret = palf_async_index_map_.erase_refactored(palf_id);
  if (OB_SUCCESS != tmp_ret && OB_ENTRY_NOT_EXIST != tmp_ret) {
    PALF_LOG_RET(WARN, tmp_ret, "erase palf async index failed", K(palf_id));
  } else {
    PALF_LOG(INFO, "unregister palf async index", K(palf_id), KR(tmp_ret));
  }
}

int LogIOWorkerWrapper::get_async_worker_index_by_submitter_(LogIOWorkerBase *submitter,
                                                             int64_t &async_index) const
{
  int ret = OB_SUCCESS;
  async_index = -1;
  if (OB_ISNULL(submitter)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid submitter", KR(ret), KP(submitter));
  } else if (!enable_async_io_ || OB_ISNULL(async_workers_) || worker_count_ <= 0) {
    ret = OB_ENTRY_NOT_EXIST;
    PALF_LOG(TRACE, "async worker index lookup skipped", KR(ret), KP(submitter),
             K_(enable_async_io), KP(async_workers_), K_(worker_count));
  } else {
    for (int64_t i = 0; i < worker_count_ && async_index < 0; ++i) {
      if (static_cast<LogIOWorkerBase *>(async_workers_ + i) == submitter) {
        async_index = i;
      }
    }
    if (async_index < 0) {
      ret = OB_ENTRY_NOT_EXIST;
      PALF_LOG(WARN, "submitter is not an async worker", KR(ret), KP(submitter),
               K_(worker_count), KP(async_workers_));
    }
  }
  return ret;
}

LogAsyncIOWorker *LogIOWorkerWrapper::get_async_io_worker_(const int64_t palf_id)
{
  LogAsyncIOWorker *worker = NULL;
  if (enable_async_io_ && OB_NOT_NULL(async_workers_) && worker_count_ > 0) {
    int64_t idx = -1;
    if (OB_SUCCESS == palf_async_index_map_.get_refactored(palf_id, idx)
        && idx >= 0 && idx < worker_count_) {
      worker = async_workers_ + idx;
    }
  }
  return worker;
}

LogAsyncIOWorker *LogIOWorkerWrapper::get_async_io_worker_by_index_(const int64_t async_index)
{
  LogAsyncIOWorker *worker = NULL;
  if (enable_async_io_ && OB_NOT_NULL(async_workers_)
      && async_index >= 0 && async_index < worker_count_) {
    worker = async_workers_ + async_index;
  }
  return worker;
}

// ---- Facade: async ctx lifecycle ----
int LogIOWorkerWrapper::register_async_palf_ctx_if_needed(
    const int64_t palf_id,
    const int cb_thread_pool_tg_id,
    LogIOWorkerBase *submitter)
{
  int ret = OB_SUCCESS;
  LogAsyncIOWorker *worker = NULL;
  int64_t async_index = -1;
  const AsyncThrottleContext throttle_ctx =
      is_sys_palf_id(palf_id)
          ? AsyncThrottleContext()
          : AsyncThrottleContext(&throttle_, &need_purging_throttling_func_,
                                 &purge_task_count_);
  if (OB_ISNULL(submitter)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid submitter for async ctx registration",
             KR(ret), K(palf_id), KP(submitter));
  } else if (OB_FAIL(get_async_worker_index_by_submitter_(submitter, async_index))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      if (!enable_async_io_) {
        PALF_LOG(INFO, "palf skips async ctx registration",
                 KR(ret), K(palf_id), KP(submitter));
        ret = OB_SUCCESS;
      } else {
        PALF_LOG(WARN, "async ctx submitter is not found in async pool",
                 KR(ret), K(palf_id), KP(submitter),
                 K_(enable_async_io), K_(worker_count), KP(async_workers_));
      }
    } else {
      PALF_LOG(WARN, "get async worker index by submitter failed",
               KR(ret), K(palf_id), KP(submitter));
    }
  } else if (OB_ISNULL(palf_env_impl_)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "palf_env_impl_ is null for async ctx registration",
             KR(ret), K(palf_id), K(async_index));
  } else if (FALSE_IT(worker = get_async_io_worker_by_index_(async_index))) {
  } else if (OB_ISNULL(worker) || !worker->is_valid()) {
    ret = OB_ENTRY_NOT_EXIST;
    PALF_LOG(WARN, "async worker not found for async ctx registration",
             KR(ret), K(palf_id), K(async_index));
  } else if (OB_FAIL(worker->register_and_create_ctx(
                 palf_id, cb_thread_pool_tg_id, palf_env_impl_,
                 throttle_ctx))) {
    PALF_LOG(WARN, "register_and_create_ctx failed", KR(ret), K(palf_id));
  } else if (OB_FAIL(register_palf_async_index_(palf_id, async_index))) {
    const int register_ret = ret;
    const int unregister_ret = worker->unregister_palf_ctx_and_wait(palf_id);
    PALF_LOG(WARN, "register_palf_async_index_ failed", KR(ret), K(palf_id), K(async_index));
    if (OB_ENTRY_NOT_EXIST == unregister_ret) {
      ret = register_ret;
      PALF_LOG(INFO, "rolled back async ctx after async index publish failure",
               KR(ret), K(palf_id), K(async_index));
    } else if (OB_FAIL(unregister_ret)) {
      PALF_LOG(WARN, "rollback async ctx after async index publish failure failed",
               KR(ret), K(register_ret), K(palf_id), K(async_index));
    } else {
      ret = register_ret;
      PALF_LOG(INFO, "rolled back async ctx after async index publish failure",
               KR(ret), K(palf_id), K(async_index));
    }
  } else {
    PALF_LOG(INFO, "async palf ctx registered and async index published",
             K(palf_id), K(async_index));
  }
  return ret;
}

int LogIOWorkerWrapper::unregister_async_palf_ctx(const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  LogAsyncIOWorker *worker = get_async_io_worker_(palf_id);
  if (OB_ISNULL(worker)) {
    ret = OB_ENTRY_NOT_EXIST;
    PALF_LOG(TRACE, "async worker does not exist for unregister", KR(ret),
             K(palf_id), K_(enable_async_io));
  } else {
    int wait_ret = worker->unregister_palf_ctx_and_wait(palf_id);
    if (OB_ENTRY_NOT_EXIST == wait_ret) {
      unregister_palf_async_index_(palf_id);
    } else if (OB_SUCCESS != wait_ret) {
      ret = wait_ret;
      if (OB_TIMEOUT == wait_ret) {
        PALF_LOG(ERROR, "unregister_palf_ctx_and_wait timed out",
                 KR(ret), K(palf_id));
      } else {
        PALF_LOG(WARN, "unregister_palf_ctx_and_wait failed",
                 KR(ret), K(palf_id));
      }
    } else {
      unregister_palf_async_index_(palf_id);
    }
  }
  return ret;
}

int LogIOWorkerWrapper::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(start_())) {
    LOG_WARN("failed to start log_io_workers_");
  } else {
    LOG_INFO("success to start LogIOWorkerWrapper", KPC(this));
  }
  return ret;
}

void LogIOWorkerWrapper::stop()
{
  PALF_LOG(INFO, "LogIOWorkerWrapper starts stopping", KPC(this));
  stop_();
  PALF_LOG(INFO, "LogIOWorkerWrapper has finished stopping", KPC(this));
}

void LogIOWorkerWrapper::wait()
{
  PALF_LOG(INFO, " LogIOWorkerWrapper starts waiting", KPC(this));
  wait_();
  PALF_LOG(INFO, "LogIOWorkerWrapper has finished waiting", KPC(this));
}

int LogIOWorkerWrapper::notify_need_writing_throttling(const bool need_throttling)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    throttle_.notify_need_writing_throttling(need_throttling);
    if (need_throttling) {
      LOG_INFO("success to notify_need_writing_throttling True");
    }
  }
  return ret;
}

int64_t LogIOWorkerWrapper::get_oldest_pending_io_start_ts() const
{
  int64_t last_working_time = OB_INVALID_TIMESTAMP;
  if (IS_NOT_INIT) {
    PALF_LOG_RET(ERROR, OB_NOT_INIT, "LogIOWorkerWrapper not inited", KPC(this));
  } else {
    for (int64_t i = 0; i < worker_count_; i++) {
      const LogIOWorkerBase *worker = get_io_worker_by_index_(i);
      if (NULL != worker) {
        merge_last_working_time_(worker->get_oldest_pending_io_start_ts(), last_working_time);
      }
    }
  }
  return last_working_time;
}

LogIOWorkerBase *LogIOWorkerWrapper::get_io_worker_by_index_(const int64_t worker_index)
{
  return const_cast<LogIOWorkerBase *>(
      static_cast<const LogIOWorkerWrapper *>(this)->get_io_worker_by_index_(worker_index));
}

const LogIOWorkerBase *LogIOWorkerWrapper::get_io_worker_by_index_(
    const int64_t worker_index) const
{
  const LogIOWorkerBase *worker = NULL;
  if (OB_UNLIKELY(worker_index < 0 || worker_index >= worker_count_)) {
  } else if (enable_async_io_) {
    worker = OB_ISNULL(async_workers_)
        ? NULL
        : static_cast<const LogIOWorkerBase *>(async_workers_ + worker_index);
  } else {
    worker = OB_ISNULL(log_io_workers_)
        ? NULL
        : static_cast<const LogIOWorkerBase *>(log_io_workers_ + worker_index);
  }
  return worker;
}

void LogIOWorkerWrapper::merge_last_working_time_(const int64_t worker_last_working_time,
                                                  int64_t &last_working_time) const
{
  if (OB_INVALID_TIMESTAMP == worker_last_working_time) {
    // skip
  } else if (OB_INVALID_TIMESTAMP == last_working_time) {
    last_working_time = worker_last_working_time;
  } else {
    last_working_time = MIN(last_working_time, worker_last_working_time);
  }
}

int LogIOWorkerWrapper::create_and_init_log_io_workers_(const LogIOWorkerConfig &config,
                                                        const int64_t tenant_id,
                                                        const int cb_thread_pool_tg_id,
                                                        ObIAllocator *allocator,
                                                        IPalfEnvImpl *palf_env_impl)
{
  int ret = OB_SUCCESS;
  worker_count_ = 0;
  const int64_t legacy_count = config.io_worker_num_;
  log_io_workers_ = reinterpret_cast<LogIOWorker *>(share::mtl_malloc(
    legacy_count * sizeof(LogIOWorker), "LogIOWS"));
  if (NULL == log_io_workers_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(WARN, "allocate memory failed", K(legacy_count));
  }
  for (int64_t i = 0; i < legacy_count && OB_SUCC(ret); i++) {
    LogIOWorker *iow = log_io_workers_ + i;
    iow = new(iow)LogIOWorker();
    // Legacy index 0 serves the user tenant's SYS PALF without write throttling.
    bool need_ignoring_throttling = (i == SYS_LOG_IO_WORKER_INDEX && is_user_tenant(tenant_id));
    if (OB_FAIL(iow->init(config, tenant_id, cb_thread_pool_tg_id, allocator,
                          &throttle_, need_ignoring_throttling, palf_env_impl))) {
      PALF_LOG(WARN, "init LogIOWorker failed", K(i), K(config), K(tenant_id),
               K(cb_thread_pool_tg_id), KP(allocator), KP(palf_env_impl));
    } else {
      worker_count_++;
      PALF_LOG(INFO, "init legacy LogIOWorker success", K(i), K(config), K(tenant_id),
               K(cb_thread_pool_tg_id), KP(allocator), KP(palf_env_impl), KP(iow),
               KP(log_io_workers_));
    }
  }
  if (OB_FAIL(ret)) {
    destroy_and_free_log_io_workers_();
    worker_count_ = -1;
  }
  return ret;
}

int LogIOWorkerWrapper::create_and_init_async_workers_(const LogIOWorkerConfig &config,
                                                       const int64_t tenant_id,
                                                       IPalfEnvImpl *palf_env_impl)
{
  int ret = OB_SUCCESS;
  worker_count_ = 0;
  // Index 0 serves SYS PALF. Data PALFs use [1, N), or share index 0 when the
  // configured async pool contains only one worker.
  const int64_t async_count = config.io_worker_num_;
  if (async_count <= 0) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "async pool size must be > 0 when async enabled", KR(ret),
             K(config), K(tenant_id));
  } else if (NULL == (async_workers_ = reinterpret_cast<LogAsyncIOWorker *>(share::mtl_malloc(
                 async_count * sizeof(LogAsyncIOWorker), "AsyncIOWS")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(WARN, "allocate async_workers_ failed", K(async_count));
  } else {
    for (int64_t i = 0; i < async_count && OB_SUCC(ret); i++) {
      LogAsyncIOWorker *worker = new(async_workers_ + i) LogAsyncIOWorker();
      // Count immediately after placement-new. If init fails,
      // destroy_and_free_async_pool_ must also destruct this constructed object.
      worker_count_++;
      if (OB_FAIL(worker->init(tenant_id, palf_env_impl, config.io_queue_capcity_))) {
        PALF_LOG(WARN, "LogAsyncIOWorker init failed", KR(ret), K(i), K(tenant_id), K(config));
      } else {
        PALF_LOG(INFO, "init async worker success", K(i), K(tenant_id), K(config), KP(worker));
      }
    }
  }
  if (OB_FAIL(ret)) {
    destroy_and_free_async_pool_();
    worker_count_ = 0;
  }
  return ret;
}

int LogIOWorkerWrapper::start_()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < worker_count_ && OB_SUCC(ret); i++) {
    LogIOWorkerBase *worker = get_io_worker_by_index_(i);
    if (OB_ISNULL(worker)) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(WARN, "invalid io worker", KR(ret), K(i), KPC(this));
    } else if (OB_FAIL(worker->start())) {
      PALF_LOG(WARN, "start io worker failed", KR(ret), K(i), KPC(this));
    }
  }
  return ret;
}

void LogIOWorkerWrapper::stop_()
{
  for (int64_t i = 0; i < worker_count_; i++) {
    LogIOWorkerBase *worker = get_io_worker_by_index_(i);
    if (NULL != worker) {
      worker->stop();
    }
  }
}

void LogIOWorkerWrapper::wait_()
{
  for (int64_t i = 0; i < worker_count_; i++) {
    LogIOWorkerBase *worker = get_io_worker_by_index_(i);
    if (NULL != worker) {
      worker->wait();
    }
  }
}

void LogIOWorkerWrapper::destroy_and_free_log_io_workers_()
{
  PALF_LOG(INFO, "destroy_and_free_log_io_workers_", KPC(this));
  if (NULL != log_io_workers_) {
    for (int64_t i = 0; i < worker_count_; i++) {
      LogIOWorker *iow = log_io_workers_ + i;
      iow->destroy();
      iow->~LogIOWorker();
    }
    share::mtl_free(log_io_workers_);
    log_io_workers_ = NULL;
  }
}

void LogIOWorkerWrapper::destroy_and_free_async_pool_()
{
  PALF_LOG(INFO, "destroy_and_free_async_pool_", KPC(this));
  // Destroy every placement-new worker before releasing the pool storage.
  if (NULL != async_workers_) {
    for (int64_t i = 0; i < worker_count_; i++) {
      LogAsyncIOWorker *worker = async_workers_ + i;
      worker->destroy();
      worker->~LogAsyncIOWorker();
    }
    share::mtl_free(async_workers_);
    async_workers_ = NULL;
  }
}

int LogIOWorkerWrapper::select_worker_index_for_palf_(const int64_t palf_id,
                                                      const bool allow_single_worker_fallback,
                                                      int64_t &worker_index)
{
  int ret = OB_SUCCESS;
  worker_index = -1;
  if (worker_count_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "worker pool is empty", KR(ret), K(palf_id), K_(worker_count));
  } else if (is_sys_palf_id(palf_id)) {
    worker_index = SYS_LOG_IO_WORKER_INDEX;
  } else {
    const int64_t data_pool_size = worker_count_ - 1;
    if (data_pool_size <= 0) {
      if (allow_single_worker_fallback) {
        worker_index = SYS_LOG_IO_WORKER_INDEX;
      } else {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "data worker pool is empty", KR(ret), K(palf_id),
                 K_(worker_count), K(allow_single_worker_fallback));
      }
    } else {
      const int64_t old_round_robin_idx = ATOMIC_FAA(&round_robin_idx_, 1);
      worker_index = (old_round_robin_idx % data_pool_size) + 1;
    }
  }
  if (OB_SUCC(ret)
      && OB_UNLIKELY(worker_index < 0 || worker_index >= worker_count_)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "invalid selected worker index", KR(ret), K(palf_id),
             K(worker_index), K_(worker_count), K(allow_single_worker_fallback));
  }
  return ret;
}

int LogIOWorkerWrapper::build_palf_io_submitter_(const int64_t palf_id,
                                                 LogIOWorkerBase *&submitter)
{
  int ret = OB_SUCCESS;
  int64_t worker_idx = -1;
  submitter = NULL;
  if (!is_user_tenant_) {
    // Non-user tenants always use the dedicated legacy worker.
    worker_idx = SYS_LOG_IO_WORKER_INDEX;
  } else if (enable_async_io_) {
    // Async enabled: SYS PALF uses index 0; data PALFs round-robin over
    // [1, worker_count_). If the async pool has only one worker, data PALFs
    // fall back to index 0 rather than legacy.
    if (OB_FAIL(select_worker_index_for_palf_(
            palf_id, true /* allow_single_worker_fallback */, worker_idx))) {
      PALF_LOG(ERROR, "select async worker index failed", KR(ret), K(palf_id));
    }
  } else {
    // Sync-mode user tenants assign a legacy worker at create or reload time.
    if (OB_FAIL(select_worker_index_for_palf_(
            palf_id, false /* allow_single_worker_fallback */, worker_idx))) {
      PALF_LOG(ERROR, "select legacy worker index failed", KR(ret), K(palf_id));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(worker_idx < 0 || worker_idx >= worker_count_)) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "invalid worker submitter index", KR(ret), K(palf_id),
               K(worker_idx), K_(worker_count),
               K_(is_user_tenant), K_(enable_async_io));
    } else if (is_user_tenant_ && enable_async_io_) {
      if (OB_ISNULL(async_workers_)) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "async worker pool is NULL", KR(ret), K(palf_id),
                 K(worker_idx), K_(worker_count));
      } else {
        submitter = async_workers_ + worker_idx;
      }
    } else if (OB_ISNULL(log_io_workers_)) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "legacy worker pool is NULL", KR(ret), K(palf_id),
               K(worker_idx), K_(worker_count));
    } else {
      submitter = log_io_workers_ + worker_idx;
    }
  }
  if (OB_SUCC(ret)) {
    PALF_LOG(TRACE, "build_palf_io_submitter_ success", KPC(this), K(palf_id),
             K_(is_user_tenant), K_(enable_async_io), K(worker_idx), KP(submitter));
  }
  return ret;
}
}//end of namespace palf
}//end of namespace oceanbase
