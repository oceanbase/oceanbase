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
#include "lib/ob_define.h"
#include "share/rc/ob_tenant_base.h"          // mtl_free
#include "log_define.h"

namespace oceanbase
{
namespace palf
{

LogIOWorkerWrapper::LogIOWorkerWrapper()
    : is_user_tenant_(false),
      log_writer_parallelism_(-1),
      log_io_workers_(NULL),
      throttle_(),
      round_robin_idx_(-1),
      is_inited_(false) {}


LogIOWorkerWrapper::~LogIOWorkerWrapper()
{
  destroy();
}

void LogIOWorkerWrapper::destroy()
{
  is_inited_ = false;
  round_robin_idx_ = -1;
  throttle_.reset();
  destory_and_free_log_io_workers_();
  // reset after destory_and_free_log_io_workers_
  log_writer_parallelism_ = -1;
  is_user_tenant_ = false;
}
int LogIOWorkerWrapper::init(const LogIOWorkerConfig &config,
                             const int64_t tenant_id,
                             int cb_thread_pool_tg_id,
                             ObIAllocator *allocator,
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
  } else if (OB_FAIL(create_and_init_log_io_workers_(config, tenant_id, cb_thread_pool_tg_id,
                                          allocator, palf_env_impl))) {
    LOG_WARN("init_log_io_workers_ failed", K(config));
  } else {
    is_user_tenant_ = is_user_tenant(tenant_id);
    log_writer_parallelism_ = config.io_worker_num_;
    throttle_.reset();
    round_robin_idx_ = 0;
    is_inited_ = true;
    LOG_INFO("success to init LogIOWorkerWrapper", K(config), K(tenant_id), KPC(this));
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destory_and_free_log_io_workers_();
  }
  return ret;
}

LogIOWorker *LogIOWorkerWrapper::get_log_io_worker(const int64_t palf_id)
{
  int64_t index = palf_id_to_index_(palf_id);
  LogIOWorker *iow = log_io_workers_ + index;
  PALF_LOG(INFO, "get_log_io_worker success", KPC(this), K(palf_id), K(index), KP(iow));
  return iow;
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

int LogIOWorkerWrapper::notify_need_writing_throttling(const bool &need_throttling)
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

int64_t LogIOWorkerWrapper::get_last_working_time() const
{
  int64_t last_working_time = OB_INVALID_TIMESTAMP;
  if (IS_NOT_INIT) {
    PALF_LOG_RET(ERROR, OB_NOT_INIT, "LogIOWorkerWrapper not inited", KPC(this));
  } else {
    for (int64_t i = 0; i < log_writer_parallelism_; i++) {
      last_working_time = MAX(last_working_time, log_io_workers_[i].get_last_working_time());
    }
  }
  return last_working_time;
}

int LogIOWorkerWrapper::create_and_init_log_io_workers_(const LogIOWorkerConfig &config,
                                                        const int64_t tenant_id,
                                                        const int cb_thread_pool_tg_id,
                                                        ObIAllocator *allocator,
                                                        IPalfEnvImpl *palf_env_impl)
{
  int ret = OB_SUCCESS;
  const int64_t log_writer_parallelism = config.io_worker_num_;
  log_io_workers_ = reinterpret_cast<LogIOWorker *>(share::mtl_malloc(
    (log_writer_parallelism) * sizeof(LogIOWorker), "LogIOWS"));
  if (NULL == log_io_workers_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(WARN, "allocate memory failed", K(log_writer_parallelism));
  }
  for (int64_t i = 0; i < log_writer_parallelism && OB_SUCC(ret); i++) {
    LogIOWorker *iow = log_io_workers_ + i;
    iow = new(iow)LogIOWorker();
    // NB:only sys log streams of user tenants need to ignore throtting
    bool need_ignoring_throttling = (i == SYS_LOG_IO_WORKER_INDEX && is_user_tenant(tenant_id));
    if (OB_FAIL(iow->init(config, tenant_id, cb_thread_pool_tg_id, allocator,
                          &throttle_, need_ignoring_throttling, palf_env_impl))) {
      PALF_LOG(WARN, "init LogIOWorker failed", K(i), K(config), K(tenant_id),
               K(cb_thread_pool_tg_id), KP(allocator), KP(palf_env_impl));
    } else {
      PALF_LOG(INFO, "init LogIOWorker success", K(i), K(config), K(tenant_id),
               K(cb_thread_pool_tg_id), KP(allocator), KP(palf_env_impl), KP(iow),
               KP(log_io_workers_));
    }
  }
  return ret;
}

int LogIOWorkerWrapper::start_()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < log_writer_parallelism_ && OB_SUCC(ret); i++) {
    LogIOWorker *iow = log_io_workers_ + i;
    if (OB_FAIL(iow->start())) {
      PALF_LOG(WARN, "start LogIOWorker failed", K(i));
    }
  }
  return ret;
}

void LogIOWorkerWrapper::stop_()
{
  for (int64_t i = 0; i < log_writer_parallelism_; i++) {
    LogIOWorker *iow = log_io_workers_ + i;
    iow->stop();
  }
}

void LogIOWorkerWrapper::wait_()
{
  for (int64_t i = 0; i < log_writer_parallelism_; i++) {
    LogIOWorker *iow = log_io_workers_ + i;
    iow->wait();
  }
}

void LogIOWorkerWrapper::destory_and_free_log_io_workers_()
{
  PALF_LOG(INFO, "destory_log_io_workers_ success", KPC(this));
  if (NULL != log_io_workers_) {
    for (int64_t i = 0; i < log_writer_parallelism_; i++) {
      LogIOWorker *iow = log_io_workers_ + i;
      iow->stop();
      iow->wait();
      iow->destroy();
      iow->~LogIOWorker();
    }
    share::mtl_free(log_io_workers_);
    log_io_workers_ = NULL;
  }
}

int64_t LogIOWorkerWrapper::palf_id_to_index_(const int64_t palf_id)
{
  int64_t index = -1;
  // For sys log stream, index set to 0.
  if (is_sys_palf_id(palf_id)) {
    index = SYS_LOG_IO_WORKER_INDEX;
  } else {
    const int64_t hash_factor = log_writer_parallelism_ - 1;
    if (hash_factor <= 0 && is_user_tenant_) {
      PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "unexpected error, log_writer_parallelism_ must be greater than 1 when it's user tenant",
               KPC(this), K(palf_id));
      OB_ASSERT(false);
    }
    // NB: SYS_LOG_IO_WORKER_INDEX is 0, others should not use this LogIOWorker.
    index = (round_robin_idx_++ % hash_factor) + 1;
    PALF_LOG(INFO, "palf_id_to_index_ success", KPC(this), K(palf_id), K(index));
    OB_ASSERT(index < log_writer_parallelism_);
  }
  return index;
}
}//end of namespace palf
}//end of namespace oceanbase
