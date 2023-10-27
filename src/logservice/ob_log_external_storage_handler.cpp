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

#include "ob_log_external_storage_handler.h"
#include "lib/string/ob_string.h"                                 // ObString
#include "lib/stat/ob_latch_define.h"                             // LOG_EXTERNAL_STORAGE_HANDLER_LOCK
#include "share/rc/ob_tenant_base.h"                              // MTL_NEW
#include "ob_log_external_storage_io_task.h"                      // ObLogExternalStorageIOTask

namespace oceanbase
{
namespace logservice
{
using namespace common;

const int64_t ObLogExternalStorageHandler::CONCURRENCY_LIMIT = 128;
const int64_t ObLogExternalStorageHandler::DEFAULT_RETRY_INTERVAL = 2 * 1000;
const int64_t ObLogExternalStorageHandler::DEFAULT_TIME_GUARD_THRESHOLD = 2 * 1000;
const int64_t ObLogExternalStorageHandler::DEFAULT_PREAD_TIME_GUARD_THRESHOLD = 100 * 1000;
const int64_t ObLogExternalStorageHandler::DEFAULT_RESIZE_TIME_GUARD_THRESHOLD = 100 * 1000;
const int64_t ObLogExternalStorageHandler::CAPACITY_COEFFICIENT = 64;
const int64_t ObLogExternalStorageHandler::SINGLE_TASK_MINIMUM_SIZE = 2 * 1024 * 1024;

ObLogExternalStorageHandler::ObLogExternalStorageHandler()
    : concurrency_(-1),
      capacity_(-1),
      resize_rw_lock_(common::ObLatchIds::LOG_EXTERNAL_STORAGE_HANDLER_RW_LOCK),
      construct_async_task_lock_(common::ObLatchIds::LOG_EXTERNAL_STORAGE_HANDLER_LOCK),
      handle_adapter_(NULL),
      is_running_(false),
      is_inited_(false)
{}

ObLogExternalStorageHandler::~ObLogExternalStorageHandler()
{
  destroy();
}

int ObLogExternalStorageHandler::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLogExternalStorageHandler inited twice", KPC(this));
  } else if (NULL == (handle_adapter_ = MTL_NEW(ObLogExternalStorageIOTaskHandleAdapter, "ObLogEXTHandler"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "allocate memory failed");
  } else if (FALSE_IT(share::ObThreadPool::set_run_wrapper(MTL_CTX()))) {
  } else if (OB_FAIL(ObSimpleThreadPool::init(1, CAPACITY_COEFFICIENT * 1, "ObLogEXTTP", MTL_ID()))) {
    CLOG_LOG(WARN, "invalid argument", KPC(this));
  } else {
    concurrency_ = 1;
    capacity_ = CAPACITY_COEFFICIENT;
    is_running_ = false;
    is_inited_ = true;
    CLOG_LOG(INFO, "ObLogExternalStorageHandler inits successfully", KPC(this));
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

int ObLogExternalStorageHandler::start(const int64_t concurrency)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(resize_rw_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogExternalStorageHandler not inited", K(concurrency), KPC(this));
  } else if (is_running_) {
    CLOG_LOG(WARN, "ObLogExternalStorageHandler has run", K(concurrency), KPC(this));
  } else if (!is_valid_concurrency_(concurrency)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(concurrency), KPC(this));
  } else if (OB_FAIL(resize_(concurrency))) {
    CLOG_LOG(WARN, "resize_ failed", K(concurrency), KPC(this));
  } else {
    is_running_ = true;
  }
  return ret;
}

void ObLogExternalStorageHandler::stop()
{
  WLockGuard guard(resize_rw_lock_);
  is_running_ = false;
  ObSimpleThreadPool::stop();
}

void ObLogExternalStorageHandler::wait()
{
  WLockGuard guard(resize_rw_lock_);
  ObSimpleThreadPool::wait();
}

void ObLogExternalStorageHandler::destroy()
{
  CLOG_LOG_RET(WARN, OB_SUCCESS, "ObLogExternalStorageHandler destroy");
  is_inited_ = false;
  stop();
  wait();
  ObSimpleThreadPool::destroy();
  lib::ThreadPool::destroy();
  concurrency_ = -1;
  capacity_ = -1;
  if (OB_NOT_NULL(handle_adapter_)) {
    MTL_DELETE(ObLogExternalStorageIOTaskHandleIAdapter, "ObLogEXTHandler", handle_adapter_);
  }
  handle_adapter_ = NULL;
}

int ObLogExternalStorageHandler::resize(const int64_t new_concurrency,
                                        const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("resize thread pool", DEFAULT_RESIZE_TIME_GUARD_THRESHOLD);
  time_guard.click("after hold lock");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogExternalStorageHandler not inited", KPC(this), K(new_concurrency), K(timeout_us));
  } else if (!is_valid_concurrency_(new_concurrency) || 0 >= timeout_us) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", KPC(this), K(new_concurrency), K(timeout_us));
  } else if (!check_need_resize_(new_concurrency)) {
    CLOG_LOG(TRACE, "no need resize", KPC(this), K(new_concurrency));
  } else {
    WLockGuardTimeout guard(resize_rw_lock_, timeout_us, ret);
    // hold lock failed
    if (OB_FAIL(ret)) {
      CLOG_LOG(WARN, "hold lock failed", KPC(this), K(new_concurrency), K(timeout_us));
    } else if (!is_running_) {
      ret = OB_NOT_RUNNING;
      CLOG_LOG(WARN, "ObLogExternalStorageHandler not running", KPC(this), K(new_concurrency), K(timeout_us));
    } else {
      do {
        ret = resize_(new_concurrency);
        if (OB_FAIL(ret)) {
          usleep(DEFAULT_RETRY_INTERVAL);
        }
      } while (OB_FAIL(ret));
      time_guard.click("after create new thread pool");
      CLOG_LOG(INFO, "ObLogExternalStorageHandler resize success", KPC(this), K(new_concurrency));
    }
  }
  return ret;
}

int ObLogExternalStorageHandler::pread(const common::ObString &uri,
                                       const common::ObString &storage_info,
                                       const int64_t offset,
                                       char *buf,
                                       const int64_t read_buf_size,
                                       int64_t &real_read_size)
{
  int ret = OB_SUCCESS;
  int64_t async_task_count = 0;
  ObTimeGuard time_guard("slow pread", DEFAULT_PREAD_TIME_GUARD_THRESHOLD);
  ObLogExternalStorageIOTaskCtx *async_task_ctx = NULL;
  int64_t file_size = 0;
  int64_t real_read_buf_size = 0;
  RLockGuard guard(resize_rw_lock_);
  time_guard.click("after hold by lock");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogExternalStorageHandler not init", K(uri), K(offset), KP(buf), K(read_buf_size));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObLogExternalStorageHandler not running", K(uri), K(offset), KP(buf), K(read_buf_size));
    // when uri is NFS, storage_info is empty.
  } else if (uri.empty() || 0 > offset || NULL == buf || 0 >= read_buf_size) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "ObLogExternalStorageHandler invalid argument", K(uri), K(offset), KP(buf), K(read_buf_size));
  } else if (OB_FAIL(handle_adapter_->get_file_size(uri, storage_info, file_size))) {
    CLOG_LOG(WARN, "get_file_size failed", K(uri), K(offset), KP(buf), K(read_buf_size));
  } else if (offset > file_size) {
    ret = OB_FILE_LENGTH_INVALID;
    CLOG_LOG(WARN, "read position lager than file size, invalid argument", K(file_size), K(offset), K(uri));
  } else if (offset == file_size) {
    real_read_size = 0;
    CLOG_LOG(TRACE, "read position equal to file size, no need read data", K(file_size), K(offset), K(uri));
  } else if (FALSE_IT(time_guard.click("after get file size"))) {
    // NB: limit read size.
  } else if (FALSE_IT(real_read_buf_size = std::min(file_size - offset, read_buf_size))) {
  } else if (OB_FAIL(construct_async_tasks_and_push_them_into_thread_pool_(
      uri, storage_info, offset, buf, real_read_buf_size, real_read_size, async_task_ctx))) {
    CLOG_LOG(WARN, "construct_async_task_and_push_them_into_thread_pool_ failed", K(uri),
             K(offset), KP(buf), K(read_buf_size));
  } else if (FALSE_IT(time_guard.click("after construct async tasks"))) {
  } else if (OB_FAIL(wait_async_tasks_finished_(async_task_ctx))) {
    CLOG_LOG(WARN, "wait_async_tasks_finished_ failed", K(uri),
             K(offset), KP(buf), K(read_buf_size), KPC(async_task_ctx));
  } else if (FALSE_IT(time_guard.click("after wait async tasks"))) {
  } else {
    // if there is a failure of any async task, return the error of it, otherwise, return OB_SUCCESS.
    ret  = async_task_ctx->get_ret_code();
    if (OB_FAIL(ret)) {
      CLOG_LOG(WARN, "pread finished", K(time_guard), K(uri), K(offset), K(read_buf_size),
               K(real_read_size));
    } else {
      CLOG_LOG(TRACE, "pread finished", K(time_guard), K(uri), K(offset), K(read_buf_size),
               K(real_read_size));
    }
  }
  // FIXME: consider use shared ptr.
  if (NULL != async_task_ctx) {
    MTL_DELETE(ObLogExternalStorageIOTaskCtx, "ObLogEXTHandler", async_task_ctx);
    async_task_ctx = NULL;
  }
  return ret;
}

void ObLogExternalStorageHandler::handle(void *task)
{
  int ret = OB_SUCCESS;
  ObLogExternalStorageIOTask *io_task = reinterpret_cast<ObLogExternalStorageIOTask*>(task);
  if (OB_ISNULL(io_task)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "io_task is nullptr, unexpected error!!!", KP(io_task), KPC(this));
  } else if (OB_FAIL(io_task->do_task())) {
    CLOG_LOG(WARN, "do_task failed", KP(io_task), KPC(this));
  } else {
    CLOG_LOG(TRACE, "do_task success", KP(io_task), KPC(this));
  }
  if (OB_NOT_NULL(io_task)) {
    MTL_DELETE(ObLogExternalStorageIOTask, "ObLogEXTHandler", io_task);
  }
}

int64_t ObLogExternalStorageHandler::get_recommend_concurrency_in_single_file() const
{
  return palf::PALF_PHY_BLOCK_SIZE / SINGLE_TASK_MINIMUM_SIZE;
}

bool ObLogExternalStorageHandler::is_valid_concurrency_(const int64_t concurrency) const
{
  return 0 <= concurrency;
}

int64_t ObLogExternalStorageHandler::get_async_task_count_(const int64_t total_size) const
{
  // TODO by runlin: consider free async thread num.
  int64_t minimum_async_task_size = SINGLE_TASK_MINIMUM_SIZE;
  int64_t minimum_async_task_count = 1;
  int64_t async_task_count = std::max(minimum_async_task_count,
                                      std::min(concurrency_,
                                               (total_size + minimum_async_task_size - 1)/minimum_async_task_size));
  return async_task_count;
}

int ObLogExternalStorageHandler::construct_async_tasks_and_push_them_into_thread_pool_(
    const common::ObString &uri,
    const common::ObString &storage_info,
    const int64_t offset,
    char *read_buf,
    const int64_t read_buf_size,
    int64_t &real_read_size,
    ObLogExternalStorageIOTaskCtx *&async_task_ctx)
{
  int ret = OB_SUCCESS;
  int64_t async_task_count = get_async_task_count_(read_buf_size);
  int64_t async_task_size = (read_buf_size + async_task_count - 1) / async_task_count;
  int64_t remained_task_count = async_task_count;
  int64_t remained_total_size = read_buf_size;
  real_read_size = 0;
  async_task_ctx = NULL;
  if (NULL == (async_task_ctx = MTL_NEW(ObLogExternalStorageIOTaskCtx, "ObLogEXTHandler"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "allocate memory failed", KP(async_task_ctx));
  } else if (OB_FAIL(async_task_ctx->init(async_task_count))) {
    CLOG_LOG(WARN, "init ObLogExternalStorageIOTaskCtx failed", KP(async_task_ctx), K(async_task_count));
  } else {
    CLOG_LOG(TRACE, "begin construct async tasks", K(async_task_count), K(async_task_size),
             K(remained_task_count), K(remained_total_size));
    int64_t curr_read_offset = offset;
    int64_t curr_read_buf_pos = 0;
    // construct async task and push it into thread pool, last async task will be executed in current thread.
    ObLogExternalStorageIOTask *last_io_task = NULL;
    int64_t curr_task_idx = 0;
    while (remained_task_count > 0) {
      ObLogExternalStoragePreadTask *pread_task = NULL;
      int64_t async_task_read_buf_size = std::min(remained_total_size, async_task_size);
      construct_async_read_task_(uri, storage_info, curr_read_offset, read_buf + curr_read_buf_pos,
                                 async_task_read_buf_size, real_read_size, curr_task_idx,
                                 async_task_ctx, pread_task);
      ++curr_task_idx;
      curr_read_offset += async_task_read_buf_size;
      curr_read_buf_pos += async_task_read_buf_size;
      remained_total_size -= async_task_read_buf_size;
      last_io_task = pread_task;

      CLOG_LOG(TRACE, "construct async tasks idx success", K(curr_task_idx), K(async_task_count), K(async_task_size),
               K(remained_task_count), K(remained_total_size));

      // NB: use current thread to execute last io task.
      if (--remained_task_count > 0) {
        push_async_task_into_thread_pool_(pread_task);
      }
    }
    // defense code, last_io_task must not be NULL.
    if (NULL != last_io_task) {
      (void)last_io_task->do_task();
      MTL_DELETE(ObLogExternalStorageIOTask, "ObLogEXTHandler", last_io_task);
      last_io_task = NULL;
    }
  }
  return ret;
}

int ObLogExternalStorageHandler::wait_async_tasks_finished_(
  ObLogExternalStorageIOTaskCtx *async_task_ctx)
{
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_WAIT_US = 50 * 1000;
  int64_t print_log_interval = OB_INVALID_TIMESTAMP;
  // if async_task_ctx->wait return OB_SUCCESS, means there is no flying task.
  while (OB_FAIL(async_task_ctx->wait(DEFAULT_WAIT_US))) {
    if (palf::palf_reach_time_interval(500*1000, print_log_interval)) {
      CLOG_LOG(WARN, "wait ObLogExternalStorageIOTaskCtx failed", KPC(async_task_ctx));
    }
  }
  // even if wait async_task_ctx failed, there is no flying async task at here.
  if (OB_FAIL(ret)) {
    CLOG_LOG(WARN, "wait ObLogExternalStorageIOTaskCtx failed", KPC(async_task_ctx));
  }
  return ret;
}

void ObLogExternalStorageHandler::construct_async_read_task_(
  const common::ObString &uri,
  const common::ObString &storage_info,
  const int64_t offset,
  char *read_buf,
  const int64_t read_buf_size,
  int64_t &real_read_size,
  const int64_t task_idx,
  ObLogExternalStorageIOTaskCtx *async_task_ctx,
  ObLogExternalStoragePreadTask *&pread_task)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("construct pread task", DEFAULT_TIME_GUARD_THRESHOLD);
  int64_t print_log_interval = OB_INVALID_TIMESTAMP;
  do {
    RunningStatus *running_status = NULL;
    if (OB_FAIL(async_task_ctx->get_running_status(task_idx, running_status))) {
      CLOG_LOG(ERROR, "unexpected error!!!", K(task_idx), KP(running_status), KPC(async_task_ctx));
    } else if (NULL == (pread_task = MTL_NEW(ObLogExternalStoragePreadTask,
                                             "ObLogEXTHandler",
                                             uri, storage_info,
                                             running_status,
                                             async_task_ctx,
                                             handle_adapter_,
                                             offset, read_buf_size,
                                             read_buf, real_read_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      if (palf::palf_reach_time_interval(1*1000*1000, print_log_interval)) {
        CLOG_LOG(WARN, "allocate memory failed", K(task_idx), KP(running_status), KPC(async_task_ctx));
      }
    } else {
      CLOG_LOG(TRACE, "construct_async_read_task_ success", KPC(pread_task));
    }
    if (OB_FAIL(ret)) {
      usleep(DEFAULT_RETRY_INTERVAL);
    }
  } while (OB_FAIL(ret));
}

void ObLogExternalStorageHandler::push_async_task_into_thread_pool_(
  ObLogExternalStorageIOTask *io_task)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("push pread task", DEFAULT_TIME_GUARD_THRESHOLD);
  int64_t print_log_interval = OB_INVALID_TIMESTAMP;
  do {
    if (OB_FAIL(push(io_task))) {
      if (palf::palf_reach_time_interval(1*1000*1000, print_log_interval)) {
        CLOG_LOG(WARN, "push task into thread pool failed");
      }
    }
    if (OB_FAIL(ret)) {
      usleep(DEFAULT_RETRY_INTERVAL);
    }
  } while (OB_FAIL(ret));
}

int ObLogExternalStorageHandler::resize_(const int64_t new_concurrency)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("resize impl", 10 * 1000);
  int64_t real_concurrency = MIN(new_concurrency, CONCURRENCY_LIMIT);
  if (real_concurrency == concurrency_) {
    CLOG_LOG(TRACE, "no need resize_", K(new_concurrency), K(real_concurrency), KPC(this));
  } else if (OB_FAIL(ObSimpleThreadPool::set_thread_count(real_concurrency))) {
    CLOG_LOG(WARN, "set_thread_count failed", K(new_concurrency), KPC(this), K(real_concurrency));
  } else {
    CLOG_LOG_RET(INFO, OB_SUCCESS, "resize_ success", K(time_guard), KPC(this), K(new_concurrency), K(real_concurrency));
    concurrency_ = real_concurrency;
    capacity_ = CAPACITY_COEFFICIENT * real_concurrency;
  }
  time_guard.click("set thread count");
  return ret;
}

bool ObLogExternalStorageHandler::check_need_resize_(const int64_t new_concurrency) const
{
  RLockGuard guard(resize_rw_lock_);
  return new_concurrency != concurrency_;
}

} // end namespace logservice
} // end namespace oceanbase
