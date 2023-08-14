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

#define USING_LOG_PREFIX CLOG
#include "ob_remote_fetch_log_worker.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/ob_define.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/restore/ob_storage.h"                     // is_io_error
#include "lib/utility/ob_tracepoint.h"                  // EventTable
#include "share/ob_errno.h"
#include "share/rc/ob_tenant_base.h"                    // mtl_alloc
#include "storage/tx_storage/ob_ls_service.h"           // ObLSService
#include "storage/ls/ob_ls.h"                           // ObLS
#include "logservice/palf/log_group_entry.h"            // LogGroupEntry
#include "logservice/palf/lsn.h"                        // LSN
#include "ob_log_restore_service.h"                     // ObLogRestoreService
#include "share/scn.h"                        // SCN
#include "ob_fetch_log_task.h"                          // ObFetchLogTask
#include "ob_log_restore_handler.h"                     // ObLogRestoreHandler
#include "ob_log_restore_allocator.h"                       // ObLogRestoreAllocator
#include "storage/tx_storage/ob_ls_handle.h"            // ObLSHandle
#include "logservice/archiveservice/ob_archive_define.h"   // archive
#include "storage/tx_storage/ob_ls_map.h"               // ObLSIterator
#include "logservice/archiveservice/large_buffer_pool.h"

namespace oceanbase
{
namespace logservice
{
using namespace oceanbase::palf;
using namespace oceanbase::storage;
using namespace share;

#define GET_RESTORE_HANDLER_CTX(id)       \
  ObLS *ls = NULL;      \
  ObLSHandle ls_handle;         \
  ObLogRestoreHandler *restore_handler = NULL;       \
  if (OB_FAIL(ls_svr_->get_ls(id, ls_handle, ObLSGetMod::LOG_MOD))) {   \
    LOG_WARN("get ls failed", K(ret), K(id));     \
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {      \
    ret = OB_ERR_UNEXPECTED;       \
    LOG_INFO("get ls is NULL", K(ret), K(id));      \
  } else if (OB_ISNULL(restore_handler = ls->get_log_restore_handler())) {     \
    ret = OB_ERR_UNEXPECTED;      \
    LOG_INFO("restore_handler is NULL", K(ret), K(id));   \
  }    \
  if (OB_SUCC(ret))

ObRemoteFetchWorker::ObRemoteFetchWorker() :
  inited_(false),
  tenant_id_(OB_INVALID_TENANT_ID),
  restore_service_(NULL),
  ls_svr_(NULL),
  task_queue_(),
  allocator_(NULL),
  log_ext_handler_(),
  cond_()
{}

ObRemoteFetchWorker::~ObRemoteFetchWorker()
{
  destroy();
}

int ObRemoteFetchWorker::init(const uint64_t tenant_id,
    ObLogRestoreAllocator *allocator,
    ObLogRestoreService *restore_service,
    ObLSService *ls_svr)
{
  int ret = OB_SUCCESS;
  const int64_t FETCH_LOG_MEMORY_LIMIT = 1024 * 1024 * 1024L;  // 1GB
  const int64_t FETCH_LOG_TASK_LIMIT = 1024;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObRemoteFetchWorker has been initialized", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(allocator)
      || OB_ISNULL(restore_service)
      || OB_ISNULL(ls_svr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(allocator), K(restore_service), K(ls_svr));
  } else if (OB_FAIL(task_queue_.init(FETCH_LOG_TASK_LIMIT, "RFLTaskQueue", MTL_ID()))) {
    LOG_WARN("task_queue_ init failed", K(ret));
  } else if (OB_FAIL(log_ext_handler_.init())) {
    LOG_WARN("log_ext_handler_ init failed", K(ret));
  } else {
    tenant_id_ = tenant_id;
    allocator_ = allocator;
    restore_service_ = restore_service;
    ls_svr_ = ls_svr;
    inited_ = true;
  }
  return ret;
}

void ObRemoteFetchWorker::destroy()
{
  int ret = OB_SUCCESS;
  stop();
  wait();
  if (inited_) {
    void *data = NULL;
    while (OB_SUCC(ret) && 0 < task_queue_.size()) {
      if (OB_FAIL(task_queue_.pop(data))) {
        LOG_WARN("pop failed", K(ret));
      } else {
        ObFetchLogTask *task = static_cast<ObFetchLogTask*>(data);
        LOG_INFO("free residual fetch log task when RFLWorker destroy", KPC(task));
        inner_free_task_(*task);
      }
    }
    tenant_id_ = OB_INVALID_TENANT_ID;
    task_queue_.reset();
    task_queue_.destroy();
    restore_service_ = NULL;
    ls_svr_ = NULL;
    allocator_ = NULL;
    log_ext_handler_.destroy();
    inited_ = false;
  }
}

int ObRemoteFetchWorker::start()
{
  int ret = OB_SUCCESS;
  ObThreadPool::set_run_wrapper(MTL_CTX());
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObRemoteFetchWorker not init", K(ret));
  } else if (OB_FAIL(log_ext_handler_.start(0))) {
    LOG_WARN("ObLogExtStorageHandler start failed");
  } else if (OB_FAIL(ObThreadPool::start())) {
    LOG_WARN("ObRemoteFetchWorker start failed", K(ret));
  } else {
    LOG_INFO("ObRemoteFetchWorker start succ", K_(tenant_id));
  }
  return ret;
}

void ObRemoteFetchWorker::stop()
{
  LOG_INFO("ObRemoteFetchWorker thread stop", K_(tenant_id));
  log_ext_handler_.stop();
  ObThreadPool::stop();
}

void ObRemoteFetchWorker::wait()
{
  LOG_INFO("ObRemoteFetchWorker thread wait", K_(tenant_id));
  log_ext_handler_.wait();
  ObThreadPool::wait();
}

void ObRemoteFetchWorker::signal()
{
  cond_.signal();
}

int ObRemoteFetchWorker::submit_fetch_log_task(ObFetchLogTask *task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObRemoteFetchWorker not init", K(ret), K(inited_));
  } else if (OB_ISNULL(task) || OB_UNLIKELY(! task->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(task));
  } else if (FALSE_IT(task->iter_.reset())) {
  } else if (OB_FAIL(task_queue_.push(task))) {
    LOG_WARN("push task failed", K(ret), KPC(task));
  } else {
    LOG_TRACE("submit_fetch_log_task succ", KP(task));
  }
  return ret;
}

int ObRemoteFetchWorker::modify_thread_count(const int64_t log_restore_concurrency)
{
  int ret = OB_SUCCESS;
  int64_t thread_count = 0;
  if (OB_FAIL(log_ext_handler_.resize(log_restore_concurrency))) {
    LOG_WARN("log_ext_handler_ resize failed", K(log_restore_concurrency), K(thread_count));
  } else if (FALSE_IT(thread_count = calcuate_thread_count_(log_restore_concurrency))) {
    LOG_WARN("calcuate_thread_count_ failed", K(log_restore_concurrency), K(thread_count));
  } else if (thread_count == lib::Threads::get_thread_count()) {
    // do nothing
  } else if (OB_FAIL(set_thread_count(thread_count))) {
    LOG_WARN("set thread count failed", K(ret));
  } else {
    LOG_INFO("set thread count succ", K(thread_count), K(log_restore_concurrency));
  }
  return ret;
}

int ObRemoteFetchWorker::get_thread_count(int64_t &thread_count) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    LOG_WARN("ObRemoteFetchWorker not init");
  } else {
    thread_count = lib::Threads::get_thread_count();
  }
  return ret;
}

void ObRemoteFetchWorker::run1()
{
  LOG_INFO("ObRemoteFetchWorker thread start");
  lib::set_thread_name("RFLWorker");
  ObCurTraceId::init(GCONF.self_addr_);

  const int64_t THREAD_RUN_INTERVAL = 100 * 1000L;
  if (OB_UNLIKELY(! inited_)) {
    LOG_INFO("ObRemoteFetchWorker not init");
  } else {
    while (! has_set_stop() && !(OB_NOT_NULL(&lib::Thread::current()) ? lib::Thread::current().has_set_stop() : false)) {
      int64_t begin_tstamp = ObTimeUtility::current_time();
      do_thread_task_();
      int64_t end_tstamp = ObTimeUtility::current_time();
      int64_t wait_interval = THREAD_RUN_INTERVAL - (end_tstamp - begin_tstamp);
      if (wait_interval > 0) {
        cond_.timedwait(wait_interval);
      }
    }
  }
}

void ObRemoteFetchWorker::do_thread_task_()
{
  int ret = OB_SUCCESS;
  int64_t size = task_queue_.size();
  for (int64_t i = 0; i < size && OB_SUCC(ret) && !has_set_stop(); i++) {
    if (OB_FAIL(handle_single_task_())) {
      LOG_WARN("handle single task failed", K(ret));
    }
  }

  if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
    LOG_INFO("ObRemoteFetchWorker is running", "thread_index", get_thread_idx());
  }
}

int ObRemoteFetchWorker::handle_single_task_()
{
  DEBUG_SYNC(BEFORE_RESTORE_HANDLE_FETCH_LOG_TASK);
  int ret = OB_SUCCESS;
  void *data = NULL;
  if (OB_FAIL(task_queue_.pop(data))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
        LOG_WARN("no task exist, just skip", K(ret));
      }
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("pop failed", K(ret));
    }
  } else if (OB_ISNULL(data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("data is NULL", K(ret), K(data));
  } else {
    ObFetchLogTask *task = static_cast<ObFetchLogTask *>(data);
    ObLSID id = task->id_;
    palf::LSN cur_lsn = task->cur_lsn_;
    // after task handle, DON'T print it any more
    if (OB_FAIL(handle_fetch_log_task_(task))) {
      LOG_WARN("handle fetch log task failed", K(ret), KP(task), K(id));
    }

    // only fatal error report fail, retry with others
    if (is_fatal_error_(ret)) {
      report_error_(id, ret, cur_lsn, ObLogRestoreErrorContext::ErrorType::FETCH_LOG);
    }
  }
  return ret;
}

int ObRemoteFetchWorker::handle_fetch_log_task_(ObFetchLogTask *task)
{
  int ret = OB_SUCCESS;
  bool empty = true;
  const int64_t DEFAULT_BUF_SIZE = 64 * 1024 * 1024L;

  if (OB_UNLIKELY(! task->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), K(task));
  } else if (OB_FAIL(task->iter_.init(tenant_id_, task->id_, task->pre_scn_,
          task->cur_lsn_, task->end_lsn_, allocator_->get_buferr_pool(),
          &log_ext_handler_, DEFAULT_BUF_SIZE))) {
    LOG_WARN("ObRemoteLogIterator init failed", K(ret), K_(tenant_id), KPC(task));
  } else if (!need_fetch_log_(task->id_)) {
    LOG_TRACE("no need fetch log", KPC(task));
  } else if (OB_FAIL(task->iter_.pre_read(empty))) {
    LOG_WARN("pre_read failed", K(ret), KPC(task));
  } else if (empty) {
    // do nothing
  } else if (OB_FAIL(push_submit_array_(*task))) {
    LOG_WARN("push submit array failed", K(ret));
  }

  if (OB_SUCC(ret) && ! empty) {
    // pre_read succ and push submit array succ, do nothing,
  } else {
    if (is_fatal_error_(ret)) {
      // fatal error may be false positive, for example restore in parallel, the range in pre-read maybe surpass the current log archive round, which not needed.
      LOG_WARN("fatal error occur", K(ret), KPC(task));
    } else if (! empty && OB_FAIL(ret)) {
      LOG_WARN("task data not empty and push submit array failed, try retire task", K(ret), KPC(task));
    } else if (OB_SUCC(ret)) {
      // pre_read data is empty, do notning
    } else {
      LOG_TRACE("pre read data is empty and failed", K(ret), KPC(task));
    }
    if (! is_fatal_error_(ret)) {
      task->iter_.update_source_cb();
    }
    // not encounter fatal error or push submit array succ, just try retire task
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = try_retire_(task))) {
      LOG_WARN("retire task failed", K(tmp_ret), KP(task));
    }
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_RESTORE_LOG_FAILED) OB_SUCCESS;
  }
#endif
  return ret;
}

bool ObRemoteFetchWorker::need_fetch_log_(const share::ObLSID &id)
{
  int ret = OB_SUCCESS;
  bool bret = false;
  GET_RESTORE_HANDLER_CTX(id) {
    bret = !restore_handler->restore_to_end();
  }
  return bret;
}

int ObRemoteFetchWorker::push_submit_array_(ObFetchLogTask &task)
{
  int ret = OB_SUCCESS;
  const ObLSID id = task.id_;
  DEBUG_SYNC(BEFORE_RESTORE_SERVICE_PUSH_FETCH_DATA);
  GET_RESTORE_HANDLER_CTX(id) {
    if (OB_FAIL(restore_handler->submit_sorted_task(task))) {
      LOG_WARN("submit sort task failed", K(ret), K(task));
    }
  }
  return ret;
}

bool ObRemoteFetchWorker::is_fatal_error_(const int ret_code) const
{
  return OB_ARCHIVE_ROUND_NOT_CONTINUOUS == ret_code
    || OB_ARCHIVE_LOG_RECYCLED == ret_code
    || OB_INVALID_BACKUP_DEST == ret_code;
}

int ObRemoteFetchWorker::try_retire_(ObFetchLogTask *&task)
{
  int ret = OB_SUCCESS;
  bool done = false;
  GET_RESTORE_HANDLER_CTX(task->id_) {
    if (OB_FAIL(restore_handler->try_retire_task(*task, done))) {
      LOG_WARN("try retire task failed", KPC(task), KPC(restore_handler));
    } else if (done) {
      inner_free_task_(*task);
      task = NULL;
    } else {
      if (OB_FAIL(submit_fetch_log_task(task))) {
        LOG_ERROR("submit fetch log task failed", K(ret), KPC(task));
        inner_free_task_(*task);
        task = NULL;
      } else {
        task = NULL;
      }
    }
  } else {
    // ls not exist, just free task
    inner_free_task_(*task);
    task = NULL;
  }
  return ret;
}

void ObRemoteFetchWorker::inner_free_task_(ObFetchLogTask &task)
{
  task.reset();
  mtl_free(&task);
}

void ObRemoteFetchWorker::report_error_(const ObLSID &id,
                                        const int ret_code,
                                        const palf::LSN &lsn,
                                        const ObLogRestoreErrorContext::ErrorType &error_type)
{
  int ret = OB_SUCCESS;
  GET_RESTORE_HANDLER_CTX(id) {
    restore_handler->mark_error(*ObCurTraceId::get_trace_id(), ret_code, lsn, error_type);
  }
}

int64_t ObRemoteFetchWorker::calcuate_thread_count_(const int64_t log_restore_concurrency)
{
  int64_t thread_count = 0;
  int64_t recommend_concurrency_in_single_file = log_ext_handler_.get_recommend_concurrency_in_single_file();
  thread_count = static_cast<int64_t>(
    log_restore_concurrency + recommend_concurrency_in_single_file - 1) / recommend_concurrency_in_single_file;
  return thread_count;
}

} // namespace logservice
} // namespace oceanbase
