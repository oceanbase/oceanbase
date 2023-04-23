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
#include "ob_log_restore_controller.h"
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
  restore_controller_(NULL),
  restore_service_(NULL),
  ls_svr_(NULL),
  task_queue_(),
  allocator_(NULL),
  cond_()
{}

ObRemoteFetchWorker::~ObRemoteFetchWorker()
{
  destroy();
}

int ObRemoteFetchWorker::init(const uint64_t tenant_id,
    ObLogRestoreAllocator *allocator,
    ObLogRestoreController *restore_controller,
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
      || OB_ISNULL(restore_controller)
      || OB_ISNULL(restore_service)
      || OB_ISNULL(ls_svr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(restore_controller),
        K(allocator), K(restore_service), K(ls_svr));
  } else if (OB_FAIL(task_queue_.init(FETCH_LOG_TASK_LIMIT, "RFLTaskQueue", MTL_ID()))) {
    LOG_WARN("task_queue_ init failed", K(ret));
  } else {
    tenant_id_ = tenant_id;
    allocator_ = allocator;
    restore_controller_ = restore_controller;
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
    restore_controller_ = NULL;
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
  ObThreadPool::stop();
}

void ObRemoteFetchWorker::wait()
{
  LOG_INFO("ObRemoteFetchWorker thread wait", K_(tenant_id));
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

int ObRemoteFetchWorker::modify_thread_count(const int64_t thread_count)
{
  int ret = OB_SUCCESS;
  int64_t count = thread_count;
  if (thread_count < MIN_FETCH_LOG_WORKER_THREAD_COUNT) {
    count = MIN_FETCH_LOG_WORKER_THREAD_COUNT;
  } else if (thread_count > MAX_FETCH_LOG_WORKER_THREAD_COUNT) {
    count = MAX_FETCH_LOG_WORKER_THREAD_COUNT;
  }
  if (count == get_thread_count()) {
    // do nothing
  } else if (OB_FAIL(set_thread_count(count))) {
    LOG_WARN("set thread count failed", K(ret));
  } else {
    LOG_INFO("set thread count succ", K(count));
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
  if (0 != get_thread_idx() || get_thread_count() <= 1) {
    for (int64_t i = 0; i < size && OB_SUCC(ret) && !has_set_stop(); i++) {
      if (OB_FAIL(handle_single_task_())) {
        LOG_WARN("handle single task failed", K(ret));
      }
    }
  }

  if (0 == get_thread_idx())
  {
    if (OB_FAIL(try_consume_data_())) {
      LOG_WARN("try_consume_data_ failed", K(ret));
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
      report_error_(id, ret, cur_lsn);
    }
  }
  return ret;
}

int ObRemoteFetchWorker::handle_fetch_log_task_(ObFetchLogTask *task)
{
  int ret = OB_SUCCESS;
  bool empty = true;

  if (OB_UNLIKELY(! task->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), K(task));
  } else if (OB_FAIL(task->iter_.init(tenant_id_, task->id_, task->pre_scn_,
          task->cur_lsn_, task->end_lsn_, allocator_->get_buferr_pool()))) {
    LOG_WARN("ObRemoteLogIterator init failed", K(ret), K_(tenant_id), KPC(task));
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
    // not encounter fatal error or push submit array succ, just try retire task
    int tmp_ret = OB_SUCCESS;
    task->iter_.update_source_cb();
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

int ObRemoteFetchWorker::submit_entries_(ObFetchLogTask &task)
{
  int ret = OB_SUCCESS;
  LogGroupEntry entry;
  const char *buf = NULL;
  int64_t size = 0;
  LSN lsn;
  const ObLSID id = task.id_;
  while (OB_SUCC(ret) && ! has_set_stop()) {
    bool quota_done = false;
    if (OB_FAIL(task.iter_.next(entry, lsn, buf, size))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("ObRemoteLogIterator next failed", K(task));
      } else {
        LOG_TRACE("ObRemoteLogIterator to end", K(task.iter_));
      }
    } else if (OB_UNLIKELY(! entry.check_integrity())) {
      ret = OB_INVALID_DATA;
      LOG_WARN("entry is invalid", K(entry), K(lsn), K(task));
    } else if (task.cur_lsn_ > lsn) {
      LOG_INFO("repeated log, just skip", K(lsn), K(entry), K(task));
    } else if (OB_FAIL(wait_restore_quota_(entry.get_serialize_size(), quota_done))) {
      LOG_WARN("wait restore quota failed", K(entry), K(task));
    } else if (! quota_done) {
      break;
    } else if (OB_FAIL(submit_log_(id, task.proposal_id_, lsn,
            entry.get_scn(), buf, entry.get_serialize_size()))) {
      LOG_WARN("submit log failed", K(buf), K(entry), K(lsn), K(task));
    } else {
      task.cur_lsn_ = lsn + entry.get_serialize_size();
    }
  } // while
  if (OB_ITER_END == ret) {
    if (lsn.is_valid()) {
      LOG_INFO("submit_entries_ succ", K(id), K(lsn), K(entry.get_scn()), K(task));
    }
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObRemoteFetchWorker::wait_restore_quota_(const int64_t size, bool &done)
{
  int ret = OB_SUCCESS;
  done = false;
  while (OB_SUCC(ret) && ! done && ! has_set_stop()) {
    if (OB_FAIL(restore_controller_->get_quota(size, done))) {
      LOG_WARN("get quota failed");
    } else if (! done) {
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
        LOG_INFO("clog disk is not enough, just wait", K(size));
      } else {
        LOG_TRACE("get quota succ", K(size));
      }
      usleep(100 * 1000L);  // if get quota not done, sleep 100ms
    }
  }
  return ret;
}

int ObRemoteFetchWorker::submit_log_(const ObLSID &id,
    const int64_t proposal_id,
    const LSN &lsn,
    const SCN &scn,
    const char *buf,
    const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  do {
    GET_RESTORE_HANDLER_CTX(id) {
      if (OB_FAIL(restore_handler->raw_write(proposal_id, lsn, scn, buf, buf_size))) {
        if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
          ret = OB_SUCCESS;
        } else if (OB_RESTORE_LOG_TO_END == ret) {
          // do nothing
        } else {
          LOG_WARN("raw write failed", K(ret), K(id), K(lsn), K(buf), K(buf_size));
        }
      }
    }
  } while (OB_LOG_OUTOF_DISK_SPACE == ret && ! has_set_stop());
  // submit log until successfully if which can succeed with retry
  // except NOT MASTER or OTHER FATAL ERROR
  return ret;
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
      restore_service_->signal();
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

int ObRemoteFetchWorker::try_consume_data_()
{
  int ret = OB_SUCCESS;
  if (0 != get_thread_idx()) {
    // do nothing
  } else if (OB_FAIL(do_consume_data_())) {
    LOG_WARN("do consume data failed", K(ret));
  }
  return ret;
}

int ObRemoteFetchWorker::do_consume_data_()
{
  int ret = OB_SUCCESS;
  ObFetchLogTask *task = NULL;
  ObLS *ls = NULL;
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  if (OB_FAIL(ls_svr_->get_ls_iter(guard, ObLSGetMod::LOG_MOD))) {
    LOG_WARN("get ls iter failed", K(ret));
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("iter is NULL", K(ret), K(iter));
  } else {
    while (OB_SUCC(ret) && ! has_set_stop()) {
     ls = NULL;
     if (OB_FAIL(iter->get_next(ls))) {
       if (OB_ITER_END != ret) {
         LOG_WARN("iter ls get next failed", K(ret));
       } else {
         LOG_TRACE("iter to end", K(ret));
       }
      } else if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("ls is NULL", K(ret), K(ls));
      } else if (OB_FAIL(foreach_ls_(ls->get_ls_id()))) {
        LOG_WARN("foreach ls failed", K(ret), K(ls));
      }
    }
    // rewrite ret code
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObRemoteFetchWorker::foreach_ls_(const ObLSID &id)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_TASK_COUNT = 6;  // max task count for single turn
  ObFetchLogTask *task = NULL;
  for (int64_t i = 0; i < MAX_TASK_COUNT && OB_SUCC(ret); i++) {
    GET_RESTORE_HANDLER_CTX(id) {
      task = NULL;
      // get task only if it is in turn
      if (OB_FAIL(restore_handler->get_next_sorted_task(task))) {
        if (OB_NOT_MASTER == ret) {
          // do nothing
          LOG_TRACE("ls not master, just skip", K(ret), K(id));
        } else {
          LOG_WARN("get sorted task failed", K(ret), K(id));
        }
      } else if (NULL == task) {
        break;
      } else if (OB_FAIL(submit_entries_(*task))) {
        if (OB_RESTORE_LOG_TO_END != ret) {
          LOG_WARN("submit_entries_ failed", K(ret), KPC(task));
        }
      }

      // try retire task, if task is consumed done or stale, free it,
      // otherwise push_back to task_queue_
      if (NULL != task) {
        int tmp_ret = OB_SUCCESS;
        task->iter_.update_source_cb();
        if (OB_SUCCESS != (tmp_ret = try_retire_(task))) {
          LOG_WARN("retire task failed", K(tmp_ret), KP(task));
        }
      }
    }
  }
  // rewrite ret code
  ret = OB_SUCCESS;
  return ret;
}

void ObRemoteFetchWorker::inner_free_task_(ObFetchLogTask &task)
{
  task.reset();
  mtl_free(&task);
}

bool ObRemoteFetchWorker::is_retry_ret_code_(const int ret_code) const
{
  return OB_ITER_END == ret_code
    || OB_NOT_MASTER == ret_code
    || OB_EAGAIN == ret_code
    || OB_ALLOCATE_MEMORY_FAILED == ret_code
    || OB_LS_NOT_EXIST == ret_code
    || OB_ENTRY_NOT_EXIST == ret_code
    || is_io_error(ret_code);
}

bool ObRemoteFetchWorker::is_fatal_error_(const int ret_code) const
{
  return OB_ARCHIVE_ROUND_NOT_CONTINUOUS == ret_code
    || OB_ARCHIVE_LOG_RECYCLED == ret_code;
}

void ObRemoteFetchWorker::report_error_(const ObLSID &id, const int ret_code, const palf::LSN &lsn)
{
  int ret = OB_SUCCESS;
  GET_RESTORE_HANDLER_CTX(id) {
    restore_handler->mark_error(*ObCurTraceId::get_trace_id(), ret_code, lsn);
  }
}

} // namespace logservice
} // namespace oceanbase
