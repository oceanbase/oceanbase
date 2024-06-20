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
#include "ob_remote_log_writer.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/ob_define.h"
#include "share/rc/ob_tenant_base.h"                    // mtl_alloc
#include "storage/tx_storage/ob_ls_map.h"               // ObLSIterator
#include "storage/tx_storage/ob_ls_service.h"           // ObLSService
#include "ob_fetch_log_task.h"                          // ObFetchLogTask
#include "ob_remote_fetch_log_worker.h"                 // ObRemoteFetchWorker
#include "ob_log_restore_service.h"                     // ObLogRestoreService

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

ObRemoteLogWriter::ObRemoteLogWriter() :
  inited_(false),
  tenant_id_(OB_INVALID_TENANT_ID),
  ls_svr_(NULL),
  restore_service_(NULL),
  worker_(NULL)
{}

ObRemoteLogWriter::~ObRemoteLogWriter()
{
  destroy();
}

int ObRemoteLogWriter::init(const uint64_t tenant_id,
    ObLSService *ls_svr,
    ObLogRestoreService *restore_service,
    ObRemoteFetchWorker *worker)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObRemoteLogWriter init twice", K(inited_));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(ls_svr)
      || OB_ISNULL(restore_service)
      || OB_ISNULL(worker)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(ls_svr), K(restore_service), K(worker));
  } else {
    tenant_id_ = tenant_id;
    ls_svr_ = ls_svr;
    restore_service_ = restore_service;
    worker_ = worker;
    inited_ = true;
  }
  return ret;
}

void ObRemoteLogWriter::destroy()
{
  inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_svr_ = NULL;
  restore_service_ = NULL;
  worker_ = NULL;
}

int ObRemoteLogWriter::start()
{
  int ret = OB_SUCCESS;
  share::ObThreadPool::set_run_wrapper(MTL_CTX());
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObRemoteLogWriter not init", K(ret));
  } else if (OB_FAIL(share::ObThreadPool::start())) {
    LOG_WARN("ObRemoteLogWriter start failed", K(ret));
  } else {
    LOG_INFO("ObRemoteLogWriter start succ");
  }
  return ret;
}

void ObRemoteLogWriter::stop()
{
  LOG_INFO("ObRemoteLogWriter thread stop", K_(tenant_id));
  share::ObThreadPool::stop();
}

void ObRemoteLogWriter::wait()
{
  LOG_INFO("ObRemoteFetchWorker thread wait", K_(tenant_id));
  share::ObThreadPool::wait();
}

void ObRemoteLogWriter::run1()
{
  LOG_INFO("ObRemoteLogWriter thread start");
  lib::set_thread_name("RFLWorker");

  const int64_t THREAD_RUN_INTERVAL = 100 * 1000L;
  if (OB_UNLIKELY(! inited_)) {
    LOG_INFO("ObRemoteLogWriter not init");
  } else {
    while (! has_set_stop()) {
      int64_t begin_tstamp = ObTimeUtility::current_time();
      do_thread_task_();
      int64_t end_tstamp = ObTimeUtility::current_time();
      int64_t wait_interval = THREAD_RUN_INTERVAL - (end_tstamp - begin_tstamp);
      if (wait_interval > 0) {
        ob_usleep(wait_interval);
      }
    }
  }
}

void ObRemoteLogWriter::do_thread_task_()
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
}

int ObRemoteLogWriter::foreach_ls_(const ObLSID &id)
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
        LOG_TRACE("task is null", K(id));
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

int ObRemoteLogWriter::submit_entries_(ObFetchLogTask &task)
{
  int ret = OB_SUCCESS;
  LogGroupEntry entry;
  const char *buf = NULL;
  int64_t size = 0;
  LSN lsn;
  const ObLSID id = task.id_;
  const int64_t proposal_id = task.proposal_id_;
  int64_t entry_size = 0;
  LSN max_submit_lsn;
  SCN max_submit_scn;
  while (OB_SUCC(ret) && ! has_set_stop()) {
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
    } else if (FALSE_IT(entry_size = entry.get_serialize_size())) {
    } else if (OB_FAIL(submit_log_(id, proposal_id, lsn,
            entry.get_scn(), buf, entry_size))) {
      LOG_WARN("submit log failed", K(buf), K(entry), K(lsn), K(task));
    } else {
      task.cur_lsn_ = lsn + entry_size;
      max_submit_lsn = lsn + entry_size;
      max_submit_scn = entry.get_scn();
    }
  } // while

  if (OB_ITER_END == ret) {
    if (lsn.is_valid()) {
      LOG_INFO("submit_entries_ succ", K(id), K(lsn), K(entry.get_scn()), K(task));
    }
    ret = OB_SUCCESS;
  }

  if (max_submit_lsn.is_valid() && max_submit_scn.is_valid()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(update_max_fetch_info_(id, proposal_id, max_submit_lsn, max_submit_scn))) {
      LOG_WARN("update max fetch info failed", K(id), K(proposal_id), K(max_submit_lsn), K(max_submit_scn));
    } else {
      LOG_INFO("update max fetch context succ", K(id), K(proposal_id), K(max_submit_lsn), K(max_submit_scn));
    }
  }
  return ret;
}

int ObRemoteLogWriter::submit_log_(const ObLSID &id,
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

  if (OB_ERR_UNEXPECTED == ret) {
    report_error_(id, ret, lsn, ObLogRestoreErrorContext::ErrorType::SUBMIT_LOG);
  }
  return ret;
}

int ObRemoteLogWriter::update_max_fetch_info_(const ObLSID &id,
    const int64_t proposal_id,
    const palf::LSN &lsn,
    const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  GET_RESTORE_HANDLER_CTX(id) {
    if (OB_FAIL(restore_handler->update_max_fetch_info(proposal_id, lsn, scn))) {
      LOG_WARN("update max fetch info failed", K(id), K(proposal_id), K(lsn), K(scn));
    }
  }
  return ret;
}

int ObRemoteLogWriter::try_retire_(ObFetchLogTask *&task)
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
      if (OB_FAIL(worker_->submit_fetch_log_task(task))) {
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

void ObRemoteLogWriter::inner_free_task_(ObFetchLogTask &task)
{
  task.reset();
  mtl_free(&task);
}

void ObRemoteLogWriter::report_error_(const ObLSID &id,
                                        const int ret_code,
                                        const palf::LSN &lsn,
                                        const ObLogRestoreErrorContext::ErrorType &error_type)
{
  int ret = OB_SUCCESS;
  GET_RESTORE_HANDLER_CTX(id) {
    restore_handler->mark_error(*ObCurTraceId::get_trace_id(), ret_code, lsn, error_type);
  }
}
} // namespace logservice
} // namespace oceanbase
