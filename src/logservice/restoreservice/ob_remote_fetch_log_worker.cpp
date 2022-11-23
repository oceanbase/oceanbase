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
//#include "share/restore/ob_log_archive_source.h"
#include "share/rc/ob_tenant_base.h"                    // mtl_free
#include "logservice/ob_log_handler.h"                  // ObLogHandler
#include "logservice/palf/palf_base_info.h"             // PalfBaseInfo
#include "storage/tx_storage/ob_ls_service.h"           // ObLSService
#include "storage/ls/ob_ls.h"                           // ObLS
#include "logservice/palf/log_group_entry.h"            // LogGroupEntry
#include "logservice/palf/lsn.h"                        // LSN
#include "ob_log_restore_service.h"                     // ObLogRestoreService
#include "ob_fetch_log_task.h"                          // ObFetchLogTask
#include "ob_remote_log_source.h"                       // ObRemoteLogParent
#include "ob_remote_log_iterator.h"                     // ObRemoteLogIterator
#include "ob_log_restore_handler.h"                     // ObLogRestoreHandler
#include "storage/tx_storage/ob_ls_handle.h"            // ObLSHandle

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
  cond_()
{}

ObRemoteFetchWorker::~ObRemoteFetchWorker()
{
  destroy();
}

int ObRemoteFetchWorker::init(const uint64_t tenant_id,
    ObLogRestoreService *restore_service,
    ObLSService *ls_svr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObRemoteFetchWorker has been initialized", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id) || OB_ISNULL(restore_service) || OB_ISNULL(ls_svr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(restore_service), K(ls_svr));
  } else if (OB_FAIL(task_queue_.init(1024 * 1024, "RFLTaskQueue", MTL_ID()))) {
    LOG_WARN("task_queue_ init failed", K(ret));
  } else {
    tenant_id_ = tenant_id;
    restore_service_ = restore_service;
    ls_svr_ = ls_svr;
    inited_ = true;
  }
  return ret;
}

void ObRemoteFetchWorker::destroy()
{
  inited_ = false;
  stop();
  wait();
  ls_svr_ = NULL;
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
  } else if (OB_FAIL(task_queue_.push(task))) {
    LOG_WARN("push task failed", K(ret), KPC(task));
  } else {
    LOG_INFO("submit_fetch_log_task succ", KPC(task));
  }
  return ret;
}

void ObRemoteFetchWorker::run1()
{
  LOG_INFO("ObRemoteFetchWorker thread start");
  lib::set_thread_name("RFLWorker");
  ObCurTraceId::init(GCONF.self_addr_);

  const int64_t THREAD_RUN_INTERVAL = 1 * 1000 * 1000L;
  if (OB_UNLIKELY(! inited_)) {
    LOG_INFO("ObRemoteFetchWorker not init");
  } else {
    while (! has_set_stop()) {
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
    if (OB_FAIL(handle(*task))) {
      LOG_WARN("handle task failed", K(ret), KPC(task));
    }

    // only fatal error report fail, retry with others
    if (is_fatal_error_(ret)) {
      report_error_(id, ret);
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = try_retire_(task))) {
      LOG_WARN("retire task failed", K(tmp_ret), KPC(task));
    }
  }
}

int ObRemoteFetchWorker::handle(ObFetchLogTask &task)
{
  int ret = OB_SUCCESS;
  auto get_source_func = [](const share::ObLSID &id, ObRemoteSourceGuard &source_guard) -> int {
    int ret = OB_SUCCESS;
    ObLSHandle handle;
    ObLS *ls = NULL;
    ObLogRestoreHandler *restore_handler = NULL;
    if (OB_FAIL(MTL(storage::ObLSService *)->get_ls(id, handle, ObLSGetMod::LOG_MOD))) {
      LOG_WARN("get ls failed", K(ret), K(id));
    } else if (OB_ISNULL(ls = handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ls is NULL", K(ret), K(id), K(ls));
    } else if (OB_ISNULL(restore_handler = ls->get_log_restore_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("restore_handler is NULL", K(ret), K(id));
    } else {
      restore_handler->deep_copy_source(source_guard);
      LOG_INFO("print get_source_", KPC(restore_handler), KPC(source_guard.get_source()));
    }
    return ret;
  };

  auto update_source_func = [](const share::ObLSID &id, ObRemoteLogParent *source) -> int {
    int ret = OB_SUCCESS;
    ObLSHandle handle;
    ObLS *ls = NULL;
    ObLogRestoreHandler *restore_handler = NULL;
    if (OB_ISNULL(source) || ! share::is_valid_log_source_type(source->get_source_type())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid source type", K(ret), KPC(source));
    } else
    if (OB_FAIL(MTL(storage::ObLSService *)->get_ls(id, handle, ObLSGetMod::LOG_MOD))) {
      LOG_WARN("get ls failed", K(ret), K(id));
    } else if (OB_ISNULL(ls = handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ls is NULL", K(ret), K(id), K(ls));
    } else if (OB_ISNULL(restore_handler = ls->get_log_restore_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("restore_handler is NULL", K(ret), K(id));
    } else if (OB_FAIL(restore_handler->update_location_info(source))) {
      LOG_WARN("update locate info failed", K(ret), KPC(source), KPC(restore_handler));
    }
    return ret;
  };

  auto refresh_storage_info_func = [](share::ObBackupDest &dest) -> int {
    return OB_NOT_SUPPORTED;
  };

  LSN end_lsn;
  int64_t upper_limit_ts = OB_INVALID_TIMESTAMP;
  int64_t max_fetch_log_ts = OB_INVALID_TIMESTAMP;
  int64_t max_submit_log_ts = OB_INVALID_TIMESTAMP;
  ObRemoteLogIterator iter(get_source_func, update_source_func, refresh_storage_info_func);

  if (OB_UNLIKELY(! task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task));
  } else if (OB_FAIL(iter.init(tenant_id_, task.id_, task.pre_log_ts_,
          task.cur_lsn_, task.end_lsn_))) {
    LOG_WARN("ObRemoteLogIterator init failed", K(ret), K_(tenant_id), K(task));
  } else if (OB_FAIL(get_upper_limit_ts_(task.id_, upper_limit_ts))) {
    LOG_WARN("get upper_limit_ts failed", K(ret), K(task));
  } else if (OB_FAIL(submit_entries_(task.id_, upper_limit_ts, iter, max_submit_log_ts))) {
    LOG_WARN("submit entries failed", K(ret), K(task), K(iter));
  } else if (OB_FAIL(iter.get_cur_lsn_ts(end_lsn, max_fetch_log_ts))) {
    // TODO iterator可能是没有数据的, 此处暂不处理, 后续不需要cut日志, 后续处理逻辑会放到restore_handler, 此处不处理该异常
    // 仅支持备份恢复, 不存在为空场景
    LOG_WARN("iter get cur_lsn failed", K(ret), K(iter), K(task));
  } else if (! end_lsn.is_valid()) {
    // no log, just skip
    LOG_TRACE("no log restore, just skip", K(task));
  } else if (OB_FAIL(task.update_cur_lsn_ts(end_lsn, max_submit_log_ts, max_fetch_log_ts))) {
    LOG_WARN("update cur_lsn failed", K(ret), K(task));
  } else if (FALSE_IT(mark_if_to_end_(task, upper_limit_ts, max_submit_log_ts))) {
  } else {
    LOG_INFO("ObRemoteFetchWorker handle succ", K(task));
  }

  if (is_retry_ret_code_(ret)) {
    ret = OB_SUCCESS;
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_RESTORE_LOG_FAILED) OB_SUCCESS;
  }
#endif
  return ret;
}

int ObRemoteFetchWorker::get_upper_limit_ts_(const ObLSID &id, int64_t &ts)
{
  int ret = OB_SUCCESS;
  ObLSHandle handle;
  ObLS *ls = NULL;
  ObLogRestoreHandler *restore_handler = NULL;
  if (OB_FAIL(ls_svr_->get_ls(id, handle, ObLSGetMod::LOG_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(id));
  } else if (OB_ISNULL(ls = handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls is NULL", K(ret), K(id), K(ls));
  } else if (OB_ISNULL(restore_handler = ls->get_log_restore_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("restore_handler is NULL", K(ret), K(id));
  } else if (OB_FAIL(restore_handler->get_upper_limit_ts(ts))) {
    LOG_WARN("get upper limit ts failed", K(ret), K(id));
  }
  return ret;
}

int ObRemoteFetchWorker::submit_entries_(const ObLSID &id,
    const int64_t upper_limit_ts,
    ObRemoteLogIterator &iter,
    int64_t &max_submit_log_ts)
{
  int ret = OB_SUCCESS;
  LogGroupEntry entry;
  char *buf = NULL;
  int64_t size = 0;
  LSN lsn;
  while (OB_SUCC(ret) && ! has_set_stop()) {
    if (OB_FAIL(iter.next(entry, lsn, buf, size))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("ObRemoteLogIterator next failed", K(ret), K(iter));
      } else {
        LOG_TRACE("ObRemoteLogIterator to end", K(iter));
      }
    } else if (OB_UNLIKELY(! entry.check_integrity())) {
      ret = OB_INVALID_DATA;
      LOG_WARN("entry is invalid", K(ret), K(entry), K(lsn), K(iter));
    } else {
      int64_t pos = 0;
      const int64_t origin_entry_size = entry.get_data_len();
      if (OB_FAIL(cut_group_log_(id, lsn, upper_limit_ts, entry))) {
        LOG_WARN("check and rebuild group entry failed", K(ret), K(id), K(lsn));
      } else if (OB_UNLIKELY(0 == entry.get_data_len())) {
        // no log entry, do nothing
        LOG_INFO("no data exist after cut log", K(entry));
      } else if (origin_entry_size != entry.get_data_len()
          && OB_FAIL(entry.get_header().serialize(buf, size, pos))) {
        LOG_WARN("serialize header failed", K(ret), K(entry));
      } else if (OB_FAIL(submit_log_(id, lsn, buf, entry.get_serialize_size()))) {
        LOG_WARN("submit log failed", K(ret), K(iter), K(buf), K(entry), K(lsn));
      } else {
        max_submit_log_ts = entry.get_header().get_max_timestamp();
      }
      if (entry.get_serialize_size() != size) {
        break;
      }
    }
  } // while
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObRemoteFetchWorker::submit_log_(const ObLSID &id,
    const LSN &lsn,
    char *buf,
    const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  GET_RESTORE_HANDLER_CTX(id) {
    if (OB_FAIL(restore_handler->raw_write(lsn, buf, buf_size))) {
      if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("raw write failed", K(ret), K(id), K(lsn), K(buf), K(buf_size));
      }
    }
  }
  return ret;
}

int ObRemoteFetchWorker::try_retire_(ObFetchLogTask *&task)
{
  int ret = OB_SUCCESS;
  const bool is_finish = task->is_finish();
  const bool is_to_end = task->is_to_end();
  bool is_stale = false;
  GET_RESTORE_HANDLER_CTX(task->id_) {
  if (OB_FAIL(restore_handler->update_fetch_log_progress(task->id_.id(), task->proposal_id_,
          task->cur_lsn_, task->max_submit_log_ts_, is_finish, is_to_end, is_stale))) {
    LOG_WARN("update fetch log progress failed", KPC(task), KPC(restore_handler));
  } else if (is_finish || is_stale || is_to_end) {
    mtl_free(task);
    task = NULL;
    restore_service_->signal();
  } else {
    if (OB_FAIL(task_queue_.push(task))) {
      LOG_ERROR("push failed", K(ret), KPC(task));
    } else {
      task = NULL;
    }
  }
  }
  return ret;
}

int ObRemoteFetchWorker::cut_group_log_(const ObLSID &id,
    const LSN &lsn,
    const int64_t cut_ts,
    LogGroupEntry &entry)
{
  int ret = OB_SUCCESS;
  int64_t group_log_checksum = -1;
  int64_t pre_accum_checksum = -1;
  int64_t accum_checksum = -1;

  if (entry.get_header().get_max_timestamp() <= cut_ts) {
    // do nothing
  } else if (OB_FAIL(get_pre_accum_checksum_(id, lsn, pre_accum_checksum))) {
    LOG_WARN("get pre accsum checksum failed", K(ret), K(id), K(lsn));
  } else if (OB_FAIL(entry.truncate(cut_ts, pre_accum_checksum))) {
    LOG_WARN("truncate entry failed", K(ret), K(cut_ts), K(entry));
  }
  return ret;
}

int ObRemoteFetchWorker::get_pre_accum_checksum_(const ObLSID &id,
    const LSN &lsn,
    int64_t &pre_accum_checksum)
{
  int ret = OB_SUCCESS;
  ObLS *ls = NULL;
  ObLSHandle ls_handle;
  ObLogHandler *log_handler = NULL;
  palf::PalfBaseInfo base_info;
  if (OB_FAIL(ls_svr_->get_ls(id, ls_handle, ObLSGetMod::LOG_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_INFO("get ls is NULL", K(ret), K(id));
  } else if (OB_ISNULL(log_handler = ls->get_log_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_INFO("restore_handler is NULL", K(ret), K(id));
  } else if (OB_FAIL(log_handler->get_palf_base_info(lsn, base_info))) {
    LOG_WARN("get palf base info failed", K(id), K(lsn));
  } else {
    pre_accum_checksum = base_info.prev_log_info_.accum_checksum_;
  }
  return ret;
}

void ObRemoteFetchWorker::mark_if_to_end_(ObFetchLogTask &task,
    const int64_t upper_limit_ts,
    const int64_t timestamp)
{
  if (timestamp == upper_limit_ts) {
    task.mark_to_end();
  }
}

bool ObRemoteFetchWorker::is_retry_ret_code_(const int ret_code) const
{
  return OB_ITER_END == ret_code
    || OB_NOT_MASTER == ret_code
    || OB_EAGAIN == ret_code
    || OB_ALLOCATE_MEMORY_FAILED == ret_code
    || OB_LS_NOT_EXIST == ret_code
    || is_io_error(ret_code);
}

bool ObRemoteFetchWorker::is_fatal_error_(const int ret_code) const
{
  return OB_ARCHIVE_ROUND_NOT_CONTINUOUS == ret_code
    || OB_ARCHIVE_LOG_RECYCLED == ret_code;
}

void ObRemoteFetchWorker::report_error_(const ObLSID &id, const int ret_code)
{
  int ret = OB_SUCCESS;
  GET_RESTORE_HANDLER_CTX(id) {
    restore_handler->mark_error(*ObCurTraceId::get_trace_id(), ret_code);
  }
}

} // namespace
} // namespace oceanbase
