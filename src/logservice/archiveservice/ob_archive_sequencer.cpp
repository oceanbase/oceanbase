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

#include "ob_archive_sequencer.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "ob_ls_mgr.h"                    // ObArchiveLSMgr
#include "logservice/ob_log_service.h"    // ObLogService
#include "ob_archive_define.h"            // ArchiveWorkStation
#include "ob_ls_task.h"                   // ObLSArchiveTask
#include "ob_archive_task.h"              // ObArchiveLogFetchTask
#include "ob_archive_fetcher.h"           // ObArchiveFetcher
#include "ob_archive_round_mgr.h"         // ObArchiveRoundMgr
#include "logservice/palf_handle_guard.h" // PalfHandleGuard
#include "share/backup/ob_archive_struct.h"
#include <cstdint>

namespace oceanbase
{
namespace archive
{
using namespace oceanbase::logservice;
using namespace oceanbase::share;
using namespace oceanbase::palf;

ObArchiveSequencer::ObArchiveSequencer() :
  inited_(false),
  tenant_id_(OB_INVALID_TENANT_ID),
  round_(OB_INVALID_ARCHIVE_ROUND_ID),
  archive_fetcher_(NULL),
  ls_mgr_(NULL),
  round_mgr_(NULL),
  min_scn_(),
  seq_cond_()
{}

ObArchiveSequencer::~ObArchiveSequencer()
{
  ARCHIVE_LOG(INFO, "ObArchiveSequencer destroy");
  destroy();
}

int ObArchiveSequencer::init(const uint64_t tenant_id,
    ObLogService *log_service,
    ObArchiveFetcher *fetcher,
    ObArchiveLSMgr *ls_mgr,
    ObArchiveRoundMgr *round_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    ARCHIVE_LOG(ERROR, "ObArchiveSequencer has been initialized", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(log_service_ = log_service)
      || OB_ISNULL(archive_fetcher_ = fetcher)
      || OB_ISNULL(ls_mgr_ = ls_mgr)
      || OB_ISNULL(round_mgr_ = round_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(log_service),
        K(fetcher), K(ls_mgr), K(round_mgr));
  } else {
    tenant_id_ = tenant_id;
    inited_ = true;
  }
  return ret;
}

void ObArchiveSequencer::destroy()
{
  inited_ = false;
  stop();
  wait();
  tenant_id_ = OB_INVALID_TENANT_ID;
  round_ = OB_INVALID_ARCHIVE_ROUND_ID;
  archive_fetcher_ = NULL;
  ls_mgr_ = NULL;
  min_scn_.reset();
}

int ObArchiveSequencer::start()
{
  int ret = OB_SUCCESS;
  ObThreadPool::set_run_wrapper(MTL_CTX());
  if (OB_UNLIKELY(! inited_)) {
    ARCHIVE_LOG(ERROR, "ObArchiveSequencer not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(ObThreadPool::start())) {
    ARCHIVE_LOG(WARN, "ObArchiveSequencer start fail", K(ret));
  } else {
    ARCHIVE_LOG(INFO, "ObArchiveSequencer start succ");
  }
  return ret;
}

void ObArchiveSequencer::stop()
{
  ARCHIVE_LOG(INFO, "ObArchiveSequencer stop");
  ObThreadPool::stop();
}

void ObArchiveSequencer::wait()
{
  ARCHIVE_LOG(INFO, "ObArchiveSequencer wait");
  ObThreadPool::wait();
}

void ObArchiveSequencer::notify_start()
{
  seq_cond_.signal();
  ARCHIVE_LOG(INFO, "sequencer notify start succ");
}

void ObArchiveSequencer::signal()
{
  seq_cond_.signal();
}

void ObArchiveSequencer::run1()
{
  ARCHIVE_LOG(INFO, "ObArchiveSequencer thread start");
  lib::set_thread_name("ArcSeq");
  const int64_t THREAD_RUN_INTERVAL = 100 * 1000L;

  lib::set_thread_name("ArcSeq");
  ObCurTraceId::init(GCONF.self_addr_);

  if (OB_UNLIKELY(! inited_)) {
    ARCHIVE_LOG_RET(ERROR, OB_NOT_INIT, "ObArchiveSequencer not init");
  } else {
    while (! has_set_stop()) {
      int64_t begin_tstamp = ObTimeUtility::current_time();
      do_thread_task_();
      int64_t end_tstamp = ObTimeUtility::current_time();
      int64_t wait_interval = THREAD_RUN_INTERVAL - (end_tstamp - begin_tstamp);
      if (wait_interval > 0) {
        seq_cond_.timedwait(wait_interval);
      }
    }
  }
  ARCHIVE_LOG(INFO, "ObArchiveSequencer thread end");
}

void ObArchiveSequencer::do_thread_task_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(produce_log_fetch_task_())) {
    ARCHIVE_LOG(WARN, "produce log fetch task failed", K(ret));
  }
}

int ObArchiveSequencer::produce_log_fetch_task_()
{
  int ret = OB_SUCCESS;
  ObLSID unused_id;
  ArchiveKey key;
  share::ObArchiveRoundState state;
  round_mgr_->get_archive_round_info(key, state);
  if (! state.is_doing()) {
    // just skip
    if (REACH_TIME_INTERVAL(60 * 1000 * 1000L)) {
      ARCHIVE_LOG(INFO, "archive round not in doing status, just skip", K(key), K(state));
    }
  } else {
    GenFetchTaskFunctor functor(tenant_id_, key, unused_id, log_service_, archive_fetcher_, ls_mgr_);
    if (OB_FAIL(ls_mgr_->foreach_ls(functor))) {
      ARCHIVE_LOG(WARN, "foreach ls failed", K(ret), K(key));
    }
  }
  return ret;
}

// ====================== start of GenFetchTaskFunctor =======================

bool GenFetchTaskFunctor::operator()(const ObLSID &id, ObLSArchiveTask *ls_archive_task)
{
  int ret = OB_SUCCESS;
  ArchiveWorkStation station;
  LSN seq_lsn;
  LSN commit_lsn;
  LSN end_lsn;

  LSN fetch_lsn;
  SCN fetch_scn;

  LogFileTuple archive_tuple;
  int64_t unused_file_id = 0;
  int64_t unused_file_offset = 0;
  int64_t unused_timestamp = common::OB_INVALID_TIMESTAMP;
  if (OB_ISNULL(ls_archive_task)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "ls_archive_task is NULL", K(ret), K(id), K(ls_archive_task));
  } else if (OB_FAIL(ls_archive_task->get_sequencer_progress(key_, station, seq_lsn))) {
    ARCHIVE_LOG(WARN, "get sequence progress failed", K(ret), K(id), KPC(ls_archive_task));
  } else if (OB_FAIL(ls_archive_task->get_archive_progress(station, unused_file_id, unused_file_offset, archive_tuple))) {
    ARCHIVE_LOG(WARN, "get archive progress failed", K(ret), K(id), KPC(ls_archive_task));
  } else if (OB_UNLIKELY(seq_lsn < archive_tuple.get_lsn())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "seq_lsn smaller than archive progress lsn", K(id), K(seq_lsn), K(archive_tuple));
  } else if (seq_lsn - archive_tuple.get_lsn() >= MAX_LS_ARCHIVE_MEMORY_LIMIT) {
    // just skip
    ARCHIVE_LOG(TRACE, "cache sequenced log size reach limit, just wait", K(id), K(seq_lsn), K(archive_tuple));
  } else if (OB_FAIL(get_commit_index_(id, commit_lsn))) {
    ARCHIVE_LOG(WARN, "get commit index failed", K(ret), K(id));
  } else if (OB_FAIL(ls_archive_task->get_fetcher_progress(station, fetch_lsn, fetch_scn, unused_timestamp))) {
    ARCHIVE_LOG(WARN, "get fetch progress failed", K(ret), K(ls_archive_task));
  } else {
    LSN lsn = seq_lsn;
    LSN end_lsn;
    while (lsn <= commit_lsn && OB_SUCC(ret)) {
      ObArchiveLogFetchTask *task = NULL;
      cal_end_lsn_(lsn, fetch_lsn, commit_lsn, end_lsn);
      if (end_lsn <= lsn) {
        break;
      } else if (OB_FAIL(generate_log_fetch_task_(id, station, lsn, end_lsn, task))) {
        ARCHIVE_LOG(WARN, "generate log fetch task failed", K(ret));
      } else if (OB_FAIL(archive_fetcher_->submit_log_fetch_task(task))) {
        ARCHIVE_LOG(WARN, "submit log fetch task failed", K(ret), KPC(task));
        archive_fetcher_->free_log_fetch_task(task);
        task = NULL;
      } else {
        archive_fetcher_->signal();
        if (OB_FAIL(ls_archive_task->update_sequencer_progress(station, end_lsn - lsn, end_lsn))) {
          ARCHIVE_LOG(WARN, "update sequencer progress failed", K(ret),
              K(id), K(station), K(lsn), K(end_lsn));
        } else {
          lsn = end_lsn;
          ARCHIVE_LOG(INFO, "generate log fetch task succ", K(id), K(station), K(lsn));
        }
      }
    }
  }
  return true;
}

int GenFetchTaskFunctor::get_commit_index_(const ObLSID &id, LSN &lsn)
{
  int ret = OB_SUCCESS;
  palf::PalfHandleGuard palf_handle_guard;
  if (OB_ISNULL(log_service_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "log service is NULL", K(ret), K(log_service_));
  } else if (OB_FAIL(log_service_->open_palf(id, palf_handle_guard))) {
    ARCHIVE_LOG(WARN, "get log service failed", K(ret), K(id));
  } else if (OB_FAIL(palf_handle_guard.get_end_lsn(lsn))) {
    ARCHIVE_LOG(WARN, "get commit index failed", K(ret), K(id));
  } else if (OB_UNLIKELY(! lsn.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "invalid log offset", K(ret), K(id), K(lsn));
  } else {
    ARCHIVE_LOG(TRACE, "get commit index succ", K(id), K(lsn));
  }
  return ret;
}

void GenFetchTaskFunctor::cal_end_lsn_(const LSN &base_lsn,
    const LSN &fetch_lsn,
    const LSN &commit_lsn,
    LSN &end_lsn)
{
  if (base_lsn >= commit_lsn) {
    end_lsn = base_lsn;
  } else if ((base_lsn - fetch_lsn) / MAX_ARCHIVE_FILE_SIZE + 1 >= MAX_FETCH_TASK_NUM) {
    end_lsn = base_lsn;
  } else {
    end_lsn = LSN((base_lsn.val_ / MAX_ARCHIVE_FILE_SIZE + 1) * MAX_ARCHIVE_FILE_SIZE);
  }
  ARCHIVE_LOG(TRACE, "print cal_end_lsn_", K(base_lsn), K(fetch_lsn), K(commit_lsn), K(end_lsn));
}

int GenFetchTaskFunctor::generate_log_fetch_task_(const ObLSID &id,
    const ArchiveWorkStation &station,
    const LSN &start_lsn,
    const LSN &end_lsn,
    ObArchiveLogFetchTask *&task)
{
  int ret = OB_SUCCESS;
  ObArchiveLogFetchTask *tmp_task = NULL;
  palf::PalfHandleGuard palf_handle;
  share::SCN scn;
  task = NULL;

  if (OB_ISNULL(tmp_task = archive_fetcher_->alloc_log_fetch_task())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ARCHIVE_LOG(WARN, "alloc log fetch task failed", K(ret), K(id));
  } else if (OB_FAIL(log_service_->open_palf(id, palf_handle))) {
    ARCHIVE_LOG(WARN, "open_palf failed", K(id));
  } else if (OB_FAIL(palf_handle.locate_by_lsn_coarsely(start_lsn, scn))) {
    ARCHIVE_LOG(WARN, "locate by lsn failed", K(id), K(start_lsn));
  } else if (OB_FAIL(tmp_task->init(tenant_id_, id, station, scn, start_lsn, end_lsn))) {
    ARCHIVE_LOG(WARN, "log fetch task init failed", K(ret), K(id), K(station));
  } else {
    task = tmp_task;
    ARCHIVE_LOG(INFO, "generate log fetch task succ", K(ret), KPC(task));
  }

  // only task not pushed into array should be free here
  if (OB_FAIL(ret) && NULL != tmp_task) {
    archive_fetcher_->free_log_fetch_task(tmp_task);
  }

  if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
    int tmp_ret = OB_CLOG_RECYCLE_BEFORE_ARCHIVE;
    ObArchiveInterruptReason reason;
    reason.set(ObArchiveInterruptReason::Factor::LOG_RECYCLE, lbt(), tmp_ret);
    LOG_DBA_ERROR(OB_CLOG_RECYCLE_BEFORE_ARCHIVE, "msg", "observer clog is recycled "
        "before archive, check if archive speed is less than clog writing speed "
        "or archive device is full or archive device is not healthy",
        "ret", tmp_ret);
    ls_mgr_->mark_fatal_error(id, station.get_round(), reason);
  }
  return ret;
}

// ==================== start of QueryMinLogTsFunctor ========================

bool QueryMinLogTsFunctor::operator()(const ObLSID &id, ObLSArchiveTask *ls_archive_task)
{
  UNUSED(id);
  UNUSED(ls_archive_task);
  return true;
}

} // namespace archive
} // namespace oceanbase
