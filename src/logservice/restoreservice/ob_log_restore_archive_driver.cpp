/**
 * Copyright (c) 2023 OceanBase
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
#include "ob_log_restore_archive_driver.h"
#include "ob_remote_fetch_log.h"
#include "lib/allocator/ob_allocator.h"       // ObMemAttr
#include "share/ob_ls_id.h"                   // ObLSID
#include "share/ls/ob_ls_recovery_stat_operator.h"
#include "storage/tx_storage/ob_ls_map.h"     // ObLSIterator
#include "storage/tx_storage/ob_ls_service.h" // ObLSService
#include "logservice/palf/lsn.h"              // LSN
#include "logservice/ob_log_service.h"        // ObLogService
#include "logservice/palf_handle_guard.h"     // PalfHandleGuard
#include "ob_log_restore_handler.h"           // ObTenantRole
#include "ob_remote_fetch_log_worker.h"       // ObRemoteFetchWorker
#include "ob_fetch_log_task.h"                // ObFetchLogTask
#include "ob_log_restore_define.h"            // MAX_LS_FETCH_LOG_TASK_CONCURRENCY
#include "observer/omt/ob_tenant_config_mgr.h"  // tenant_config
#include "logservice/archiveservice/ob_archive_define.h"
namespace oceanbase
{
namespace logservice
{
using namespace oceanbase::share;
using namespace oceanbase::palf;
ObLogRestoreArchiveDriver::ObLogRestoreArchiveDriver() :
  ObLogRestoreDriverBase(),
  worker_(NULL)
{}

ObLogRestoreArchiveDriver::~ObLogRestoreArchiveDriver()
{
  destroy();
}

int ObLogRestoreArchiveDriver::init(const uint64_t tenant_id,
    ObLSService *ls_svr,
    ObLogService *log_service,
    ObRemoteFetchWorker *worker)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(ls_svr)
      || OB_ISNULL(log_service)
      || OB_ISNULL(worker)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(tenant_id), K(ls_svr), K(log_service), K(worker));
  } else if (OB_FAIL(ObLogRestoreDriverBase::init(tenant_id, ls_svr, log_service))) {
    CLOG_LOG(WARN, "init failed", K(tenant_id), K(ls_svr));
  } else {
    worker_ = worker;
  }
  return ret;
}

void ObLogRestoreArchiveDriver::destroy()
{
  ObLogRestoreDriverBase::destroy();
  worker_ = NULL;
}

int ObLogRestoreArchiveDriver::do_fetch_log_(ObLS &ls)
{
  int ret = OB_SUCCESS;
  bool can_fetch_log = false;
  bool need_schedule = false;
  int64_t proposal_id = -1;
  int64_t version = 0;
  LSN lsn;
  SCN pre_scn;   // scn to locate piece
  LSN max_fetch_lsn;
  int64_t last_fetch_ts = OB_INVALID_TIMESTAMP;
  int64_t task_count = 0;
  const ObLSID &id = ls.get_ls_id();
  if (OB_UNLIKELY(! id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(id));
  } else if (OB_FAIL(check_replica_status_(ls, can_fetch_log))) {
    LOG_WARN("check replica status failed", K(ret), K(ls));
  } else if (! can_fetch_log) {
    // just skip
  } else if (OB_FAIL(check_need_schedule_(ls, need_schedule, proposal_id,
          version, max_fetch_lsn, last_fetch_ts, task_count))) {
    LOG_WARN("check need schedule failed", K(ret), K(id));
  } else if (! need_schedule) {
    LOG_TRACE("no need_schedule in do_fetch_log", K(need_schedule));
  } else if (OB_FAIL(get_fetch_log_base_lsn_(ls, max_fetch_lsn, last_fetch_ts, pre_scn, lsn))) {
    LOG_WARN("get fetch log base lsn failed", K(ret), K(id));
  } else if (OB_FAIL(submit_fetch_log_task_(ls, pre_scn, lsn, task_count, proposal_id, version))) {
    LOG_WARN("submit fetch log task failed", K(ret), K(id), K(lsn), K(task_count));
  } else {
    LOG_TRACE("do fetch log succ", K(id), K(lsn), K(task_count));
  }
  LOG_INFO("print do_fetch_log_", K(lsn), K(max_fetch_lsn), K(need_schedule),
      K(proposal_id), K(last_fetch_ts), K(task_count), K(ls));
  return ret;
}

int ObLogRestoreArchiveDriver::check_need_schedule_(ObLS &ls,
    bool &need_schedule,
    int64_t &proposal_id,
    int64_t &version,
    LSN &lsn,
    int64_t &last_fetch_ts,
    int64_t &task_count)
{
  int ret = OB_SUCCESS;
  ObRemoteFetchContext context;
  ObLogRestoreHandler *restore_handler = NULL;
  bool need_delay = false;
  need_schedule = false;
  int64_t concurrency = 0;
  int64_t fetch_log_worker_count = 0;
  if (OB_ISNULL(restore_handler = ls.get_log_restore_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get restore_handler failed", K(ret), "id", ls.get_ls_id());
  } else if (OB_FAIL(restore_handler->need_schedule(need_schedule, proposal_id, context))) {
    LOG_WARN("get fetch log context failed", K(ret), K(ls));
  } else if (! need_schedule) {
    LOG_TRACE("no need_schedule in check_need_schedule_", K(need_schedule));
    // do nothing
  } else if (context.max_fetch_scn_ >= global_recovery_scn_) {
    need_schedule = false;
    LOG_WARN("max_fetch_scn reach global_recovery_scn, no need to schedule fetch task",
     K_(context.max_fetch_scn), K_(global_recovery_scn));
  } else if (OB_FAIL(worker_->get_thread_count(fetch_log_worker_count))) {
    LOG_WARN("get_thread_count from worker_ failed", K(ret), K(ls));
  } else if (FALSE_IT(concurrency = std::min(fetch_log_worker_count, MAX_LS_FETCH_LOG_TASK_CONCURRENCY))) {
  } else if (context.issue_task_num_ >= concurrency) {
    need_schedule = false;
    LOG_TRACE("concurrency not enough check_need_schedule", K_(context.issue_task_num), K(concurrency));
  } else if (OB_FAIL(check_need_delay_(ls.get_ls_id(), need_delay))) {
    LOG_WARN("check need delay failed", K(ret), K(ls));
  } else if (need_delay) {
    need_schedule = false;
    LOG_TRACE("need_delay in check_need_schedule", K(need_delay));
  } else {
    version = context.issue_version_;
    lsn = context.max_submit_lsn_;
    last_fetch_ts = context.last_fetch_ts_;
    task_count = concurrency - context.issue_task_num_;
  }
  return ret;
}

// Restore log need be under control, otherwise log disk may be full as single ls restore log too fast
// NB: Logs can be replayed only if its scn not bigger than upper_limit
int ObLogRestoreArchiveDriver::check_need_delay_(const ObLSID &id, bool &need_delay)
{
  int ret = OB_SUCCESS;
  SCN upper_limit;
  SCN end_scn;
  PalfHandleGuard palf_handle;
  if (OB_FAIL(get_upper_resotore_scn(upper_limit))) {
    ARCHIVE_LOG(WARN, "get replayable point failed", K(ret));
  } else if (OB_FAIL(log_service_->open_palf(id, palf_handle))) {
    LOG_WARN("open palf failed", K(ret), K(id));
  } else if (OB_FAIL(palf_handle.get_end_scn(end_scn))) {
    LOG_WARN("get end log ts failed", K(ret), K(id));
  } else {
    need_delay = end_scn > upper_limit;
    if (need_delay) {
      LOG_TRACE("need_delay is true", K(id), K(upper_limit), K(end_scn));
    }
  }
  return ret;
}

int ObLogRestoreArchiveDriver::get_fetch_log_base_lsn_(ObLS &ls,
    const LSN &max_fetch_lsn,
    const int64_t last_fetch_ts,
    SCN &heuristic_scn,
    LSN &lsn)
{
  int ret = OB_SUCCESS;
  LSN end_lsn;
  LSN restore_lsn;
  const bool ignore_restore = OB_INVALID_TIMESTAMP == last_fetch_ts
    || last_fetch_ts < ObTimeUtility::current_time() - 5 * 1000 * 1000L
    || ! max_fetch_lsn.is_valid();
  if (OB_FAIL(get_palf_base_lsn_scn_(ls, end_lsn, heuristic_scn))) {
    LOG_WARN("get palf base lsn failed", K(ret), K(ls));
  } else {
    lsn = ignore_restore ? end_lsn : max_fetch_lsn;
  }
  return ret;
}

// 如果日志流当前LSN为0, 以日志流create_scn作为基准时间戳
int ObLogRestoreArchiveDriver::get_palf_base_lsn_scn_(ObLS &ls, LSN &lsn, SCN &scn)
{
  int ret = OB_SUCCESS;
  PalfHandleGuard palf_handle_guard;
  const ObLSID &id = ls.get_ls_id();
  if (OB_FAIL(log_service_->open_palf(id, palf_handle_guard))) {
    LOG_WARN("open palf failed", K(ret));
  } else if (OB_FAIL(palf_handle_guard.get_end_scn(scn))) {
    LOG_WARN("get end log scn failed", K(ret), K(id));
  } else if (OB_FAIL(palf_handle_guard.get_end_lsn(lsn))) {
    LOG_WARN("get end lsn failed", K(ret), K(id));
  } else if (OB_UNLIKELY(!scn.is_valid() || !lsn.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get_palf_base_lsn_scn_, return invalid scn or lsn", K(id), K(scn), K(lsn));
  } else if (!ls.get_clog_checkpoint_scn().is_valid()) {
    // checkpoint_scn not valid, just skip it
    LOG_ERROR("ls checkpoint_scn not valid", K(ls));
  } else {
    // The start scn for palf to restore is the min scn of a block,
    // so if the min scn exists in two or more archive rounds and the the rounds are not continuous,
    // the location of the scn will hit the first round, which results to restore failure.
    //
    // Use the ls checkpoint_scn to help the first location.
    //
    // For example, the round 1 range: [100, 200], the checkpoint_scn of the matched backup fall in the range [100, 200],
    // the round 2 range: [300, 500], the checkpoint_scn of the matched backup in [300, 500].
    // But the start_scn for restore maybe only 50, so it will locate failed.
    // The checkpoint_scn is precise, it will help to locate the correct round.
    scn = std::max(scn, ls.get_clog_checkpoint_scn());
  }
  return ret;
}

int ObLogRestoreArchiveDriver::submit_fetch_log_task_(ObLS &ls,
    const SCN &scn,
    const LSN &lsn,
    const int64_t task_count,
    const int64_t proposal_id,
    const int64_t version)
{
  int ret = OB_SUCCESS;
  LSN start_lsn = lsn;
  const int64_t max_size = archive::MAX_ARCHIVE_FILE_SIZE;
  bool scheduled = false;
  for (int64_t index = 0; OB_SUCC(ret) && index < task_count; index++) {
    // To avoid fetch log from one archive repeatedly, then range of task equals to a single archive file
    const LSN end_lsn = LSN((start_lsn.val_ / max_size + 1) * max_size);
    const int64_t size = static_cast<int64_t>(end_lsn - start_lsn);
    scheduled = false;
    if (OB_FAIL(do_submit_fetch_log_task_(ls, scn, start_lsn, size, proposal_id, version, scheduled))) {
      LOG_WARN("do submit fetch log task failed", K(ret), K(ls));
    } else if (! scheduled) {
      break;
    } else {
      start_lsn = end_lsn;
    }
  }
  return ret;
}

int ObLogRestoreArchiveDriver::do_submit_fetch_log_task_(ObLS &ls,
    const SCN &scn,
    const LSN &lsn,
    const int64_t size,
    const int64_t proposal_id,
    const int64_t version,
    bool &scheduled)
{
  int ret = OB_SUCCESS;
  void *data = NULL;
  ObFetchLogTask *task = NULL;
  ObMemAttr attr(MTL_ID(), "FetchLogTask");
  const ObLSID &id = ls.get_ls_id();
  if (OB_ISNULL(data = mtl_malloc(sizeof(ObFetchLogTask), "RFLTask"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(id));
  } else {
    task = new (data) ObFetchLogTask(id, scn, lsn, size, proposal_id, version);
    ObLogRestoreHandler *restore_handler = NULL;
    if (OB_ISNULL(restore_handler = ls.get_log_restore_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get restore_handler failed", K(ret), K(ls));
    } else if (OB_FAIL(restore_handler->schedule(id.id(), proposal_id, version, lsn + size, scheduled))) {
      LOG_WARN("schedule failed", K(ret), K(ls));
    } else if (! scheduled) {
      // not scheduled
    } else if (OB_FAIL(worker_->submit_fetch_log_task(task))) {
      scheduled = false;
      LOG_ERROR("submit fetch log task failed", K(ret), K(id));
    } else {
      worker_->signal();
      LOG_INFO("submit fetch log task succ", K(task),
          "id", ls.get_ls_id(), K(scn), K(lsn), K(size), K(proposal_id));
    }
    if (! scheduled && NULL != data) {
      mtl_free(data);
      data = NULL;
    }
  }
  return ret;
}

} // namespace logservice
} // namespace oceanbase
