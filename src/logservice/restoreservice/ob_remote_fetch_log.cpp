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

ObRemoteFetchLogImpl::ObRemoteFetchLogImpl() :
  inited_(false),
  tenant_id_(OB_INVALID_TENANT_ID),
  ls_svr_(NULL),
  log_service_(NULL),
  worker_(NULL)
{}

ObRemoteFetchLogImpl::~ObRemoteFetchLogImpl()
{
  destroy();
}

int ObRemoteFetchLogImpl::init(const uint64_t tenant_id,
    ObLSService *ls_svr,
    ObLogService *log_service,
    ObRemoteFetchWorker *worker)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObRemoteFetchLogImpl init twice", K(ret), K(inited_));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(ls_svr)
      || OB_ISNULL(log_service)
      || OB_ISNULL(worker)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ls_svr), K(log_service), K(worker));
  } else {
    tenant_id_ = tenant_id;
    ls_svr_ = ls_svr;
    log_service_ = log_service;
    worker_ = worker;
    inited_ = true;
  }
  return ret;
}

void ObRemoteFetchLogImpl::destroy()
{
  inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_svr_ = NULL;
  log_service_ = NULL;
  worker_ = NULL;
}

int ObRemoteFetchLogImpl::do_schedule()
{
  int ret = OB_SUCCESS;
  ObLS *ls = NULL;
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRemoteFetchLogImpl not init", K(ret));
  } else if (OB_FAIL(ls_svr_->get_ls_iter(guard, ObLSGetMod::LOG_MOD))) {
    LOG_WARN("get log stream iter failed", K(ret));
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("iter is NULL", K(ret), K(iter));
  } else {
    while (OB_SUCC(ret)) {
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
      } else if (OB_FAIL(do_fetch_log_(*ls))) {
        LOG_WARN("do fetch log failed", K(ret), K(ls));
      }
    } // while
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObRemoteFetchLogImpl::do_fetch_log_(ObLS &ls)
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

int ObRemoteFetchLogImpl::check_replica_status_(ObLS &ls, bool &can_fetch_log)
{
  int ret = OB_SUCCESS;
  ObLSRestoreStatus restore_status;
  if (OB_FAIL(ls.get_restore_status(restore_status))) {
    LOG_WARN("get restore status failed", K(ret), K(ls));
  } else {
    can_fetch_log = restore_status.can_restore_log();
  }
  return ret;
}

int ObRemoteFetchLogImpl::check_need_schedule_(ObLS &ls,
    bool &need_schedule,
    int64_t &proposal_id,
    int64_t &version,
    LSN &lsn,
    int64_t &last_fetch_ts,
    int64_t &task_count)
{
  int ret = OB_SUCCESS;
  int64_t concurrency = 0;
  ObRemoteFetchContext context;
  ObLogRestoreHandler *restore_handler = NULL;
  bool need_delay = false;
  need_schedule = false;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  const int64_t log_restore_concurrency = tenant_config.is_valid() ? tenant_config->log_restore_concurrency : 1L;
  if (0 == log_restore_concurrency) {
    concurrency = std::min(get_restore_concurrency_by_max_cpu(tenant_id_), MAX_LS_FETCH_LOG_TASK_CONCURRENCY);
  } else {
    concurrency = std::min(log_restore_concurrency, MAX_LS_FETCH_LOG_TASK_CONCURRENCY);
  }
  if (OB_ISNULL(restore_handler = ls.get_log_restore_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get restore_handler failed", K(ret), "id", ls.get_ls_id());
  } else if (OB_FAIL(restore_handler->need_schedule(need_schedule, proposal_id, context))) {
    LOG_WARN("get fetch log context failed", K(ret), K(ls));
  } else if (! need_schedule) {
    // do nothing
  } else if (context.issue_task_num_ >= concurrency) {
    need_schedule = false;
  } else if (OB_FAIL(check_need_delay_(ls.get_ls_id(), need_delay))) {
    LOG_WARN("check need delay failed", K(ret), K(ls));
  } else if (need_delay) {
    need_schedule = false;
  } else {
    version = context.issue_version_;
    lsn = context.max_submit_lsn_;
    last_fetch_ts = context.last_fetch_ts_;
    task_count = concurrency - context.issue_task_num_;
  }
  return ret;
}

// Restore log need be under control, otherwise log disk may be full as single ls restore log too fast
// NB: Logs can be replayed only if its scn not bigger than replayable_point
int ObRemoteFetchLogImpl::check_need_delay_(const ObLSID &id, bool &need_delay)
{
  int ret = OB_SUCCESS;
  SCN replayable_point;
  SCN end_scn;
  PalfHandleGuard palf_handle;
  if (OB_FAIL(log_service_->get_replayable_point(replayable_point))) {
    ARCHIVE_LOG(WARN, "get replayable point failed", K(ret));
  } else if (OB_FAIL(log_service_->open_palf(id, palf_handle))) {
    LOG_WARN("open palf failed", K(ret), K(id));
  } else if (OB_FAIL(palf_handle.get_end_scn(end_scn))) {
    LOG_WARN("get end log ts failed", K(ret), K(id));
  } else {
    need_delay = (end_scn.convert_to_ts() - replayable_point.convert_to_ts()) > FETCH_LOG_AHEAD_THRESHOLD_US;
  }
  return ret;
}

int ObRemoteFetchLogImpl::get_fetch_log_base_lsn_(ObLS &ls,
    const LSN &max_fetch_lsn,
    const int64_t last_fetch_ts,
    SCN &heuristic_scn,
    LSN &lsn)
{
  int ret = OB_SUCCESS;
  LSN end_lsn;
  LSN restore_lsn;
  const bool ignore_restore = OB_INVALID_TIMESTAMP == last_fetch_ts
    || last_fetch_ts < ObTimeUtility::current_time() - 5 * 1000 * 1000L;
  if (OB_FAIL(get_palf_base_lsn_scn_(ls, end_lsn, heuristic_scn))) {
    LOG_WARN("get palf base lsn failed", K(ret), K(ls));
  } else if (! max_fetch_lsn.is_valid()) {
    lsn = end_lsn;
  } else {
    lsn = ignore_restore ? end_lsn : max_fetch_lsn;
  }
  return ret;
}

// 如果日志流当前LSN为0, 以日志流create_scn作为基准时间戳
int ObRemoteFetchLogImpl::get_palf_base_lsn_scn_(ObLS &ls, LSN &lsn, SCN &scn)
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
  }
  return ret;
}

int ObRemoteFetchLogImpl::submit_fetch_log_task_(ObLS &ls,
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

int ObRemoteFetchLogImpl::do_submit_fetch_log_task_(ObLS &ls,
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
