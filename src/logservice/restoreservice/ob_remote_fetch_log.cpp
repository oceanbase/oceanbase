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
  LSN lsn;
  int64_t pre_log_ts = OB_INVALID_TIMESTAMP;   // log_ts to locate piece
  LSN max_fetch_lsn;
  int64_t last_fetch_ts = OB_INVALID_TIMESTAMP;
  int64_t size = 0;
  const ObLSID &id = ls.get_ls_id();
  ObLSRestoreStatus restore_status;
  if (OB_UNLIKELY(! id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(id));
  } else if (OB_FAIL(ls.get_restore_status(restore_status))) {
    LOG_WARN("failed to get ls restore status", K(ret), K(id));
  } else if (!restore_status.can_restore_log()) {
  } else if (OB_FAIL(check_replica_status_(ls, can_fetch_log))) {
    LOG_WARN("check replica status failed", K(ret), K(ls));
  } else if (! can_fetch_log) {
    // just skip
  } else if (OB_FAIL(check_need_schedule_(ls, need_schedule,
          proposal_id, max_fetch_lsn, last_fetch_ts))) {
    LOG_WARN("check need schedule failed", K(ret), K(id));
  } else if (! need_schedule) {
  } else if (OB_FAIL(get_fetch_log_base_lsn_(ls, max_fetch_lsn, last_fetch_ts, pre_log_ts, lsn, size))) {
    LOG_WARN("get fetch log base lsn failed", K(ret), K(id));
  } else if (OB_FAIL(submit_fetch_log_task_(ls, pre_log_ts, lsn, size, proposal_id))) {
    LOG_WARN("submit fetch log task failed", K(ret), K(id), K(lsn), K(size));
  } else {
    LOG_TRACE("do fetch log succ", K(id), K(lsn), K(size));
  }
  LOG_INFO("print do_fetch_log_", K(lsn), K(max_fetch_lsn), K(need_schedule),
      K(proposal_id), K(last_fetch_ts), K(size), K(ls));
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
    LSN &lsn,
    int64_t &last_fetch_ts)
{
  int ret = OB_SUCCESS;
  ObRemoteFetchContext context;
  ObLogRestoreHandler *restore_handler = NULL;
  need_schedule = false;
  if (OB_ISNULL(restore_handler = ls.get_log_restore_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get restore_handler failed", K(ret), "id", ls.get_ls_id());
  } else if (OB_FAIL(restore_handler->need_schedule(need_schedule, proposal_id, context))) {
    LOG_WARN("get fetch log context failed", K(ret), K(ls));
  } else {
    lsn = context.max_fetch_lsn_;
    last_fetch_ts = context.last_fetch_ts_;
  }
  return ret;
}

// TODO 参考租户同步位点/可回放点/日志盘空间等, 计算可以拉取日志上限
int ObRemoteFetchLogImpl::get_fetch_log_max_lsn_(ObLS &ls, palf::LSN &max_lsn)
{
  UNUSED(ls);
  UNUSED(max_lsn);
  int ret = OB_SUCCESS;
  return ret;
}

//TODO fetch log strategy
int ObRemoteFetchLogImpl::get_fetch_log_base_lsn_(ObLS &ls,
    const LSN &max_fetch_lsn,
    const int64_t last_fetch_ts,
    int64_t &heuristic_log_ts,
    LSN &lsn,
    int64_t &size)
{
  int ret = OB_SUCCESS;
  LSN end_lsn;
  LSN restore_lsn;
  const int64_t default_fetch_size = LEADER_DEFAULT_GROUP_BUFFER_SIZE;
  const bool ignore_restore = OB_INVALID_TIMESTAMP == last_fetch_ts
    || last_fetch_ts < ObTimeUtility::current_time() - 5 * 1000 * 1000L;
  if (OB_FAIL(get_palf_base_lsn_ts_(ls, end_lsn, heuristic_log_ts))) {
    LOG_WARN("get palf base lsn failed", K(ret), K(ls));
  } else {
    lsn = ignore_restore ? end_lsn : max_fetch_lsn;
    size = default_fetch_size;
  }
  return ret;
}

// 如果日志流当前LSN为0, 以日志流create_ts作为基准时间戳
int ObRemoteFetchLogImpl::get_palf_base_lsn_ts_(ObLS &ls, LSN &lsn, int64_t &log_ts)
{
  int ret = OB_SUCCESS;
  PalfHandleGuard palf_handle_guard;
  const ObLSID &id = ls.get_ls_id();
  if (OB_FAIL(log_service_->open_palf(id, palf_handle_guard))) {
    LOG_WARN("open palf failed", K(ret));
  } else if (OB_FAIL(palf_handle_guard.get_end_ts_ns(log_ts))) {
    LOG_WARN("get end log ts failed", K(ret), K(id));
  } else if (OB_FAIL(palf_handle_guard.get_end_lsn(lsn))) {
    LOG_WARN("get end lsn failed", K(ret), K(id));
  } else {
    log_ts = std::max(log_ts, ls.get_clog_checkpoint_ts());
  }
  return ret;
}

int ObRemoteFetchLogImpl::submit_fetch_log_task_(ObLS &ls,
    const int64_t log_ts,
    const LSN &lsn,
    const int64_t size,
    const int64_t proposal_id)
{
  int ret = OB_SUCCESS;
  void *data = NULL;
  ObFetchLogTask *task = NULL;
  ObMemAttr attr(MTL_ID(), "FetchLogTask");
  const ObLSID &id = ls.get_ls_id();
  bool scheduled = false;
  if (OB_ISNULL(data = mtl_malloc(sizeof(FetchLogTask), "RFLTask"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(id));
  } else {
    task = new (data) ObFetchLogTask(id, log_ts, lsn, size, proposal_id);
    ObLogRestoreHandler *restore_handler = NULL;
    if (OB_ISNULL(restore_handler = ls.get_log_restore_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get restore_handler failed", K(ret), K(ls));
    } else if (OB_FAIL(restore_handler->schedule(id.id(), proposal_id, lsn + size, scheduled))) {
      LOG_WARN("schedule failed", K(ret), K(ls));
    } else if (! scheduled) {
      // not scheduled
    } else if (OB_FAIL(worker_->submit_fetch_log_task(task))) {
      scheduled = false;
      LOG_ERROR("submit fetch log task failed", K(ret), K(id));
    } else {
      worker_->signal();
      LOG_INFO("submit fetch log task succ", KPC(task));
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
