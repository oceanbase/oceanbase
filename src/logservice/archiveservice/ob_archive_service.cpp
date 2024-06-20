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

#include "ob_archive_service.h"
#include "lib/ob_define.h"                          // is_meta_tenant is_sys_tenant
#include "lib/compress/ob_compress_util.h"          // ObCompressorType
#include "lib/ob_errno.h"
#include "share/backup/ob_archive_struct.h"         // ObTenantArchiveRoundAttr
#include "share/backup/ob_tenant_archive_round.h"   // ObArchiveRoundHandler
#include "share/rc/ob_tenant_base.h"                // MTL_*
#include "observer/ob_server_struct.h"              // GCTX
#include "logservice/palf/lsn.h"                    // LSN
#include "share/scn.h"                    // LSN
#include "share/backup/ob_backup_connectivity.h"
#include "share/ob_debug_sync.h"

namespace oceanbase
{
namespace archive
{
using namespace oceanbase::share;
using namespace oceanbase::palf;
ObArchiveService::ObArchiveService() :
  inited_(false),
  allocator_(),
  archive_round_mgr_(),
  ls_mgr_(),
  sequencer_(),
  fetcher_(),
  sender_(),
  persist_mgr_(),
  scheduler_(),
  ls_meta_recorder_(),
  timer_(),
  log_service_(NULL),
  ls_svr_(NULL),
  cond_()
{}

ObArchiveService::~ObArchiveService()
{
  destroy();
}

int ObArchiveService::mtl_init(ObArchiveService *&archive_svr)
{
  return archive_svr->init(MTL(ObLogService*), MTL(ObLSService*), MTL_ID());
}

int ObArchiveService::init(logservice::ObLogService *log_service,
    storage::ObLSService *ls_svr,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *mysql_proxy = GCTX.sql_proxy_;

  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    ARCHIVE_LOG(WARN, "archive service init twice", K(ret), K(tenant_id_));
  } else if (OB_ISNULL(log_service)
      || OB_ISNULL(ls_svr)
      || OB_ISNULL(mysql_proxy)
      || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(log_service), K(ls_svr),
        K(mysql_proxy), K(tenant_id));
  } else if (OB_FAIL(allocator_.init(tenant_id))) {
    ARCHIVE_LOG(WARN, "allocator init failed", K(ret));
  } else if (OB_FAIL(archive_round_mgr_.init())) {
    ARCHIVE_LOG(WARN, "archive round mgr init failed", K(ret));
  } else if (OB_FAIL(persist_mgr_.init(tenant_id, mysql_proxy, ls_svr,
                                       &ls_mgr_, &archive_round_mgr_))) {
    ARCHIVE_LOG(WARN, "persist mgr init failed", K(ret));
  } else if (OB_FAIL(ls_mgr_.init(tenant_id, log_service, ls_svr, &allocator_,
                                  &sequencer_, &archive_round_mgr_, &persist_mgr_))) {
    ARCHIVE_LOG(WARN, "ls mgr init failed", K(ret));
  } else if (OB_FAIL(sender_.init(tenant_id, &allocator_, &ls_mgr_,
                                  &persist_mgr_, &archive_round_mgr_))) {
    ARCHIVE_LOG(WARN, "sender init failed", K(ret));
  } else if (OB_FAIL(fetcher_.init(tenant_id, log_service, &allocator_, &sender_,
                                   &sequencer_, &ls_mgr_, &archive_round_mgr_))) {
    ARCHIVE_LOG(WARN, "fetcher init failed", K(ret));
  } else if (OB_FAIL(sequencer_.init(tenant_id, log_service, &fetcher_, &ls_mgr_,
                                     &archive_round_mgr_))) {
    ARCHIVE_LOG(WARN, "sequencer init failed", K(ret));
  } else if (OB_FAIL(scheduler_.init(tenant_id, &fetcher_, &sender_, &allocator_))) {
    ARCHIVE_LOG(WARN, "scheduler init failed", K(ret));
  } else if (OB_FAIL(ls_meta_recorder_.init(&archive_round_mgr_))) {
    ARCHIVE_LOG(WARN, "ls_meta_recorder init failed", K(ret));
  } else if (OB_FAIL(timer_.init(tenant_id, &ls_meta_recorder_, &archive_round_mgr_))) {
    ARCHIVE_LOG(WARN, "timer init failed", K(ret));
  } else {
    tenant_id_ = tenant_id;
    inited_ = true;
    ARCHIVE_LOG(INFO, "archive service init succ", K_(tenant_id));
  }
  return ret;
}

int ObArchiveService::start()
{
  int ret = OB_SUCCESS;
  ObThreadPool::set_run_wrapper(MTL_CTX());
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(ERROR, "archive service not init", K(ret));
  } else if (OB_FAIL(ls_mgr_.start())) {
    ARCHIVE_LOG(WARN, "ls archive mgr start failed", K(ret));
  } else if (OB_FAIL(sequencer_.start())) {
    ARCHIVE_LOG(WARN, "archive sequencer start failed", K(ret));
  } else if (OB_FAIL(fetcher_.start())) {
    ARCHIVE_LOG(WARN, "archive fetcher start failed", K(ret));
  } else if (OB_FAIL(sender_.start())) {
    ARCHIVE_LOG(WARN, "archive sender start failed", K(ret));
  } else if (OB_FAIL(timer_.start())) {
    ARCHIVE_LOG(WARN, "archive timer start failed", K(ret));
  } else if (OB_FAIL(ObThreadPool::start())) {
    ARCHIVE_LOG(WARN, "archive service start failed", K(ret));
  } else {
    ARCHIVE_LOG(INFO, "archive service start succ", K_(tenant_id));
  }
  return ret;
}

void ObArchiveService::stop()
{
  ls_mgr_.stop();
  sequencer_.stop();
  fetcher_.stop();
  sender_.stop();
  timer_.stop();
  ObThreadPool::stop();
  ARCHIVE_LOG(INFO, "archive service stop succ", K_(tenant_id));
}

void ObArchiveService::wait()
{
  ls_mgr_.wait();
  sequencer_.wait();
  fetcher_.wait();
  sender_.wait();
  timer_.wait();
  ObThreadPool::wait();
  ARCHIVE_LOG(INFO, "archive service wait succ", K_(tenant_id));
}

void ObArchiveService::destroy()
{
  inited_ = false;
  stop();
  wait();
  tenant_id_ = OB_INVALID_TENANT_ID;
  archive_round_mgr_.destroy();
  ls_mgr_.destroy();
  sequencer_.destroy();
  fetcher_.destroy();
  sender_.destroy();
  persist_mgr_.destroy();
  scheduler_.destroy();
  ls_meta_recorder_.destroy();
  timer_.destroy();
  allocator_.destroy();
  ObThreadPool::destroy();
  log_service_ = NULL;
  ls_svr_ = NULL;
}

int ObArchiveService::get_ls_archive_progress(const ObLSID &id,
    LSN &lsn,
    SCN &scn,
    bool &force_wait,
    bool &ignore)
{
  return persist_mgr_.get_ls_archive_progress(id, lsn, scn, force_wait, ignore);
}

int ObArchiveService::check_tenant_in_archive(bool &in_archive)
{
  return persist_mgr_.check_tenant_in_archive(in_archive);
}

int ObArchiveService::get_ls_archive_speed(const ObLSID &id,
    int64_t &speed,
    bool &force_wait,
    bool &ignore)
{
  return persist_mgr_.get_ls_archive_speed(id, speed, force_wait, ignore);
}

void ObArchiveService::wakeup()
{
  cond_.signal();
}

void ObArchiveService::flush_all()
{
  ls_mgr_.flush_all();
}

int ObArchiveService::iterate_ls(const std::function<int (const ObLSArchiveTask &)> &func)
{
  return ls_mgr_.iterate_ls(func);
}

void ObArchiveService::run1()
{
  ARCHIVE_LOG(INFO, "ObArchiveService thread start", K_(tenant_id));
  lib::set_thread_name("ArcSrv");
  ObCurTraceId::init(GCONF.self_addr_);

  if (OB_UNLIKELY(! inited_)) {
    ARCHIVE_LOG_RET(ERROR, OB_NOT_INIT, "archive service not init", K_(tenant_id));
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
  ARCHIVE_LOG(INFO, "ObArchiveService thread end", K_(tenant_id));
}

void ObArchiveService::do_thread_task_()
{
  if (! is_user_tenant(tenant_id_)) {
  } else {
    do_check_switch_archive_();
    check_and_set_archive_stop_();
    print_archive_status_();
    persist_mgr_.persist_and_load();
    scheduler_.schedule();
  }
}

void ObArchiveService::do_check_switch_archive_()
{
  int ret = OB_SUCCESS;
  ObTenantArchiveRoundAttr attr;
  ArchiveRoundOp op = ArchiveRoundOp::NONE;
  ArchiveKey key;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(ERROR, "ObArchiveService not init", K(ret));
  } else if (is_meta_tenant(tenant_id_) || is_sys_tenant(tenant_id_)) {
    ARCHIVE_LOG(TRACE, "meta or sys tenant ignore", K_(tenant_id));
  } else if (OB_FAIL(load_archive_round_attr_(attr))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      ARCHIVE_LOG(WARN, "load archive round attr failed", K(ret), K_(tenant_id));
    }
  } else if (OB_UNLIKELY(! attr.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "round attr is not valid", K(ret), K(attr));
  } else if (OB_FAIL(check_if_need_switch_log_archive_(attr, op))) {
    ARCHIVE_LOG(WARN, "check if need switch log archive failed", K(ret), K(attr));
  } else if (FALSE_IT(key = ArchiveKey(attr.incarnation_, attr.dest_id_, attr.round_id_))) {
  } else if (ArchiveRoundOp::START == op) {
    // 开启新一轮归档
    if (OB_FAIL(start_archive_(attr))) {
      ARCHIVE_LOG(WARN, "start archive failed", K(ret), K(attr));
    }
  } else if (ArchiveRoundOp::STOP == op) {
    // 关闭归档
    stop_archive_();
  } else if (ArchiveRoundOp::FORCE_STOP == op) {
    // 强制stop
    archive_round_mgr_.set_archive_force_stop(key);
    ARCHIVE_LOG(INFO, "force set log_archive_state STOPPED");
  } else if (ArchiveRoundOp::MARK_INTERRUPT == op) {
    // set archive round state interrupt
    archive_round_mgr_.set_archive_interrupt(key);
    ARCHIVE_LOG(INFO, "set log_archive_state INTERRUPTED");
  } else if (ArchiveRoundOp::SUSPEND == op) {
    if (OB_FAIL(suspend_archive_(attr))) {
      ARCHIVE_LOG(WARN, "suspend archive failed", K(ret), K(attr));
    }
    ARCHIVE_LOG(INFO, "set log_archive_state SUSPEND");
  } else {
    if (REACH_TIME_INTERVAL(60 * 1000 * 1000L)) {
      ARCHIVE_LOG(INFO, "check_log_archive_state_, no switch round op", K(attr));
    }
  }
}

int ObArchiveService::load_archive_round_attr_(ObTenantArchiveRoundAttr &attr)
{
  DEBUG_SYNC(BEFORE_LOAD_ARCHIVE_ROUND);
  return persist_mgr_.load_archive_round_attr(attr);
}

// tenant_state: beginning / doing / stopping / stop / interrupt
// loca_state: doing / stopping / stop / interrupt / invalid
//
// current_state                    final_state
//
//               no operation
// stopping -----------------------> stop (ignore any conditions)
//
//                   need_start
// invalid / stop -----------------> start (tenant_state: beginning / doing)
//
//                                need_stop
// doing / interrupt / suspend ----------------> stop (tenant_state: stopping / stop)
//
//                                  need_stop
// doing / interrupt / suspend ----------------> any (local round lag)
//
//                   force_stop
// invalid / stop -----------------> stop (tenant_state: stopping / stop && local round lag)
//
//                   mark_interrupt
// doing / suspend ------------------> interrupt (tenant_state: interrupt)
//
//             need_start
// suspend -------------------> start (tenant_state: beginning / doing && no round lag)
//
//         need_suspend
// doing ------------------> suspend (tenant_state: suspending / suspend && no round lag)
//
//                  need_suspend
// invalid / stop ------------------> suspend (tenant_state: suspending / suspend && local round lag)
//
int ObArchiveService::check_if_need_switch_log_archive_(
    const ObTenantArchiveRoundAttr &attr,
    ArchiveRoundOp &op)
{
  int ret = OB_SUCCESS;
  ArchiveKey tenant_key(attr.incarnation_, attr.dest_id_, attr.round_id_);
  ArchiveKey local_key;
  share::ObArchiveRoundState local_state;
  archive_round_mgr_.get_archive_round_info(local_key, local_state);
  share::ObArchiveRoundState tenant_state = attr.state_;
  const bool local_round_lag = local_key != tenant_key;
  op = ArchiveRoundOp::NONE;

  if (local_state.is_stopping()) {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
      ARCHIVE_LOG(INFO, "stopping state, just wait", K(local_key),
          K(local_state), K(attr), K_(archive_round_mgr));
    }
  } else if ((local_state.is_invalid() || local_state.is_stop())
      && (tenant_state.is_beginning() || tenant_state.is_doing())) {
    op = ArchiveRoundOp::START;
    ARCHIVE_LOG(INFO, "need_start", K(local_key), K(local_state), K(attr), K_(archive_round_mgr));
  } else if (local_state.is_suspend()
      && (tenant_state.is_beginning() || tenant_state.is_doing())
      && ! local_round_lag) {
    op = ArchiveRoundOp::START;
    ARCHIVE_LOG(INFO, "need_start", K(local_key), K(local_state), K(attr), K_(archive_round_mgr));
  } else if ((local_state.is_doing() || local_state.is_interrupted() || local_state.is_suspend())
      && (tenant_state.is_stopping() || tenant_state.is_stop())) {
    op = ArchiveRoundOp::STOP;
    ARCHIVE_LOG(INFO, "need_stop", K(local_key), K(local_state), K(attr), K_(archive_round_mgr));
  } else if ((local_state.is_doing() || local_state.is_interrupted() || local_state.is_suspend())
      && local_round_lag) {
    op = ArchiveRoundOp::STOP;
    ARCHIVE_LOG(INFO, "round lag, need_stop first", K(local_key),
        K(local_state), K(attr), K_(archive_round_mgr));
  } else if (local_state.is_doing()
      && (tenant_state.is_suspending() || tenant_state.is_suspend())
      && ! local_round_lag) {
    op = ArchiveRoundOp::SUSPEND;
    ARCHIVE_LOG(INFO, "need_suspend", K(local_key), K(local_state), K(attr), K_(archive_round_mgr));
  } else if ((local_state.is_valid() || local_state.is_stop())
      && (tenant_state.is_suspending() || tenant_state.is_suspend())
      && local_round_lag) {
    op = ArchiveRoundOp::SUSPEND;
    ARCHIVE_LOG(INFO, "need_suspend", K(local_key), K(local_state), K(attr), K_(archive_round_mgr));
  } else if ((local_state.is_invalid() || local_state.is_stop())
      && (tenant_state.is_stopping() || tenant_state.is_stop())
      && local_round_lag) {
    op = ArchiveRoundOp::FORCE_STOP;
    ARCHIVE_LOG(INFO, "need_force_stop", K(local_key), K(local_state), K(attr), K_(archive_round_mgr));
  } else if ((local_state.is_doing() || local_state.is_suspend())
      && tenant_state.is_interrupted()
      && ! local_round_lag) {
    // 标记interrupt需谨慎, 只有当前round并且doing状态, 遇到租户interrupt, 才置本地为interrupt
    op = ArchiveRoundOp::MARK_INTERRUPT;
    ARCHIVE_LOG(INFO, "need_mark_interrupt", K(local_key), K(local_state), K(attr), K_(archive_round_mgr));
  } else {/*do nothing*/}

  return ret;
}

int ObArchiveService::start_archive_(const ObTenantArchiveRoundAttr &attr)
{
  int ret = OB_SUCCESS;
  ArchiveKey key(attr.incarnation_, attr.dest_id_, attr.round_id_);
  ObBackupDest dest;
  ObMySQLProxy *mysql_proxy = GCTX.sql_proxy_;
  if (OB_ISNULL(mysql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(set_log_archive_info_(attr))) {
    ARCHIVE_LOG(WARN, "set_log_archive_info_ fail", K(ret), K_(tenant_id), K(attr));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(
          *mysql_proxy, attr.key_.tenant_id_, attr.path_, dest))) {
    ARCHIVE_LOG(ERROR, "get backup dest failed", K(ret), K(attr));
  } else if (OB_FAIL(archive_round_mgr_.set_archive_start(key, attr.start_scn_,
          attr.piece_switch_interval_, attr.start_scn_, attr.base_piece_id_,
          share::ObTenantLogArchiveStatus::COMPATIBLE::COMPATIBLE_VERSION_2, dest))) {
    ARCHIVE_LOG(ERROR, "archive round mgr set archive info failed", K(ret), K(attr));
  } else {
    notify_start_();
    ARCHIVE_LOG(INFO, "start archive succ", K_(tenant_id));
  }
  return ret;
}

int ObArchiveService::set_log_archive_info_(const ObTenantArchiveRoundAttr &attr)
{
  int ret = OB_SUCCESS;
  ArchiveKey key(attr.incarnation_, attr.dest_id_, attr.round_id_);
  const int64_t piece_interval = attr.piece_switch_interval_;
  const SCN &genesis_scn = attr.start_scn_;
  const int64_t base_piece_id = attr.base_piece_id_;
  const int64_t unit_size = 100;
  const bool need_compress = false;
  ObCompressorType type = INVALID_COMPRESSOR;
  const bool need_encrypt = false;
  const SCN &round_start_scn = attr.start_scn_;
  if (OB_FAIL(fetcher_.set_archive_info(piece_interval, genesis_scn, base_piece_id,
                                        unit_size, need_compress, type, need_encrypt))) {
    ARCHIVE_LOG(ERROR, "archive fetcher set archive info failed", K(ret));
  } else if (OB_FAIL(ls_mgr_.set_archive_info(round_start_scn, piece_interval, genesis_scn, base_piece_id))) {
    ARCHIVE_LOG(ERROR, "archive sequencer set archive info failed", K(ret));
  } else {
    ARCHIVE_LOG(INFO, "set log archive info succ", K_(tenant_id));
  }

  return ret;
}

int ObArchiveService::suspend_archive_(const ObTenantArchiveRoundAttr &attr)
{
  int ret = OB_SUCCESS;
  ArchiveKey key(attr.incarnation_, attr.dest_id_, attr.round_id_);
  if (OB_FAIL(set_log_archive_info_(attr))) {
    ARCHIVE_LOG(WARN, "set_log_archive_info_ fail", K(ret), K_(tenant_id), K(attr));
  } else {
    archive_round_mgr_.set_archive_suspend(key);
    ARCHIVE_LOG(INFO, "suspend archive succ", K_(tenant_id));
  }
  return ret;
}

void ObArchiveService::notify_start_()
{
  ls_mgr_.notify_start();
  sequencer_.notify_start();
}

void ObArchiveService::stop_archive_()
{
  archive_round_mgr_.update_log_archive_status(ObArchiveRoundState::Status::STOPPING);
}

void ObArchiveService::check_and_set_archive_stop_()
{
  int ret = OB_SUCCESS;
  ArchiveKey key;
  archive_round_mgr_.get_round(key);

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(ERROR, "ObArchiveMgr not init", KR(ret));
  } else if (! archive_round_mgr_.is_in_archive_stopping_status(key)) {
    // skip
  } else if (! check_archive_task_empty_()) {
    // skip
  } else if (OB_FAIL(clear_ls_task_())) {
    ARCHIVE_LOG(WARN, "clear_ls_task_ fail", KR(ret));
  } else if (FALSE_IT(clear_archive_info_())) {
    ARCHIVE_LOG(WARN, "clear_archive_info_ fail", KR(ret));
  } else if (OB_FAIL(set_log_archive_stop_status_(key))) {
    ARCHIVE_LOG(WARN, "set_log_archive_stop_status_ fail", KR(ret));
  } else {
    // succ
    ARCHIVE_LOG(INFO, "switch log archive status from in_stopping to stopped succ", K_(tenant_id));
  }
}

int ObArchiveService::clear_ls_task_()
{
  ls_mgr_.reset_task();
  return OB_SUCCESS;
}

void ObArchiveService::clear_archive_info_()
{
  ls_mgr_.clear_archive_info();
  fetcher_.clear_archive_info();
}

bool ObArchiveService::check_archive_task_empty_() const
{
  bool bret = false;
  const int64_t ls_task_count = ls_mgr_.get_ls_task_count();
  const int64_t log_fetch_task_count = fetcher_.get_log_fetch_task_count();
  const int64_t send_task_status_count = sender_.get_send_task_status_count();

  // size of light_queue may be smaller than zero if queue is empty and a thread is pop
  if (0 == ls_task_count && 0 >= log_fetch_task_count && 0 >= send_task_status_count) {
    bret = true;
    ARCHIVE_LOG(INFO, "pending task is clear", K_(tenant_id));
  } else {
    ARCHIVE_LOG(INFO, "pendding tasks have not been cleared", K_(tenant_id),
        K(ls_task_count), K(log_fetch_task_count), K(send_task_status_count));
  }
  return bret;
}

int ObArchiveService::set_log_archive_stop_status_(const ArchiveKey &key)
{
  int ret = OB_SUCCESS;

  if (! archive_round_mgr_.is_in_archive_stopping_status(key)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "invalid archive_status switch", K(ret), K(key));
  } else {
    archive_round_mgr_.update_log_archive_status(ObArchiveRoundState::Status::STOP);
  }

  return ret;
}

void ObArchiveService::print_archive_status_()
{
  ls_mgr_.print_tasks();
}

} // namespace archive
} // namespace oceanbase
