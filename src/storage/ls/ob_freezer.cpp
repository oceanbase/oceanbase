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

#define USING_LOG_PREFIX STORAGE
#include "common/ob_tablet_id.h"
#include "logservice/ob_log_service.h"
#include "share/ob_force_print_log.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_freezer.h"
#include "storage/ls/ob_ls_tx_service.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/checkpoint/ob_data_checkpoint.h"
#include "storage/compaction/ob_compaction_diagnose.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx_storage/ob_tenant_freezer.h"

namespace oceanbase
{
using namespace logservice;
using namespace share;
using namespace compaction;
using namespace memtable;
namespace storage
{
ObFrozenMemtableInfo::ObFrozenMemtableInfo()
  : tablet_id_(),
    start_scn_(share::ObScnRange::MIN_SCN),
    end_scn_(share::ObScnRange::MIN_SCN),
    write_ref_cnt_(0),
    unsubmitted_cnt_(0),
    current_right_boundary_(0)
{}

ObFrozenMemtableInfo::ObFrozenMemtableInfo(const ObTabletID &tablet_id,
                                           const SCN &start_scn,
                                           const SCN &end_scn,
                                           const int64_t write_ref_cnt,
                                           const int64_t unsubmitted_cnt,
                                           const int64_t current_right_boundary)
 : tablet_id_(tablet_id),
   start_scn_(start_scn),
   end_scn_(end_scn),
   write_ref_cnt_(write_ref_cnt),
   unsubmitted_cnt_(unsubmitted_cnt),
   current_right_boundary_(current_right_boundary)
{}

ObFrozenMemtableInfo::~ObFrozenMemtableInfo()
{
  reset();
}

void ObFrozenMemtableInfo::reset()
{
  tablet_id_.reset();
  start_scn_ = share::ObScnRange::MIN_SCN;
  end_scn_ = share::ObScnRange::MIN_SCN;
  write_ref_cnt_ = 0;
  unsubmitted_cnt_ = 0;
  current_right_boundary_ = 0;
}

void ObFrozenMemtableInfo::set(const ObTabletID &tablet_id,
                               const SCN &start_scn,
                               const SCN &end_scn,
                               const int64_t write_ref_cnt,
                               const int64_t unsubmitted_cnt,
                               const int64_t current_right_boundary)
{
  tablet_id_ = tablet_id;
  start_scn_ = start_scn;
  end_scn_ = end_scn;
  write_ref_cnt_ = write_ref_cnt;
  unsubmitted_cnt_ = unsubmitted_cnt;
  current_right_boundary_ = current_right_boundary;
}

bool ObFrozenMemtableInfo::is_valid()
{
  return tablet_id_.is_valid() && start_scn_ > share::ObScnRange::MIN_SCN && end_scn_ > share::ObScnRange::MIN_SCN;
}

ObFreezerStat::ObFreezerStat()
  : tablet_id_(),
    need_rewrite_meta_(false),
    state_(ObFreezeState::INVALID),
    freeze_clock_(0),
    start_time_(0),
    end_time_(INT64_MAX),
    ret_code_(-1),
    freeze_snapshot_version_(),
    diagnose_info_(),
    memtables_info_(),
    lock_()
{}

ObFreezerStat::~ObFreezerStat()
{
  reset();
}

void ObFreezerStat::reset()
{
  ObSpinLockGuard guard(lock_);
  tablet_id_.reset();
  need_rewrite_meta_ = false;
  state_ = ObFreezeState::INVALID;
  freeze_clock_ = 0;
  start_time_ = 0;
  end_time_ = INT64_MAX;
  ret_code_ = -1;
  freeze_snapshot_version_.reset();
  diagnose_info_.reset();
  memtables_info_.reset();
}

bool ObFreezerStat::is_valid()
{
  return get_start_time() > 0 && get_state() > ObFreezeState::INVALID;
}

int ObFreezerStat::add_memtable_info(const ObTabletID &tablet_id,
                                     const SCN &start_scn,
                                     const SCN &end_scn,
                                     const int64_t write_ref_cnt,
                                     const int64_t unsubmitted_cnt,
                                     const int64_t current_right_boundary)
{
  int ret = OB_SUCCESS;

  ObSpinLockGuard guard(lock_);
  if (memtables_info_.count() < FROZEN_MEMTABLE_INFO_CNT) {
    ObFrozenMemtableInfo memtable_info(tablet_id,
                                       start_scn,
                                       end_scn,
                                       write_ref_cnt,
                                       unsubmitted_cnt,
                                       current_right_boundary);
    if (OB_FAIL(memtables_info_.push_back(memtable_info))) {
      TRANS_LOG(WARN, "fail to push_back memtable_info", K(ret), K(tablet_id));
    }
  }

  return ret;
}

int ObFreezerStat::remove_memtable_info(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;

  ObSpinLockGuard guard(lock_);
  for (int i = 0; i < memtables_info_.count(); ++i) {
    if (tablet_id == memtables_info_[i].tablet_id_ && OB_FAIL(memtables_info_.remove(i))) {
      TRANS_LOG(WARN, "fail to remove from memtable_info", K(ret), K(tablet_id));
    }

  }

  return ret;
}

int ObFreezerStat::get_memtables_info(common::ObSArray<ObFrozenMemtableInfo> &memtables_info)
{
  int ret = OB_SUCCESS;

  ObSpinLockGuard guard(lock_);
  if (OB_FAIL(memtables_info.assign(memtables_info_))) {
    TRANS_LOG(WARN, "fail to assign", K(ret));
  }

  return ret;
}

int ObFreezerStat::set_memtables_info(const common::ObSArray<ObFrozenMemtableInfo> &memtables_info)
{
  int ret = OB_SUCCESS;

  ObSpinLockGuard guard(lock_);
  if (OB_FAIL(memtables_info_.assign(memtables_info))) {
    TRANS_LOG(WARN, "fail to assign", K(ret));
  }

  return ret;
}

int ObFreezerStat::add_diagnose_info(const ObString &str)
{
  int ret = OB_SUCCESS;

  ObSpinLockGuard guard(lock_);
  if (OB_FAIL(diagnose_info_.assign(str))) {
    TRANS_LOG(WARN, "fail to add diagnose_info");
  }

  return ret;
}

int ObFreezerStat::get_diagnose_info(ObStringHolder &diagnose_info)
{
  int ret = OB_SUCCESS;

  ObSpinLockGuard guard(lock_);
  if (OB_FAIL(diagnose_info.assign(diagnose_info_))) {
    TRANS_LOG(WARN, "fail to assign diagnose_info", K(ret));
  }

  return ret;
}

void ObFreezerStat::set_freeze_clock(const uint32_t freeze_clock)
{
  ObSpinLockGuard guard(lock_);
  freeze_clock_ = freeze_clock;
}

uint32_t ObFreezerStat::get_freeze_clock()
{
  ObSpinLockGuard guard(lock_);
  return freeze_clock_;
}

void ObFreezerStat::set_tablet_id(const ObTabletID &tablet_id)
{
  ObSpinLockGuard guard(lock_);
  tablet_id_ = tablet_id;
}

ObTabletID ObFreezerStat::get_tablet_id()
{
  ObSpinLockGuard guard(lock_);
  return tablet_id_;
}

void ObFreezerStat::set_need_rewrite_meta(bool need_rewrite)
{
  ObSpinLockGuard guard(lock_);
  need_rewrite_meta_ = need_rewrite;
}
bool ObFreezerStat::need_rewrite_meta()
{
  ObSpinLockGuard guard(lock_);
  return need_rewrite_meta_;
}

void ObFreezerStat::set_state(int state)
{
  ObSpinLockGuard guard(lock_);
  state_ = state;
}

int ObFreezerStat::get_state()
{
  ObSpinLockGuard guard(lock_);
  return state_;
}

void ObFreezerStat::set_start_time(int64_t start_time)
{
  ObSpinLockGuard guard(lock_);
  start_time_ = start_time;
}

int64_t ObFreezerStat::get_start_time()
{
  ObSpinLockGuard guard(lock_);
  return start_time_;
}

void ObFreezerStat::set_end_time(int64_t end_time)
{
  ObSpinLockGuard guard(lock_);
  end_time_ = end_time;
}

int64_t ObFreezerStat::get_end_time()
{
  ObSpinLockGuard guard(lock_);
  return end_time_;
}

void ObFreezerStat::set_ret_code(int ret_code)
{
  ObSpinLockGuard guard(lock_);
  ret_code_ = ret_code;
}

int ObFreezerStat::get_ret_code()
{
  ObSpinLockGuard guard(lock_);
  return ret_code_;
}

void ObFreezerStat::set_freeze_snapshot_version(const share::SCN &freeze_snapshot_version)
{
  ObSpinLockGuard guard(lock_);
  freeze_snapshot_version_ = freeze_snapshot_version;
}

share::SCN ObFreezerStat::get_freeze_snapshot_version()
{
  ObSpinLockGuard guard(lock_);
  return freeze_snapshot_version_;
}

int ObFreezerStat::deep_copy_to(ObFreezerStat &other)
{
  int ret = OB_SUCCESS;
  other.reset();
  ObSpinLockGuard guard(lock_);
  other.set_tablet_id(tablet_id_);
  other.set_need_rewrite_meta(need_rewrite_meta_);
  other.set_state(state_);
  other.set_freeze_clock(freeze_clock_);
  other.set_start_time(start_time_);
  other.set_end_time(end_time_);
  other.set_ret_code(ret_code_);
  other.set_freeze_snapshot_version(freeze_snapshot_version_);
  if (OB_FAIL(other.add_diagnose_info(diagnose_info_.get_ob_string()))) {
    TRANS_LOG(WARN, "fail to add diagnose_info", K(ret));
  } else if (OB_FAIL(other.set_memtables_info(memtables_info_))) {
    TRANS_LOG(WARN, "fail to set memtables_info", K(ret));
  }
  return ret;
}

int ObFreezerStat::begin_set_freeze_stat(const uint32_t freeze_clock,
                                         const int64_t start_time,
                                         const int state,
                                         const share::SCN &freeze_snapshot_version,
                                         const ObTabletID &tablet_id,
                                         const bool need_rewrite_meta)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);

  freeze_clock_ = freeze_clock;
  start_time_ = start_time;
  state_ = state;
  freeze_snapshot_version_ = freeze_snapshot_version;
  tablet_id_ = tablet_id;
  need_rewrite_meta_ = need_rewrite_meta;

  return ret;
}

int ObFreezerStat::end_set_freeze_stat(const int state,
                                       const int64_t end_time,
                                       const int ret_code)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);

  state_ = state;
  end_time_ = end_time;
  ret_code_ = ret_code;

  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_FREEZER_FREEZE_FAILURE);
ERRSIM_POINT_DEF(ERRSIM_BATCH_TABLET_FREEZE_FAILURE);

ObFreezer::ObFreezer()
  : freeze_flag_(0),
    freeze_snapshot_version_(),
    max_decided_scn_(),
    ls_(nullptr),
    stat_(),
    empty_memtable_cnt_(0),
    high_priority_freeze_cnt_(0),
    low_priority_freeze_cnt_(0),
    pend_replay_cnt_(0),
    byte_lock_(),
    need_resubmit_log_(false),
    enable_(false),
    is_inited_(false),
    is_async_tablet_freeze_task_running_(false),
    throttle_is_skipping_(false),
    tenant_replay_is_pending_(false),
    async_freeze_tablets_() {}

ObFreezer::ObFreezer(ObLS *ls)
  : freeze_flag_(0),
    freeze_snapshot_version_(),
    max_decided_scn_(),
    ls_(ls),
    stat_(),
    empty_memtable_cnt_(0),
    high_priority_freeze_cnt_(0),
    low_priority_freeze_cnt_(0),
    pend_replay_cnt_(0),
    byte_lock_(),
    need_resubmit_log_(false),
    enable_(false),
    is_inited_(false),
    is_async_tablet_freeze_task_running_(false),
    throttle_is_skipping_(false),
    tenant_replay_is_pending_(false),
    async_freeze_tablets_() {}

ObFreezer::~ObFreezer()
{
  reset();
}

void ObFreezer::reset()
{
  freeze_flag_ = 0;
  freeze_snapshot_version_.reset();
  max_decided_scn_.reset();
  ls_ = nullptr;
  stat_.reset();
  empty_memtable_cnt_ = 0;
  high_priority_freeze_cnt_ = 0;
  low_priority_freeze_cnt_ = 0;
  pend_replay_cnt_ = 0;
  need_resubmit_log_ = false;
  enable_ = false;
  async_freeze_tablets_.reuse();
  is_inited_ = false;
  is_async_tablet_freeze_task_running_ = false;
  throttle_is_skipping_ = false;
  tenant_replay_is_pending_ = false;
}

int ObFreezer::init(ObLS *ls)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "[Freezer] invalid argument", K(ret));
  } else if (OB_FAIL(async_freeze_tablets_.create(1024, "AsyncFreezeTabletID", "AsyncFreezeTabletID", MTL_ID()))) {
    LOG_WARN("[Freezer] fail to create hash set for ls freezer", KR(ret));
  } else {
    freeze_flag_ = 0;
    freeze_snapshot_version_.reset();
    max_decided_scn_.reset();
    ls_ = ls;
    stat_.reset();
    empty_memtable_cnt_ = 0;
    high_priority_freeze_cnt_ = 0;
    low_priority_freeze_cnt_ = 0;
    pend_replay_cnt_ = 0;
    need_resubmit_log_ = false;
    is_async_tablet_freeze_task_running_ = false;
    throttle_is_skipping_ = false;
    tenant_replay_is_pending_ = false;

    is_inited_ = true;
  }

  return ret;
}

/* ls info */
share::ObLSID ObFreezer::get_ls_id()
{
  return OB_ISNULL(ls_) ? INVALID_LS : ls_->get_ls_id();
}

checkpoint::ObDataCheckpoint* ObFreezer::get_ls_data_checkpoint()
{
  return OB_ISNULL(ls_) ? nullptr : ls_->get_data_checkpoint();
}

ObLSTxService* ObFreezer::get_ls_tx_svr()
{
  return OB_ISNULL(ls_) ? nullptr : ls_->get_tx_svr();
}

ObLSTabletService* ObFreezer::get_ls_tablet_svr()
{
  return OB_ISNULL(ls_) ? nullptr : ls_->get_tablet_svr();
}

logservice::ObILogHandler* ObFreezer::get_ls_log_handler()
{
  return OB_ISNULL(ls_) ? nullptr : ls_->get_log_handler();
}

ObLSWRSHandler* ObFreezer::get_ls_wrs_handler()
{
  return OB_ISNULL(ls_) ? nullptr : ls_->get_ls_wrs_handler();
}

/* logstream freeze */
int ObFreezer::logstream_freeze(int64_t trace_id)
{
  int ret = OB_SUCCESS;
  SCN freeze_snapshot_version;
  SCN max_decided_scn;
  share::ObLSID ls_id = get_ls_id();
  FLOG_INFO("[Freezer] logstream_freeze start", K(ret), K(ls_id));
  int64_t start_time = ObClockGenerator::getClock();

  ObLSFreezeGuard guard(*this);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "[Freezer] not inited", K(ret), K(ls_id));
  } else if (OB_UNLIKELY(!enable_)) {
    ret = OB_NOT_RUNNING;
    STORAGE_LOG(WARN, "freezer is offline, can not freeze now", K(ret), K(ls_id));
  } else if (OB_FAIL(decide_max_decided_scn(max_decided_scn))) {
    STORAGE_LOG(WARN, "[Freezer] decide max decided log ts failure", K(ret), K(ls_id));
  } else if (OB_FAIL(get_ls_weak_read_scn(freeze_snapshot_version))) {
    STORAGE_LOG(WARN, "[Freezer] get ls weak read ts failure", K(ret), K(ls_id));
  } else if (freeze_snapshot_version.is_max()
             || ObScnRange::MIN_SCN >= freeze_snapshot_version) {
    ret = OB_MINOR_FREEZE_NOT_ALLOW;
    STORAGE_LOG(WARN, "[Freezer] invalid weak read scn", K(ret), K(ls_id), K(freeze_snapshot_version));
#ifdef ERRSIM
  } else if (OB_FAIL(ret = ERRSIM_FREEZER_FREEZE_FAILURE)) {
    LOG_WARN("[Freezer] errsim failure during freezer freeze", K(ret));
#endif
  } else if (OB_FAIL(set_freeze_flag())) {
    FLOG_INFO("[Freezer] freeze is running", K(ret), K(ls_id));
  } else {
    // ATTENTION! : once freeze_flag is set, ret code must be OB_SUCCESS
    max_decided_scn_ = max_decided_scn;
    freeze_snapshot_version_ = freeze_snapshot_version;

    set_need_resubmit_log(false);
    stat_.reset();
    MTL(checkpoint::ObCheckpointDiagnoseMgr *)->update_freeze_clock(ls_id, trace_id, get_freeze_clock());

    (void)stat_.begin_set_freeze_stat(get_freeze_clock(),
                                      start_time,
                                      ObFreezeState::NOT_SUBMIT_LOG,
                                      freeze_snapshot_version,
                                      ObTabletID(ObTabletID::INVALID_TABLET_ID),
                                      false /*need_rewrite_meta*/);
    (void)submit_checkpoint_task();
    (void)try_submit_log_for_freeze_(false);
  }

  return ret;
}

void ObFreezer::try_submit_log_for_freeze_(const bool is_tablet_freeze)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(submit_log_for_freeze(is_tablet_freeze, true/*try*/))) {
    TRANS_LOG(WARN, "fail to try submit log for freeze", K(ret));
    set_need_resubmit_log(true);
  }
}

void ObFreezer::submit_checkpoint_task()
{
  int ret = OB_SUCCESS;

  do {
    if (OB_FAIL(get_ls_data_checkpoint()->ls_freeze(SCN::max_scn()))) {
      TRANS_LOG(WARN, "[Freezer] data_checkpoint freeze failed", K(ret));
      stat_.add_diagnose_info("data_checkpoint freeze failed");
      ob_usleep(100);
    }
  } while (OB_FAIL(ret));
}

int ObFreezer::wait_ls_freeze_finish()
{
  int ret = OB_SUCCESS;
  const share::ObLSID ls_id = get_ls_id();
  const int64_t start_time = ObClockGenerator::getClock();
  uint32_t freeze_clock = get_freeze_clock();
  {
    PendTenantReplayHelper pend_replay_helper(*this, ls_);
    (void)pend_replay_helper.set_skip_throttle_flag();

    TRANS_LOG(INFO,
              "[Freezer] wait freeze : Logstream ",
              K(ls_id),
              K(freeze_clock),
              K(throttle_is_skipping_),
              K(tenant_replay_is_pending_));

    // wait till all memtables are moved from frozen_list to prepare_list
    // this means that all memtables can be dumped
    int64_t time_counter = 0;
    while (!get_ls_data_checkpoint()->ls_freeze_finished()) {
      if (TC_REACH_TIME_INTERVAL(1LL * 1000LL * 1000LL /* 1 second */)) {
        ++time_counter;

        // check pend condition every second
        (void)pend_replay_helper.check_pend_condition_once();

        // check resubmit log condition and report some debug info every 5 seconds
        if (time_counter >= 5 && time_counter % 5 == 0) {
          (void)resubmit_log_if_needed_(start_time, false /* is_tablet_freeze */, false /* is_try */);
        }
      }
      ob_usleep(100);
    }

    stat_.add_diagnose_info("logstream_freeze success");
    stat_.end_set_freeze_stat(ObFreezeState::FINISH, ObClockGenerator::getClock(), ret);
    unset_freeze_();
    const int64_t wait_freeze_finish_spend_time = ObClockGenerator::getClock() - start_time;
    FLOG_INFO("[Freezer] logstream_freeze success", K(ls_id), K(freeze_clock), K(wait_freeze_finish_spend_time));
  }

  // freeze tx data out of PendTenantReplayGuard
  (void)try_freeze_tx_data_();
  return ret;
}

void ObFreezer::resubmit_log_if_needed_(const int64_t start_time,
                                        const bool is_tablet_freeze,
                                        const bool is_try,
                                        const ObITabletMemtable *memtable)
{
  const share::ObLSID ls_id = get_ls_id();
  int64_t last_submit_log_time = start_time;
  if (need_resubmit_log() ||
      // In order to prevent the txn has already passed the try_submit test
      // while failing to submit some logs due to an unexpected bug, we need
      // retry to submit the log to go around the above case
      (ObClockGenerator::getClock() - last_submit_log_time >= 1_min)) {
    last_submit_log_time = ObClockGenerator::getClock();

    (void)submit_log_for_freeze(is_tablet_freeze, is_try);
  }

  const int64_t cost_time = ObClockGenerator::getClock() - start_time;

  TRANS_LOG_RET(WARN,
                OB_ERR_TOO_MUCH_TIME,
                "[Freezer] wait freeze finish costs too much time",
                K(ls_id),
                K(is_tablet_freeze),
                K(is_try),
                K(cost_time),
                K(throttle_is_skipping_),
                K(tenant_replay_is_pending_),
                KPC(memtable));
  stat_.add_diagnose_info("wait freeze finish costs too much time");
}

// Define a functor to avoid using lambda
struct AsyncFreezeFunctor {
  const int64_t trace_id_;
  const bool is_ls_freeze_;
  ObFreezer *freezer_;
  // hold ls handle to avoid logstream being destroyed
  ObLSHandle ls_handle_;
  AsyncFreezeFunctor(const int64_t trace_id, const bool is_ls_freeze, ObFreezer *freezer, ObLSHandle &ls_handle)
      : trace_id_(trace_id), is_ls_freeze_(is_ls_freeze), freezer_(freezer), ls_handle_(ls_handle) {}
  int operator()()
  {
    int ret = OB_SUCCESS;
    ObLS *ls = nullptr;
    if (OB_ISNULL(freezer_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected nullptr ", KR(ret), KP(this));
    } else if (FALSE_IT(ls = freezer_->ls_)) {
    } else if (OB_ISNULL(ls)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected nullptr ", KR(ret), KP(this));
    } else {
      share::ObLSID ls_id = freezer_->get_ls_id();
      STORAGE_LOG(INFO, "[Freezer] An Async Freeze Task Start", K(trace_id_), K(ls_id), K(is_ls_freeze_), KP(freezer_));
      if (is_ls_freeze_) {
        (void)ls->logstream_freeze_task(trace_id_, INT64_MAX);
      } else {
        (void)freezer_->async_tablet_freeze_consumer(trace_id_);
      }
    }
    return ret;
  }
};

void ObFreezer::commit_an_async_freeze_task(const int64_t trace_id, const bool is_ls_freeze)
{
  int ret = OB_SUCCESS;
  ObTenantFreezer *tenant_freezer = nullptr;
  share::ObLSID ls_id = get_ls_id();
  const int64_t start_time = ObClockGenerator::getClock();

  if (OB_UNLIKELY(!enable_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("freezer is offline, can not freeze now", K(ret), K(ls_id));
  } else if (OB_ISNULL(tenant_freezer = MTL(storage::ObTenantFreezer *))) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ObTenantFreezer is null", K(ret), K(ls_id));
  } else {
    ObSpinLockGuard freeze_thread_pool(tenant_freezer->freeze_thread_pool_lock_);

    ObLSHandle ls_handle;
    if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      STORAGE_LOG(WARN, "get ls handle failed. stop async freeze task", KR(ret), K(ls_id));
    } else {
      AsyncFreezeFunctor async_freeze_functor(trace_id, is_ls_freeze, this, ls_handle);
      do {
        ret = tenant_freezer->freeze_thread_pool_.commit_task_ignore_ret(async_freeze_functor);
        if (OB_FAIL(ret) && REACH_TIME_INTERVAL(5LL * 1000LL * 1000LL)) {
          STORAGE_LOG(WARN, "commit task to freeze thread pool failed", KR(ret), K(ls_id));
        }
      } while (OB_FAIL(ret));
      STORAGE_LOG(INFO, "finish commit async freeze task", KR(ret), K(is_ls_freeze));
    }
  }
}

void ObFreezer::async_tablet_freeze_consumer(const int64_t trace_id)
{
  if (OB_ISNULL(ls_)) {
    STORAGE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "unexpected nullptr of ls", KP(this));
  } else if (set_async_tablet_freeze_task_start_succ()) {
    const int64_t start_time = ObClockGenerator::getClock();
    STORAGE_LOG(INFO, "Async Tablet Freeze Task Start", K(get_ls_id()));

    ObSEArray<ObTabletID, 128> tablet_ids;
    tablet_ids.reuse();
    const int64_t current_epoch = ls_->get_switch_epoch();
    (void)get_all_async_freeze_tablets(current_epoch, tablet_ids);

    int ret = OB_SUCCESS;
    bool need_resubmit_task = false;
    if (tablet_ids.empty()) {
      // no tablet need freeze
    } else {
      const int64_t abs_timeout_ts = start_time + (3600LL * 1000LL * 1000LL/*an hour*/);
      const bool need_rewrite_meta = false;
      const bool is_sync = false;
      if (OB_FAIL(ls_->tablet_freeze_task(
              trace_id, tablet_ids, need_rewrite_meta, is_sync, abs_timeout_ts, current_epoch))) {
        need_resubmit_task = true;
      }
    }

    const int64_t end_time = ObClockGenerator::getClock();
    const int64_t spend_time_ms = (end_time - start_time) / 1000;
    STORAGE_LOG(INFO, "Async Tablet Freeze Task finish", K(get_ls_id()), K(spend_time_ms));
    (void)set_async_freeze_task_stop();

    // NOTE : must check is_async_freeze_tablets_empty() after set_async_freeze_task_stop()
    if (need_resubmit_task || !is_async_freeze_tablets_empty()) {
      const bool is_ls_freeze = false;
      (void)commit_an_async_freeze_task(trace_id, is_ls_freeze);
    }
  }
}

void ObFreezer::try_freeze_tx_data_()
{
  int ret = OB_SUCCESS;
  const int64_t MAX_RETRY_DURATION = 10LL * 1000LL * 1000LL;  // 10 seconds
  int64_t retry_times = 0;
  int64_t start_freeze_ts = ObClockGenerator::getClock();
  do {
    if (OB_FAIL(ls_->get_tx_table()->self_freeze_task())) {
      if (OB_EAGAIN == ret) {
        // sleep and retry
        retry_times++;
        usleep(100);
      } else {
        STORAGE_LOG(WARN, "freeze tx data table failed", KR(ret), K(get_ls_id()));
      }
    }
  } while (OB_EAGAIN == ret && ObClockGenerator::getClock() - start_freeze_ts < MAX_RETRY_DURATION);
  STORAGE_LOG(INFO, "freeze tx data after logstream freeze", KR(ret), K(retry_times), KTIME(start_freeze_ts));
}

// must be used under the protection of ls_lock
int ObFreezer::check_ls_state()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(ls_->is_stopped())) {
    ret = OB_NOT_RUNNING;
    STORAGE_LOG(WARN, "ls stopped", K(ret), K_(ls_->ls_meta));
  } else if (OB_UNLIKELY(!(ls_->get_log_handler()->is_replay_enabled()))) {
    ret = OB_NOT_RUNNING;
    STORAGE_LOG(WARN, "log handler not enable replay, should not freeze", K(ret), K_(ls_->ls_meta));
  }

  return ret;
}

int ObFreezer::ls_inner_tablet_freeze(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_ls_tx_svr())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "[Freezer] ls_tx_svr is unexpected nullptr", KR(ret), K(get_ls_id()), K(tablet_id));
  } else if (OB_FAIL(get_ls_tx_svr()->flush_ls_inner_tablet(tablet_id))) {
    TRANS_LOG(WARN, "[Freezer] freeze ls inner tablet failed", KR(ret), K(get_ls_id()), K(tablet_id));
  }
  return ret;
}

int ObFreezer::tablet_freeze(const int64_t trace_id,
                             const ObIArray<ObTabletID> &tablet_ids,
                             const bool need_rewrite_meta,
                             ObIArray<ObTableHandleV2> &frozen_memtable_handles,
                             ObIArray<ObTabletID> &freeze_failed_tablets)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();
  const int64_t start_time = ObClockGenerator::getClock();
  int64_t max_loop_time = 0;
  ObTabletID record_tablet_id;
  bool try_guard = true;
  SCN freeze_snapshot_version;
  (void)init_tablet_freeze_param_(tablet_ids, need_rewrite_meta, max_loop_time, record_tablet_id, try_guard);

  frozen_memtable_handles.reuse();
  freeze_failed_tablets.reuse();
  FLOG_INFO("[Freezer] tablet freeze start", K(ls_id), K(tablet_ids));

  ObTabletFreezeGuard guard(*this, try_guard);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "[Freezer] not inited", K(ret), K(ls_id), K(tablet_ids));
  } else if (OB_UNLIKELY(!enable_)) {
    STORAGE_LOG(WARN, "[Freezer] freezer is offline, can not freeze now", K(ret), K(ls_id));
  } else if (OB_FAIL(frozen_memtable_handles.reserve(tablet_ids.count()))) {
    TRANS_LOG(WARN, "fail to reserve memtable handles", K(ret), K(tablet_ids));
  } else if (OB_FAIL(freeze_failed_tablets.reserve(tablet_ids.count()))) {
    TRANS_LOG(WARN, "fail to reserve tablet ids", K(ret), K(tablet_ids));
  } else if (OB_FAIL(get_ls_weak_read_scn(freeze_snapshot_version))) {
    TRANS_LOG(WARN, "[Freezer] get ls weak read scn failure", K(ret), K(ls_id));
  } else if (ObScnRange::MAX_SCN == freeze_snapshot_version || ObScnRange::MIN_SCN >= freeze_snapshot_version) {
    ret = OB_MINOR_FREEZE_NOT_ALLOW;
    LOG_WARN("[Freezer] invalid weak read scn", K(ret), K(ls_id));
  } else if (try_guard && OB_FAIL(guard.try_set_tablet_freeze_begin())) {
    STORAGE_LOG(WARN, "[Freezer] ls freeze is running", KR(ret), K(ls_id), K(tablet_ids));
  } else if (OB_FAIL(loop_set_freeze_flag(max_loop_time))) {
    STORAGE_LOG(WARN, "[Freezer] another freeze function is running", KR(ret), K(ls_id), K(tablet_ids));
  } else {
    // freeze flag has been set
    freeze_snapshot_version_ = freeze_snapshot_version;
    (void)set_need_resubmit_log(false);
    (void)stat_.reset();
    (void)stat_.begin_set_freeze_stat(get_freeze_clock(),
                                      start_time,
                                      ObFreezeState::NOT_SUBMIT_LOG,
                                      freeze_snapshot_version,
                                      record_tablet_id,
                                      need_rewrite_meta);

    if (OB_FAIL(tablet_freeze_(trace_id,
                               tablet_ids,
                               need_rewrite_meta,
                               freeze_snapshot_version,
                               frozen_memtable_handles,
                               freeze_failed_tablets))) {
      STORAGE_LOG(WARN, "[Freezer] batch_tablet_freeze failed", K(ls_id), K(tablet_ids));
    }

    if (frozen_memtable_handles.empty()) {
      stat_.end_set_freeze_stat(ObFreezeState::FINISH, ObClockGenerator::getClock(), ret);
      print_freezer_statistics();
      unset_freeze_();
    }
  }

  if (OB_FAIL(ret) && freeze_failed_tablets.empty()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(freeze_failed_tablets.assign(tablet_ids))) {
      STORAGE_LOG(ERROR, "assign tablet ids failed", KR(ret));
    }
  }
  return ret;
}

void ObFreezer::init_tablet_freeze_param_(const ObIArray<ObTabletID> &tablet_ids,
                                          const bool need_rewrite_meta,
                                          int64_t &max_loop_time,
                                          ObTabletID &record_tablet_id,
                                          bool &try_guard)
{
  if (need_rewrite_meta || tablet_ids.count() > 1) {
    // if need_rewrite_meta or freeze more than one tablet, retry set_freeze_flag for 10 seconds
    max_loop_time = 10LL * 1000LL * 1000LL;
  } else {
    // do not retry set_freeze_flag
    max_loop_time = 0;
  }

  if (1 == tablet_ids.count()) {
    record_tablet_id = tablet_ids.at(0);
  } else {
    record_tablet_id = ObTabletID(ObTabletID::INVALID_TABLET_ID);
  }

  if (need_rewrite_meta) {
    try_guard = false;
  } else {
    try_guard = true;
  }
}

int ObFreezer::tablet_freeze_(const int64_t trace_id,
                              const ObIArray<ObTabletID> &tablet_ids,
                              const bool need_rewrite_meta,
                              const SCN freeze_snapshot_version,
                              ObIArray<ObTableHandleV2> &frozen_memtable_handles,
                              ObIArray<ObTabletID> &freeze_failed_tablets)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  // set is_tablet_freeze flag for all tablets and ignore ret code
  for (int64_t i = 0; i < tablet_ids.count(); i++) {
    const ObTabletID &tablet_id = tablet_ids.at(i);
    if (OB_TMP_FAIL(set_tablet_freeze_flag_(
            trace_id, tablet_id, need_rewrite_meta, freeze_snapshot_version, frozen_memtable_handles))) {
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
      while (OB_TMP_FAIL(freeze_failed_tablets.push_back(tablet_id))) {
        TRANS_LOG(ERROR, "[Freezer] fail to push_back", K(ret), K(tablet_id));
        stat_.add_diagnose_info("fail to push_back");
        usleep(100 * 1000);  // sleep 100ms
      }
    }
  }

  // NB: During the above traversal process, there may be memtables that have
  // successfully had set_tablet_freeze set, but subsequent traversal encounters
  // failure scenarios. In this scenario, we still need to handle the tablets
  // that have been successfully frozen, otherwise, they will use an incorrect
  // freeze_snapshot_version.
  //
  // Additionally, we must correctly propagate the error code.
  if (frozen_memtable_handles.empty()) {
    STORAGE_LOG(INFO, "[Freezer] empty freezed tablet array", K(ret), K(tablet_ids), K(freeze_failed_tablets));
    stat_.add_diagnose_info("no need to freeze tablet");
  } else {
    (void)submit_log_if_needed_(frozen_memtable_handles);
  }

  STORAGE_LOG(INFO,
              "[Freezer] TABLET FREEZE STEP 1 : set is_tablet_freeze flag done",
              KR(ret),
              K(get_ls_id()),
              K(need_rewrite_meta),
              K(tablet_ids),
              K(freeze_failed_tablets));
  return ret;
}

int ObFreezer::set_tablet_freeze_flag_(const int64_t trace_id,
                                       const ObTabletID tablet_id,
                                       const bool need_rewrite_meta,
                                       const SCN freeze_snapshot_version,
                                       ObIArray<ObTableHandleV2> &frozen_memtable_handles)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  ObProtectedMemtableMgrHandle *protected_handle = nullptr;
  ObLSID ls_id = get_ls_id();
  ObTabletHandle tablet_handle;
  ObTableHandleV2 frozen_memtable_handle;

  if (OB_FAIL(get_ls_tablet_svr()->get_tablet(tablet_id,
                                              tablet_handle,
                                              ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT,
                                              ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    TRANS_LOG(WARN, "[Freezer] fail to get tablet", K(ret), K(tablet_id));
    stat_.add_diagnose_info("fail to get tablet");
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (OB_FAIL(tablet->get_protected_memtable_mgr_handle(protected_handle))) {
    LOG_WARN("[Freezer] failed to get_protected_memtable_mgr_handle", K(ret), KPC(tablet));
#ifdef ERRSIM
  } else if (frozen_memtable_handles.count() >= 1 && OB_FAIL(ret = ERRSIM_BATCH_TABLET_FREEZE_FAILURE)) {
    LOG_WARN("[Freezer] errsim failure during freezer freeze", K(ret));
#endif
  } else if (OB_FAIL(protected_handle->set_is_tablet_freeze_for_active_memtable(frozen_memtable_handle, trace_id))) {
    (void)handle_set_tablet_freeze_failed(need_rewrite_meta, tablet_id, ls_id, tablet, freeze_snapshot_version, ret);
  } else if (!frozen_memtable_handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "[Freezer] frozen_memtable_handle is invalid", K(ret), K(tablet_id));
    stat_.add_diagnose_info("frozen_memtable_handle is invalid");
  } else {
    // NB: we donnot allow failure after each set tablet freeze!!!
    while (OB_FAIL(frozen_memtable_handles.push_back(frozen_memtable_handle))) {
      TRANS_LOG(ERROR, "[Freezer] fail to push_back", K(ret), K(tablet_id));
      stat_.add_diagnose_info("fail to push_back");
      usleep(100 * 1000);  // sleep 100ms
    }
  }

  return ret;
}

void ObFreezer::handle_set_tablet_freeze_failed(const bool need_rewrite_meta,
                                                const ObTabletID &tablet_id,
                                                const ObLSID &ls_id,
                                                const ObTablet *tablet,
                                                const share::SCN freeze_snapshot_version,
                                                int &ret)
{
  if (ret == OB_ENTRY_NOT_EXIST) {
    if (need_rewrite_meta) {
      ret = handle_no_active_memtable_(tablet_id, tablet, freeze_snapshot_version);
    } else {
      ret = OB_SUCCESS;
      TRANS_LOG(INFO, "[Freezer] no need to freeze", K(ret), K(ls_id), K(tablet_id));
      stat_.add_diagnose_info("no need to freeze");
    }
  } else {
    TRANS_LOG(WARN, "[Freezer] fail to set is_tablet_freeze", K(ret), K(ls_id), K(tablet_id));
    stat_.add_diagnose_info("fail to set is_tablet_freeze");
  }
}

int ObFreezer::decide_real_snapshot_version_(const ObTabletID &tablet_id,
                                             const ObTablet *tablet,
                                             const SCN freeze_snapshot_version,
                                             SCN &real_snapshot_version)
{
  int ret = OB_SUCCESS;
  ObTabletCreateDeleteMdsUserData user_data;
  bool is_committed = false;
  share::SCN transfer_scn = share::SCN::max_scn();

  if (tablet_id.is_ls_inner_tablet()) {
    //do nothing
  } else if (OB_FAIL(tablet->ObITabletMdsInterface::get_latest_tablet_status(user_data,
                                                                             is_committed))) {
    LOG_WARN("fail to get latest tablet status", K(ret), KPC(tablet));
  } else if (ObTabletStatus::TRANSFER_OUT != user_data.tablet_status_
             && ObTabletStatus::TRANSFER_OUT_DELETED != user_data.tablet_status_) {
    //do nothing
  } else if (user_data.transfer_scn_.is_valid()) {
    transfer_scn = user_data.transfer_scn_;
  }

  if (OB_SUCC(ret)) {
    real_snapshot_version = MIN(freeze_snapshot_version, transfer_scn);
    if (real_snapshot_version != freeze_snapshot_version) {
      FLOG_INFO("update tablet snapshot version changed snapshot version for transfer",
                K(real_snapshot_version), K(freeze_snapshot_version), K(transfer_scn), K(user_data));
    }
  }

  return ret;
}

int ObFreezer::handle_no_active_memtable_(const ObTabletID &tablet_id,
                                          const ObTablet *tablet,
                                          SCN freeze_snapshot_version)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();
  ObProtectedMemtableMgrHandle *protected_handle = NULL;
  if (OB_FAIL(tablet->get_protected_memtable_mgr_handle(protected_handle))) {
    LOG_WARN("failed to get_protected_memtable_mgr_handle", K(ret), KPC(tablet));
  } else if (OB_FAIL(decide_real_snapshot_version_(tablet_id,
                                                   tablet,
                                                   freeze_snapshot_version,
                                                   freeze_snapshot_version))) {
    LOG_WARN("failed to decide real snapshot version", K(ret), KPC(tablet));
  } else if (!protected_handle->has_memtable()) {
    // We need trigger a dag to rewrite the snapshot version of tablet
    // meta for the major merge and medium merge. While the implementation
    // need pay much attentio.
    // 1. If we trigger the dag each time the freeze happened, there may
    //    be alive dag not finished, and we rewrite the tablet meta first.
    //    It will cause the snapshot version to be pushed by mistake.
    // 2. We need fetch the weak read timestamp at first, otherwise the
    //    concurrent memtable creation may miscalculate the version
    //
    // NOTICE: use freeze_snapshot_version as input parameter

    // check exist mini compaction dag running
    ObTabletMergeDagParam param;
    param.merge_type_ = ObMergeType::MINI_MERGE;
    param.merge_version_ = ObVersionRange::MIN_VERSION;
    param.ls_id_ = ls_id;
    param.tablet_id_ = tablet_id;
    param.skip_get_tablet_ = true;

    ObProtectedMemtableMgrHandle *protected_handle = NULL;
    ObTabletMiniMergeDag tmp_mini_dag;
    bool is_exist = false;
    if (OB_FAIL(tmp_mini_dag.init_by_param(&param))) {
      LOG_WARN("failed to init mini dag", K(ret), K(param));
    } else if (OB_FAIL(MTL(ObTenantDagScheduler *)->check_dag_exist(&tmp_mini_dag, is_exist))) {
      LOG_WARN("failed to check dag exists", K(ret), K(ls_id), K(tablet_id));
    } else if (is_exist) {
      // we need to wait the current mini compaction dag to complete
      ret = OB_EAGAIN;
      LOG_WARN("exist running mini compaction dag, try later", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(get_ls_tablet_svr()->update_tablet_snapshot_version(tablet_id,
                                                                           freeze_snapshot_version.get_val_for_tx()))) {
      LOG_WARN("failed to update tablet snapshot version", K(ret), K(ls_id), K(tablet_id), K(freeze_snapshot_version));
    } else if (OB_FAIL(tablet->get_protected_memtable_mgr_handle(protected_handle))) {
      LOG_WARN("failed to get_protected_memtable_mgr_handle", K(ret), KPC(tablet));
    } else if (protected_handle->get_max_saved_version_from_medium_info_recorder() >=
               freeze_snapshot_version.get_val_for_tx()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(compaction::ObTenantTabletScheduler::schedule_merge_dag(
              ls_id, *tablet, MEDIUM_MERGE, freeze_snapshot_version.get_val_for_tx()))) {
        if (OB_SIZE_OVERFLOW != tmp_ret && OB_EAGAIN != tmp_ret) {
          ret = tmp_ret;
          LOG_WARN("failed to schedule medium merge dag", K(ret), K(ls_id), K(tablet_id));
        }
      } else {
        TRANS_LOG(INFO, "[Freezer] memtable_mgr doesn't have memtable", K(ret), K(ls_id), K(tablet_id));
      }
    }
  } else {
    TRANS_LOG(INFO, "[Freezer] memtable_mgr has memtable", K(ret), K(ls_id), K(tablet_id));
  }
  return ret;
}

void ObFreezer::submit_log_if_needed_(ObIArray<ObTableHandleV2> &frozen_memtable_handles)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; i < frozen_memtable_handles.count(); i++) {
    ObITabletMemtable *tablet_memtable = nullptr;
    if (OB_FAIL(frozen_memtable_handles.at(i).get_tablet_memtable(tablet_memtable))) {
      TRANS_LOG(WARN, "get tablet memtable failed", KR(ret), K(i), K(frozen_memtable_handles));
    } else if (OB_ISNULL(tablet_memtable)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "tablet memtable is unexpected null", KR(ret), K(i));
    } else if (tablet_memtable->is_direct_load_memtable()) {
      // skip submit log cause direct load memtable unneeded
    } else if (tablet_memtable->is_data_memtable()) {
      // find one data memtable, submit log and break
      (void)try_submit_log_for_freeze_(true /* is_tablet_freeze */);
      break;
    }
  }
}

int ObFreezer::wait_tablet_freeze_finish(ObIArray<ObTableHandleV2> &memtable_handles,
                                         ObIArray<ObTabletID> &freeze_failed_tablets)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();
  const int64_t start_time = ObClockGenerator::getClock();

  for (int i = 0; i < memtable_handles.count(); ++i) {
    int tmp_ret = OB_SUCCESS;
    ObTableHandleV2 &memtable_handle = memtable_handles.at(i);
    if (OB_TMP_FAIL(inner_wait_memtable_freeze_finish_(memtable_handle))) {
      STORAGE_LOG(WARN, "wait tablet freeze failed", KR(ret));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
      ObITabletMemtable *tablet_memtable = nullptr;
      if (OB_TMP_FAIL(memtable_handle.get_tablet_memtable(tablet_memtable))) {
        STORAGE_LOG(WARN, "get tablet memtable failed", KR(ret), K(memtable_handle));
      } else if (OB_TMP_FAIL(freeze_failed_tablets.push_back(tablet_memtable->get_tablet_id()))) {
        STORAGE_LOG(WARN, "push back tablet id failed", KR(ret));
      }
    }
  }

  const int64_t wait_freeze_finish_spend_time = ObClockGenerator::getClock() - start_time;
  STORAGE_LOG(INFO,
              "[Freezer] TABLET FREEZE STEP 2 : wait tablet freeze finish done",
              KR(ret),
              K(get_ls_id()),
              K(wait_freeze_finish_spend_time),
              K(freeze_failed_tablets));

  stat_.end_set_freeze_stat(ObFreezeState::FINISH, ObClockGenerator::getClock(), ret);
  print_freezer_statistics();
  unset_freeze_();
  return ret;
}

int ObFreezer::inner_wait_memtable_freeze_finish_(ObTableHandleV2 &memtable_handle)
{
  int ret = OB_SUCCESS;
  ObITabletMemtable *tablet_memtable = nullptr;

  if (!memtable_handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "memtable cannot be null", K(ret), K(get_ls_id()));
  } else if (OB_FAIL(memtable_handle.get_tablet_memtable(tablet_memtable))) {
    STORAGE_LOG(WARN, "fail to get memtable", K(ret), K(memtable_handle));
  } else if (OB_ISNULL(tablet_memtable)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to get memtable", K(ret), K(memtable_handle));
  } else {
    if (tablet_memtable->is_data_memtable()) {
      ret = wait_data_memtable_freeze_finish_(tablet_memtable);
    } else if (tablet_memtable->is_direct_load_memtable()) {
      ret = wait_direct_load_memtable_freeze_finish_(tablet_memtable);
    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected memtable type", KR(ret), KPC(tablet_memtable));
    }
  }

  STORAGE_LOG(DEBUG, "inner wait memtable freeze finish", KR(ret), K(memtable_handle));

  return ret;
}

int ObFreezer::wait_data_memtable_freeze_finish_(ObITabletMemtable *tablet_memtable)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();
  ObMemtable *memtable = static_cast<ObMemtable*>(tablet_memtable);
  if (OB_FAIL(wait_memtable_ready_for_flush_(memtable))) {
    TRANS_LOG(WARN, "[Freezer] fail to wait memtable ready_for_flush", K(ret), K(ls_id));
  } else {
    int64_t read_lock = LSLOCKALL;
    int64_t write_lock = 0;
    ObLSLockGuard lock_ls(ls_, ls_->lock_, read_lock, write_lock);
    if (OB_FAIL(check_ls_state())) {
    } else if (OB_FAIL(memtable->finish_freeze())) {
      TRANS_LOG(ERROR, "[Freezer] memtable cannot be flushed", K(ret), K(ls_id), KPC(memtable));
      stat_.add_diagnose_info("memtable cannot be flushed");
    } else {
      stat_.add_diagnose_info("tablet_freeze success");
      FLOG_INFO("[Freezer] tablet_freeze_task success(DataMemtable)", K(ret), K(ls_id), KPC(memtable));
    }
  }
  return ret;
}

int ObFreezer::wait_direct_load_memtable_freeze_finish_(ObITabletMemtable *tablet_memtable)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();
  ObDDLKV *direct_load_memtable = static_cast<ObDDLKV*>(tablet_memtable);
  if (OB_FAIL(direct_load_memtable->decide_right_boundary())) {
    STORAGE_LOG(WARN, "freeze direct load memtable failed", KR(ret), K(ls_id), KPC(tablet_memtable));
  } else {
    int64_t read_lock = LSLOCKALL;
    int64_t write_lock = 0;
    ObLSLockGuard lock_ls(ls_, ls_->lock_, read_lock, write_lock);
    if (OB_FAIL(check_ls_state())) {
    } else if (OB_FAIL(tablet_memtable->finish_freeze())) {
      TRANS_LOG(ERROR, "[Freezer] direct load memtable cannot be flushed", K(ret), K(ls_id), KPC(tablet_memtable));
      stat_.add_diagnose_info("direct load memtable cannot be flushed");
    } else {
      stat_.add_diagnose_info("tablet_freeze success");
      FLOG_INFO("[Freezer] tablet_freeze_task success(DirectLoadMemtable)", K(ls_id), KP(this), KP(tablet_memtable));
    }
  }
  return ret;
}

int ObFreezer::wait_memtable_ready_for_flush_(ObITabletMemtable *tablet_memtable)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();
  const int64_t start_time = ObClockGenerator::getClock();
  bool ready_for_flush = false;
  bool is_force_released = false;

  {
    PendTenantReplayHelper pend_replay_helper(*this, ls_);
    (void)pend_replay_helper.set_skip_throttle_flag();

    TRANS_LOG(INFO,
              "[Freezer] wait freeze : Tablet",
              K(ls_id),
              KP(tablet_memtable),
              K(throttle_is_skipping_),
              K(tenant_replay_is_pending_));

    int64_t time_counter = 0;
    do {
      if (OB_ISNULL(tablet_memtable)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "[Freezer] memtable cannot be null", K(ret));
      } else if (FALSE_IT(ready_for_flush = tablet_memtable->ready_for_flush())) {
      } else if (FALSE_IT(is_force_released = tablet_memtable->is_force_released())) {
      } else if (!ready_for_flush && !is_force_released) {
        if (TC_REACH_TIME_INTERVAL(1LL * 1000LL * 1000LL /* 1 second */)) {
          ++time_counter;

          // check pend condition every second
          (void)pend_replay_helper.check_pend_condition_once();

          // check resubmit log condition and report some debug info every 5 seconds
          if (time_counter >= 5 && time_counter % 5 == 0) {
            (void)resubmit_log_if_needed_(start_time, true /* is_tablet_freeze */, false /* is_try */, tablet_memtable);
          }
        }
        ob_usleep(100);
      }
    } while (OB_SUCC(ret) && !ready_for_flush && !is_force_released);
  }

  return ret;
}

namespace {
  struct FreezeDiagnoseInfo {
    const char *fmt_;
    const int ret_;
    FreezeDiagnoseInfo(const char *fmt, const int ret): fmt_(fmt), ret_(ret) {}
    DECLARE_TO_STRING
    {
      int64_t pos = 0;
      BUF_PRINTF(fmt_, ObCurTraceId::get_trace_id_str(), ret_);
      return pos;
    }
  };
}

int ObFreezer::submit_log_for_freeze(const bool is_tablet_freeze, const bool is_try)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();
  const int64_t start = ObClockGenerator::getClock();
  ObTabletID tablet_id(INT64_MAX); // used for diagnose
  bool trace_id_need_reset = false;
  if (!ObCurTraceId::get_trace_id()->is_valid()) {
    ObCurTraceId::init(GCONF.self_addr_);
    bool trace_id_need_reset = true;
  }
  do {
    ret = OB_SUCCESS;
    transaction::ObTransID fail_tx_id;

    {
      int64_t read_lock = LSLOCKALL;
      int64_t write_lock = 0;
      ObLSLockGuard lock_ls(ls_, ls_->lock_, read_lock, write_lock);
      if (OB_FAIL(check_ls_state())) {
        // ls has died, we need break the loop for submitting log
        TRANS_LOG(WARN, "ls state has die", K(ls_id));
        break;
      }
    }

    // because tablet freeze will not inc freeze clock, fake the freeze clock
    const uint32_t freeze_clock = is_tablet_freeze ? get_freeze_clock() + 1 : get_freeze_clock();
    if (OB_FAIL(get_ls_tx_svr()->traverse_trans_to_submit_redo_log(fail_tx_id, freeze_clock))) {
      const int64_t cost_time = ObClockGenerator::getClock() - start;
      if (cost_time > 1000 * 1000) {
        if (TC_REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
          TRANS_LOG(WARN, "[Freezer] traverse_trans_to_submit_redo_log failed", K(ret),
                    K(ls_id), K(cost_time), K(fail_tx_id));
          FreezeDiagnoseInfo diagnose("traverse_trans_to_submit_redo_log failed, traceId:%s, errorCode:%d", ret);
          stat_.add_diagnose_info(to_cstring(diagnose));
          if (OB_TMP_FAIL(ADD_SUSPECT_INFO(MINI_MERGE, ObDiagnoseTabletType::TYPE_MINI_MERGE,
                          ls_id, tablet_id, ObSuspectInfoType::SUSPECT_SUBMIT_LOG_FOR_FREEZE,
                          static_cast<int64_t>(ret), fail_tx_id.get_id()))) {
            TRANS_LOG(WARN, "failed to add suspect info", K(tmp_ret));
          }
        }
      }
    }
    if (OB_LOG_OUTOF_DISK_SPACE == ret) {
      ob_usleep(100 * 1000);
    }
  } while (!(is_try && (ObClockGenerator::getClock() - start > 10 * 1_s))
           // we break the loop if we retry with a long time with the try semantic
           && OB_FAIL(ret));

  if (OB_SUCC(ret)) {
    DEL_SUSPECT_INFO(MINI_MERGE, ls_id, tablet_id, ObDiagnoseTabletType::TYPE_MINI_MERGE);

    if (OB_FAIL(get_ls_tx_svr()->traverse_trans_to_submit_next_log())) {
      TRANS_LOG(WARN, "traverse trans ctx to submit next log failed", K(ret));
    }

    stat_.set_state(ObFreezeState::WAIT_READY_FOR_FLUSH);
  }
  if (trace_id_need_reset) {
    ObCurTraceId::reset();
  }
  return ret;
}

/* private subfunctions about freeze flag*/
int ObFreezer::loop_set_freeze_flag(const int64_t max_loop_time)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();
  const int64_t start = ObClockGenerator::getClock();
  int64_t cost_time = 0;
  do {
    ret = OB_SUCCESS;
    if (OB_FAIL(set_freeze_flag_without_inc_freeze_clock())) {
      cost_time = ObClockGenerator::getClock() - start;
      if (cost_time > max_loop_time) {
        break;
      }
      ob_usleep(100);
    }
  } while (OB_FAIL(ret));

  const int64_t LOG_WARN_TIME = 10LL * 1000LL * 1000LL;  // 10 seconds
  if (cost_time > LOG_WARN_TIME) {
    TRANS_LOG(WARN, "[Freezer] wait the running freeze too long time", K(ls_id), K(cost_time));
  }
  return ret;
}

int ObFreezer::set_freeze_flag_without_inc_freeze_clock()
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();
  uint32_t old_v = 0;
  uint32_t new_v = 0;

  do {
    old_v = ATOMIC_LOAD(&freeze_flag_);
    if (is_freeze(old_v)) {
      ret = OB_EAGAIN;
      break;
    }
    new_v = old_v | (1 << 31);
  } while (ATOMIC_CAS(&freeze_flag_, old_v, new_v) != old_v);

  return ret;
}

int ObFreezer::set_freeze_flag()
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();
  uint32_t old_v = 0;
  uint32_t new_v = 0;

  // set freeze_flag to flag the start of freeze
  // set the first bit 1
  // inc the freeze clock
  do {
    old_v = ATOMIC_LOAD(&freeze_flag_);
    if (is_freeze(old_v)) {
      ret = OB_EAGAIN;
      TRANS_LOG(WARN, "[Freezer] freeze is running!", K(ret), K(ls_id));
      break;
    }
    new_v = (old_v + 1) | (1 << 31);
  } while (ATOMIC_CAS(&freeze_flag_, old_v, new_v) != old_v);

  return ret;
}

void ObFreezer::unset_freeze_()
{
  uint32_t old_v;
  uint32_t new_v;

  // Step1: unset freeze_snapshot_version to invalid value
  freeze_snapshot_version_.reset();

  // Step2: unset max_decided_scn to invalid value
  max_decided_scn_.reset();

  // Step3: unset need_resubmit_log_
  set_need_resubmit_log(false);

  // Step4: unset freeze_flag to flag the end of freeze
  // set the first bit 0
  do {
    old_v = ATOMIC_LOAD(&freeze_flag_);
    new_v = old_v & (~(1 << 31));
  } while (ATOMIC_CAS(&freeze_flag_, old_v, new_v) != old_v);
}

/* public function about freeze_flag */
bool ObFreezer::is_freeze(uint32_t freeze_flag) const
{
  return 1 == (freeze_flag >> 31);
}

/* other public functions */
int ObFreezer::decide_max_decided_scn(SCN &max_decided_scn)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[Freezer] not inited", K(ret), K(ls_id));
  } else if (OB_FAIL(get_ls_log_handler()->get_max_decided_scn(max_decided_scn))) {
    if (OB_STATE_NOT_MATCH == ret) {
      max_decided_scn.reset();
      ret = OB_SUCCESS;
    } else {
      TRANS_LOG(WARN, "[Freezer] fail to get max_decided_scn", K(ret), K(ls_id),
                K(max_decided_scn));
    }
  }

  if (OB_SUCC(ret)) {
    TRANS_LOG(TRACE, "[Freezer] decide max decided log ts", K(ret), K(ls_id), K(max_decided_scn));
  }

  return ret;
}

int ObFreezer::get_max_consequent_callbacked_scn(SCN &max_consequent_callbacked_scn)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[Freezer] not inited", K(ret), K(ls_id));
  } else if (OB_FAIL(get_ls_log_handler()->get_max_decided_scn(max_consequent_callbacked_scn))) {
    if (OB_STATE_NOT_MATCH == ret) {
      max_consequent_callbacked_scn.set_min();
      ret = OB_SUCCESS;
    } else {
      TRANS_LOG(WARN, "[Freezer] fail to get min_unreplay_scn", K(ret), K(ls_id), K(max_consequent_callbacked_scn));
    }
  } else {
    TRANS_LOG(TRACE, "[Freezer] get_max_decided_scn", K(ret), K(ls_id), K(max_consequent_callbacked_scn));
  }
  return ret;
}

int ObFreezer::get_ls_weak_read_scn(SCN &weak_read_scn)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();
  weak_read_scn.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "[Freezer] not inited", K(ret));
  } else {
    weak_read_scn = get_ls_wrs_handler()->get_ls_weak_read_ts();
    TRANS_LOG(TRACE, "[Freezer] get_ls_weak_read_scn", K(ret), K(ls_id), K(weak_read_scn));
  }

  return ret;
}

int ObFreezer::get_newest_clog_checkpoint_scn(const ObTabletID &tablet_id,
                                              SCN &clog_checkpoint_scn)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();
  ObTabletHandle handle;
  clog_checkpoint_scn.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "[Freezer] not inited", K(ret));
  } else if (OB_FAIL(get_ls_tablet_svr()->get_tablet(tablet_id,
      handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    TRANS_LOG(WARN, "[Freezer] fail to get tablet", K(ret), K(ls_id), K(tablet_id));
  } else {
    clog_checkpoint_scn = handle.get_obj()->get_tablet_meta().clog_checkpoint_scn_;
    TRANS_LOG(TRACE, "[Freezer] get_newest_clog_checkpoint_scn", K(ret), K(ls_id), K(tablet_id),
              K(clog_checkpoint_scn));
  }

  return ret;
}

int ObFreezer::get_newest_snapshot_version(const ObTabletID &tablet_id,
                                           SCN &snapshot_version)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();
  ObTabletHandle handle;
  snapshot_version.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "[Freezer] not inited", K(ret));
  } else if (OB_FAIL(get_ls_tablet_svr()->get_tablet(tablet_id,
      handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    TRANS_LOG(WARN, "[Freezer] fail to get tablet", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(snapshot_version.convert_for_tx(handle.get_obj()->get_snapshot_version()))) {
    TRANS_LOG(WARN, "[Freezer] fail to convert from ts", K(ret), K(ls_id), K(tablet_id));
  } else {
    TRANS_LOG(TRACE, "[Freezer] get_snapshot_version", K(ret), K(ls_id), K(tablet_id), K(snapshot_version));
  }

  return ret;
}

void ObFreezer::inc_empty_memtable_cnt()
{
  ATOMIC_INC(&empty_memtable_cnt_);
}

void ObFreezer::clear_empty_memtable_cnt()
{
  ATOMIC_STORE(&empty_memtable_cnt_, 0);
}

int64_t ObFreezer::get_empty_memtable_cnt()
{
  return empty_memtable_cnt_;
}

void ObFreezer::print_freezer_statistics()
{
  // print every 10s
  if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
    TRANS_LOG(INFO, "[Freezer] empty table statistics: ", K(get_ls_id()), K(get_empty_memtable_cnt()));
    clear_empty_memtable_cnt();
  }
}

int ObFreezer::try_set_tablet_freeze_begin_()
{
  int ret = OB_SUCCESS;
  if (ATOMIC_LOAD(&high_priority_freeze_cnt_) != 0) {
    // high priority freeze waiting now, can not do tablet freeze now.
    ret = OB_EAGAIN;
  } else {
    ATOMIC_INC(&low_priority_freeze_cnt_);
    // double check
    if (ATOMIC_LOAD(&high_priority_freeze_cnt_) != 0) {
      ret = OB_EAGAIN;
      ATOMIC_DEC(&low_priority_freeze_cnt_);
    }
  }
  return ret;
}

void ObFreezer::set_tablet_freeze_begin_()
{
  int ret = OB_SUCCESS;
  const int64_t SLEEP_INTERVAL = 100 * 1000; // 100 ms
  int64_t retry_times = 0;
  while (OB_FAIL(try_set_tablet_freeze_begin_())) {
    retry_times++;
    ob_usleep(SLEEP_INTERVAL);
    if (retry_times % 100 == 0) { // 10 s
      LOG_WARN_RET(OB_ERR_TOO_MUCH_TIME, "wait high priority freeze finish cost too much time",
               K(ret), K(high_priority_freeze_cnt_), K(retry_times));
    }
  }
}

void ObFreezer::set_ls_freeze_begin_()
{
  const int64_t SLEEP_INTERVAL = 100 * 1000; // 100 ms
  int64_t retry_times = 0;
  ATOMIC_INC(&high_priority_freeze_cnt_);
  while (ATOMIC_LOAD(&low_priority_freeze_cnt_) != 0) {
    retry_times++;
    ob_usleep(SLEEP_INTERVAL);
    if (retry_times % 100 == 0) { // 10 s
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "wait low priority freeze finish cost too much time",
               K(low_priority_freeze_cnt_), K(retry_times));
    }
  }
}

int ObFreezer::pend_ls_replay()
{
  int ret = OB_SUCCESS;
  common::ObByteLockGuard guard(byte_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "[Freezer] not inited", KR(ret), K(get_ls_id()), KP(this));
  } else if (pend_replay_cnt_ < 0) {
    TRANS_LOG(ERROR, "[Freezer] invalid pend_replay_cnt", KR(ret), K(get_ls_id()), KP(this), K(pend_replay_cnt_));
  } else if (FALSE_IT(pend_replay_cnt_++)) {
  } else if (1 == pend_replay_cnt_) {
    if (OB_FAIL(get_ls_log_handler()->pend_submit_replay_log())) {
      pend_replay_cnt_--;
      TRANS_LOG(WARN, "[Freezer] pend ls replay failed", KR(ret), K(get_ls_id()), KP(this));
    }
  }
  return ret;
}

int ObFreezer::restore_ls_replay()
{
  int ret = OB_SUCCESS;
  common::ObByteLockGuard guard(byte_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "[Freezer] not inited", KR(ret), K(get_ls_id()), KP(this));
  } else if (0 >= pend_replay_cnt_) {
    TRANS_LOG(INFO,
              "[Freezer] no need restore replay cause pend replay may failed",
              KR(ret),
              K(get_ls_id()),
              KP(this),
              K(pend_replay_cnt_));
  } else if (FALSE_IT(pend_replay_cnt_--)) {
  } else if (0 == pend_replay_cnt_) {
    if (OB_FAIL(get_ls_log_handler()->restore_submit_replay_log())) {
      if (get_ls_log_handler()->is_valid()) {
        TRANS_LOG(ERROR, "restore replay failed. please check if logstream is removed", KR(ret), K(get_ls_id()));
      } else {
        TRANS_LOG(WARN, "restore replay failed. ls log handler is invalid", KR(ret), K(get_ls_id()));
      }
    }
  }
  return ret;
}

struct EraseTabletInfoCmpFunctor {
  EraseTabletInfoCmpFunctor(AsyncFreezeTabletInfo &tablet_info) : tablet_info_(tablet_info) {}
  bool operator()(common::hash::HashMapPair<AsyncFreezeTabletInfo, common::hash::HashNullObj> &entry)
  { return tablet_info_ == entry.first; }

public:
  AsyncFreezeTabletInfo &tablet_info_;
};

struct GetAsyncFreezeTabletIDFunctor {
public:
  GetAsyncFreezeTabletIDFunctor(ObFreezer &host, const int64_t ls_epoch, ObIArray<ObTabletID> &tablet_ids)
      : host_(host), ls_epoch_(ls_epoch), tablet_ids_(tablet_ids), tablets_to_erase_() {}
  ~GetAsyncFreezeTabletIDFunctor(){};
  int operator()(common::hash::HashSetTypes<AsyncFreezeTabletInfo>::pair_type &kv)
  {
    int ret = OB_SUCCESS;
    bool need_erase = false;
    bool is_erased = false;
    AsyncFreezeTabletInfo &tablet_info = kv.first;
    if (ls_epoch_ != tablet_info.epoch_) {
      STORAGE_LOG(INFO, "this tablet no need merge because ls epoch has changed", K(ls_epoch_), K(tablet_info));
      need_erase = true;
    } else if (OB_FAIL(tablet_ids_.push_back(tablet_info.tablet_id_))) {
      STORAGE_LOG(WARN, "push back tablet id to async freeze array failed", KR(ret), K(tablet_info));
    } else {
      need_erase = true;
    }

    if (need_erase) {
      (void)tablets_to_erase_.push_back(tablet_info);
    }
    return ret;
  }

  ObIArray<AsyncFreezeTabletInfo> &get_tablets_to_erase() { return tablets_to_erase_; }

public:
  ObFreezer &host_;
  const int64_t ls_epoch_;
  ObIArray<ObTabletID> &tablet_ids_;
  ObSEArray<AsyncFreezeTabletInfo, 128> tablets_to_erase_;
};

int ObFreezer::get_all_async_freeze_tablets(const int64_t ls_epoch, ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObClockGenerator::getClock();
  GetAsyncFreezeTabletIDFunctor get_tablet_ids_func(*this, ls_epoch, tablet_ids);

  if (OB_FAIL(async_freeze_tablets_.foreach_refactored(get_tablet_ids_func))) {
    STORAGE_LOG(WARN, "iterate async freeze tablets set failed", KR(ret), K(ls_epoch), K(tablet_ids));
  }

  ObIArray<AsyncFreezeTabletInfo> &tablets_to_erase = get_tablet_ids_func.get_tablets_to_erase();
  for (int64_t i = 0; i < tablets_to_erase.count(); i++) {
    (void)erase_async_freeze_tablet(tablets_to_erase.at(i));
  }

  STORAGE_LOG(INFO,
              "get all async freeze tablets done",
              KR(ret),
              K(ls_epoch),
              KTIME(start_time),
              K(tablet_ids.count()),
              K(tablet_ids));
  return ret;
}

void ObFreezer::record_async_freeze_tablet(const AsyncFreezeTabletInfo &async_freeze_tablet_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(async_freeze_tablets_.set_refactored(async_freeze_tablet_info))) {
    STORAGE_LOG(WARN, "record freeze failed tablet failed", KR(ret), K(async_freeze_tablet_info));
  }
}

void ObFreezer::erase_async_freeze_tablet(const AsyncFreezeTabletInfo &async_freeze_tablet_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(async_freeze_tablets_.erase_refactored(async_freeze_tablet_info))) {
    if (OB_HASH_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "erase tablet id from tablets set failed", K(ret), K(async_freeze_tablet_info));
    }
  }
}

ObFreezer::ObLSFreezeGuard::ObLSFreezeGuard(ObFreezer &parent)
  : parent_(parent)
{
  parent_.set_ls_freeze_begin_();
}
ObFreezer::ObLSFreezeGuard::~ObLSFreezeGuard()
{
  parent_.set_ls_freeze_end_();
}

ObFreezer::ObTabletFreezeGuard::ObTabletFreezeGuard(ObFreezer &parent, const bool try_guard)
  : need_release_(!try_guard),
    parent_(parent)
{
  if (!try_guard) {
    parent_.set_tablet_freeze_begin_();
  }
}

ObFreezer::ObTabletFreezeGuard::~ObTabletFreezeGuard()
{
  if (need_release_) {
    parent_.set_tablet_freeze_end_();
  }
}

int ObFreezer::ObTabletFreezeGuard::try_set_tablet_freeze_begin()
{
  int ret = OB_SUCCESS;
  if (need_release_) {
    // this is not a try guard or has try succeed, just return success.
  } else if (OB_FAIL(parent_.try_set_tablet_freeze_begin_())) {
    LOG_WARN("try set tablet freeze failed", K(ret));
  } else {
    need_release_ = true;
  }
  return ret;
}


void ObFreezer::PendTenantReplayHelper::set_skip_throttle_flag()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(current_freeze_ls_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "invalid ls pointer", KP(current_freeze_ls_));
  } else if (current_ls_is_leader_()){
    // leader do not need skip throttle
  } else {
    // pend replay before set_throttle_is_skipping to avoid skipping too much replay
    if (remain_memory_is_exhausting_()) {
      (void)pend_tenant_replay_();
    }
    host_.set_throttle_is_skipping();
  }
}

bool ObFreezer::PendTenantReplayHelper::current_ls_is_leader_()
{
  int ret = OB_SUCCESS;
  // set default value as leader because leader do not skip throttle
  bool is_leader = true;
  ObRole role;
  int64_t proposal_id = 0;
  if (OB_FAIL(current_freeze_ls_->get_log_handler()->get_role(role, proposal_id))) {
    LOG_WARN("get ls role failed", KR(ret), K(current_freeze_ls_->get_ls_id()));
  } else if (common::is_strong_leader(role)) {
    is_leader = true;
  } else {
    is_leader = false;
  }
  return is_leader;
}

void ObFreezer::PendTenantReplayHelper::check_pend_condition_once()
{
  // only check pend condition when throttle is skipping
  if (host_.throttle_is_skipping()) {
    if (host_.tenant_replay_is_pending()) {
      if (!remain_memory_is_exhausting_()) {
        (void)restore_tenant_replay_();
      } else {
        // keep pending replay
      }
    } else {
      // tenant replay is not pending
      if (remain_memory_is_exhausting_()) {
        (void)pend_tenant_replay_();
      } else {
        // keep not pending replay
      }
    }
  }
}

bool ObFreezer::PendTenantReplayHelper::remain_memory_is_exhausting_() {
  TxShareThrottleTool &throttle_tool = (MTL(ObSharedMemAllocMgr *)->share_resource_throttle_tool());
  const bool has_triggered_throttle = throttle_tool.has_triggered_throttle<ObMemstoreAllocator>();
  const bool remain_memory_is_exhausting =
      has_triggered_throttle || MTL(ObTenantFreezer *)->memstore_remain_memory_is_exhausting();
  STORAGE_LOG(INFO, "finish check remain memory", K(has_triggered_throttle), K(remain_memory_is_exhausting));

  return remain_memory_is_exhausting;
}

void ObFreezer::PendTenantReplayHelper::pend_tenant_replay_()
{
  int ret = OB_SUCCESS;
  ObLSService *ls_srv = MTL(ObLSService *);
  common::ObSharedGuard<ObLSIterator> iter;
  if (OB_FAIL(ls_srv->get_ls_iter(iter, ObLSGetMod::STORAGE_MOD))) {
    STORAGE_LOG(WARN, "[ObFreezer] fail to get ls iterator", KR(ret));
  } else {
    ObLS *ls = nullptr;
    ls_handle_array_.reuse();
    int64_t iterate_ls_count = 0;
    int64_t pend_ls_replay_count = 0;

    // Here we need to perform a pend_ls_replay on all ls, because if a switch_leader occurs for any ls during the
    // freezing process, the absence of pend_ls_replay might result in an unthrottled memory usage
    while (OB_SUCC(iter->get_next(ls))) {
      ObLSHandle ls_handle;
      iterate_ls_count++;
      if (OB_FAIL(ls_srv->get_ls(ls->get_ls_id(), ls_handle, ObLSGetMod::STORAGE_MOD))) {
        STORAGE_LOG(WARN, "[ObFreezer] get ls handle failed", KR(ret), KP(ls));
      } else if (OB_FAIL(ls_handle_array_.push_back(ls_handle))) {
        STORAGE_LOG(WARN, "[ObFreezer] push back ls handle failed", KR(ret), KP(ls));
      } else if (OB_FAIL(ls->get_freezer()->pend_ls_replay())) {
        STORAGE_LOG(WARN, "[ObFreezer] pend replay failed", KR(ret), KPC(ls));
        (void)ls_handle_array_.pop_back();
      } else {
        pend_ls_replay_count++;
      }
    }

    // only skip throttle when pend all ls replay successfully, or reset this guard to restore replay
    if (iterate_ls_count == pend_ls_replay_count) {
      (void)host_.set_tenant_replay_is_pending();
      STORAGE_LOG(INFO,
                  "pend replay finish",
                  K(pend_ls_replay_count),
                  K(host_.throttle_is_skipping()),
                  K(host_.tenant_replay_is_pending()));
    } else {
      (void)restore_tenant_replay_();
    }
  }
}

void ObFreezer::PendTenantReplayHelper::restore_tenant_replay_()
{
  for (int64_t i = 0; i < ls_handle_array_.count(); i++) {
    int ret = OB_SUCCESS;
    ObLS *ls = nullptr;
    if (OB_ISNULL(ls = ls_handle_array_.at(i).get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "[ObFreezer] invalid ls handle", KR(ret), KPC(ls));
    } else if (OB_FAIL(ls->get_freezer()->restore_ls_replay())) {
      STORAGE_LOG(ERROR, "[ObFreezer] restore replay failed", KR(ret), KPC(ls));
    }
  }
  host_.unset_tenant_replay_is_pending();
  STORAGE_LOG(INFO,
              "restore tenant replay",
              K(ls_handle_array_.count()),
              K(host_.tenant_replay_is_pending()),
              K(host_.throttle_is_skipping()));
  ls_handle_array_.reuse();
}

} // namespace storage
} // namespace oceanbase
