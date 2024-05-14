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
    is_inited_(false)
{}

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
    is_inited_(false)
{}

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
  is_inited_ = false;
}

int ObFreezer::init(ObLS *ls)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "[Freezer] invalid argument", K(ret));
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
int ObFreezer::logstream_freeze(int64_t trace_id,
                                ObFuture<int> *result)
{
  int ret = OB_SUCCESS;
  SCN freeze_snapshot_version;
  SCN max_decided_scn;
  share::ObLSID ls_id = get_ls_id();
  FLOG_INFO("[Freezer] logstream_freeze start", K(ret), K(ls_id));
  int64_t start_time = ObTimeUtility::current_time();

  ObLSFreezeGuard guard(*this);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[Freezer] not inited", K(ret), K(ls_id));
  } else if (OB_UNLIKELY(!enable_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("freezer is offline, can not freeze now", K(ret), K(ls_id));
  } else if (OB_FAIL(decide_max_decided_scn(max_decided_scn))) {
    TRANS_LOG(WARN, "[Freezer] decide max decided log ts failure", K(ret), K(ls_id));
  } else if (OB_FAIL(get_ls_weak_read_scn(freeze_snapshot_version))) {
    TRANS_LOG(WARN, "[Freezer] get ls weak read ts failure", K(ret), K(ls_id));
  } else if (ObScnRange::MAX_SCN == freeze_snapshot_version
             || ObScnRange::MIN_SCN >= freeze_snapshot_version) {
    ret = OB_MINOR_FREEZE_NOT_ALLOW;
    LOG_WARN("[Freezer] invalid weak read scn", K(ret), K(ls_id), K(freeze_snapshot_version));
#ifdef ERRSIM
  } else if (OB_FAIL(ret = ERRSIM_FREEZER_FREEZE_FAILURE)) {
    LOG_WARN("[Freezer] errsim failure during freezer freeze", K(ret));
#endif
  } else if (OB_FAIL(set_freeze_flag())) {
    FLOG_INFO("[Freezer] freeze is running", K(ret), K(ls_id));
  } else {
    // NB: we donnot allow failure after increasing freeze flag!!!

    if (FALSE_IT(max_decided_scn_ = max_decided_scn)) {
    } else if (FALSE_IT(freeze_snapshot_version_ = freeze_snapshot_version)) {
    } else if (FALSE_IT(set_need_resubmit_log(false))) {
    } else if (FALSE_IT(stat_.reset())) {
    } else if (FALSE_IT(stat_.begin_set_freeze_stat(get_freeze_clock(),
                                                    start_time,
                                                    ObFreezeState::NOT_SUBMIT_LOG,
                                                    freeze_snapshot_version,
                                                    ObTabletID(ObTabletID::INVALID_TABLET_ID),
                                                    false/*need_rewrite_meta*/))) {
      TRANS_LOG(WARN, "[Freezer] fail to begin_set_freeze_stat", K(ls_id));
    }

    MTL(checkpoint::ObCheckpointDiagnoseMgr*)->update_freeze_clock(ls_id, trace_id, get_freeze_clock());

    (void)inner_logstream_freeze(result);
  }

  print_freezer_statistics();

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

void ObFreezer::inner_logstream_freeze(ObFuture<int> *result)
{
  share::ObLSID ls_id = get_ls_id();
  ObTableHandleV2 handle;

  if (FALSE_IT(submit_checkpoint_task())) {
  } else if (FALSE_IT(try_submit_log_for_freeze_(false/*tablet freeze*/))) {
  } else if (FALSE_IT(submit_freeze_task_(true/*is_ls_freeze*/, result, handle))) {
  } else {
    TRANS_LOG(INFO, "[Freezer] succeed to start ls_freeze_task", K(ls_id));
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

int ObFreezer::ls_freeze_task_()
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();
  const int64_t start = ObTimeUtility::current_time();
  int64_t last_submit_log_time = start;
  uint32_t freeze_clock = get_freeze_clock();
  PendTenantReplayGuard pend_replay_guard;
  TRANS_LOG(INFO, "[Freezer] freeze_clock", K(ls_id), K(freeze_clock));

  // wait till all memtables are moved from frozen_list to prepare_list
  // this means that all memtables can be dumped
  while (!get_ls_data_checkpoint()->ls_freeze_finished()) {
    if (TC_REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      if (need_resubmit_log() ||
          // In order to prevent the txn has already passed the try_submit test
          // while failing to submit some logs due to an unexpected bug, we need
          // retry to submit the log to go around the above case
          (ObTimeUtility::current_time() - last_submit_log_time >= 1_min)) {
        last_submit_log_time = ObTimeUtility::current_time();
        int64_t read_lock = LSLOCKALL;
        int64_t write_lock = 0;
        ObLSLockGuard lock_ls(ls_, ls_->lock_, read_lock, write_lock);
        if (OB_FAIL(check_ls_state())) {
        } else {
          submit_log_for_freeze(false/*tablet freeze*/, false/*try*/);
          TRANS_LOG(INFO, "[Freezer] resubmit log for ls_freeze", K(ls_id));
        }
      }

      const int64_t cost_time = ObTimeUtility::current_time() - start;

      if (cost_time > 5 * 1000 * 1000) {
        TRANS_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "[Freezer] finish ls_freeze costs too much time",
                      K(ls_id), K(cost_time));
        stat_.add_diagnose_info("finish ls_freeze costs too much time");
      }
    }

    ob_usleep(100);
  }
  stat_.add_diagnose_info("logstream_freeze success");
  FLOG_INFO("[Freezer] logstream_freeze success", K(ls_id), K(freeze_clock));

  stat_.end_set_freeze_stat(ObFreezeState::FINISH, ObTimeUtility::current_time(), ret);
  unset_freeze_();

  (void)try_freeze_tx_data_();

  return ret;
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

/* tablet freeze */
int ObFreezer::tablet_freeze(const ObTabletID &tablet_id, ObFuture<int> *result)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();

  if (tablet_id.is_ls_inner_tablet()) {
    if (OB_FAIL(freeze_ls_inner_tablet_(tablet_id))) {
      TRANS_LOG(WARN, "freeze ls inner tablet failed", KR(ret), K(ls_id), K(tablet_id));
    }
  } else if (OB_FAIL(freeze_normal_tablet_(tablet_id, result))) {
    TRANS_LOG(WARN, "freeze normal tablet failed", KR(ret), K(ls_id), K(tablet_id));
  }
  return ret;
}

int ObFreezer::freeze_ls_inner_tablet_(const ObTabletID &tablet_id)
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

/**
 * @brief freeze normal tablet logic
 *
 * @param for_direct_load if this freeze is called by direct load logic, the freeze must be executed even if there is a
 * logstream freeze running. So the ret code OB_EAGAIN cannot be reset to OB_SUCCESS
 */
int ObFreezer::freeze_normal_tablet_(const ObTabletID &tablet_id, ObFuture<int> *result, const bool for_direct_load)
{
  int ret = OB_SUCCESS;
  const share::ObLSID ls_id = get_ls_id();
  const int64_t start_time = ObClockGenerator::getClock();
  FLOG_INFO("[Freezer] tablet_freeze start", K(ret), K(ls_id), K(tablet_id), KTIME(start_time));

  ObTabletFreezeGuard guard(*this, true /* try guard */);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "[Freezer] not inited", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_UNLIKELY(!enable_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("freezer is offline, can not freeze now", K(ret), K(ls_id));
  } else if (OB_FAIL(guard.try_set_tablet_freeze_begin())) {
    if (for_direct_load) {
      // ret == OB_EAGAIN, need try freeze again
    } else {
      ret = OB_SUCCESS;
      LOG_INFO("[Freezer] ls freeze is running, no need freeze again", K(ret), K(ls_id), K(tablet_id));
    }
  } else if (OB_FAIL(set_freeze_flag_without_inc_freeze_clock())) {
    LOG_INFO("[Freezer] freeze is running", K(ret), K(ls_id), K(tablet_id));
  } else {
    ObTableHandleV2 frozen_memtable_handle;
    const bool need_rewrite_meta = false;
    if (OB_FAIL(do_tablet_freeze_(need_rewrite_meta, start_time, tablet_id, result, frozen_memtable_handle))) {
      STORAGE_LOG(WARN, "tablet freeze failed", KR(ret), K(ls_id), K(tablet_id));
    }
    if (OB_FAIL(ret) || !frozen_memtable_handle.is_valid()) {
      stat_.end_set_freeze_stat(ObFreezeState::FINISH, ObClockGenerator::getClock(), ret);
      print_freezer_statistics();
      unset_freeze_();
    }
  }
  return ret;
}

// Define a functor to avoid using lambda
struct FreezeTaskFunctor {
  ObFreezer *freezer_;
  const ObTabletID tablet_id_;
  const uint64_t epoch_;
  // hold ls handle to avoid logstream being destroyed
  ObLSHandle ls_handle_;
  FreezeTaskFunctor(ObFreezer *freezer, const ObTabletID tablet_id, const uint64_t epoch, ObLSHandle &ls_handle)
      : freezer_(freezer), tablet_id_(tablet_id), epoch_(epoch), ls_handle_(ls_handle) {}
  int operator()()
  {
    int ret = OB_SUCCESS;
    ObLS *ls = freezer_->ls_;
    STORAGE_LOG(INFO, "async tablet freeze task start", K(tablet_id_), K(freezer_->get_ls_id()));
    const int64_t RETRY_INTERVAL = 100 * 1000;  // 100 ms
    if (OB_ISNULL(ls)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected nullptr ", KR(ret), KP(this));
    } else if (OB_FAIL(ls->tablet_freeze_task_for_direct_load(tablet_id_, epoch_))) {
      // NOTICE : call function on logstream to avoid some concurrent situation, like ls offline
      if (REACH_TIME_INTERVAL(1LL * 1000LL * 1000LL /* 1 second */)) {
        LOG_INFO("fail to start tablet freeze. need retry", K(ret), K(tablet_id_));
      }

      // sleep for some time and commit another freeze task to avoid occuping this thread for a long time
      usleep(RETRY_INTERVAL);
      (void)freezer_->commit_async_tablet_freeze_task_once(tablet_id_, epoch_);
    }
    return ret;
  }
};

void ObFreezer::commit_async_tablet_freeze_task_once(const ObTabletID &tablet_id, const uint64_t epoch)
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
    do {
      ObLSHandle ls_handle;
      if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
        STORAGE_LOG(WARN, "get ls handle failed. stop async freeze task", KR(ret), K(ls_id));
        break;
      } else {
        FreezeTaskFunctor freeze_task_functor(this, tablet_id, epoch, ls_handle);
        ret = tenant_freezer->freeze_thread_pool_.commit_task_ignore_ret(freeze_task_functor);

        if (OB_FAIL(ret) && REACH_TIME_INTERVAL(10LL * 1000LL * 1000LL /* 10 second */)) {
          STORAGE_LOG(WARN, "[Freezer] commit async tablet freeze task failed", KR(ret), K(ls_id), K(tablet_id));
        }
      }
    } while (OB_FAIL(ret));
  }
}

/**
 * @brief This function is executed in TenantFreezer freeze_thread_pool as an async task and the freeze logic would be
 * retried until success or logstream offlined.
 *
 */
int ObFreezer::tablet_freeze_task_for_direct_load(const ObTabletID &tablet_id, ObFuture<int> *result)
{
  return freeze_normal_tablet_(tablet_id, result, true /*for_direct_load*/);
}

int ObFreezer::tablet_freeze_with_rewrite_meta(const ObTabletID &tablet_id, ObFuture<int> *result)
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObClockGenerator::getClock();
  const share::ObLSID ls_id = get_ls_id();
  FLOG_INFO("[Freezer] tablet_freeze_with_rewrite_meta start", K(ret), K(ls_id), K(tablet_id), KTIME(start_time));

  ObTabletFreezeGuard guard(*this);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "[Freezer] not inited", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_UNLIKELY(!enable_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("freezer is offline, can not freeze now", K(ret), K(ls_id));
  } else if (OB_FAIL(loop_set_freeze_flag())) {
    TRANS_LOG(WARN, "[Freezer] failed to set freeze_flag", K(ret), K(ls_id), K(tablet_id));
  } else {
    ObTableHandleV2 frozen_memtable_handle;
    const bool need_rewrite_meta = true;
    if (OB_FAIL(do_tablet_freeze_(need_rewrite_meta, start_time, tablet_id, result, frozen_memtable_handle))) {
      STORAGE_LOG(WARN, "tablet freeze failed", KR(ret), K(ls_id), K(tablet_id));
    }
    if (OB_FAIL(ret) || !frozen_memtable_handle.is_valid()) {
      stat_.end_set_freeze_stat(ObFreezeState::FINISH, ObClockGenerator::getClock(), ret);
      print_freezer_statistics();
      unset_freeze_();
    }
  }
  return ret;
}

int ObFreezer::do_tablet_freeze_(const bool need_rewrite_meta,
                                  const int64_t start_time,
                                  const ObTabletID &tablet_id,
                                  ObFuture<int> *result,
                                  ObTableHandleV2 &frozen_memtable_handle)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();
  SCN freeze_snapshot_version;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObProtectedMemtableMgrHandle *protected_handle = NULL;
  if (OB_FAIL(get_ls_weak_read_scn(freeze_snapshot_version))) {
    TRANS_LOG(WARN, "[Freezer] get ls weak read ts failure", K(ret), K(ls_id));
  } else if (ObScnRange::MAX_SCN == freeze_snapshot_version || ObScnRange::MIN_SCN >= freeze_snapshot_version) {
    ret = OB_MINOR_FREEZE_NOT_ALLOW;
    LOG_WARN("[Freezer] invalid weak read scn", K(ret), K(ls_id));
  } else if (FALSE_IT(freeze_snapshot_version_ = freeze_snapshot_version)) {
  } else if (FALSE_IT(set_need_resubmit_log(false))) {
  } else if (FALSE_IT(stat_.reset())) {
  } else if (FALSE_IT(stat_.begin_set_freeze_stat(get_freeze_clock(),
                                                  start_time,
                                                  ObFreezeState::NOT_SUBMIT_LOG,
                                                  freeze_snapshot_version,
                                                  tablet_id,
                                                  need_rewrite_meta))) {
    TRANS_LOG(WARN, "[Freezer] fail to begin_set_freeze_stat", K(ret), K(ls_id));
  } else if (OB_FAIL(get_ls_tablet_svr()->get_tablet(tablet_id,
                                                     tablet_handle,
                                                     ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT,
                                                     ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    TRANS_LOG(WARN, "[Freezer] fail to get tablet for freeze", K(ret), K(ls_id), K(tablet_id));
    stat_.add_diagnose_info("fail to get tablet");
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (OB_FAIL(tablet->get_protected_memtable_mgr_handle(protected_handle))) {
    LOG_WARN("failed to get_protected_memtable_mgr_handle", K(ret), KPC(tablet));
#ifdef ERRSIM
  } else if (OB_FAIL(ret = ERRSIM_FREEZER_FREEZE_FAILURE)) {
    LOG_WARN("[Freezer] errsim failure during freezer freeze", K(ret));
#endif
  } else if (OB_FAIL(protected_handle->set_is_tablet_freeze_for_active_memtable(frozen_memtable_handle))) {
    (void)handle_set_tablet_freeze_failed(need_rewrite_meta, tablet_id, ls_id, tablet, freeze_snapshot_version, ret);
  } else {
    // NB: we donnot allow failure after set tablet freeze!!!

    if (FALSE_IT(submit_log_if_needed_for_tablet_freeze_(frozen_memtable_handle))) {
      TRANS_LOG(WARN, "submit log if needed failed", KR(ret), K(frozen_memtable_handle));
    } else if (FALSE_IT(submit_freeze_task_(false /*is_ls_freeze*/, result, frozen_memtable_handle))) {
    } else {
      TRANS_LOG(INFO,
                "[Freezer] succeed to start tablet freeze task",
                K(ret),
                K(ls_id),
                K(tablet_id),
                K(need_rewrite_meta),
                K(frozen_memtable_handle));
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

int ObFreezer::handle_no_active_memtable_(const ObTabletID &tablet_id,
                                          const ObTablet *tablet,
                                          const SCN freeze_snapshot_version)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();
  ObProtectedMemtableMgrHandle *protected_handle = NULL;
  if (OB_FAIL(tablet->get_protected_memtable_mgr_handle(protected_handle))) {
    LOG_WARN("failed to get_protected_memtable_mgr_handle", K(ret), KPC(tablet));
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

void ObFreezer::submit_log_if_needed_for_tablet_freeze_(ObTableHandleV2 &handle)
{
  int ret = OB_SUCCESS;
  ObITabletMemtable *tablet_memtable = nullptr;

  if (OB_FAIL(handle.get_tablet_memtable(tablet_memtable))) {
    TRANS_LOG(WARN, "get tablet memtable failed", KR(ret), K(handle));
  } else if (OB_ISNULL(tablet_memtable)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "tablet memtable is unexpected null", KR(ret), K(handle));
  } else if (tablet_memtable->is_direct_load_memtable()) {
    // skip submit log cause direct load memtable unneeded
  } else if (tablet_memtable->is_data_memtable()) {
    (void)try_submit_log_for_freeze_(true /* is_tablet_freeze */);
  }

  if (OB_FAIL(ret)) {
    set_need_resubmit_log(true);
  }
}

int ObFreezer::tablet_freeze_task_(ObTableHandleV2 handle)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();
  ObITabletMemtable *tablet_memtable = nullptr;

  if (!handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "memtable cannot be null", K(ret), K(ls_id));
  } else if (OB_FAIL(handle.get_tablet_memtable(tablet_memtable))) {
    LOG_WARN("fail to get memtable", K(ret), K(handle));
  } else if (OB_ISNULL(tablet_memtable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get memtable", K(ret), K(handle));
  } else {
    if (tablet_memtable->is_data_memtable()) {
      ret = do_data_memtable_tablet_freeze_(tablet_memtable);
    } else if (tablet_memtable->is_direct_load_memtable()) {
      ret = do_direct_load_memtable_tablet_freeze_(tablet_memtable);
    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected memtable type", KR(ret), KPC(tablet_memtable));
    }
    stat_.end_set_freeze_stat(ObFreezeState::FINISH, ObClockGenerator::getClock(), ret);
    print_freezer_statistics();
    unset_freeze_();
  }

  return ret;
}

int ObFreezer::do_data_memtable_tablet_freeze_(ObITabletMemtable *tablet_memtable)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();
  ObMemtable *memtable = static_cast<ObMemtable*>(tablet_memtable);
  if (OB_FAIL(wait_memtable_ready_for_flush_with_ls_lock(memtable))) {
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
      FLOG_INFO("[Freezer] tablet_freeze_task success", K(ret), K(ls_id), KPC(memtable));
    }
  }
  return ret;
}

int ObFreezer::do_direct_load_memtable_tablet_freeze_(ObITabletMemtable *tablet_memtable)
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
      FLOG_INFO("[Freezer] freeze direct load memtable success", K(ls_id), KP(this), KP(tablet_memtable));
    }
  }
  return ret;
}

int ObFreezer::wait_memtable_ready_for_flush_with_ls_lock(ObITabletMemtable *tablet_memtable)
{
  share::ObLSID ls_id = get_ls_id();
  const int64_t start = ObTimeUtility::current_time();
  int64_t last_submit_log_time = start;
  int ret = OB_SUCCESS;
  bool ready_for_flush = false;

  do {
    if (OB_FAIL(try_wait_memtable_ready_for_flush_with_ls_lock(tablet_memtable,
                                                               ready_for_flush,
                                                               start,
                                                               last_submit_log_time))) {
      TRANS_LOG(WARN, "[Freezer] memtable is not ready_for_flush", K(ret));
    }
  } while (OB_SUCC(ret) && !ready_for_flush);

  return ret;
}

int ObFreezer::try_wait_memtable_ready_for_flush_with_ls_lock(ObITabletMemtable *tablet_memtable,
                                                              bool &ready_for_flush,
                                                              const int64_t start,
                                                              int64_t &last_submit_log_time)
{
  int ret = OB_SUCCESS;
  int64_t read_lock = LSLOCKALL;
  int64_t write_lock = 0;
  ObLSLockGuard lock_ls(ls_, ls_->lock_, read_lock, write_lock);

  if (OB_FAIL(check_ls_state())) {
  } else if (OB_ISNULL(tablet_memtable)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "[Freezer] memtable cannot be null", K(ret));
  } else if (FALSE_IT(ready_for_flush = tablet_memtable->ready_for_flush())) {
  } else if (!ready_for_flush) {
    if (TC_REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      if (need_resubmit_log() ||
          // In order to prevent the txn has already passed the try_submit test
          // while failing to submit some logs due to an unexpected bug, we need
          // retry to submit the log to go around the above case
          (ObTimeUtility::current_time() - last_submit_log_time >= 1_min)) {
        last_submit_log_time = ObTimeUtility::current_time();
        submit_log_for_freeze(true/*tablet freeze*/, false/*try*/);
        TRANS_LOG(INFO, "[Freezer] resubmit log", K(ret));
      }
      const int64_t cost_time = ObTimeUtility::current_time() - start;

      if (cost_time > 5 * 1000 * 1000) {
        TRANS_LOG(WARN, "[Freezer] ready_for_flush costs too much time",
                  K(cost_time), KPC(tablet_memtable));
        stat_.add_diagnose_info("ready_for_flush costs too much time");
      }
    }

    ob_usleep(100);
  }

  return ret;
}

int ObFreezer::batch_tablet_freeze(const int64_t trace_id, const ObIArray<ObTabletID> &tablet_ids, ObFuture<int> *result)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();
  SCN freeze_snapshot_version;
  FLOG_INFO("[Freezer] batch_tablet_freeze start", K(ls_id), K(tablet_ids));
  int64_t start_time = ObTimeUtility::current_time();
  bool need_freeze = false;

  ObTabletFreezeGuard guard(*this, true /* try guard */);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "[Freezer] not inited", K(ret), K(ls_id), K(tablet_ids));
  } else if (OB_UNLIKELY(!enable_)) {
    LOG_WARN("freezer is offline, can not freeze now", K(ret), K(ls_id));
  } else if (OB_FAIL(get_ls_weak_read_scn(freeze_snapshot_version))) {
    TRANS_LOG(WARN, "[Freezer] get ls weak read scn failure", K(ret), K(ls_id));
  } else if (ObScnRange::MAX_SCN == freeze_snapshot_version
             || ObScnRange::MIN_SCN >= freeze_snapshot_version) {
    ret = OB_MINOR_FREEZE_NOT_ALLOW;
    LOG_WARN("[Freezer] invalid weak read scn", K(ret), K(ls_id));
  } else if (OB_FAIL(guard.try_set_tablet_freeze_begin())) {
    // no need freeze now, a ls freeze is running or will be running
    ret = OB_SUCCESS;
    FLOG_INFO("[Freezer] ls freeze is running, no need freeze again", K(ret), K(ls_id), K(tablet_ids));
  } else if (OB_FAIL(loop_set_freeze_flag())) {
    ret = OB_SUCCESS;
    FLOG_INFO("[Freezer] freeze is running", K(ret), K(ls_id), K(tablet_ids));
  } else {
    // succeed to set freeze flag
    if (FALSE_IT(freeze_snapshot_version_ = freeze_snapshot_version)) {
    } else if (FALSE_IT(set_need_resubmit_log(false))) {
    } else if (FALSE_IT(stat_.reset())) {
    } else if (FALSE_IT(stat_.begin_set_freeze_stat(get_freeze_clock(),
                                                    start_time,
                                                    ObFreezeState::NOT_SUBMIT_LOG,
                                                    freeze_snapshot_version,
                                                    ObTabletID(ObTabletID::INVALID_TABLET_ID),
                                                    false/*need_rewrite_meta*/))) {
      TRANS_LOG(WARN, "[Freezer] fail to begin_set_freeze_stat", K(ret), K(ls_id));
    } else if (OB_FAIL(batch_tablet_freeze_(trace_id, tablet_ids, result, need_freeze))) {
      TRANS_LOG(WARN, "[Freezer] batch_tablet_freeze failed", K(ret), K(ls_id), K(tablet_ids));
    }
    if (!need_freeze) {
      stat_.end_set_freeze_stat(ObFreezeState::FINISH, ObTimeUtility::current_time(), ret);
      print_freezer_statistics();
      unset_freeze_();
    }
  }

  return ret;
}

int ObFreezer::batch_tablet_freeze_(const int64_t trace_id,
                                    const ObIArray<ObTabletID> &tablet_ids,
                                    ObFuture<int> *result,
                                    bool &need_freeze)
{
  const int64_t start = ObTimeUtility::current_time();
  // succeed to set freeze flag
  int ret = OB_SUCCESS;
  ObTableHandleArray memtable_handles;
  need_freeze = false;
  ObProtectedMemtableMgrHandle *protected_handle = NULL;

  if (OB_FAIL(memtable_handles.reserve(tablet_ids.count()))) {
    TRANS_LOG(WARN, "fail to reserve memtable handles", K(ret), K(tablet_ids));
  } else {
    for (int i = 0; i < tablet_ids.count() && OB_SUCC(ret); ++i) {
      const ObTabletID &tablet_id = tablet_ids.at(i);
      ObTabletHandle handle;
      ObTablet *tablet = nullptr;
      ObTableHandleV2 frozen_memtable_handle;
      if (OB_FAIL(get_ls_tablet_svr()->get_tablet(tablet_id,
                                                  handle,
                                                  ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT,
                                                  ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
        TRANS_LOG(WARN, "[Freezer] fail to get tablet", K(ret), K(tablet_id));
        stat_.add_diagnose_info("fail to get tablet");
      } else if (FALSE_IT(tablet = handle.get_obj())) {
      } else if (OB_FAIL(tablet->get_protected_memtable_mgr_handle(protected_handle))) {
        LOG_WARN("[Freezer] failed to get_protected_memtable_mgr_handle", K(ret), KPC(tablet));
#ifdef ERRSIM
      } else if (memtable_handles.count() >= 1
                 && OB_FAIL(ret = ERRSIM_BATCH_TABLET_FREEZE_FAILURE)) {
        LOG_WARN("[Freezer] errsim failure during freezer freeze", K(ret));
#endif
      } else if (OB_FAIL(protected_handle->set_is_tablet_freeze_for_active_memtable(frozen_memtable_handle, trace_id))) {
        if (ret == OB_ENTRY_NOT_EXIST) {
          ret = OB_SUCCESS;
          TRANS_LOG(INFO, "[Freezer] no need to freeze since there is no active memtable", K(ret),
                    K(tablet_id));
          stat_.add_diagnose_info("no need to freeze since there is no active memtable");
        } else {
          TRANS_LOG(WARN, "[Freezer] fail to set is_tablet_freeze", K(ret), K(tablet_id));
          stat_.add_diagnose_info("fail to set is_tablet_freeze");
        }
      } else if (!frozen_memtable_handle.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "[Freezer] frozen_memtable_handle is invalid", K(ret), K(tablet_id));
        stat_.add_diagnose_info("frozen_memtable_handle is invalid");
      } else {
        // NB: we donnot allow failure after each set tablet freeze!!!

        while (OB_FAIL(memtable_handles.push_back(frozen_memtable_handle))) {
          TRANS_LOG(ERROR, "[Freezer] fail to push_back", K(ret), K(tablet_id));
          stat_.add_diagnose_info("fail to push_back");
          usleep(100 * 1000); // sleep 100ms
        }
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
  if (0 == memtable_handles.count()) {
    need_freeze = false;
    TRANS_LOG(INFO, "[Freezer] no need to freeze batch tablets", K(ret), K(tablet_ids));
    stat_.add_diagnose_info("no need to freeze batch tablets");
  } else {
    // NB: we donnot allow failure after set tablet freeze for each successful tablet!!!
    need_freeze = true;

    if (FALSE_IT(try_submit_log_for_freeze_(true/*tablet freeze*/))) {
    } else if (FALSE_IT(submit_batch_tablet_freeze_task(memtable_handles, result))) {
      TRANS_LOG(WARN, "[Freezer] fail to submit batch_tablet_freeze task", K(ret));
    } else {
      TRANS_LOG(INFO, "[Freezer] succeed to start batch_tablet_freeze task", K(ret),
                K(get_ls_id()), K(tablet_ids), K(memtable_handles));
    }
  }

  return ret;
}

void ObFreezer::submit_batch_tablet_freeze_task(const ObTableHandleArray &tables_array,
                                                ObFuture<int> *result)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();
  const int64_t start = ObTimeUtility::current_time();

  ObTenantFreezer *tenant_freezer = MTL(storage::ObTenantFreezer*);

  ObSpinLockGuard freeze_thread_pool(tenant_freezer->freeze_thread_pool_lock_);
  do {
    struct TmpFreezerFunc {
      int operator() () {
        return freezer_->batch_tablet_freeze_task(tables_array_);
      }
      TmpFreezerFunc(ObFreezer *freezer, const ObTableHandleArray &tables_array)
        : freezer_(freezer), tables_array_(tables_array) {
      }
      ObFreezer *freezer_;
      ObTableHandleArray tables_array_;
    } tmp_freezer_func(this, tables_array);
    if (OB_ISNULL(result)) {
      ret = tenant_freezer->freeze_thread_pool_.commit_task_ignore_ret(
        tmp_freezer_func);
    } else {
      ret = tenant_freezer->freeze_thread_pool_.commit_task(
        *result, tmp_freezer_func);
    }

    if (OB_FAIL(ret)) {
      const int64_t cost_time = ObTimeUtility::current_time() - start;
      if (cost_time > 100 * 1000) {
        if (TC_REACH_TIME_INTERVAL(100 * 1000)) {
          TRANS_LOG(WARN, "[Freezer] failed to start freeze_task", K(ret), K(ls_id));
        }
      }
    }
  } while (OB_FAIL(ret));
}

int ObFreezer::batch_tablet_freeze_task(ObTableHandleArray tables_array)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();
  const int64_t start = ObTimeUtility::current_time();
  int64_t last_submit_log_time = start;
  int64_t try_times = 0;

  // We must wait until finish without error
  while (tables_array.count() > 0) {
    try_times++;
    ret = OB_SUCCESS;
    for (int i = 0; OB_SUCC(ret) && i < tables_array.count(); ++i) {
      ObTableHandleV2 &handle = tables_array.at(i);
      ObITabletMemtable *tablet_memtable = nullptr;
      bool ready_for_flush = false;
      if (!handle.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "memtable cannot be null", K(ret), K(ls_id));
      } else if (OB_FAIL(handle.get_tablet_memtable(tablet_memtable))) {
        LOG_WARN("fail to get memtable", K(ret));
      } else if (OB_FAIL(try_wait_memtable_ready_for_flush_with_ls_lock(tablet_memtable,
                                                                        ready_for_flush,
                                                                        start,
                                                                        last_submit_log_time))) {
        TRANS_LOG(WARN, "[Freezer] fail to wait memtable ready_for_flush", K(ret), K(ls_id));
      } else if (!ready_for_flush) {
      } else if (OB_FAIL(finish_freeze_with_ls_lock(tablet_memtable))) {
        TRANS_LOG(WARN, "[Freezer] fail to finish_freeze", K(ret), K(ls_id), KPC(tablet_memtable));
      } else if (OB_FAIL(tables_array.pop_back(handle))) {
        TRANS_LOG(WARN, "[Freezer] fail to pop_back", K(ret), K(ls_id), K(handle), KPC(tablet_memtable));
      }
    }
  }

  TRANS_LOG(INFO, "[Freezer] batch_tablet_freeze success", K(ret), K(ls_id), K(try_times));

  stat_.end_set_freeze_stat(ObFreezeState::FINISH, ObTimeUtility::current_time(), ret);
  print_freezer_statistics();
  unset_freeze_();

  return ret;
}

int ObFreezer::finish_freeze_with_ls_lock(ObITabletMemtable *tablet_memtable)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();
  int64_t read_lock = LSLOCKALL;
  int64_t write_lock = 0;
  ObLSLockGuard lock_ls(ls_, ls_->lock_, read_lock, write_lock);

  if (OB_FAIL(check_ls_state())) {
  } else if (OB_ISNULL(tablet_memtable)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "[Freezer] memtable cannot be null", K(ret));
  } else if (OB_FAIL(tablet_memtable->finish_freeze())) {
    TRANS_LOG(ERROR, "[Freezer] memtable cannot be flushed",
              K(ret), K(ls_id), KPC(tablet_memtable));
    stat_.add_diagnose_info("memtable cannot be flushed");
  }

  return ret;
}

// NOTICE: not called by user now
/* private subfunctions for freeze process */
int ObFreezer::handle_memtable_for_tablet_freeze(ObIMemtable *imemtable)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();

  if (OB_ISNULL(imemtable)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObITabletMemtable *tablet_memtable = static_cast<ObITabletMemtable*>(imemtable);
    try_submit_log_for_freeze_(true/*tablet freeze*/);
    wait_memtable_ready_for_flush(tablet_memtable);
    if (OB_FAIL(tablet_memtable->finish_freeze())) {
      TRANS_LOG(ERROR, "[Freezer] memtable cannot be flushed",
                K(ret), K(ls_id), KPC(tablet_memtable));
      stat_.add_diagnose_info("memtable cannot be flushed");
    } else {
      TRANS_LOG(INFO, "[Freezer] memtable is ready to be flushed",
                K(ret), K(ls_id), KPC(tablet_memtable));
    }
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
  const int64_t start = ObTimeUtility::current_time();
  ObTabletID tablet_id(INT64_MAX); // used for diagnose
  bool trace_id_need_reset = false;
  if (!ObCurTraceId::get_trace_id()->is_valid()) {
    ObCurTraceId::init(GCONF.self_addr_);
    bool trace_id_need_reset = true;
  }
  do {
    ret = OB_SUCCESS;
    transaction::ObTransID fail_tx_id;
    // because tablet freeze will not inc freeze clock, fake the freeze clock
    const uint32_t freeze_clock = is_tablet_freeze ? get_freeze_clock() + 1 : get_freeze_clock();
    if (OB_FAIL(get_ls_tx_svr()->traverse_trans_to_submit_redo_log(fail_tx_id, freeze_clock))) {
      const int64_t cost_time = ObTimeUtility::current_time() - start;
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
  } while (!(is_try && (ObTimeUtility::current_time() - start > 10 * 1_s))
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

void ObFreezer::submit_freeze_task_(const bool is_ls_freeze,
                                    ObFuture<int> *result,
                                    ObTableHandleV2 &memtable_handle)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();
  const int64_t start = ObTimeUtility::current_time();

  ObTenantFreezer *tenant_freezer = MTL(storage::ObTenantFreezer*);

  ObSpinLockGuard freeze_thread_pool(tenant_freezer->freeze_thread_pool_lock_);
  do {
    if (OB_ISNULL(result)) {
      if (is_ls_freeze) {
        struct TmpFreezerFunc {
          int operator() () {
            return freezer_->ls_freeze_task_();
          }
          TmpFreezerFunc(ObFreezer *freezer) : freezer_(freezer) {
          }
          ObFreezer *freezer_;
        } tmp_freezer_func(this);
        ret = tenant_freezer->freeze_thread_pool_.commit_task_ignore_ret(tmp_freezer_func);
      } else {
        struct TmpFreezerFunc {
          int operator() () {
            return freezer_->tablet_freeze_task_(memtable_handle_);
          }
          TmpFreezerFunc(ObFreezer *freezer, ObTableHandleV2 memtable_handle) : freezer_(freezer), memtable_handle_(memtable_handle) {
          }
          ObFreezer *freezer_;
          ObTableHandleV2 memtable_handle_;
        } tmp_freezer_func(this, memtable_handle);
        ret = tenant_freezer->freeze_thread_pool_.commit_task_ignore_ret(tmp_freezer_func);
      }
    } else {
      if (is_ls_freeze) {
        struct TmpFreezerFunc {
          int operator() () {
            return freezer_->ls_freeze_task_();
          }
          TmpFreezerFunc(ObFreezer *freezer) : freezer_(freezer) {
          }
          ObFreezer *freezer_;
        } tmp_freezer_func(this);
        ret = tenant_freezer->freeze_thread_pool_.commit_task(*result, tmp_freezer_func);
      } else {
        struct TmpFreezerFunc {
          int operator() () {
            return freezer_->tablet_freeze_task_(memtable_handle_);
          }
          TmpFreezerFunc(ObFreezer *freezer, ObTableHandleV2 memtable_handle) : freezer_(freezer), memtable_handle_(memtable_handle) {
          }
          ObFreezer *freezer_;
          ObTableHandleV2 memtable_handle_;
        } tmp_freezer_func(this, memtable_handle);
        ret = tenant_freezer->freeze_thread_pool_.commit_task(*result, tmp_freezer_func);
      }
    }

    if (OB_FAIL(ret)) {
      const int64_t cost_time = ObTimeUtility::current_time() - start;
      if (cost_time > 100 * 1000) {
        if (TC_REACH_TIME_INTERVAL(100 * 1000)) {
          TRANS_LOG(WARN, "[Freezer] failed to start freeze_task", K(ret), K(ls_id), K(is_ls_freeze));
        }
      }
    }
  } while (OB_FAIL(ret));
}

int ObFreezer::wait_freeze_finished(ObFuture<int> &result)
{
  int ret = OB_SUCCESS;

  if (result.is_valid()) {
    share::ObLSID ls_id = get_ls_id();
    int *ret_code = nullptr;
    const int64_t start = ObTimeUtility::current_time();
    result.get(ret_code);
    const int64_t cost_time = ObTimeUtility::current_time() - start;
    ret = OB_ISNULL(ret_code) ? OB_ERR_UNEXPECTED : *ret_code;

    if (OB_FAIL(ret)) {
      TRANS_LOG(WARN, "failed to freeze", K(ret), K(cost_time), K(ls_id));
    } else if (cost_time > 3 * 1000 * 1000) {
      TRANS_LOG(WARN, "waiting ready_for_flush costs too much time", K(ret), K(cost_time), K(ls_id));
    }
  }

  return ret;
}

void ObFreezer::wait_memtable_ready_for_flush(ObITabletMemtable *tablet_memtable)
{
  share::ObLSID ls_id = get_ls_id();
  const int64_t start = ObTimeUtility::current_time();
  int64_t last_submit_log_time = start;
  int ret = OB_SUCCESS;

  while (!tablet_memtable->ready_for_flush()) {
    if (TC_REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      if (need_resubmit_log() ||
          // In order to prevent the txn has already passed the try_submit test
          // while failing to submit some logs due to an unexpected bug, we need
          // retry to submit the log to go around the above case
          (ObTimeUtility::current_time() - last_submit_log_time >= 1_min)) {
        last_submit_log_time = ObTimeUtility::current_time();
        submit_log_for_freeze(true/*tablet freeze*/, false/*try*/);
        TRANS_LOG(INFO, "[Freezer] resubmit log for tablet_freeze", K(ls_id));
      }
      const int64_t cost_time = ObTimeUtility::current_time() - start;

      if (cost_time > 5 * 1000 * 1000) {
        TRANS_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "[Freezer] ready_for_flush costs too much time",
                      K(ls_id), K(cost_time), KPC(tablet_memtable));
        stat_.add_diagnose_info("ready_for_flush costs too much time");
        tablet_memtable->print_ready_for_flush();
      }
    }
    ob_usleep(100);
  }
}


/* private subfunctions about freeze flag*/
int ObFreezer::loop_set_freeze_flag()
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();
  const int64_t start = ObClockGenerator::getClock();

  do {
    ret = OB_SUCCESS;
    if (OB_FAIL(set_freeze_flag_without_inc_freeze_clock())) {
      const int64_t MAX_LOOP_TIME = 10LL * 1000LL * 1000LL;  // 10 seconds
      const int64_t cost_time = ObClockGenerator::getClock() - start;
      if (cost_time > MAX_LOOP_TIME) {
        TRANS_LOG(WARN, "[Freezer] wait the running freeze too long time", K(ls_id), K(cost_time));
        break;
      }
      ob_usleep(100);
    }
  } while (OB_FAIL(ret));

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

void ObFreezer::undo_freeze_()
{
  uint32_t old_v;
  uint32_t new_v;

  // Step1: unset freeze_snapshot_version to invalid value
  freeze_snapshot_version_.reset();

  // Step2: unset max_decided_scn to invalid value
  max_decided_scn_.reset();

  // Step3: unset need_resubmit_log_
  set_need_resubmit_log(false);

  // Step4: unset freeze_flag and dec freeze_clock
  // used when freeze fails
  do {
    old_v = ATOMIC_LOAD(&freeze_flag_);
    new_v = (old_v - 1) & (~(1 << 31));
  } while (ATOMIC_CAS(&freeze_flag_, old_v, new_v) != old_v);
}

/* public function about freeze_flag */
bool ObFreezer::is_freeze(uint32_t freeze_flag) const
{
  if (freeze_flag == UINT32_MAX) {
    freeze_flag = (ATOMIC_LOAD(&freeze_flag_));
  }
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

void ObFreezer::set_tablet_freeze_end_()
{
  ATOMIC_DEC(&low_priority_freeze_cnt_);
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

void ObFreezer::set_ls_freeze_end_()
{
  ATOMIC_DEC(&high_priority_freeze_cnt_);
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

ObFreezer::PendTenantReplayGuard::PendTenantReplayGuard()
{
  int ret = OB_SUCCESS;
  common::ObSharedGuard<ObLSIterator> iter;
  ObLSService *ls_srv = MTL(ObLSService *);
  if (OB_FAIL(ls_srv->get_ls_iter(iter, ObLSGetMod::STORAGE_MOD))) {
    STORAGE_LOG(WARN, "[ObFreezer] fail to get ls iterator", KR(ret));
  } else {
    ObLS *ls = nullptr;
    while (OB_SUCC(iter->get_next(ls))) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ls->get_freezer()->pend_ls_replay())) {
        STORAGE_LOG(WARN, "[ObFreezer] pend replay failed", KR(ret), KPC(ls));
      }
    }
  }
}

ObFreezer::PendTenantReplayGuard::~PendTenantReplayGuard()
{
  int ret = OB_SUCCESS;
  common::ObSharedGuard<ObLSIterator> iter;
  ObLSService *ls_srv = MTL(ObLSService *);
  if (OB_FAIL(ls_srv->get_ls_iter(iter, ObLSGetMod::STORAGE_MOD))) {
    STORAGE_LOG(WARN, "[ObFreezer] fail to get ls iterator", KR(ret));
  } else {
    ObLS *ls = nullptr;
    while (OB_SUCC(iter->get_next(ls))) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ls->get_freezer()->restore_ls_replay())) {
        STORAGE_LOG(WARN, "[ObFreezer] restore replay failed", KR(ret), KPC(ls));
      }
    }
  }
}

} // namespace storage
} // namespace oceanbase
