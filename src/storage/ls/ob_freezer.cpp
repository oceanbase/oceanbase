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
#include "storage/ls/ob_freezer.h"
#include "share/ob_force_print_log.h"
#include "storage/ls/ob_ls_tx_service.h"
#include "storage/checkpoint/ob_data_checkpoint.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/compaction/ob_compaction_diagnose.h"
#include "logservice/ob_log_service.h"

namespace oceanbase
{
using namespace logservice;
using namespace compaction;
namespace storage
{
ObFrozenMemtableInfo::ObFrozenMemtableInfo()
  : tablet_id_(),
    start_scn_(share::ObScnRange::MIN_SCN),
    end_scn_(share::ObScnRange::MIN_SCN),
    write_ref_cnt_(0),
    unsubmitted_cnt_(0),
    unsynced_cnt_(0),
    current_right_boundary_(0)
{}

ObFrozenMemtableInfo::ObFrozenMemtableInfo(const ObTabletID &tablet_id,
                                           const palf::SCN &start_scn,
                                           const palf::SCN &end_scn,
                                           const int64_t write_ref_cnt,
                                           const int64_t unsubmitted_cnt,
                                           const int64_t unsynced_cnt,
                                           const int64_t current_right_boundary)
 : tablet_id_(tablet_id),
   start_scn_(start_scn),
   end_scn_(end_scn),
   write_ref_cnt_(write_ref_cnt),
   unsubmitted_cnt_(unsubmitted_cnt),
   unsynced_cnt_(unsynced_cnt),
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
  unsynced_cnt_ = 0;
  current_right_boundary_ = 0;
}

void ObFrozenMemtableInfo::set(const ObTabletID &tablet_id,
                               const palf::SCN &start_scn,
                               const palf::SCN &end_scn,
                               const int64_t write_ref_cnt,
                               const int64_t unsubmitted_cnt,
                               const int64_t unsynced_cnt,
                               const int64_t current_right_boundary)
{
  tablet_id_ = tablet_id;
  start_scn_ = start_scn;
  end_scn_ = end_scn;
  write_ref_cnt_ = write_ref_cnt;
  unsubmitted_cnt_ = unsubmitted_cnt;
  unsynced_cnt_ = unsynced_cnt;
  current_right_boundary_ = current_right_boundary;
}

bool ObFrozenMemtableInfo::is_valid()
{
  return tablet_id_.is_valid() && start_scn_ > share::ObScnRange::MIN_SCN && end_scn_ > share::ObScnRange::MIN_SCN;
}

ObFreezerStat::ObFreezerStat()
  : tablet_id_(),
    is_force_(false),
    state_(ObFreezeState::INVALID),
    start_time_(0),
    end_time_(INT64_MAX),
    ret_code_(-1),
    diagnose_info_(),
    memtables_info_(),
    memtables_info_lock_(),
    diagnose_info_lock_()
{}

ObFreezerStat::~ObFreezerStat()
{
  reset();
}

void ObFreezerStat::reset()
{
  ObSpinLockGuard memtables_info_guard(memtables_info_lock_);
  ObSpinLockGuard diagnose_info_guard(diagnose_info_lock_);
  tablet_id_.reset();
  is_force_ = false;
  state_ = ObFreezeState::INVALID;
  start_time_ = 0;
  end_time_ = INT64_MAX;
  ret_code_ = -1;
  diagnose_info_.reset();
  memtables_info_.reset();
}

bool ObFreezerStat::is_valid()
{
  return start_time_ > 0 && state_ > ObFreezeState::INVALID;
}

int ObFreezerStat::add_memtable_info(const ObTabletID &tablet_id,
                                     const palf::SCN &start_scn,
                                     const palf::SCN &end_scn,
                                     const int64_t write_ref_cnt,
                                     const int64_t unsubmitted_cnt,
                                     const int64_t unsynced_cnt,
                                     const int64_t current_right_boundary)
{
  int ret = OB_SUCCESS;

  ObSpinLockGuard guard(memtables_info_lock_);
  if (memtables_info_.count() < FROZEN_MEMTABLE_INFO_CNT) {
    ObFrozenMemtableInfo memtable_info(tablet_id,
                                       start_scn,
                                       end_scn,
                                       write_ref_cnt,
                                       unsubmitted_cnt,
                                       unsynced_cnt,
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

  ObSpinLockGuard guard(memtables_info_lock_);
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

  ObSpinLockGuard guard(memtables_info_lock_);
  if (OB_FAIL(memtables_info.assign(memtables_info_))) {
    TRANS_LOG(WARN, "fail to assign", K(ret));
  }

  return ret;
}

void ObFreezerStat::add_diagnose_info(const ObString &str)
{
  ObSpinLockGuard guard(diagnose_info_lock_);
  diagnose_info_.assign(str);
}

ObFreezer::ObFreezer()
  : freeze_flag_(0),
    freeze_snapshot_version_(),
    max_decided_scn_(),
    ls_wrs_handler_(nullptr),
    ls_tx_svr_(nullptr),
    ls_tablet_svr_(nullptr),
    data_checkpoint_(nullptr),
    loghandler_(nullptr),
    ls_id_(),
    stat_(),
    empty_memtable_cnt_(0),
    is_inited_(false)
{}

ObFreezer::ObFreezer(ObLSWRSHandler *ls_loop_worker,
                     ObLSTxService *ls_tx_svr,
                     ObLSTabletService *ls_tablet_svr,
                     checkpoint::ObDataCheckpoint *data_checkpoint,
                     ObILogHandler *ob_loghandler,
                     const share::ObLSID &ls_id)
  : freeze_flag_(0),
    freeze_snapshot_version_(),
    max_decided_scn_(),
    ls_wrs_handler_(ls_loop_worker),
    ls_tx_svr_(ls_tx_svr),
    ls_tablet_svr_(ls_tablet_svr),
    data_checkpoint_(data_checkpoint),
    loghandler_(ob_loghandler),
    ls_id_(ls_id),
    stat_(),
    empty_memtable_cnt_(0),
    is_inited_(false)
{}

ObFreezer::~ObFreezer()
{
  reset();
}

void ObFreezer::set(ObLSWRSHandler *ls_loop_worker,
                    ObLSTxService *ls_tx_svr,
                    ObLSTabletService *ls_tablet_svr,
                    checkpoint::ObDataCheckpoint *data_checkpoint,
                    ObILogHandler *ob_loghandler,
                    const share::ObLSID &ls_id,
                    uint32_t freeze_flag)
{
  freeze_flag_ = freeze_flag;
  freeze_snapshot_version_.reset();
  max_decided_scn_.reset();
  ls_wrs_handler_ = ls_loop_worker;
  ls_tx_svr_ = ls_tx_svr;
  ls_tablet_svr_ = ls_tablet_svr;
  data_checkpoint_ = data_checkpoint;
  loghandler_ = ob_loghandler;
  ls_id_ = ls_id;
  stat_.reset();
  empty_memtable_cnt_ = 0;
}

void ObFreezer::reset()
{
  freeze_flag_ = 0;
  freeze_snapshot_version_.reset();
  max_decided_scn_.reset();
  ls_wrs_handler_ = nullptr;
  ls_tx_svr_ = nullptr;
  data_checkpoint_ = nullptr;
  ls_tablet_svr_ = nullptr;
  loghandler_ = nullptr;
  ls_id_.reset();
  stat_.reset();
  empty_memtable_cnt_ = 0;
  is_inited_ = false;
}

int ObFreezer::init(ObLSWRSHandler *ls_loop_worker,
                    ObLSTxService *ls_tx_svr,
                    ObLSTabletService *ls_tablet_svr,
                    checkpoint::ObDataCheckpoint *data_checkpoint,
                    ObILogHandler *log_handler,
                    const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ls_loop_worker) ||
      OB_ISNULL(ls_tx_svr) ||
      OB_ISNULL(ls_tablet_svr) ||
      OB_ISNULL(data_checkpoint) ||
      OB_ISNULL(log_handler) ||
      !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "[Freezer] invalid argument", K(ret), K(ls_loop_worker), K(ls_tx_svr),
              K(ls_tablet_svr), K(data_checkpoint), K(log_handler), K(ls_id));
  } else {
    set(ls_loop_worker, ls_tx_svr, ls_tablet_svr, data_checkpoint, log_handler, ls_id);
    is_inited_ = true;
  }
  return ret;
}

/* logstream freeze */
int ObFreezer::logstream_freeze()
{
  int ret = OB_SUCCESS;
  palf::SCN freeze_snapshot_version;
  palf::SCN max_decided_scn;
  FLOG_INFO("[Freezer] logstream_freeze start", K(ret), K_(ls_id));
  stat_.reset();
  stat_.start_time_ = ObTimeUtility::current_time();
  stat_.state_ = ObFreezeState::NOT_SET_FREEZE_FLAG;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[Freezer] not inited", K(ret), K_(ls_id));
  } else if (OB_FAIL(decide_max_decided_scn(max_decided_scn))) {
    TRANS_LOG(WARN, "[Freezer] decide max decided log ts failure", K(ret), K_(ls_id));
  } else if (OB_FAIL(get_ls_weak_read_scn(freeze_snapshot_version))) {
    TRANS_LOG(WARN, "[Freezer] get ls weak read ts failure", K(ret), K_(ls_id));
  } else if (ObScnRange::MAX_SCN == freeze_snapshot_version
             || ObScnRange::MIN_SCN >= freeze_snapshot_version) {
    ret = OB_NOT_INIT;
    LOG_WARN("[Freezer] weak read service not inited", K(ret), K_(ls_id), K(freeze_snapshot_version));
  } else if (OB_FAIL(set_freeze_flag())) {
    FLOG_INFO("[Freezer] freeze is running", K(ret), K_(ls_id));
  } else if (FALSE_IT(max_decided_scn_ = max_decided_scn)) {
  } else if (FALSE_IT(freeze_snapshot_version_ = freeze_snapshot_version)) {
  } else if (FALSE_IT(stat_.state_ = ObFreezeState::NOT_SUBMIT_LOG)) {
  } else if (OB_FAIL(inner_logstream_freeze())) {
    undo_freeze_();
    TRANS_LOG(WARN, "[Freezer] logstream_freeze failure", K(ret), K_(ls_id));
  } else {
    stat_.add_diagnose_info("logstream_freeze success");
    unset_freeze_();
    uint32_t freeze_clock = get_freeze_clock();
    FLOG_INFO("[Freezer] logstream_freeze success", K(ret), K_(ls_id), K(freeze_clock));
  }
  stat_.state_ = ObFreezeState::FINISH;
  stat_.end_time_ = ObTimeUtility::current_time();
  stat_.ret_code_ = ret;

  print_freezer_statistics();

  return ret;
}

int ObFreezer::inner_logstream_freeze()
{
  int ret = OB_SUCCESS;
  const int64_t start = ObTimeUtility::current_time();
  uint32_t freeze_clock = get_freeze_clock();
  TRANS_LOG(INFO, "[Freezer] freeze_clock", K(ret), K_(ls_id), K(freeze_clock));

  if (OB_FAIL(data_checkpoint_->ls_freeze(palf::SCN::max_scn()))) {
    // move memtables from active_list to frozen_list
    TRANS_LOG(WARN, "[Freezer] data_checkpoint freeze failed", K(ret), K_(ls_id));
    stat_.add_diagnose_info("data_checkpoint freeze failed");
  } else {
    submit_log_for_freeze();
    // wait till all memtables are moved from frozen_list to prepare_list
    // this means that all memtables can be dumped
    while (!data_checkpoint_->ls_freeze_finished()) {
      const int64_t cost_time = ObTimeUtility::current_time() - start;
      if (cost_time > 10 * 1000 * 1000) {
        if (TC_REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
          TRANS_LOG(WARN, "[Freezer] finish ls_freeze costs too much time",
                    K_(ls_id), K(cost_time));
          stat_.add_diagnose_info("finish ls_freeze costs too much time");
        }
      }
      ob_usleep(100);
    }
  }

  return ret;
}

/* tablet freeze */
int ObFreezer::tablet_freeze(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObTabletHandle handle;
  ObTablet *tablet = nullptr;
  ObTabletMemtableMgr *memtable_mgr = nullptr;
  memtable::ObIMemtable *imemtable = nullptr;
  palf::SCN freeze_snapshot_version;
  FLOG_INFO("[Freezer] tablet_freeze start", K(ret), K_(ls_id), K(tablet_id));
  stat_.reset();
  stat_.start_time_ = ObTimeUtility::current_time();
  stat_.state_ = ObFreezeState::NOT_SET_FREEZE_FLAG;
  stat_.tablet_id_ = tablet_id;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "[Freezer] not inited", K(ret), K_(ls_id), K(tablet_id));
  } else if (OB_FAIL(set_freeze_flag_without_inc_freeze_clock())) {
    ret = OB_SUCCESS;
    FLOG_INFO("[Freezer] freeze is running", K(ret), K_(ls_id), K(tablet_id));
  } else if (FALSE_IT(stat_.state_ = ObFreezeState::NOT_SUBMIT_LOG)) {
  } else {
    // succeed to set freeze flag
    if (OB_FAIL(get_ls_weak_read_scn(freeze_snapshot_version))) {
      TRANS_LOG(WARN, "[Freezer] get ls weak read scn failure", K(ret), K_(ls_id));
    } else if (ObScnRange::MAX_SCN == freeze_snapshot_version
               || ObScnRange::MIN_SCN >= freeze_snapshot_version) {
      ret = OB_NOT_INIT;
      LOG_WARN("[Freezer] weak read service not inited", K(ret), K_(ls_id));
    } else if (FALSE_IT(freeze_snapshot_version_ = freeze_snapshot_version)) {
    } else if (OB_FAIL(ls_tablet_svr_->get_tablet(tablet_id,
                                                  handle,
                                                  ObTabletCommon::NO_CHECK_GET_TABLET_TIMEOUT_US))) {
      TRANS_LOG(WARN, "[Freezer] fail to get tablet", K(ret), K_(ls_id), K(tablet_id));
      stat_.add_diagnose_info("fail to get tablet");
    } else if (FALSE_IT(tablet = handle.get_obj())) {
    } else if (OB_ISNULL(memtable_mgr = static_cast<ObTabletMemtableMgr*>(tablet->get_memtable_mgr()))) {
      TRANS_LOG(WARN, "[Freezer] tablet_memtable_mgr is null", K(ret), K_(ls_id), K(tablet_id));
    } else if (OB_FAIL(memtable_mgr->set_is_tablet_freeze_for_active_memtable(imemtable))) {
      if (ret == OB_ENTRY_NOT_EXIST) {
        ret = OB_SUCCESS;
        TRANS_LOG(INFO, "[Freezer] no need to freeze since there is no active memtable", K(ret),
                  K_(ls_id), K(tablet_id));
        stat_.add_diagnose_info("no need to freeze since there is no active memtable");
      } else {
        TRANS_LOG(WARN, "[Freezer] fail to set is_tablet_freeze", K(ret), K_(ls_id), K(tablet_id));
        stat_.add_diagnose_info("fail to set is_tablet_freeze");
      }
    } else if (OB_FAIL(handle_memtable_for_tablet_freeze(imemtable))) {
      TRANS_LOG(WARN, "[Freezer] fail to handle memtable", K(ret), K_(ls_id), K(tablet_id));
    } else {
      stat_.add_diagnose_info("tablet_freeze success");
    }
    unset_freeze_();
  }

  if (OB_SUCC(ret)) {
    FLOG_INFO("[Freezer] tablet_freeze success", K(ret), K_(ls_id), K(tablet_id));
  }

  stat_.state_ = ObFreezeState::FINISH;
  stat_.end_time_ = ObTimeUtility::current_time();
  stat_.ret_code_ = ret;

  print_freezer_statistics();

  return ret;
}

int ObFreezer::force_tablet_freeze(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObTabletHandle handle;
  ObTablet *tablet = nullptr;
  ObTabletMemtableMgr *memtable_mgr = nullptr;
  memtable::ObIMemtable *imemtable = nullptr;
  palf::SCN freeze_snapshot_version;
  FLOG_INFO("[Freezer] force_tablet_freeze start", K(ret), K_(ls_id), K(tablet_id));
  stat_.reset();
  stat_.start_time_ = ObTimeUtility::current_time();
  stat_.state_ = ObFreezeState::NOT_SET_FREEZE_FLAG;
  stat_.tablet_id_ = tablet_id;
  stat_.is_force_ = true;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "[Freezer] not inited", K(ret), K_(ls_id), K(tablet_id));
  } else if (OB_FAIL(loop_set_freeze_flag())) {
    TRANS_LOG(WARN, "[Freezer] failed to set freeze_flag", K(ret), K_(ls_id), K(tablet_id));
  } else if (FALSE_IT(stat_.state_ = ObFreezeState::NOT_SUBMIT_LOG)) {
  } else {
    if (OB_FAIL(get_ls_weak_read_scn(freeze_snapshot_version))) {
      TRANS_LOG(WARN, "[Freezer] get ls weak read ts failure", K(ret), K_(ls_id));
    } else if (ObScnRange::MAX_SCN == freeze_snapshot_version
               || ObScnRange::MIN_SCN >= freeze_snapshot_version) {
      ret = OB_NOT_INIT;
      LOG_WARN("[Freezer] weak read service not inited", K(ret), K_(ls_id));
    } else if (FALSE_IT(freeze_snapshot_version_ = freeze_snapshot_version)) {
    } else if (OB_FAIL(ls_tablet_svr_->get_tablet(tablet_id,
        handle, ObTabletCommon::NO_CHECK_GET_TABLET_TIMEOUT_US))) {
      TRANS_LOG(WARN, "[Freezer] fail to get tablet for freeze", K(ret), K_(ls_id), K(tablet_id));
      stat_.add_diagnose_info("fail to get tablet");
    } else if (FALSE_IT(tablet = handle.get_obj())) {
    } else if (OB_FAIL(create_memtable_if_no_active_memtable(tablet))) {
      LOG_WARN("[Freezer] fail to create an active memtable for force_tablet_freeze", K(ret), K_(ls_id), K(tablet_id));
      stat_.add_diagnose_info("fail to create an active memtable for force_tablet_freeze");
    } else if (OB_ISNULL(memtable_mgr = static_cast<ObTabletMemtableMgr*>(tablet->get_memtable_mgr()))) {
      TRANS_LOG(WARN, "[Freezer] tablet_memtable_mgr is null", K(ret), K_(ls_id), K(tablet_id));
    } else if (OB_FAIL(memtable_mgr->set_is_tablet_freeze_for_active_memtable(imemtable, true))) {
      TRANS_LOG(WARN, "[Freezer] fail to set is_tablet_freeze", K(ret), K_(ls_id), K(tablet_id));
      stat_.add_diagnose_info("fail to set is_tablet_freeze");
    } else if (OB_FAIL(handle_memtable_for_tablet_freeze(imemtable))) {
      TRANS_LOG(WARN, "[Freezer] fail to handle memtable", K(ret), K_(ls_id), K(tablet_id));
    } else {
      stat_.add_diagnose_info("force_tablet_freeze success");
    }
    unset_freeze_();
  }

  if (OB_SUCC(ret)) {
    FLOG_INFO("[Freezer] force_tablet_freeze success", K(ret), K_(ls_id), K(tablet_id));
  } else {
    FLOG_INFO("[Freezer] force_tablet_freeze failed", K(ret), K_(ls_id), K(tablet_id));
  }

  stat_.state_ = ObFreezeState::FINISH;
  stat_.end_time_ = ObTimeUtility::current_time();
  stat_.ret_code_ = ret;

  print_freezer_statistics();

  return ret;
}

/* private subfunctions for freeze process */
int ObFreezer::handle_memtable_for_tablet_freeze(memtable::ObIMemtable *imemtable)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(imemtable)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    memtable::ObMemtable *memtable = static_cast<memtable::ObMemtable*>(imemtable);
    submit_log_for_freeze();
    wait_memtable_ready_for_flush(memtable);
    if (OB_FAIL(memtable->finish_freeze())) {
      TRANS_LOG(ERROR, "[Freezer] memtable cannot be flushed",
                K(ret), K_(ls_id), K(*memtable));
      stat_.add_diagnose_info("memtable cannot be flushed");
    } else {
      TRANS_LOG(INFO, "[Freezer] memtable is ready to be flushed",
                K(ret), K_(ls_id), K(*memtable));
    }
  }

  return ret;
}

int ObFreezer::submit_log_for_freeze()
{
  int ret = OB_SUCCESS;
  const int64_t start = ObTimeUtility::current_time();
  ObTabletID tablet_id(INT64_MAX); // used for diagnose

  do {
    ret = OB_SUCCESS;
    transaction::ObTransID fail_tx_id;

    if (OB_FAIL(ls_tx_svr_->traverse_trans_to_submit_redo_log(fail_tx_id))) {
      const int64_t cost_time = ObTimeUtility::current_time() - start;
      if (cost_time > 1000 * 1000) {
        if (TC_REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
          TRANS_LOG(WARN, "[Freezer] failed to traverse trans ctx to submit redo log", K(ret),
                    K_(ls_id), K(cost_time), K(fail_tx_id));
          ADD_SUSPECT_INFO(MINI_MERGE,
                           ls_id_, tablet_id,
                           "traverse_trans_to_submit_redo_log failed",
                           K(ret),
                           K(fail_tx_id));
          stat_.add_diagnose_info("traverse_trans_to_submit_redo_log failed");
        }
      }
    }
    if (OB_LOG_OUTOF_DISK_SPACE == ret) {
      ob_usleep(100 * 1000);
    }
  } while (OB_FAIL(ret));
  DEL_SUSPECT_INFO(MINI_MERGE, ls_id_, tablet_id);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ls_tx_svr_->traverse_trans_to_submit_next_log())) {
      TRANS_LOG(WARN, "traverse trans ctx to submit next log failed", K(ret));
    }
  }

  stat_.state_ = ObFreezeState::WAIT_READY_FOR_FLUSH;

  return ret;
}

void ObFreezer::wait_memtable_ready_for_flush(memtable::ObMemtable *memtable)
{
  const int64_t start = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;

  while (!memtable->ready_for_flush()) {
    const int64_t cost_time = ObTimeUtility::current_time() - start;
    if (cost_time > 10 * 1000 * 1000) {
      if (TC_REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        TRANS_LOG(WARN, "[Freezer] ready_for_flush costs too much time",
                  K_(ls_id), K(cost_time), KPC(memtable));
        stat_.add_diagnose_info("ready_for_flush costs too much time");
        memtable->print_ready_for_flush();
      }
    }
    ob_usleep(100);
  }
}

int ObFreezer::create_memtable_if_no_active_memtable(ObTablet *tablet)
{
  int ret = OB_SUCCESS;
  ObTabletMemtableMgr *memtable_mgr = nullptr;
  memtable::ObMemtable *last_frozen_memtable = nullptr;
  const common::ObTabletID &tablet_id = tablet->get_tablet_meta().tablet_id_;
  int64_t schema_version = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[Freezer] not inited", K(ret), K_(is_inited), K_(ls_id), K(tablet_id));
  } else if (OB_ISNULL(memtable_mgr = static_cast<ObTabletMemtableMgr *>(tablet->get_memtable_mgr()))) {
    LOG_WARN("[Freezer] memtable mgr should not be null", K(ret), K_(ls_id), K(tablet_id));
  } else if (memtable_mgr->has_active_memtable()) {
    LOG_INFO("[Freezer] no need to create an active memtable", K(ret), K_(ls_id), K(tablet_id));
  } else { // create a new memtable since there is no active memtable
    // get schema_version
    if (OB_NOT_NULL(last_frozen_memtable = memtable_mgr->get_last_frozen_memtable())) {
      schema_version = last_frozen_memtable->get_max_schema_version();
    } else if (OB_FAIL(tablet->get_schema_version_from_storage_schema(schema_version))) {
      LOG_WARN("[Freezer] failed to get schema version", K(ret), K_(ls_id), K(tablet_id));
    } else {
      //do nothing
    }
    // create new memtable
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ls_tablet_svr_->create_memtable(tablet_id, schema_version))) {
        if (OB_MINOR_FREEZE_NOT_ALLOW != ret) {
          LOG_WARN("[Freezer] failed to create memtable", K(ret), K_(ls_id), K(tablet_id),
                   K(schema_version));
        }
      } else {
        LOG_INFO("[Freezer] succeed to create new active memtable", K(ret), K_(ls_id),
                 K(tablet_id), K(schema_version));
      }
    }
  }

  return ret;
}

/* private subfunctions about freeze flag*/
int ObFreezer::loop_set_freeze_flag()
{
  int ret = OB_SUCCESS;
  const int64_t start = ObTimeUtility::current_time();

  do {
    ret = OB_SUCCESS;
    if (OB_FAIL(set_freeze_flag_without_inc_freeze_clock())) {
      const int64_t cost_time = ObTimeUtility::current_time() - start;
      if (cost_time > 3 * 1000 * 1000) {
        TRANS_LOG(WARN, "[Freezer] wait the running freeze too long time", K_(ls_id),
                  K(cost_time));
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
  uint32_t old_v = 0;
  uint32_t new_v = 0;

  do {
    old_v = ATOMIC_LOAD(&freeze_flag_);
    if (is_freeze(old_v)) {
      ret = OB_ENTRY_EXIST;
      TRANS_LOG(WARN, "[Freezer] freeze is running!", K(ret), K_(ls_id));
      break;
    }
    new_v = old_v | (1 << 31);
  } while (ATOMIC_CAS(&freeze_flag_, old_v, new_v) != old_v);

  return ret;
}

int ObFreezer::set_freeze_flag()
{
  int ret = OB_SUCCESS;
  uint32_t old_v = 0;
  uint32_t new_v = 0;

  // set freeze_flag to flag the start of freeze
  // set the first bit 1
  // inc the freeze clock
  do {
    old_v = ATOMIC_LOAD(&freeze_flag_);
    if (is_freeze(old_v)) {
      ret = OB_ENTRY_EXIST;
      TRANS_LOG(WARN, "[Freezer] freeze is running!", K(ret), K_(ls_id));
      break;
    }
    new_v = (old_v + 1) | (1 << 31);
  } while (ATOMIC_CAS(&freeze_flag_, old_v, new_v) != old_v);

  return ret;
}

int ObFreezer::inc_freeze_clock()
{
  int ret = OB_SUCCESS;
  uint32_t old_v = 0;
  uint32_t new_v = 0;

  // inc freeze_clock when freeze_flag has been set
  // before used by force_freeze to resolve concurrency problems
  do {
    old_v = ATOMIC_LOAD(&freeze_flag_);
    if (!is_freeze(old_v)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "[Freezer] cannot inc freeze clock without setting freeze flag!",
                K(ret), K_(ls_id));
      break;
    }
    new_v = old_v + 1;
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

  // Step3: unset freeze_flag to flag the end of freeze
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

  // Step3: unset freeze_flag and dec freeze_clock
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
int ObFreezer::decide_max_decided_scn(palf::SCN &max_decided_scn)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[Freezer] not inited", K(ret), K_(ls_id));
  } else if (OB_FAIL(loghandler_->get_max_decided_scn(max_decided_scn))) {
    if (OB_STATE_NOT_MATCH == ret) {
      max_decided_scn.reset();
      ret = OB_SUCCESS;
    } else {
      TRANS_LOG(WARN, "[Freezer] fail to get max_decided_scn", K(ret), K_(ls_id),
                K(max_decided_scn));
    }
  }

  if (OB_SUCC(ret)) {
    TRANS_LOG(TRACE, "[Freezer] decide max decided log ts", K(ret), K_(ls_id), K(max_decided_scn));
  }
  return ret;
}

int ObFreezer::get_max_consequent_callbacked_scn(palf::SCN &max_consequent_callbacked_scn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[Freezer] not inited", K(ret), K_(ls_id));
  } else if (OB_FAIL(loghandler_->get_max_decided_scn(max_consequent_callbacked_scn))) {
    if (OB_STATE_NOT_MATCH == ret) {
      max_consequent_callbacked_scn.set_min();
      ret = OB_SUCCESS;
    } else {
      TRANS_LOG(WARN, "[Freezer] fail to get min_unreplay_log_ts", K(ret), K_(ls_id), K(max_consequent_callbacked_scn));
    }
  } else {
    TRANS_LOG(TRACE, "[Freezer] get_max_decided_log_ts", K(ret), K_(ls_id), K(max_consequent_callbacked_scn));
  }
  return ret;
}

int ObFreezer::get_ls_weak_read_scn(palf::SCN &weak_read_scn)
{
  int ret = OB_SUCCESS;
  weak_read_scn.reset();

  if (OB_ISNULL(ls_wrs_handler_)) {
    ret = OB_ENTRY_NOT_EXIST;
    TRANS_LOG(WARN, "[Freezer] service should not be null", K(ret), K_(ls_id));
  } else {
    weak_read_scn = ls_wrs_handler_->get_ls_weak_read_ts();
    TRANS_LOG(TRACE, "[Freezer] get_ls_weak_read_scn", K(ret), K_(ls_id), K(weak_read_scn));
  }

  return ret;
}

int ObFreezer::get_newest_clog_checkpoint_scn(const ObTabletID &tablet_id,
                                              palf::SCN &clog_checkpoint_scn)
{
  int ret = OB_SUCCESS;
  ObTabletHandle handle;
  clog_checkpoint_scn.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "[Freezer] not inited", K(ret));
  } else if (OB_FAIL(ls_tablet_svr_->get_tablet(tablet_id,
      handle, ObTabletCommon::NO_CHECK_GET_TABLET_TIMEOUT_US))) {
    TRANS_LOG(WARN, "[Freezer] fail to get tablet", K(ret), K_(ls_id), K(tablet_id));
  } else {
    clog_checkpoint_scn = handle.get_obj()->get_tablet_meta().clog_checkpoint_scn_;
    TRANS_LOG(TRACE, "[Freezer] get_newest_clog_checkpoint_scn", K(ret), K_(ls_id), K(tablet_id),
              K(clog_checkpoint_scn));
  }

  return ret;
}

int ObFreezer::get_newest_snapshot_version(const ObTabletID &tablet_id,
                                           palf::SCN &snapshot_version)
{
  int ret = OB_SUCCESS;
  ObTabletHandle handle;
  snapshot_version.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "[Freezer] not inited", K(ret));
  } else if (OB_FAIL(ls_tablet_svr_->get_tablet(tablet_id,
      handle, ObTabletCommon::NO_CHECK_GET_TABLET_TIMEOUT_US))) {
    TRANS_LOG(WARN, "[Freezer] fail to get tablet", K(ret), K_(ls_id), K(tablet_id));
  } else if (OB_FAIL(snapshot_version.convert_for_lsn_allocator(handle.get_obj()->get_snapshot_version()))) {
    TRANS_LOG(WARN, "[Freezer] fail to convert from ts", K(ret), K_(ls_id), K(tablet_id));
  } else {
    TRANS_LOG(TRACE, "[Freezer] get_snapshot_version", K(ret), K_(ls_id), K(tablet_id), K(snapshot_version));
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
    TRANS_LOG(INFO, "[Freezer] empty table statistics: ", K_(ls_id), K(get_empty_memtable_cnt()));
    clear_empty_memtable_cnt();
  }
}

} // namespace storage
} // namespace oceanbase
