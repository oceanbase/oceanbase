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

#include "storage/ob_i_tablet_memtable.h"

#include "storage/compaction/ob_compaction_diagnose.h"
#include "storage/ls/ob_freezer.h"
#include "storage/tablet/ob_tablet_memtable_mgr.h"

namespace oceanbase {

namespace storage {


int ObITabletMemtable::inc_unsubmitted_cnt()
{
  int ret = OB_SUCCESS;
  const share::ObLSID ls_id = get_ls_id();
  int64_t unsubmitted_cnt = inc_unsubmitted_cnt_();
  TRANS_LOG(DEBUG, "inc_unsubmitted_cnt", K(ls_id), KPC(this), K(lbt()));

  if (OB_FAIL(get_unset_active_memtable_logging_blocked())) {
    TRANS_LOG(WARN, "cannot inc unsubmitted_cnt", K(unsubmitted_cnt), K(ls_id), KPC(this));
  }

  return ret;
}

int ObITabletMemtable::dec_unsubmitted_cnt()
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();

  // fix issue 47021079
  // To avoid the following case where logging_block cannot be unset:
  // -----------------------------------------------------
  // dec_write_ref()             dec_unsubmitted_cnt()
  // -----------------------------------------------------
  // is_frozen							     is_frozen
  //                             get write_ref_cnt 1
  // dec write_ref to 0
  // get unsubmitted_cnt 1
  //                             dec unsubmitted_cnt to 0
  // -----------------------------------------------------
  int64_t old_unsubmitted_cnt = dec_unsubmitted_cnt_();

  // must maintain the order of getting variables to avoid concurrency problems
  // is_frozen_memtable() can affect wirte_ref_cnt
  // write_ref_cnt can affect unsubmitted_cnt
  bool is_frozen = is_frozen_memtable();
  int64_t write_ref_cnt = get_write_ref();
  int64_t new_unsubmitted_cnt = get_unsubmitted_cnt();
  TRANS_LOG(DEBUG, "dec_unsubmitted_cnt", K(ls_id), KPC(this), K(lbt()));

  if (OB_UNLIKELY(old_unsubmitted_cnt < 0)) {
    TRANS_LOG(ERROR, "unsubmitted_cnt not match", K(ret), K(ls_id), KPC(this));
  } else if (is_frozen &&
             0 == write_ref_cnt &&
             0 == new_unsubmitted_cnt) {
    (void)unset_logging_blocked_for_active_memtable_();
    TRANS_LOG(INFO, "memtable log submitted", K(ret), K(ls_id), KPC(this));
  }

  return ret;
}


void ObITabletMemtable::unset_logging_blocked_for_active_memtable_()
{
  int ret = OB_SUCCESS;
  ObTabletMemtableMgr *memtable_mgr = get_memtable_mgr();

  if (OB_NOT_NULL(memtable_mgr)) {
    do {
      if (OB_FAIL(memtable_mgr->unset_logging_blocked_for_active_memtable(this))) {
        TRANS_LOG(ERROR, "fail to unset logging blocked for active memtable", K(ret), K(ls_id_), KPC(this));
        ob_usleep(100);
      }
    } while (OB_FAIL(ret));
  }
}

void ObITabletMemtable::resolve_left_boundary_for_active_memtable_()
{
  int ret = OB_SUCCESS;
  storage::ObTabletMemtableMgr *memtable_mgr = get_memtable_mgr();
  const SCN new_start_scn = MAX(get_end_scn(), get_migration_clog_checkpoint_scn());

  if (OB_NOT_NULL(memtable_mgr)) {
    do {
      if (OB_FAIL(memtable_mgr->resolve_left_boundary_for_active_memtable(this, new_start_scn))) {
        TRANS_LOG(ERROR, "fail to set start log ts for active memtable", K(ret), K(ls_id_), KPC(this));
        ob_usleep(100);
      }
    } while (OB_FAIL(ret));
  }
}

int ObITabletMemtable::get_ls_current_right_boundary_(SCN &current_right_boundary)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(freezer_)) {
    ret = OB_ENTRY_NOT_EXIST;
    TRANS_LOG(WARN, "freezer should not be null", K(ret));
  } else if (OB_FAIL(freezer_->get_max_consequent_callbacked_scn(current_right_boundary))) {
    TRANS_LOG(WARN, "fail to get min_unreplay_scn", K(ret), K(current_right_boundary));
  }

  return ret;
}

int ObITabletMemtable::set_memtable_mgr_(storage::ObTabletMemtableMgr *mgr)
{
  ObTabletMemtableMgrPool *pool = MTL(ObTabletMemtableMgrPool*);
  return memtable_mgr_handle_.set_memtable_mgr(mgr, pool);
}

ObTabletMemtableMgr *ObITabletMemtable::get_memtable_mgr()
{
  return static_cast<ObTabletMemtableMgr *>(memtable_mgr_handle_.get_memtable_mgr());
}

int ObITabletMemtable::set_freezer(ObFreezer *handler)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(handler)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "freeze handler is null", K(ret));
  } else {
    freezer_ = handler;
  }
  return ret;
}

int ObITabletMemtable::set_migration_clog_checkpoint_scn(const SCN &clog_checkpoint_scn)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret));
  } else if (clog_checkpoint_scn <= ObScnRange::MIN_SCN) {
    ret = OB_SCN_OUT_OF_BOUND;
    TRANS_LOG(WARN, "invalid clog_checkpoint_ts", K(ret));
  } else {
    (void)migration_clog_checkpoint_scn_.atomic_store(clog_checkpoint_scn);
  }

  return ret;
}

int ObITabletMemtable::set_rec_scn(const SCN rec_scn)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();

  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret));
  } else if (rec_scn.is_max()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid args", K(ret), K(rec_scn));
  } else if (rec_scn <= get_start_scn()) {
    ret = OB_SCN_OUT_OF_BOUND;
    TRANS_LOG(ERROR, "cannot set freeze log ts smaller to start log ts", K(ret), K(rec_scn), K(ls_id), KPC(this));
  } else {
    SCN old_rec_scn;
    SCN new_rec_scn = get_rec_scn();
    while ((old_rec_scn = new_rec_scn) > rec_scn) {
      if ((new_rec_scn = rec_scn_.atomic_vcas(old_rec_scn, rec_scn))
          == old_rec_scn) {
        new_rec_scn = rec_scn;
      }
    }
  }
  return ret;
}

int ObITabletMemtable::set_start_scn(const share::SCN start_scn)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();

  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret));
  } else if (ObScnRange::MAX_SCN == start_scn) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid args", K(ret), K(start_scn));
  } else if (
      // Case1: the start_scn should not greater than its end_scn. Because the
      // boundary of the memtable is left-open and right-closed.
      start_scn >= get_end_scn()
      // Case2: the start_scn should not greater than its max_end_scn except is
      // has been backoffed. Because the memtable might have failed to sync all
      // the logs and its right boundary belongs to its previos memtable.
      || (max_end_scn_ != SCN::min_scn()
          && start_scn >= max_end_scn_
          && !get_has_backoffed())
      // Case3: the start_scn should not greater than its rec_scn. Because the
      // rec_scn is updated with real log and the start_scn should not be
      // greater than the real log of its next memtable.
      || start_scn >= rec_scn_) {
    ret = OB_SCN_OUT_OF_BOUND;
    TRANS_LOG(ERROR, "cannot set start ts now", K(ret), K(start_scn), K(ls_id), KPC(this));
  } else {
    key_.scn_range_.start_scn_ = start_scn;
  }

  return ret;
}

int ObITabletMemtable::set_end_scn(const SCN freeze_scn)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();

  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret));
  } else if (ObScnRange::MAX_SCN == freeze_scn) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid args", K(ret), K(freeze_scn));
  } else if (freeze_scn < get_start_scn()) {
    ret = OB_SCN_OUT_OF_BOUND;
    TRANS_LOG(ERROR, "cannot set freeze log ts smaller to start log ts",
              K(ret), K(freeze_scn), K(ls_id), KPC(this));
  } else {
    SCN old_end_scn;
    SCN new_end_scn = get_end_scn();
    while ((old_end_scn = new_end_scn) < freeze_scn
           || new_end_scn == ObScnRange::MAX_SCN) {
      if ((new_end_scn =
           key_.scn_range_.end_scn_.atomic_vcas(old_end_scn, freeze_scn))
          == old_end_scn) {
        new_end_scn = freeze_scn;
      }
    }
    freeze_scn_ =  freeze_scn;
  }
  return ret;
}

int ObITabletMemtable::set_max_end_scn(const SCN scn, bool allow_backoff)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();

  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret));
  } else if (ObScnRange::MAX_SCN == scn) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid args", K(ret), K(scn));
  } else if (
      // Case1: the max_end_scn should not smaller than its start_scn except it
      // is during backoff. Because the memtable might have failed to sync all
      // the logs and the max_end_scn belongs to its previous memtable
      (!allow_backoff &&
       scn <= get_start_scn())
      // Case2: the max_end_scn should not smaller than its end_scn. Because the
      // end_scn is resolved only when the max_end_scn is decided.
      || scn > get_end_scn()) {
    ret = OB_SCN_OUT_OF_BOUND;
    TRANS_LOG(WARN, "cannot set max end log ts smaller to start log ts",
              K(ret), K(scn), K(ls_id), KPC(this));
  } else if (allow_backoff) {
    set_has_backoffed();
    TRANS_LOG(INFO, "set max_end_scn force", K(scn), K(max_end_scn_.atomic_get()), K(key_), KPC(this));
    if (scn != max_end_scn_.atomic_get()) {
      max_end_scn_.dec_update(scn);
      if (rec_scn_.atomic_get() > max_end_scn_) {
        TRANS_LOG(INFO, "rec_scn is greater than max_end_scn, set it to max", K_(rec_scn), K_(max_end_scn));
        rec_scn_.set_max();
      }
    }
  } else {
    SCN old_max_end_scn;
    SCN new_max_end_scn = get_max_end_scn();
    while ((old_max_end_scn = new_max_end_scn) < scn) {
      if ((new_max_end_scn =
           max_end_scn_.atomic_vcas(old_max_end_scn, scn))
          == old_max_end_scn) {
        new_max_end_scn = scn;
      }
    }
  }
  return ret;
}


// NOTE:
// CASE 1 : For Data Memtable
// call this function must ensure all TxNodes on memtable has been
// logged and the log's either synced successfully or synced failed.
// because otherwise the max_end_scn is not correct and which may be larger than
// the max value of valid TxNode(s)'s log_scn, which cause an incorrect right
// boundary value, and an incorrect left boundary value of next active memtable
//
// when TxNode's log synced failed, Txn's function will process to adjust the
// max_end_scn of this memtable, finally, the memtable will has a correct right
// boundary value, keep the safety:
//   future data's log_scn > max_end_scn of this memtable
//
// CASE 2 : For Direct Load Memtable
// call this function after write_ref_cnt == 0
//
int ObITabletMemtable::resolve_right_boundary()
{
  int ret = OB_SUCCESS;
  SCN max_end_scn = get_max_end_scn();
  SCN end_scn = max_end_scn;
  SCN father_start_scn = ObITable::get_start_scn();
  SCN ddl_start_scn = get_start_scn();

  if (max_end_scn.is_min()) {
    end_scn = father_start_scn;
    (void)freezer_->inc_empty_memtable_cnt();
  }

  if (OB_FAIL(set_end_scn(end_scn))) {
    TRANS_LOG(ERROR, "fail to set end_scn", K(ret));
  }

  return ret;
}

void ObITabletMemtable::set_max_schema_version(const int64_t schema_version)
{
  if (INT64_MAX == schema_version) {
    TRANS_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid schema version", K(schema_version), KPC(this));
  } else {
    inc_update(&max_schema_version_, schema_version);
  }
}

int64_t ObITabletMemtable::get_max_schema_version() const
{
  return ATOMIC_LOAD(&max_schema_version_);
}

int ObITabletMemtable::replay_schema_version_change_log(const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(schema_version));
  } else {
    set_max_schema_version(schema_version);
  }
  return ret;
}

int64_t ObITabletMemtable::inc_unsubmitted_cnt_() { return ATOMIC_AAF(&unsubmitted_cnt_, 1); }
int64_t ObITabletMemtable::dec_unsubmitted_cnt_() { return ATOMIC_SAF(&unsubmitted_cnt_, 1); }
int64_t ObITabletMemtable::inc_write_ref_() { return ATOMIC_AAF(&write_ref_cnt_, 1); }
int64_t ObITabletMemtable::dec_write_ref_() { return ATOMIC_SAF(&write_ref_cnt_, 1); }

bool ObITabletMemtable::can_be_minor_merged()
{
  return is_in_prepare_list_of_data_checkpoint();
}


bool ObITabletMemtable::is_force_released() const
{
  return TabletMemtableFreezeState::FORCE_RELEASED == get_freeze_state();
}


}  // namespace storage

}  // namespace oceanbase
