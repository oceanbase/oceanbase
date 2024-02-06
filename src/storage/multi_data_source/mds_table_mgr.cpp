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
#include "lib/container/ob_se_array.h"
#include "lib/ob_errno.h"
#include "mds_ctx.h"
#include "ob_tablet_id.h"
#define USING_LOG_PREFIX MDS

#include "mds_table_mgr.h"
#include "mds_table_handle.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/multi_data_source/mds_table_base.h"
#include "storage/meta_mem/ob_tablet_pointer.h"

namespace oceanbase {

using namespace share;

namespace storage {
namespace mds {

void RemovedMdsTableRecorder::record(MdsTableBase *mds_table)
{
  SpinWLockGuard guard(lock_);
  removed_mds_table_list_.append(mds_table);
}

void RemovedMdsTableRecorder::del(MdsTableBase *mds_table)
{
  SpinWLockGuard guard(lock_);
  removed_mds_table_list_.del(mds_table);
}

int ObMdsTableMgr::init(ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    MDS_LOG(WARN, "init mds table mgr twice", KR(ret), KPC(this));
  } else if (OB_ISNULL(ls) || OB_ISNULL(ls->get_tx_svr())) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG(ERROR, "invalid ls when init mds table mgr", KR(ret), KPC(ls), KPC(this));
  } else if (OB_FAIL(ls->get_tx_svr()->register_common_checkpoint(checkpoint::MDS_TABLE_TYPE, this))) {
    MDS_LOG(WARN, "register common checkpoint failed", KR(ret), KPC(this));
  } else if (OB_FAIL(mds_table_map_.init("MdsTableMgr", MTL_ID()))) {
    MDS_LOG(ERROR, "fail to init mds table map", KR(ret), KPC(ls), KPC(this));
  } else {
    ls_ = ls;
    is_inited_ = true;
  }

  return ret;
}

int ObMdsTableMgr::reset()
{
  int ret = OB_SUCCESS;
  is_inited_ = false;
  is_freezing_ = false;
  freezing_scn_.atomic_store(share::SCN::min_scn());
  ref_cnt_ = 0;
  ls_ = nullptr;
  if (OB_FAIL(mds_table_map_.reset())) {
    MDS_LOG(WARN, "fail to reset mds_table_map", KR(ret));
  }
  return ret;
}

void ObMdsTableMgr::offline()
{
  freezing_scn_.atomic_store(share::SCN::min_scn());
}

void ObMdsTableMgr::destroy() { reset(); }

int ObMdsTableMgr::register_to_mds_table_mgr(MdsTableBase *p_mds_table)
{
  int ret = OB_SUCCESS;
  common::ObTabletID tablet_id;
  if (OB_ISNULL(p_mds_table)) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG(ERROR, "invalid mdstable handle", KR(ret), KP(p_mds_table));
  } else if (FALSE_IT(tablet_id = p_mds_table->get_tablet_id())) {
  } else if (OB_FAIL(mds_table_map_.insert(tablet_id, p_mds_table))) {
    MDS_LOG(ERROR, "fail to insert mds table to map", KR(ret), KPC(p_mds_table));
  } else {
    MDS_LOG(INFO, "register success", KR(ret), KPC(p_mds_table));
  }
  return ret;
}

int ObMdsTableMgr::unregister_from_mds_table_mgr(MdsTableBase *p_mds_table)
{
  int ret = OB_SUCCESS;
  common::ObTabletID tablet_id;
  auto op = [p_mds_table, this](const ObTabletID &id, MdsTableBase *&p_mds_table_in_map) {// with map's bucket ock protected
    if (p_mds_table != p_mds_table_in_map) {
      MDS_LOG_RET(ERROR, OB_ERR_UNEXPECTED,
                  "erased mds table is not same with mds table in map, but shared same tablet id",
                  K(id), KPC(p_mds_table), KPC(p_mds_table_in_map));
    }
    removed_mds_table_recorder_.record(p_mds_table);
    return true;// always erase it
  };
  if (OB_ISNULL(p_mds_table)) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG(ERROR, "invalid mdstable handle", KR(ret), KP(p_mds_table));
  } else if (FALSE_IT(tablet_id = p_mds_table->get_tablet_id())) {
  } else if (OB_FAIL(mds_table_map_.erase_if(tablet_id, op))) {
    MDS_LOG(WARN, "fail to erase kv", KR(ret), K(tablet_id));
  } else {
    MDS_LOG(INFO, "unregister success", KR(ret), KPC(p_mds_table));
  }
  return ret;
}

void ObMdsTableMgr::unregister_from_removed_mds_table_recorder(MdsTableBase *p_mds_table)
{
  // make sure this mds_table is recored in list
  if ((OB_NOT_NULL(p_mds_table->prev_) || OB_NOT_NULL(p_mds_table->next_)) ||// if true, means mds_table is in list
      removed_mds_table_recorder_.check_is_list_head(p_mds_table)) {// but if both prev_ and next_ are NULL, maybe is list_head, means also in list
    removed_mds_table_recorder_.del(p_mds_table);
  }
}

int ObMdsTableMgr::first_scan_to_get_min_rec_scn_(share::SCN &min_rec_scn, ObIArray<ObTabletID> &min_rec_scn_ids)
{
  MDS_TG(10_s);
  int ret = OB_SUCCESS;
  int64_t scan_cnt = 0;
  auto get_min_rec_scn_op =
  [&min_rec_scn, &scan_cnt, &min_rec_scn_ids](const common::ObTabletID &, MdsTableBase *&mds_table) {
    if (!mds_table->is_switched_to_empty_shell()) {
      share::SCN rec_scn = mds_table->get_rec_scn();
      if (rec_scn == min_rec_scn) {
        if (min_rec_scn_ids.count() < 128) {
          (void) min_rec_scn_ids.push_back(mds_table->get_tablet_id());
        }
      } else if (rec_scn < min_rec_scn) {
        min_rec_scn = rec_scn;
        min_rec_scn_ids.reset();
        (void) min_rec_scn_ids.push_back(mds_table->get_tablet_id());
      }
      ++scan_cnt;
    }
    return true;// true means iterating the next mds table
  };
  if (OB_FAIL(mds_table_map_.for_each(get_min_rec_scn_op))) {
    MDS_LOG(WARN, "fail to do map for_each", KR(ret), K(*this), K(scan_cnt), K(min_rec_scn), K(min_rec_scn_ids));
  }
  return ret;
}

int ObMdsTableMgr::second_scan_to_do_flush_(share::SCN do_flush_limit_scn)
{
  MDS_TG(10_s);
  int ret = OB_SUCCESS;
  int64_t scan_mds_table_cnt = 0;
  auto flush_op = [do_flush_limit_scn, &scan_mds_table_cnt](const common::ObTabletID &tablet_id,
                                                            MdsTableBase *&mds_table) {
    int tmp_ret = OB_SUCCESS;
    if (mds_table->is_switched_to_empty_shell()) {
      MDS_LOG_RET(INFO, ret, "skip empty shell tablet mds_table flush", K(tablet_id), K(scan_mds_table_cnt));
    } else if (OB_TMP_FAIL(mds_table->flush(do_flush_limit_scn))) {
      MDS_LOG_RET(WARN, ret, "flush mds table failed", KR(tmp_ret), K(tablet_id), K(scan_mds_table_cnt));
    } else {
      ++scan_mds_table_cnt;
    }
    return true;// true means iterating the next mds table
  };
  if (OB_FAIL(mds_table_map_.for_each(flush_op))) {
    MDS_LOG(WARN, "fail to do map for_each", KR(ret), K(*this), K(scan_mds_table_cnt), K(do_flush_limit_scn));
  } else {
    MDS_LOG(INFO, "success to do second scan to do flush", KR(ret), K(*this), K(scan_mds_table_cnt), K(do_flush_limit_scn));
  }
  return ret;
}

int ObMdsTableMgr::flush(SCN recycle_scn, bool need_freeze)
{
  #define PRINT_WRAPPER KR(ret), K(ls_->get_ls_id()), K(recycle_scn), K(need_freeze), K(min_rec_scn),\
                        K(max_consequent_callbacked_scn), K(*this)
  MDS_TG(10_s);
  int ret = OB_SUCCESS;
  MdsTableFreezeGuard freeze_guard;
  freeze_guard.init(this);
  int64_t flushed_table_cnt = 0;
  MdsTableHandle *flushing_mds_table = nullptr;
  share::SCN min_rec_scn = share::SCN::max_scn();
  share::SCN max_consequent_callbacked_scn;
  ObSEArray<ObTabletID, 10> min_rec_scn_tablet_ids;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    MDS_LOG_FREEZE(ERROR, "mds table mgr not inited");
  } else if (!freeze_guard.can_freeze()) {
    MDS_LOG_FREEZE(INFO, "mds table mgr is doing flush, skip flush once");
  } else if (MDS_FAIL(first_scan_to_get_min_rec_scn_(min_rec_scn, min_rec_scn_tablet_ids))) {
    MDS_LOG_FREEZE(WARN, "do first_scan_to_get_min_rec_scn_ failed");
  } else if (min_rec_scn == share::SCN::max_scn()) {// no mds table
    MDS_LOG_FREEZE(INFO, "no valid mds table there, no need do flush");
  } else if (MDS_FAIL(ls_->get_freezer()->get_max_consequent_callbacked_scn(max_consequent_callbacked_scn))) {
    MDS_LOG_FREEZE(WARN, "fail to get max_consequent_callbacked_scn", KR(ret), K(*this));
  } else if (!max_consequent_callbacked_scn.is_valid() || max_consequent_callbacked_scn.is_max() || max_consequent_callbacked_scn.is_min()) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_FREEZE(WARN, "invalid max_consequent_callbacked_scn", KR(ret), K(*this));
  } else {
    if (need_freeze) {
      if (recycle_scn.is_max() || !recycle_scn.is_valid()) {
        recycle_scn = max_consequent_callbacked_scn;
      }
      if (recycle_scn > freezing_scn_) {
        MDS_LOG_FREEZE(INFO, "generate new freezing scn");
        freezing_scn_.atomic_store(recycle_scn);
      }
    }
    if (freezing_scn_ > max_consequent_callbacked_scn) {
      freezing_scn_ = max_consequent_callbacked_scn;
      MDS_LOG_FREEZE(INFO, "freezing_scn decline to max_consequent_callbacked_scn");
    }
    if (min_rec_scn <= freezing_scn_) {
      if (MDS_FAIL(second_scan_to_do_flush_(freezing_scn_))) {
        MDS_LOG_FREEZE(WARN, "fail to do flush");
      } else {
        MDS_LOG_FREEZE(INFO, "success to do flush");
      }
    } else {
      MDS_LOG_FREEZE(INFO, "no need do flush cause min_rec_scn is larger than freezing scn");
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

SCN ObMdsTableMgr::get_freezing_scn() const { return freezing_scn_.atomic_get(); }

SCN ObMdsTableMgr::get_rec_scn()
{
  ObTabletID tablet_id;
  return get_rec_scn(tablet_id);
}

share::SCN ObMdsTableMgr::get_rec_scn(ObTabletID &tablet_id)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  SCN min_rec_scn = share::SCN::max_scn();
  ObSEArray<ObTabletID, 10> min_rec_scn_tablet_ids;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    MDS_LOG(ERROR, "regsiter mds table failed", KR(ret));
  } else if (MDS_FAIL(first_scan_to_get_min_rec_scn_(min_rec_scn, min_rec_scn_tablet_ids))) {
    min_rec_scn = SCN::min_scn();
    MDS_LOG(WARN, "fail to scan get min_rec_scn", KR(ret), K(min_rec_scn), K(min_rec_scn_tablet_ids), K(*this));
  } else {
    if (!min_rec_scn_tablet_ids.empty()) {
      tablet_id = min_rec_scn_tablet_ids.at(0);
    }
    MDS_LOG(INFO, "get rec_scn from MdsTableMgr", KR(ret), K(min_rec_scn), K(min_rec_scn_tablet_ids), K(*this));
  }
  return min_rec_scn;
}

DEF_TO_STRING(ObMdsTableMgr)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K_(is_inited), K_(freezing_scn), KPC_(ls));
  J_OBJ_END();
  return pos;
}

int MdsTableFreezeGuard::init(ObMdsTableMgr *mds_mgr)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_ISNULL(mds_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG(WARN, "invalid tx data table", KR(ret));
  } else {
    can_freeze_ = (false == ATOMIC_CAS(&(mds_mgr->is_freezing_), false, true));
    if (can_freeze_) {
      mds_mgr_ = mds_mgr;
    }
  }
  return ret;
}

void MdsTableFreezeGuard::reset()
{
  can_freeze_ = false;
  if (OB_NOT_NULL(mds_mgr_)) {
    ATOMIC_STORE(&(mds_mgr_->is_freezing_), false);
    mds_mgr_ = nullptr;
  }
}

MdsTableMgrHandle::MdsTableMgrHandle()
  : mgr_(nullptr)
{
}

MdsTableMgrHandle::~MdsTableMgrHandle()
{
  reset();
}

int MdsTableMgrHandle::reset()
{
  if (OB_NOT_NULL(mgr_)) {
    mgr_->dec_ref();
  }
  mgr_ = nullptr;
  return OB_SUCCESS;
}

int MdsTableMgrHandle::set_mds_table_mgr(ObMdsTableMgr *mds_table_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mds_table_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(mds_table_mgr), K(lbt()));
  } else {
    reset();
    mgr_ = mds_table_mgr;
    mgr_->inc_ref();
  }
  return ret;
}

}  // namespace mds
}  // namespace storage
}  // namespace oceanbase
