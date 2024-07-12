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
#include "storage/multi_data_source/mds_table_order_flusher.h"
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
    MDS_LOG(INFO, "register to mds table mgr success", KR(ret), KPC(p_mds_table));
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

template <typename Flusher>
struct OrderOp {
  OrderOp(Flusher &flusher) : flusher_(flusher) {}
  int operator()(MdsTableBase &mds_table) {
    if (!mds_table.is_switched_to_empty_shell()) {
      share::SCN rec_scn = mds_table.get_rec_scn();
      flusher_.record_mds_table({mds_table.get_tablet_id(), rec_scn});
    }
    return OB_SUCCESS;
  }
  Flusher &flusher_;
};
int ObMdsTableMgr::flush(SCN recycle_scn, int64_t trace_id, bool need_freeze)
{
  #define PRINT_WRAPPER KR(ret), K(ls_->get_ls_id()), K(recycle_scn), K(need_freeze), K(order_flusher_for_some),\
                        K(max_consequent_callbacked_scn), K(*this)
  MDS_TG(10_s);
  int ret = OB_SUCCESS;
  MdsTableFreezeGuard freeze_guard;
  freeze_guard.init(this);
  int64_t flushed_table_cnt = 0;
  MdsTableHandle *flushing_mds_table = nullptr;
  share::SCN max_consequent_callbacked_scn;
  FlusherForSome order_flusher_for_some;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    MDS_LOG_FREEZE(ERROR, "mds table mgr not inited");
  } else if (!freeze_guard.can_freeze()) {
    MDS_LOG_FREEZE(INFO, "mds table mgr is doing flush, skip flush once");
  } else if (MDS_FAIL(for_each_in_t3m_mds_table(OrderOp<FlusherForSome>(order_flusher_for_some)))) {
    MDS_LOG_FREEZE(WARN, "do first scan failed");
  } else if (order_flusher_for_some.empty()) {// no mds table
    MDS_LOG_FREEZE(INFO, "no valid mds table there, no need do flush");
  } else if (MDS_FAIL(ls_->get_max_decided_scn(max_consequent_callbacked_scn))) {
    MDS_LOG_FREEZE(WARN, "fail to get max_consequent_callbacked_scn", KR(ret), K(*this));
  } else if (!max_consequent_callbacked_scn.is_valid() || max_consequent_callbacked_scn.is_max() || max_consequent_callbacked_scn.is_min()) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_FREEZE(WARN, "invalid max_consequent_callbacked_scn", KR(ret), K(*this));
  } else {
    if (need_freeze) {// need advance freezing scn
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
    if (order_flusher_for_some.min_key().rec_scn_ <= freezing_scn_) {
      order_flush_(order_flusher_for_some, freezing_scn_, max_consequent_callbacked_scn, trace_id);
    } else {
      MDS_LOG_FREEZE(INFO, "no need do flush cause min_rec_scn is larger than freezing scn");
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

void ObMdsTableMgr::order_flush_(FlusherForSome &order_flusher_for_some,
                                 share::SCN freezing_scn,
                                 share::SCN max_consequent_callbacked_scn,
                                 int64_t trace_id)
{
  #define PRINT_WRAPPER KR(ret), K(ls_->get_ls_id()), K(freezing_scn), K(order_flusher_for_some),\
                        K(third_sacn_mds_table_cnt), K(max_consequent_callbacked_scn), K(order_flusher_for_all.count()),\
                        K(*this)
  MDS_TG(10_s);
  int ret = OB_SUCCESS;
  int64_t third_sacn_mds_table_cnt = 0;
  FlusherForAll order_flusher_for_all;
  FlushOp flush_op(freezing_scn, third_sacn_mds_table_cnt, max_consequent_callbacked_scn, trace_id);
  if (!order_flusher_for_some.full() || order_flusher_for_some.max_key().rec_scn_ > freezing_scn_) {
    // than means all mds_tables needed be flushed is included in order_flusher_for_some
    order_flusher_for_some.flush_by_order(mds_table_map_, freezing_scn_, max_consequent_callbacked_scn);
    MDS_LOG_FREEZE(INFO, "flush all mds_tables(little number) by total order");
  } else {
    order_flusher_for_all.reserve_memory(mds_table_map_.count());
    if (MDS_FAIL(for_each_in_t3m_mds_table(OrderOp<FlusherForAll>(order_flusher_for_all)))) {// second scan
      MDS_LOG_FREEZE(WARN, "do scan failed");
    }
    order_flusher_for_all.flush_by_order(mds_table_map_, freezing_scn_, max_consequent_callbacked_scn);
    if (order_flusher_for_all.incomplete()) {// need do third scan
      if (MDS_FAIL(mds_table_map_.for_each(flush_op))) {// third scan
        MDS_LOG_FREEZE(WARN, "do scan failed");
      } else {
        MDS_LOG_FREEZE(INFO, "flush some mds_tables by order, and others out of order");
      }
    } else {
      MDS_LOG_FREEZE(INFO, "flush all mds_tables(large number) by total order");
    }
  }
  #undef PRINT_WRAPPER
}

SCN ObMdsTableMgr::get_freezing_scn() const { return freezing_scn_.atomic_get(); }

SCN ObMdsTableMgr::get_rec_scn()
{
  ObTabletID tablet_id;
  return get_rec_scn(tablet_id);
}

SCN ObMdsTableMgr::get_rec_scn(ObTabletID &tablet_id)
{
  #define PRINT_WRAPPER KR(ret), K(min_rec_scn), K(order_flusher_for_one), K(*this)
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  SCN min_rec_scn = share::SCN::max_scn();
  FlusherForOne order_flusher_for_one;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    MDS_LOG(ERROR, "regsiter mds table failed", KR(ret));
  } else if (MDS_FAIL(for_each_in_t3m_mds_table(OrderOp<FlusherForOne>(order_flusher_for_one)))) {
    min_rec_scn.set_min();
    MDS_LOG_FREEZE(WARN, "do first scan failed");
  } else if (order_flusher_for_one.empty()) {// no mds table
    MDS_LOG_FREEZE(INFO, "no valid mds table there, return MAX SCN");
  } else {
    FlushKey key = order_flusher_for_one.min_key();
    min_rec_scn = key.rec_scn_;
    tablet_id = key.tablet_id_;
    MDS_LOG_FREEZE(INFO, "get rec_scn from MdsTableMgr");
  }
  return min_rec_scn;
  #undef PRINT_WRAPPER
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

DEF_TO_STRING(ObMdsTableMgr)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K_(is_inited), K_(freezing_scn), KPC_(ls));
  J_OBJ_END();
  return pos;
}

}  // namespace mds
}  // namespace storage
}  // namespace oceanbase
