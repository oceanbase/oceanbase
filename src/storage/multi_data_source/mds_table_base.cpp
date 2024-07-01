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
#include "mds_table_base.h"
#include "lib/lock/ob_small_spin_lock.h"
#include "lib/ob_errno.h"
#include "lib/profile/ob_trace_id.h"
#include "ob_clock_generator.h"
#include "share/scn.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/multi_data_source/ob_mds_table_merge_dag_param.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{

TLOCAL(MdsTLocalInfo, TLOCAL_MDS_INFO);

int MdsTableBase::advance_state_to(State new_state) const
{
  int ret = OB_SUCCESS;
  MDS_TG(1_ms);
  bool success = false;
  while (!success && OB_SUCC(ret)) {
    State old_state = ATOMIC_LOAD(&state_);
    if (new_state < old_state) {// no need advance
      break;
    } else if (!StateChecker[old_state][new_state]) {
      ret = OB_STATE_NOT_MATCH;
      MDS_LOG(WARN, "not allow switch mds table state", KR(ret), K(*this),
                     K(state_to_string(old_state)), K(state_to_string(new_state)));
    } else {
      success = ATOMIC_BCAS(&state_, old_state, new_state);
    }
  }
  return ret;
}

int MdsTableBase::init(const ObTabletID tablet_id,
                       const share::ObLSID ls_id,
                       ObTabletPointer *pointer,
                       ObMdsTableMgr *p_mgr)
{
  int ret = OB_SUCCESS;
  MDS_TG(1_ms);
  if (!ls_id.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG(ERROR, "invalid argument", KR(ret), K(*this), K(ls_id), K(tablet_id));
  } else if (MDS_FAIL(advance_state_to(State::INIT))) {
    MDS_LOG(ERROR, "mds table maybe init twice", KR(ret), K(*this), K(ls_id), K(tablet_id));
  } else {
    tablet_id_ = tablet_id;
    ls_id_ = ls_id;
    if (OB_NOT_NULL(p_mgr)) {
      mgr_handle_.set_mds_table_mgr(p_mgr);
      debug_info_.do_init_tablet_pointer_ = pointer;
      debug_info_.init_trace_id_ = *ObCurTraceId::get_trace_id();
      debug_info_.init_ts_ = ObClockGenerator::getClock();
      if (MDS_FAIL(register_to_mds_table_mgr())) {
        MDS_LOG(WARN, "fail to register mds table", KR(ret), K(*this), K(ls_id), K(tablet_id));
      }
    }
  }
  return ret;
}

int MdsTableBase::register_to_mds_table_mgr()
{
  int ret = OB_SUCCESS;
  MDS_TG(1_ms);
  if (OB_ISNULL(mgr_handle_.get_mds_table_mgr())) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG(WARN, "mds_table_mgr ptr is null", KR(ret), K(*this));
  } else if (MDS_FAIL(mgr_handle_.get_mds_table_mgr()->register_to_mds_table_mgr(this))) {
    MDS_LOG(WARN, "fail to register mds table", KR(ret), K(*this));
  } else {
    report_construct_event_();
  }
  return ret;
}

void MdsTableBase::mark_removed_from_t3m(ObTabletPointer *pointer)
{
  MDS_TG(1_ms);
  int ret = OB_SUCCESS;
  if (ATOMIC_LOAD(&debug_info_.remove_ts_) != 0) {
    MDS_LOG_RET(WARN, OB_ERR_UNEXPECTED, "this MdsTable has been marked removed", K(*this));
  } else if (MDS_FAIL(unregister_from_mds_table_mgr())) {
    MDS_LOG(WARN, "unregister from mds_table_mgr failed", KR(ret), K(*this));
  } else {
    debug_info_.do_remove_tablet_pointer_ = pointer;
    debug_info_.remove_trace_id_ = *ObCurTraceId::get_trace_id();
    ATOMIC_STORE(&debug_info_.remove_ts_, ObClockGenerator::getClock());
  }
}

void MdsTableBase::mark_switched_to_empty_shell()
{
  if (ATOMIC_LOAD(&debug_info_.switch_to_empty_shell_ts_) != 0) {
    MDS_LOG_RET(WARN, OB_ERR_UNEXPECTED, "this MdsTable has been marked switch to empty shell", K(*this));
  } else {
    ATOMIC_STORE(&debug_info_.switch_to_empty_shell_ts_, ObClockGenerator::getClock());
  }
}

bool MdsTableBase::is_switched_to_empty_shell() const
{
  return ATOMIC_LOAD(&debug_info_.switch_to_empty_shell_ts_) != 0;
}

bool MdsTableBase::is_removed_from_t3m() const
{
  return ATOMIC_LOAD(&debug_info_.remove_ts_) != 0;
}

int64_t MdsTableBase::get_removed_from_t3m_ts() const
{
  return ATOMIC_LOAD(&debug_info_.remove_ts_);
}

int MdsTableBase::unregister_from_mds_table_mgr()
{
  int ret = OB_SUCCESS;
  MDS_TG(1_ms);
  if (!mgr_handle_.is_valid()) {
    MDS_LOG(INFO, "no need unregister from mds_table_mgr cause invalid mds_table_mgr", KR(ret), K(*this));
  } else if (!ls_id_.is_valid() || !tablet_id_.is_valid()) {
    MDS_LOG(INFO, "no need unregister from mds_table_mgr cause invalid id", KR(ret), K(*this));
  } else if (MDS_FAIL(mgr_handle_.get_mds_table_mgr()->unregister_from_mds_table_mgr(this))) {
    MDS_LOG(ERROR, "fail to unregister mds table", K(*this));
  } else {
    report_destruct_event_();
  }
  return ret;
}

int MdsTableBase::unregister_from_removed_recorder()
{
  int ret = OB_SUCCESS;
  MDS_TG(1_ms);
  if (!mgr_handle_.is_valid()) {
    MDS_LOG(INFO, "no need unregister from mds_table_mgr cause invalid mds_table_mgr", KR(ret), K(*this));
  } else if (!ls_id_.is_valid() || !tablet_id_.is_valid()) {
    MDS_LOG(INFO, "no need unregister from mds_table_mgr cause invalid id", KR(ret), K(*this));
  } else {
    mgr_handle_.get_mds_table_mgr()->unregister_from_removed_mds_table_recorder(this);
    mgr_handle_.reset();
  }
  return ret;
}

int MdsTableBase::get_ls_max_consequent_callbacked_scn_(share::SCN &max_consequent_callbacked_scn) const
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle ls_handle;
  MDS_TG(1_ms);
  if (!ls_id_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG(WARN, "ls id not valid", KR(ret), K(*this));
  } else if (OB_ISNULL(ls_service)) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG(WARN, "ls tx service is null", KR(ret), K(*this));
  } else if (MDS_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::MDS_TABLE_MOD))) {
    MDS_LOG(WARN, "fail to get ls handle", KR(ret), K(*this));
  } else if (MDS_FAIL(ls_handle.get_ls()->get_freezer()->get_max_consequent_callbacked_scn(max_consequent_callbacked_scn))) {
    MDS_LOG(WARN, "fail to get max_consequent_callbacked_scn", KR(ret), K(*this));
  }
  return ret;
}

int MdsTableBase::merge(const int64_t construct_sequence, const share::SCN &flushing_scn)
{
  int ret = OB_SUCCESS;
  ObMdsTableMergeDagParam param;
  param.ls_id_ = ls_id_;
  param.tablet_id_ = tablet_id_;
  param.flush_scn_ = flushing_scn;
  param.mds_construct_sequence_ = construct_sequence;
  param.generate_ts_ = ObClockGenerator::getClock();
  param.merge_type_ = compaction::ObMergeType::MDS_MINI_MERGE;
  param.merge_version_ = 0;
  if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_mds_table_merge_dag(param))) {
    if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
      MDS_LOG(WARN, "failed to schedule mds table merge dag", K(ret), K(param));
    }
  } else {
    MDS_LOG(DEBUG, "succeeded to schedule mds table merge dag", K(ret), K(param));
  }
  return ret;
}

int64_t MdsTableBase::get_node_cnt() const
{
  int64_t total_cnt = ATOMIC_LOAD(&total_node_cnt_);
  if (total_cnt < 0) {
    MDS_LOG_RET(ERROR, OB_ERR_SYS, "total_valid_node_cnt_ is less than 0", KP(this), K(total_cnt));
  }
  return total_cnt;
}

void MdsTableBase::inc_valid_node_cnt()
{
  if (ATOMIC_AAF(&total_node_cnt_, 1) <= 0) {
    MDS_LOG_RET(ERROR, OB_ERR_SYS, "total_valid_node_cnt_ is less than 0", KP(this), K(total_node_cnt_));
  }
}

void MdsTableBase::dec_valid_node_cnt()
{
  if (ATOMIC_AAF(&total_node_cnt_, -1) < 0) {
    MDS_LOG_RET(ERROR, OB_ERR_SYS, "total_valid_node_cnt_ is less than 0", KP(this), K(total_node_cnt_));
  }
}

void MdsTableBase::try_advance_rec_scn(const share::SCN scn)
{
  bool success = false;
  while (!success) {
    share::SCN old_scn = rec_scn_;
    if (scn > old_scn) {
      success = rec_scn_.atomic_bcas(old_scn, scn);
      if (success) {
        report_rec_scn_event_("ADVANCE_REC_SCN", old_scn, scn);
      }
    } else {
      break;
    }
  }
}

void MdsTableBase::try_decline_rec_scn(const share::SCN scn)
{
  bool success = false;
  while (!success) {
    share::SCN old_scn = rec_scn_;
    if (scn < old_scn) {
      success = rec_scn_.atomic_bcas(old_scn, scn);
      if (success) {
        report_rec_scn_event_("DECLINE_REC_SCN", old_scn, scn);
      }
    } else {
      break;
    }
  }
}

common::ObTabletID MdsTableBase::get_tablet_id() const
{
  return tablet_id_;
}

share::ObLSID MdsTableBase::get_ls_id() const
{
  return ls_id_;
}

share::SCN MdsTableBase::get_rec_scn()
{
  return rec_scn_.atomic_get();
}

bool MdsTableBase::is_flushing() const
{
  MdsRLockGuard lg(lock_);
  return flushing_scn_.is_valid();
}

bool check_node_scn_beflow_flush(const MdsNode &node, const share::SCN &flush_scn)
{
  bool need_dump = false;
  if (node.end_scn_ <= flush_scn) {// change to redo_scn <= flush_scn after support dump uncommitted node
    need_dump = true;
  }
  return need_dump;
}

}  // namespace mds
}  // namespace storage
}  // namespace oceanbase
