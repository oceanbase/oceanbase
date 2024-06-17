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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_p2p_dh_msg.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_rpc_proxy.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_rpc_process.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_mgr.h"
#include "share/detect/ob_detect_manager_utils.h"
#include "sql/engine/px/p2p_datahub/ob_runtime_filter_query_range.h"
using namespace oceanbase;
using namespace common;
using namespace sql;

DEFINE_ENUM_FUNC(ObP2PDatahubMsgBase::ObP2PDatahubMsgType, p2p_datahub_msg_type,
                 P2P_DATAHUB_MSG_TYPE, ObP2PDatahubMsgBase::);

OB_SERIALIZE_MEMBER(ObP2PDatahubMsgBase,
    trace_id_, p2p_datahub_id_, px_sequence_id_,
    task_id_, tenant_id_, timeout_ts_, msg_type_,
    msg_receive_cur_cnt_, msg_receive_expect_cnt_,
    is_active_, is_empty_, register_dm_info_);

int ObP2PDatahubMsgBase::broadcast(
    ObIArray<ObAddr> &target_addrs,
    obrpc::ObP2PDhRpcProxy &p2p_dh_proxy)
{
  int ret = OB_SUCCESS;
  ObPxP2PDatahubArg arg;
  arg.msg_ = this;
  for (int i = 0; i < target_addrs.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(p2p_dh_proxy.
        to(target_addrs.at(i)).
        by(tenant_id_).
        timeout(timeout_ts_).
        send_p2p_dh_message(arg, nullptr))) {
      LOG_WARN("fail to send p2p2 dh msg", K(ret));
    }
  }
  return ret;
}

int ObP2PDatahubMsgBase::init(int64_t p2p_dh_id,
    int64_t px_sequence_id, int64_t task_id,
    int64_t tenant_id, int64_t timeout_ts,
    const ObRegisterDmInfo &register_dm_info)
{
  int ret = OB_SUCCESS;
  trace_id_ = *ObCurTraceId::get_trace_id();
  p2p_datahub_id_ = p2p_dh_id;
  px_sequence_id_ = px_sequence_id;
  task_id_ = task_id;
  tenant_id_ = tenant_id;
  timeout_ts_ = timeout_ts;
  is_active_ = true;
  is_ready_ = false;
  is_empty_ = true;
  allocator_.set_tenant_id(tenant_id);
  allocator_.set_label("ObP2PDHMsg");
  register_dm_info_ = register_dm_info;
  return ret;
}

int ObP2PDatahubMsgBase::assign(const ObP2PDatahubMsgBase &msg)
{
  int ret = OB_SUCCESS;
  trace_id_ = msg.get_trace_id();
  p2p_datahub_id_ = msg.get_p2p_datahub_id();
  px_sequence_id_ = msg.get_px_seq_id();
  task_id_ = msg.get_task_id();
  tenant_id_ = msg.get_tenant_id();
  timeout_ts_ = msg.get_timeout_ts();
  msg_type_ = msg.get_msg_type();
  is_active_ = msg.is_active();
  is_ready_ = msg.check_ready();
  is_empty_ = msg.is_empty();
  msg_receive_cur_cnt_ = msg.get_msg_receive_cur_cnt();
  msg_receive_expect_cnt_ = msg.get_msg_receive_expect_cnt();
  allocator_.set_tenant_id(tenant_id_);
  allocator_.set_label("ObP2PDHMsg");
  register_dm_info_ = msg.register_dm_info_;
  return ret;
}

int ObP2PDatahubMsgBase::process_receive_count(ObP2PDatahubMsgBase &msg)
{
  int ret = OB_SUCCESS;
  CK(msg.get_msg_receive_expect_cnt() > 0 && msg_receive_expect_cnt_ > 0);
  if (OB_SUCC(ret)) {
    int64_t cur_cnt = ATOMIC_AAF(&msg_receive_cur_cnt_, msg.get_msg_receive_cur_cnt());
    if (cur_cnt > msg_receive_expect_cnt_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected receive count", K(ret));
    }
    check_finish_receive();
  }
  return ret;
}

void ObP2PDatahubMsgBase::check_finish_receive()
{
  if (msg_receive_expect_cnt_ == ATOMIC_LOAD(&msg_receive_cur_cnt_)) {
    (void)after_process();
    is_ready_ = true;
  }
}

int ObP2PDatahubMsgBase::process_msg_internal(bool &need_free)
{
  int ret = OB_SUCCESS;
  ObP2PDhKey dh_key(p2p_datahub_id_, px_sequence_id_, task_id_);
  ObP2PDatahubManager::P2PMsgSetCall set_call(dh_key, *this);
  ObP2PDatahubManager::P2PMsgMergeCall merge_call(*this);
  ObP2PDatahubManager::MsgMap &map = PX_P2P_DH.get_map();
  start_time_ = ObTimeUtility::current_time();
  ObP2PDatahubMsgGuard guard(this);

  bool need_merge = true;
  // to avoid data racing in the process of check_finish_receive, protect it in hashmap lock
  // if set succ, check_finish_receive() in P2PMsgSetCall
  // if set failed(with OB_HASH_EXIST, need merge), check_finish_receive() in P2PMsgMergeCall
  if (OB_FAIL(map.set_refactored(dh_key, this, 0/*flag*/, 0/*broadcast*/, 0/*overwrite_key*/, &set_call))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to set refactored", K(ret));
    }
    need_free = true;
  } else {
    need_merge = false; // set success, not need to merge
    int reg_dm_ret = OB_SUCCESS;
#ifdef ERRSIM
    reg_dm_ret = OB_E(EventTable::EN_PX_P2P_MSG_REG_DM_FAILED) OB_SUCCESS;
    if (OB_SUCCESS != reg_dm_ret) {
      LOG_WARN("p2p msg reg dm failed by design", K(ret));
      reg_dm_ret = OB_ALLOCATE_MEMORY_FAILED;
    }
#endif
    if (OB_SUCCESS == reg_dm_ret) {
      reg_dm_ret = ObDetectManagerUtils::p2p_datahub_register_check_item_into_dm(
          register_dm_info_, dh_key, dm_cb_node_seq_id_);
    }
    if (OB_SUCCESS != reg_dm_ret) {
      LOG_WARN("[DM] failed to register check item to dm", K(reg_dm_ret));
    }
  }

  // merge filter
  if (OB_SUCC(ret) && need_merge) {
    if (OB_FAIL(map.atomic_refactored(dh_key, merge_call))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("fail to merge p2p dh msg", K(ret));
      }
    }
  }

  if (need_free) {
    // msg not in map, dec ref count
    guard.dec_msg_ref_count();
  }
  return ret;
}

template <>
int ObP2PDatahubMsgBase::proc_filter_empty<IntegerFixedVec>(IntegerFixedVec *res_vec,
                                                            const ObBitVector &skip,
                                                            const EvalBound &bound,
                                                            int64_t &total_count,
                                                            int64_t &filter_count)
{
  int ret = OB_SUCCESS;
  uint64_t *data = reinterpret_cast<uint64_t *>(res_vec->get_data());
  MEMSET(data + bound.start(), 0, (bound.range_size() * res_vec->get_length(0)));

  int64_t valid_cnt = bound.range_size() - skip.accumulate_bit_cnt(bound);
  total_count += valid_cnt;
  filter_count += valid_cnt;
  return ret;
}

template <>
int ObP2PDatahubMsgBase::proc_filter_empty<IntegerUniVec>(IntegerUniVec *res_vec,
                                                          const ObBitVector &skip,
                                                          const EvalBound &bound,
                                                          int64_t &total_count,
                                                          int64_t &filter_count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBitVector::flip_foreach(
          skip, bound, [&](int64_t idx) __attribute__((always_inline)) {
            res_vec->set_int(idx, 0);
            ++filter_count;
            ++total_count;
            return OB_SUCCESS;
          }))) {
    LOG_WARN("fail to do for each operation", K(ret));
  }
  return ret;
}

int ObP2PDatahubMsgBase::preset_not_match(IntegerFixedVec *res_vec, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  uint64_t *data = reinterpret_cast<uint64_t *>(res_vec->get_data());
  MEMSET(data + bound.start(), 0, (bound.range_size() * res_vec->get_length(0)));
  return ret;
}

int ObP2PDatahubMsgBase::fill_empty_query_range(const ObPxQueryRangeInfo &query_range_info,
                             common::ObIAllocator &allocator, ObNewRange &query_range)
{
  int ret = OB_SUCCESS;
  query_range.table_id_ = query_range_info.table_id_;

  ObObj *start = NULL;
  ObObj *end = NULL;
  int64_t range_column_cnt = query_range_info.range_column_cnt_;
  if (OB_ISNULL(start = static_cast<ObObj *>(
                    allocator.alloc(sizeof(ObObj) * range_column_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory for start_obj failed", K(ret));
  } else if (OB_ISNULL(end = static_cast<ObObj *>(
                           allocator.alloc(sizeof(ObObj) * range_column_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory for end_obj failed", K(ret));
  } else {
    // fill all coloumns with (max, min)
    for (int64_t i = 0; i < range_column_cnt; ++i) {
      new (start + i) ObObj();
      new (end + i) ObObj();
      (start + i)->set_max_value();
      (end + i)->set_min_value();
    }
    ObRowkey start_key(start, range_column_cnt);
    ObRowkey end_key(end, range_column_cnt);
    query_range.start_key_ = start_key;
    query_range.end_key_ = end_key;
  }
  return ret;
}

ObP2PDatahubMsgGuard::ObP2PDatahubMsgGuard(ObP2PDatahubMsgBase *msg) : msg_(msg)
{
  // one for dh map hold msg and one for we use msg to reg dm
  msg->inc_ref_count(2);
}

ObP2PDatahubMsgGuard::~ObP2PDatahubMsgGuard()
{
  dec_msg_ref_count();
}

void ObP2PDatahubMsgGuard::release()
{
  msg_ = nullptr;
}

void ObP2PDatahubMsgGuard::dec_msg_ref_count()
{
  if (OB_NOT_NULL(msg_)) {
    msg_->dec_ref_count();
  }
}
