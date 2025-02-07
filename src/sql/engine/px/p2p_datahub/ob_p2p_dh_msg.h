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
#ifndef __SQL_ENG_P2P_DH_MSG_H__
#define __SQL_ENG_P2P_DH_MSG_H__
#include "lib/ob_define.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_iarray.h"
#include "lib/allocator/page_arena.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_rpc_proxy.h"
#include "sql/engine/expr/ob_expr_join_filter.h"
#include "share/detect/ob_detectable_id.h"


namespace oceanbase
{
namespace sql
{

class ObBatchRows;
class ObPxQueryRangeInfo;
class ObP2PDatahubMsgBase
{
  OB_UNIS_VERSION_V(1);
public:
#define P2P_DATAHUB_MSG_TYPE(ACT)                                                                  \
  ACT(NOT_INIT, = 0)                                                                               \
  ACT(BLOOM_FILTER_MSG, )                                                                          \
  ACT(RANGE_FILTER_MSG, )                                                                          \
  ACT(IN_FILTER_MSG, )                                                                             \
  ACT(BLOOM_FILTER_VEC_MSG, )                                                                      \
  ACT(RANGE_FILTER_VEC_MSG, )                                                                      \
  ACT(IN_FILTER_VEC_MSG, )                                                                         \
  ACT(PD_TOPN_FILTER_MSG, )                                                                           \
  ACT(MAX_TYPE, )

  DECLARE_ENUM(ObP2PDatahubMsgType, p2p_datahub_msg_type, P2P_DATAHUB_MSG_TYPE, static);

static int transform_vec_p2p_msg_type(const ObP2PDatahubMsgType &in_type, ObP2PDatahubMsgType &out_type) {
  int ret = OB_SUCCESS;
  switch (in_type) {
  case BLOOM_FILTER_MSG :
  {
    out_type = BLOOM_FILTER_VEC_MSG;
    break;
  }
  case RANGE_FILTER_MSG :
  {
    out_type = RANGE_FILTER_VEC_MSG;
    break;
  }
  case IN_FILTER_MSG:
  {
    out_type = IN_FILTER_VEC_MSG;
    break;
  }
  default:
    out_type = in_type;
    break;
  }
  return ret;
}

public:
  ObP2PDatahubMsgBase() : trace_id_(), p2p_datahub_id_(OB_INVALID_ID),
      px_sequence_id_(OB_INVALID_ID), task_id_(OB_INVALID_ID),
      tenant_id_(OB_INVALID_ID), timeout_ts_(0),
      start_time_(0), msg_type_(NOT_INIT),
      lock_(), allocator_(), msg_receive_cur_cnt_(0),
      msg_receive_expect_cnt_(0), is_active_(true),
      is_ready_(false), is_empty_(true), ref_count_(0),
      register_dm_info_(), dm_cb_node_seq_id_(0) {}
  virtual ~ObP2PDatahubMsgBase() {}

  // this interface will be used both in send and receive process, ensure copy all
  // members that need to been serialize.
  virtual int assign(const ObP2PDatahubMsgBase &);
  virtual int merge(ObP2PDatahubMsgBase &) = 0;
  virtual int deep_copy_msg(ObP2PDatahubMsgBase *&new_msg_ptr) = 0;
  virtual int broadcast(
      ObIArray<ObAddr> &target_addrs,
      obrpc::ObP2PDhRpcProxy &p2p_dh_proxy);
  virtual int might_contain(const ObExpr &expr,
      ObEvalCtx &ctx,
      ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx,
      ObDatum &res)
  { return OB_SUCCESS; }
  virtual int might_contain_batch(
      const ObExpr &expr,
      ObEvalCtx &ctx,
      const ObBitVector &skip,
      const int64_t batch_size,
      ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx)
  { return OB_SUCCESS; }
  virtual int might_contain_vector(
      const ObExpr &expr,
      ObEvalCtx &ctx,
      const ObBitVector &skip,
      const EvalBound &bound,
      ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx)
  { return OB_SUCCESS; }
  virtual int insert_by_row(
      const common::ObIArray<ObExpr *> &expr_array,
      const common::ObHashFuncs &hash_funcs_,
      const ObExpr *calc_tablet_id_expr,
    ObEvalCtx &eval_ctx)
  { return OB_SUCCESS; }
  virtual int insert_by_row_batch(
      const ObBatchRows *child_brs,
      const common::ObIArray<ObExpr *> &expr_array,
      const common::ObHashFuncs &hash_funcs,
      const ObExpr *calc_tablet_id_expr,
      ObEvalCtx &eval_ctx,
      uint64_t *batch_hash_values)
  { return OB_SUCCESS; }
  virtual int insert_by_row_vector(
      const ObBatchRows *child_brs,
      const common::ObIArray<ObExpr *> &expr_array,
      const common::ObHashFuncs &hash_funcs,
      const ObExpr *calc_tablet_id_expr,
      ObEvalCtx &eval_ctx,
      uint64_t *batch_hash_values)
  { return OB_SUCCESS; }
  virtual void after_process() {}
  virtual int try_extract_query_range(bool &has_extract, ObIArray<ObNewRange> &ranges,
                                      bool need_deep_copy = false,
                                      common::ObIAllocator *allocator = nullptr)
  {
    return OB_SUCCESS;
  }
  virtual int destroy() = 0;
  virtual int process_receive_count(ObP2PDatahubMsgBase &);
  virtual int process_msg_internal(bool &need_free);
  virtual int reuse() { return OB_SUCCESS; }
  virtual int regenerate() { return OB_SUCCESS; }
  virtual void check_finish_receive();
  virtual int prepare_storage_white_filter_data(ObDynamicFilterExecutor &dynamic_filter,
                                ObEvalCtx &eval_ctx,
                                ObRuntimeFilterParams &params,
                                bool &is_data_prepared) { return OB_SUCCESS; }
  bool check_ready() const { return is_ready_; }
  ObP2PDatahubMsgType get_msg_type() const { return msg_type_; }
  void set_msg_type(ObP2PDatahubMsgType type) { msg_type_ = type; }
  int64_t get_p2p_datahub_id() const { return p2p_datahub_id_; }
  int64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_timeout_ts() const { return timeout_ts_; }
  void reset_status() {
    is_active_ = true;
    is_ready_ = false;
    is_empty_ = true;
  }
  bool is_active() const { return is_active_; }
  void set_is_active(bool flag) { is_active_ = flag; }
  bool is_empty() const { return is_empty_; }
  int init(int64_t p2p_dh_id, int64_t px_seq_id, int64_t task_id,
      int64_t tenant_id, int64_t timeout_ts, const ObRegisterDmInfo &register_dm_info);
  common::ObIAllocator &get_allocator() { return allocator_; }
  int64_t get_task_id() const { return task_id_; }
  void set_is_ready(bool flag) { is_ready_ = flag; }
  int64_t get_msg_receive_expect_cnt() const { return msg_receive_expect_cnt_;};
  int64_t get_msg_receive_cur_cnt() const { return msg_receive_cur_cnt_; }
  void set_msg_cur_cnt(int64_t cnt) { msg_receive_cur_cnt_ = cnt; }
  void set_msg_expect_cnt(int64_t cnt) { msg_receive_expect_cnt_ = cnt; }
  bool is_valid_type() { return NOT_INIT < msg_type_ < MAX_TYPE; }
  common::ObCurTraceId::TraceId get_trace_id() const { return trace_id_; }
  int64_t get_start_time() const { return start_time_; }
  int64_t get_px_seq_id() const { return px_sequence_id_; }
  // in the following two scenes that ref_count_ should been increased
  // 1. if it's a shared msg, and insert into PX_P2P_DH by rpc thread (or only one server int the p2pmap, insert by local thread)
  // 2. someone use PX_P2P_DH.atomic_get_msg to get msg ptr
  void inc_ref_count(int64_t count=1) { ATOMIC_AAF(&ref_count_, count); }
  // appear in pairs with increase
  int64_t dec_ref_count() { return ATOMIC_SAF(&ref_count_, 1); }
  int64_t cas_ref_count(int64_t expect, int64_t new_val) { return ATOMIC_CAS(&ref_count_, expect, new_val); }
  const ObRegisterDmInfo &get_register_dm_info() { return register_dm_info_; }
  uint64_t &get_dm_cb_node_seq_id() { return dm_cb_node_seq_id_; }
  template <typename ResVec>
  int proc_filter_empty(ResVec *res_vec, const ObBitVector &skip, const EvalBound &bound,
                      int64_t &total_count, int64_t &filter_count);
  int preset_not_match(IntegerFixedVec *res_vec, const EvalBound &bound);
  TO_STRING_KV(K(p2p_datahub_id_), K_(px_sequence_id), K(tenant_id_), K(timeout_ts_), K(is_active_), K(msg_type_));
protected:
  int fill_empty_query_range(const ObPxQueryRangeInfo &query_range_info,
                             common::ObIAllocator &allocator, ObNewRange &query_range);

protected:
  common::ObCurTraceId::TraceId trace_id_;
  int64_t p2p_datahub_id_;
  int64_t px_sequence_id_;
  int64_t task_id_;
  int64_t tenant_id_;
  int64_t timeout_ts_;
  int64_t start_time_;
  ObP2PDatahubMsgType msg_type_;
  mutable common::ObSpinLock lock_;
  common::ObArenaAllocator allocator_;
  int64_t msg_receive_cur_cnt_;
  int64_t msg_receive_expect_cnt_;
  bool is_active_; //only for ObRFInFilterMsg, when NDV>1024, set is_active_ = false;
  bool is_ready_;
  bool is_empty_;
  int64_t ref_count_;
  ObRegisterDmInfo register_dm_info_;
  uint64_t dm_cb_node_seq_id_;
  DISALLOW_COPY_AND_ASSIGN(ObP2PDatahubMsgBase);
};

// guard for set msg into PX_P2P_DH map and register into dm
struct ObP2PDatahubMsgGuard
{
  ObP2PDatahubMsgGuard(ObP2PDatahubMsgBase *msg);
  ~ObP2PDatahubMsgGuard();
  void dec_msg_ref_count();
  void release();
  ObP2PDatahubMsgBase *msg_;
};

template <typename ResVec>
static int proc_filter_not_active(ResVec *res_vec, const ObBitVector &skip, const EvalBound &bound);

template <>
int proc_filter_not_active<IntegerUniVec>(IntegerUniVec *res_vec, const ObBitVector &skip,
                                          const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBitVector::flip_foreach(
          skip, bound, [&](int64_t idx) __attribute__((always_inline)) {
            res_vec->set_int(idx, 1);
            return OB_SUCCESS;
          }))) {
    SQL_LOG(WARN, "fail to do for each operation", K(ret));
  }
  return ret;
}

template <>
int proc_filter_not_active<IntegerFixedVec>(IntegerFixedVec *res_vec, const ObBitVector &skip,
                                            const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  uint64_t *data = reinterpret_cast<uint64_t *>(res_vec->get_data());
  MEMSET(data + bound.start(), 1, (bound.range_size() * res_vec->get_length(0)));
  return ret;
}

}
}


#endif
