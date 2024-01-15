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
#ifndef __SQL_ENG_P2P_RUNTIME_FILTER_DH_MSG_H__
#define __SQL_ENG_P2P_RUNTIME_FILTER_DH_MSG_H__
#include "lib/ob_define.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_array.h"
#include "share/datum/ob_datum.h"
#include "sql/engine/px/ob_px_bloom_filter.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_msg.h"
#include "sql/engine/px/p2p_datahub/ob_runtime_filter_query_range.h"


namespace oceanbase
{
namespace sql
{

class ObP2PDatahubMsgBase;

class ObRFBloomFilterMsg final : public ObP2PDatahubMsgBase
{
  OB_UNIS_VERSION_V(1);
public:
  enum ObSendBFPhase
  {
    FIRST_LEVEL,
    SECOND_LEVEL
  };
  ObRFBloomFilterMsg() : phase_(), bloom_filter_(),
      next_peer_addrs_(allocator_), expect_first_phase_count_(0),
      piece_size_(0), filter_indexes_(allocator_), receive_count_array_(allocator_),
      filter_idx_(nullptr), create_finish_(nullptr), need_send_msg_(true), is_finish_regen_(false),
      use_rich_format_(false) {}
  ~ObRFBloomFilterMsg() { destroy(); }
  virtual int assign(const ObP2PDatahubMsgBase &) final;
  virtual int merge(ObP2PDatahubMsgBase &) final;
  virtual int broadcast(ObIArray<ObAddr> &target_addrs,
      obrpc::ObP2PDhRpcProxy &p2p_dh_proxy) final;
  bool is_first_phase() { return FIRST_LEVEL == phase_; }
  virtual int might_contain(const ObExpr &expr,
      ObEvalCtx &ctx,
      ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx,
      ObDatum &res) override;
  virtual int might_contain_batch(
      const ObExpr &expr,
      ObEvalCtx &ctx,
      const ObBitVector &skip,
      const int64_t batch_size,
      ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx) override;
  int might_contain_vector(
      const ObExpr &expr,
      ObEvalCtx &ctx,
      const ObBitVector &skip,
      const EvalBound &bound,
      ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx) override final;
  int insert_by_row_vector(
      const ObBatchRows *child_brs,
      const common::ObIArray<ObExpr *> &expr_array,
      const common::ObHashFuncs &hash_funcs,
      const ObExpr *calc_tablet_id_expr,
      ObEvalCtx &eval_ctx,
      uint64_t *batch_hash_values) override final;
  virtual int insert_by_row(
    const common::ObIArray<ObExpr *> &expr_array,
    const common::ObHashFuncs &hash_funcs,
    const ObExpr *calc_tablet_id_expr,
    ObEvalCtx &eval_ctx) override;
  virtual int insert_by_row_batch(
    const ObBatchRows *child_brs,
    const common::ObIArray<ObExpr *> &expr_array,
    const common::ObHashFuncs &hash_funcs,
    const ObExpr *calc_tablet_id_expr,
    ObEvalCtx &eval_ctx,
    uint64_t *batch_hash_values) override;
  virtual int reuse() override;
  virtual int process_receive_count(ObP2PDatahubMsgBase &) override;
  common::ObIArray<common::ObAddr>& get_next_phase_addrs() { return next_peer_addrs_; }
  virtual int deep_copy_msg(ObP2PDatahubMsgBase *&new_msg_ptr);
  virtual int destroy();
  int generate_filter_indexes(int64_t each_group_size,
    int64_t addr_cnt, int64_t piece_size);
  int process_first_phase_recieve_count(
      ObRFBloomFilterMsg &msg, bool &first_phase_end);
  virtual int process_msg_internal(bool &need_free);
  virtual int regenerate() override;
  int atomic_merge(ObP2PDatahubMsgBase &other_msg);
  inline void set_use_rich_format(bool value) { use_rich_format_ = value; }
  inline bool get_use_rich_format() const { return use_rich_format_; }
private:
  int calc_hash_value(
      const common::ObIArray<ObExpr *> &expr_array,
      const common::ObHashFuncs &hash_funcs,
      const ObExpr *calc_tablet_id_expr,
      ObEvalCtx &eval_ctx,
      uint64_t &hash_value, bool &ignore);
  int shadow_copy(const ObRFBloomFilterMsg &msg);
  int generate_receive_count_array(int64_t piece_size, int64_t cur_begin_idx);
  int do_might_contain_vector(
      const ObExpr &expr,
      ObEvalCtx &ctx,
      const ObBitVector &skip,
      const EvalBound &bound,
      ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx);
  template <typename ArgVec>
  int insert_partition_bloom_filter(ArgVec *arg_vec, const ObBatchRows *child_brs,
                                    uint64_t *batch_hash_values);
  template <VectorFormat ResFormat, typename ResVec>
  int fill_vec_result(ResVec *res_vec, const ObBitVector &skip, const EvalBound &bound,
                      uint64_t *hash_values, int64_t &total_count, int64_t &filter_count);

public:
  ObSendBFPhase phase_;
  ObPxBloomFilter bloom_filter_;
  common::ObFixedArray<common::ObAddr, common::ObIAllocator> next_peer_addrs_;
  int64_t expect_first_phase_count_;
  int64_t piece_size_;
  common::ObFixedArray<BloomFilterIndex, common::ObIAllocator> filter_indexes_;
  common::ObFixedArray<BloomFilterReceiveCount, common::ObIAllocator> receive_count_array_;
  int64_t *filter_idx_; //for shared msg
  bool *create_finish_; //for shared msg
  bool need_send_msg_;  //for shared msg, when drain_exch, msg is not need to be sent
  bool is_finish_regen_;
  bool use_rich_format_;
};

class ObRFRangeFilterMsg : public ObP2PDatahubMsgBase
{
  OB_UNIS_VERSION_V(1);
public:
  struct MinMaxCellSize
  {
    OB_UNIS_VERSION_V(1);
  public:
    MinMaxCellSize() : min_datum_buf_size_(0), max_datum_buf_size_(0) {}
    virtual ~MinMaxCellSize() = default;
    // record the real datum buf for lower bound
    int64_t min_datum_buf_size_;
    // record the real datum buf for upper bound
    int64_t max_datum_buf_size_;
    TO_STRING_KV(K_(min_datum_buf_size), K_(max_datum_buf_size));
  };
public:
  ObRFRangeFilterMsg();
  virtual int assign(const ObP2PDatahubMsgBase &) final;
  virtual int merge(ObP2PDatahubMsgBase &) final;
  virtual int deep_copy_msg(ObP2PDatahubMsgBase *&new_msg_ptr);
  virtual int destroy() {
    lower_bounds_.reset();
    upper_bounds_.reset();
    cmp_funcs_.reset();
    need_null_cmp_flags_.reset();
    cells_size_.reset();
    query_range_info_.destroy();
    query_range_allocator_.reset();
    allocator_.reset();
    return OB_SUCCESS;
  }
  virtual int might_contain(const ObExpr &expr,
      ObEvalCtx &ctx,
      ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx,
      ObDatum &res) override;
  virtual int might_contain_batch(
      const ObExpr &expr,
      ObEvalCtx &ctx,
      const ObBitVector &skip,
      const int64_t batch_size,
      ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx) override;
  virtual int insert_by_row(
    const common::ObIArray<ObExpr *> &expr_array,
    const common::ObHashFuncs &hash_funcs,
    const ObExpr *calc_tablet_id_expr,
    ObEvalCtx &eval_ctx) override;
  virtual int insert_by_row_batch(
    const ObBatchRows *child_brs,
    const common::ObIArray<ObExpr *> &expr_array,
    const common::ObHashFuncs &hash_funcs,
    const ObExpr *calc_tablet_id_expr,
    ObEvalCtx &eval_ctx,
    uint64_t *batch_hash_values) override;
  virtual int reuse() override;
  int adjust_cell_size();
  void after_process() override;
  int try_extract_query_range(bool &has_extract, ObIArray<ObNewRange> &ranges) override;
  inline int init_query_range_info(const ObPxQueryRangeInfo &query_range_info)
  {
    return query_range_info_.assign(query_range_info);
  }

  int prepare_storage_white_filter_data(ObDynamicFilterExecutor &dynamic_filter,
                                ObEvalCtx &eval_ctx,
                                ObRuntimeFilterParams &params,
                                bool &is_data_prepared) override;
private:
  int get_min(ObIArray<ObDatum> &vals);
  int get_max(ObIArray<ObDatum> &vals);
  int get_min(ObCmpFunc &func, ObDatum &l, ObDatum &r, int64_t &cell_size);
  int get_max(ObCmpFunc &func, ObDatum &l, ObDatum &r, int64_t &cell_size);
  int dynamic_copy_cell(const ObDatum &src, ObDatum &target, int64_t &cell_size);
  // only used in might_contain_batch,
  // without adding filter_count, total_count, check_count in filter_ctx
  int do_might_contain_batch(const ObExpr &expr,
      ObEvalCtx &ctx,
      const ObBitVector &skip,
      const int64_t batch_size,
      ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx);
  int prepare_query_range();
  inline void reuse_query_range()
  {
    query_range_.reset();
    is_query_range_ready_ = false;
    query_range_allocator_.set_tenant_id(tenant_id_);
    query_range_allocator_.set_label("ObRangeMsgQR");
    query_range_allocator_.reset_remain_one_page();
  }

public:
  ObFixedArray<ObDatum, common::ObIAllocator> lower_bounds_;
  ObFixedArray<ObDatum, common::ObIAllocator> upper_bounds_;
  ObFixedArray<bool, common::ObIAllocator> need_null_cmp_flags_;
  ObFixedArray<MinMaxCellSize, common::ObIAllocator> cells_size_;
  ObCmpFuncs cmp_funcs_;
  // for extract query range
  ObPxQueryRangeInfo query_range_info_;
  ObNewRange query_range_; // not need to serialize
  bool is_query_range_ready_; // not need to serialize
  common::ObArenaAllocator query_range_allocator_;
  // ---end---
  ObFixedArray<ObObjMeta, common::ObIAllocator> build_obj_metas_;
};

class ObRFInFilterMsg : public ObP2PDatahubMsgBase
{
  OB_UNIS_VERSION_V(1);
public:
  struct ObRFInFilterNode {
    ObRFInFilterNode() = default;
    ObRFInFilterNode(ObCmpFuncs *cmp_funcs, ObHashFuncs *hash_funcs,
          ObIArray<ObDatum> *row, int64_t hash_val = 0)
        : cmp_funcs_(cmp_funcs), hash_funcs_(hash_funcs),
          row_(row), hash_val_(hash_val) {}
    int hash(uint64_t &hash_ret) const;
    inline bool operator==(const ObRFInFilterNode &other) const;
    ObCmpFuncs *cmp_funcs_;
    ObHashFuncs *hash_funcs_;
    ObIArray<ObDatum> *row_;
    int64_t hash_val_;
  };
public:
  ObRFInFilterMsg() : ObP2PDatahubMsgBase(), rows_set_(),
      cmp_funcs_(allocator_), hash_funcs_for_insert_(allocator_),
      serial_rows_(), need_null_cmp_flags_(allocator_),
      cur_row_(allocator_), col_cnt_(0),
      max_in_num_(0), query_range_info_(allocator_),
      query_range_(), is_query_range_ready_(false), query_range_allocator_(),
      build_obj_metas_(allocator_) {}
  virtual int assign(const ObP2PDatahubMsgBase &);
  virtual int merge(ObP2PDatahubMsgBase &) final;
  virtual int deep_copy_msg(ObP2PDatahubMsgBase *&new_msg_ptr);
  virtual int destroy();
  virtual int might_contain(const ObExpr &expr,
      ObEvalCtx &ctx,
      ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx,
      ObDatum &res) override;
  virtual int might_contain_batch(
      const ObExpr &expr,
      ObEvalCtx &ctx,
      const ObBitVector &skip,
      const int64_t batch_size,
      ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx) override;
  virtual int insert_by_row(
    const common::ObIArray<ObExpr *> &expr_array,
    const common::ObHashFuncs &hash_funcs,
    const ObExpr *calc_tablet_id_expr,
    ObEvalCtx &eval_ctx) override;
  virtual int insert_by_row_batch(
    const ObBatchRows *child_brs,
    const common::ObIArray<ObExpr *> &expr_array,
    const common::ObHashFuncs &hash_funcs,
    const ObExpr *calc_tablet_id_expr,
    ObEvalCtx &eval_ctx,
    uint64_t *batch_hash_values) override;
  virtual int reuse() override;
  void check_finish_receive() override final;
  void after_process() override;
  int try_extract_query_range(bool &has_extract, ObIArray<ObNewRange> &ranges) override;
  inline int init_query_range_info(const ObPxQueryRangeInfo &query_range_info)
  {
    return query_range_info_.assign(query_range_info);
  }

  int prepare_storage_white_filter_data(ObDynamicFilterExecutor &dynamic_filter,
                                ObEvalCtx &eval_ctx,
                                ObRuntimeFilterParams &params,
                                bool &is_data_prepared) override;
private:
  int append_row();
  int insert_node();
  // only used in might_contain_batch,
  // without adding filter_count, total_count, check_count in filter_ctx
  int do_might_contain_batch(const ObExpr &expr,
      ObEvalCtx &ctx,
      const ObBitVector &skip,
      const int64_t batch_size,
      ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx);
  int prepare_query_ranges();
  int process_query_ranges_with_deduplicate();
  int process_query_ranges_without_deduplicate();
  int generate_one_range(int row_idx);
  inline void reuse_query_range()
  {
    query_range_.reset();
    is_query_range_ready_ = false;
    query_range_allocator_.set_tenant_id(tenant_id_);
    query_range_allocator_.set_label("ObInMsgQR");
    query_range_allocator_.reset_remain_one_page();
  }

public:
  hash::ObHashSet<ObRFInFilterNode, hash::NoPthreadDefendMode> rows_set_;
  ObCmpFuncs cmp_funcs_;
  ObHashFuncs hash_funcs_for_insert_;
  ObSArray<ObFixedArray<ObDatum, common::ObIAllocator> *> serial_rows_;
  ObFixedArray<bool, common::ObIAllocator> need_null_cmp_flags_;
  ObFixedArray<ObDatum, common::ObIAllocator> cur_row_;
  int64_t col_cnt_;
  int64_t max_in_num_;
  // for extract query range
  ObPxQueryRangeInfo query_range_info_;
  ObSEArray<ObNewRange, 16> query_range_; // not need to serialize
  bool is_query_range_ready_; // not need to serialize
  common::ObArenaAllocator query_range_allocator_;
  // ---end---
  ObFixedArray<ObObjMeta, common::ObIAllocator> build_obj_metas_;
};

}
}

#endif
