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

#pragma once

#include "lib/ob_define.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_array.h"
#include "share/datum/ob_datum.h"
#include "sql/engine/px/ob_px_bloom_filter.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_msg.h"
#include "sql/engine/basic/ob_compact_row.h"
#include "sql/engine/px/p2p_datahub/ob_runtime_filter_query_range.h"
#include "src/sql/engine/px/p2p_datahub/ob_small_hashset.h"

namespace oceanbase
{
namespace sql
{

struct ObRFCmpInfo final
{
  OB_UNIS_VERSION_V(1);
public:
  inline int64_t to_string(char *buf, const int64_t len) const { return 0; }
  union {
    NullSafeRowCmpFunc cmp_func_;
    // helper union member for cmp_func_ serialize && deserialize
    sql::serializable_function ser_cmp_func_;
  };
  ObObjMeta obj_meta_;
};
typedef common::ObFixedArray<ObRFCmpInfo, common::ObIAllocator> ObRFCmpInfos;

class ObRFRangeFilterVecMsg : public ObP2PDatahubMsgBase
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
  ObRFRangeFilterVecMsg();
  virtual int assign(const ObP2PDatahubMsgBase &) final;
  virtual int merge(ObP2PDatahubMsgBase &) final;
  virtual int deep_copy_msg(ObP2PDatahubMsgBase *&new_msg_ptr);
  virtual int destroy() {
    build_row_cmp_info_.reset();
    probe_row_cmp_info_.reset();
    lower_bounds_.reset();
    upper_bounds_.reset();
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
  int might_contain_batch(
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
  virtual int insert_by_row(
      const common::ObIArray<ObExpr *> &expr_array,
      const common::ObHashFuncs &hash_funcs,
      const ObExpr *calc_tablet_id_expr,
      ObEvalCtx &eval_ctx) override { return OB_NOT_IMPLEMENT; }
  virtual int insert_by_row_batch(
      const ObBatchRows *child_brs,
      const common::ObIArray<ObExpr *> &expr_array,
      const common::ObHashFuncs &hash_funcs,
      const ObExpr *calc_tablet_id_expr,
      ObEvalCtx &eval_ctx,
      uint64_t *batch_hash_values) override { return OB_NOT_IMPLEMENT; }
  int insert_by_row_vector(
      const ObBatchRows *child_brs,
      const common::ObIArray<ObExpr *> &expr_array,
      const common::ObHashFuncs &hash_funcs,
      const ObExpr *calc_tablet_id_expr,
      ObEvalCtx &eval_ctx,
      uint64_t *batch_hash_values) override final;
  virtual int reuse() override;
  int adjust_cell_size();
  void after_process() override;
  int try_extract_query_range(bool &has_extract, ObIArray<ObNewRange> &ranges) override;
  inline int init_query_range_info(const ObPxQueryRangeInfo &query_range_info)
  {
    return query_range_info_.assign(query_range_info);
  }
  int prepare_storage_white_filter_data(ObDynamicFilterExecutor &dynamic_filter,
                                        ObEvalCtx &eval_ctx, ObRuntimeFilterParams &params,
                                        bool &is_data_prepared) override;

private:

  int merge_min(ObIArray<ObDatum> &vals);
  int merge_max(ObIArray<ObDatum> &vals);
  int update_min(ObRFCmpInfo &cmp_info, ObDatum &l, ObDatum &r, int64_t &cell_size);
  int update_max(ObRFCmpInfo &cmp_info, ObDatum &l, ObDatum &r, int64_t &cell_size);

  int probe_with_lower(int64_t col_idx, ObDatum &prob_datum, int &cmp_min);
  int probe_with_upper(int64_t col_idx, ObDatum &prob_datum, int &cmp_max);

  int dynamic_copy_cell(const ObDatum &src, ObDatum &target, int64_t &cell_size);
  int do_might_contain_batch(const ObExpr &expr,
      ObEvalCtx &ctx,
      const ObBitVector &skip,
      const int64_t batch_size,
      ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx);
  int do_might_contain_vector(
      const ObExpr &expr,
      ObEvalCtx &ctx,
      const ObBitVector &skip,
      const EvalBound &bound,
      ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx);
  int prepare_query_range();
  inline void reuse_query_range()
  {
    query_range_.reset();
    is_query_range_ready_ = false;
    query_range_allocator_.set_tenant_id(tenant_id_);
    query_range_allocator_.set_label("ObRangeVecMsgQR");
    query_range_allocator_.reset_remain_one_page();
  }

public:
  ObRFCmpInfos build_row_cmp_info_;
  ObRFCmpInfos probe_row_cmp_info_;
  ObFixedArray<ObDatum, common::ObIAllocator> lower_bounds_;
  ObFixedArray<ObDatum, common::ObIAllocator> upper_bounds_;
  ObFixedArray<bool, common::ObIAllocator> need_null_cmp_flags_;
  ObFixedArray<MinMaxCellSize, common::ObIAllocator> cells_size_;

  // for extract query range
  ObPxQueryRangeInfo query_range_info_;
  ObNewRange query_range_; // not need to serialize
  bool is_query_range_ready_; // not need to serialize
  common::ObArenaAllocator query_range_allocator_;
  // ---end---
};


struct ObRowWithHash
{
  OB_UNIS_VERSION_V(1);
public:
  ObRowWithHash(common::ObIAllocator &allocator) : allocator_(allocator), row_(allocator), hash_val_(0) {}
  int assign(const ObRowWithHash &other);
  TO_STRING_KV(K_(row), K_(hash_val));
public:
  common::ObIAllocator &allocator_;
  ObFixedArray<ObDatum, common::ObIAllocator> row_;
  uint64_t hash_val_;
};

class ObRFInFilterVecMsg : public ObP2PDatahubMsgBase
{
  OB_UNIS_VERSION_V(1);
public:

  class ObRFInFilterRowStore
  {
    OB_UNIS_VERSION_V(1);
  public:
    static int deep_copy_one_row(const ObCompactRow *src, ObCompactRow *&dest,
        common::ObIAllocator &allocator, int64_t row_size);
  public:
    ObRFInFilterRowStore(common::ObIAllocator &allocator)
        : allocator_(allocator), serial_rows_(), row_sizes_()
    {}
    int assign(const ObRFInFilterRowStore &other);
    void reset();
    inline ObCompactRow *get_row(int64_t idx) { return serial_rows_.at(idx); }
    inline uint64_t get_hash_value(int64_t idx, const RowMeta &row_meta) {
      return serial_rows_.at(idx)->extra_payload<uint64_t>(row_meta);
    }
    inline int64_t get_row_size(int64_t idx) { return row_sizes_.at(idx); }
    inline int64_t get_row_cnt() const { return serial_rows_.count(); }
    inline int add_row(ObCompactRow *new_row, int64_t row_size);
    int create_and_add_row(const common::ObIArray<ObExpr *> &exprs, const RowMeta &row_meta,
        const int64_t row_size, ObEvalCtx &ctx, ObCompactRow *&new_row, uint64_t hash_val);
    TO_STRING_KV(K(get_row_cnt()), K_(row_sizes), K_(serial_rows))
  private:
    common::ObIAllocator &allocator_;
    ObSArray<ObCompactRow *> serial_rows_;
    ObSArray<int64_t> row_sizes_;
  };

  struct ObRFInFilterNode
  {
    ObRFInFilterNode() = default;
    ObRFInFilterNode(ObRFCmpInfos *row_cmp_infos, RowMeta *row_meta = nullptr, ObCompactRow *compact_row = nullptr,
        ObRowWithHash *row_with_hash = nullptr)
        : row_cmp_infos_(row_cmp_infos), row_meta_(row_meta), compact_row_(compact_row),
          row_with_hash_(row_with_hash)
    {}
    int hash(uint64_t &hash_ret) const;
    inline bool operator==(const ObRFInFilterNode &other) const;
    void switch_compact_row(ObCompactRow *compact_row) { compact_row_ = compact_row; }
    ObRFCmpInfos *row_cmp_infos_;
    RowMeta *row_meta_;
    // why we need both compact_row_ and row_with_hash_?
    // when seek a node exist or not, we do not need to deep copy the row data,
    // so use row_with_hash_(with shallow copy).
    // when insert a node into hashset, we need to persisting data, the ObCompactRow
    // allowed us copy memory continuesly.
    ObCompactRow *compact_row_;
    ObRowWithHash *row_with_hash_;
  };

public:
  ObRFInFilterVecMsg()
      : ObP2PDatahubMsgBase(), build_row_cmp_info_(allocator_), probe_row_cmp_info_(allocator_),
        build_row_meta_(&allocator_), cur_row_with_hash_(allocator_), rows_set_(),
        row_store_(allocator_), need_null_cmp_flags_(allocator_), max_in_num_(0),
        hash_funcs_for_insert_(allocator_),query_range_info_(allocator_),
        query_range_(), is_query_range_ready_(false), query_range_allocator_(), sm_hash_set_()
  {}
  virtual int assign(const ObP2PDatahubMsgBase &);
  virtual int merge(ObP2PDatahubMsgBase &) final;
  virtual int deep_copy_msg(ObP2PDatahubMsgBase *&new_msg_ptr);
  virtual int destroy();
  int might_contain(const ObExpr &expr,
      ObEvalCtx &ctx,
      ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx,
      ObDatum &res) override;
  int might_contain_batch(
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
  virtual int insert_by_row(
      const common::ObIArray<ObExpr *> &expr_array,
      const common::ObHashFuncs &hash_funcs,
      const ObExpr *calc_tablet_id_expr,
      ObEvalCtx &eval_ctx) override { return OB_NOT_IMPLEMENT; }
  virtual int insert_by_row_batch(
      const ObBatchRows *child_brs,
      const common::ObIArray<ObExpr *> &expr_array,
      const common::ObHashFuncs &hash_funcs,
      const ObExpr *calc_tablet_id_expr,
      ObEvalCtx &eval_ctx,
      uint64_t *batch_hash_values) override { return OB_NOT_IMPLEMENT; }
  int insert_by_row_vector(
      const ObBatchRows *child_brs,
      const common::ObIArray<ObExpr *> &expr_array,
      const common::ObHashFuncs &hash_funcs,
      const ObExpr *calc_tablet_id_expr,
      ObEvalCtx &eval_ctx,
      uint64_t *batch_hash_values) override final;
  virtual int reuse() override;
  void check_finish_receive() override final;
  void after_process() override;
  int try_extract_query_range(bool &has_extract, ObIArray<ObNewRange> &ranges) override;
  inline int init_query_range_info(const ObPxQueryRangeInfo &query_range_info)
  {
    return query_range_info_.assign(query_range_info);
  }
  int prepare_storage_white_filter_data(ObDynamicFilterExecutor &dynamic_filter,
                                        ObEvalCtx &eval_ctx, ObRuntimeFilterParams &params,
                                        bool &is_data_prepared) override;

private:
  // for merge
  int append_node(ObRFInFilterNode &node, int64_t row_size);
  // for insert
  int append_node(ObRFInFilterNode &node, const common::ObIArray<ObExpr *> &exprs,
      ObEvalCtx &ctx);
  int try_insert_node(ObRFInFilterNode &node, const common::ObIArray<ObExpr *> &exprs,
      ObEvalCtx &ctx);
  int try_merge_node(ObRFInFilterNode &node, int64_t row_size);
  int do_might_contain_batch(const ObExpr &expr,
      ObEvalCtx &ctx,
      const ObBitVector &skip,
      const int64_t batch_size,
      ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx);
  int do_might_contain_vector(
      const ObExpr &expr,
      ObEvalCtx &ctx,
      const ObBitVector &skip,
      const EvalBound &bound,
      ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx);

  template <typename ResVec>
  int do_might_contain_vector_impl(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                                   const EvalBound &bound,
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
    query_range_allocator_.set_label("ObInVecMsgQR");
    query_range_allocator_.reset_remain_one_page();
  }

public:
  ObRFCmpInfos build_row_cmp_info_;
  ObRFCmpInfos probe_row_cmp_info_;
  RowMeta build_row_meta_;

  // when insert, we use cur_row_with_hash_ in ObRFInFilterVecMsg,
  // when probe, we use cur_row_with_hash_ in ObExprJoinFilterContext
  // both of them are not need to be serilized
  ObRowWithHash cur_row_with_hash_;
  hash::ObHashSet<ObRFInFilterNode, hash::NoPthreadDefendMode> rows_set_;
  ObRFInFilterRowStore row_store_;
  ObFixedArray<bool, common::ObIAllocator> need_null_cmp_flags_;
  int64_t max_in_num_;

  // for extract query range
  ObHashFuncs hash_funcs_for_insert_; // for deduplicate query range
  ObPxQueryRangeInfo query_range_info_;
  ObSEArray<ObNewRange, 16> query_range_; // not need to serialize
  bool is_query_range_ready_;             // not need to serialize
  common::ObArenaAllocator query_range_allocator_;
  // ---end---
  ObSmallHashSet<false> sm_hash_set_;
};


} // end namespace sql
} // end namespace oceanbase
