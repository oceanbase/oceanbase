/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


/*
 * Overview
 * - Abstract compile-time/runtime definitions (CtDef/RtDef) for DAS search operators.
 *
 * Key Responsibilities
 * - Record the immutable parts of each operator, such as is_scoring_, and the mutable parts, such as cost.
 * - Define the core interface for adaptively operator generation: compute_cost() and generate_op().
 * - Define Fusion Search CtDef/Rtdef, which bridge the search operators and vector subquery.
 * - Define ObDASRowID for unified rowid abstraction across different index types.
 */
#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_SEARCH_DEFINE_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_SEARCH_DEFINE_H_

#include "sql/das/ob_das_attach_define.h"
#include "share/hybrid_search/ob_query_parse.h"

namespace oceanbase
{
namespace sql
{

enum ObDASRowIDType {
  DAS_ROWID_TYPE_UINT64 = 0,
  DAS_ROWID_TYPE_COMPACT,
  DAS_ROWID_TYPE_MAX
};

struct ObCompactRow;

/**
 * ObDASRowID - Unified rowid abstraction for hybrid search framework.
 *
 * Design:
 * - ObDASRowID is TYPE-AGNOSTIC, similar to ObDatum. The struct itself does not know
 *   whether it holds a uint64 or a compact row pointer; it simply stores raw bytes/pointer.
 * - The actual type is determined externally by ObDASSearchCtx::rowid_type_.
 *
 * Usage:
 * - Normally, DO NOT use set_uint64()/get_uint64()/set_compact_row()/get_compact_row() directly.
 *   Instead, use the utility methods in ObIDASSearchOp which handle type dispatching automatically:
 *     - compare_rowid(rowid1, rowid2, cmp)
 *     - deep_copy_rowid(src, dst, alloc)
 *     - create_rowid_store(capacity, store)
 *     - ...
 * - The four getter/setter methods should ONLY be used when you can guarantee the specific
 *   rowid type stored in ObDASRowID.
 */
struct ObDASRowID
{

friend class RowIDStore;
friend class UInt64RowIDStore;
friend class CompactRowIDStore;
friend class ObDASSearchCtx;
friend class ObDASBitmapOp;
friend class ObDASTokenOp;
friend class ObDASSearchDriverIter;
friend class ObDASScalarPrimaryROROp;
template <ObDASRowIDType, bool> friend class ObDASRowIDCmp;

public:
  ObDASRowID() : uint64_rowid_(0), flags_(0) {}

  OB_INLINE void set_min()
  {
    flags_ |= FLAG_MIN;
    flags_ &= ~FLAG_MAX;
  }

  OB_INLINE void set_max()
  {
    flags_ |= FLAG_MAX;
    flags_ &= ~FLAG_MIN;
  }

  OB_INLINE bool is_min() const { return flags_ & FLAG_MIN; }
  OB_INLINE bool is_max() const { return flags_ & FLAG_MAX; }
  OB_INLINE bool is_normal() const { return !is_min() && !is_max(); }

  OB_INLINE void reset()
  {
    uint64_rowid_ = 0;
    flags_ = 0;
  }

  TO_STRING_KV(K_(uint64_rowid), K(is_min()), K(is_max()), K(is_normal()));

private:
  OB_INLINE void set_uint64(uint64_t value)
  {
    uint64_rowid_ = value;
    clear_min_max();
  }

  OB_INLINE void set_compact_row(const ObCompactRow *row)
  {
    ptr_rowid_ = row;
    clear_min_max();
  }

  OB_INLINE uint64_t get_uint64() const { return uint64_rowid_; }
  OB_INLINE const ObCompactRow *get_compact_row() const { return ptr_rowid_; }

  OB_INLINE void clear_min_max() { flags_ &= ~(FLAG_MIN | FLAG_MAX); }

private:
  static constexpr uint16_t FLAG_MIN = 1 << 0;
  static constexpr uint16_t FLAG_MAX = 1 << 1;

  union {
    uint64_t uint64_rowid_;
    const ObCompactRow *ptr_rowid_;
  };
  uint16_t flags_;
};

template <ObDASRowIDType RowIDType, bool HAS_MIN_MAX = false>
class ObDASRowIDCmp
{
public:
  ObDASRowIDCmp(const RowMeta &meta, const common::ObIArray<ObExpr *> &exprs)
    : meta_(meta), exprs_(exprs) {}
  ~ObDASRowIDCmp() = default;
  int cmp(const ObDASRowID &l, const ObDASRowID &r, int &cmp_ret) const;
private:
  int inner_cmp(const ObDASRowID &l, const ObDASRowID &r, int &cmp_ret) const;
private:
  const RowMeta &meta_;
  const common::ObIArray<ObExpr *> &exprs_;
};

template <ObDASRowIDType RowIDType, bool HAS_MIN_MAX>
int ObDASRowIDCmp<RowIDType, HAS_MIN_MAX>::cmp(
    const ObDASRowID &l, const ObDASRowID &r, int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  cmp_ret = 0;
  if constexpr (HAS_MIN_MAX) {
    const bool l_min = l.is_min();
    const bool l_max = l.is_max();
    const bool r_min = r.is_min();
    const bool r_max = r.is_max();
    if (l.is_normal() && r.is_normal()) {
      ret = inner_cmp(l, r, cmp_ret);
    } else if (l_max || r_max) {
      cmp_ret = l_max - r_max;
    } else if (l_min || r_min) {
      cmp_ret = -(l_min - r_min);
    } else {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected min max status", K(ret), K(l), K(r));
    }
  } else {
    ret = inner_cmp(l, r, cmp_ret);
  }
  return ret;
}

template <ObDASRowIDType RowIDType, bool HAS_MIN_MAX>
int ObDASRowIDCmp<RowIDType, HAS_MIN_MAX>::inner_cmp(
    const ObDASRowID &l, const ObDASRowID &r, int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  if constexpr (RowIDType == DAS_ROWID_TYPE_UINT64) {
    const uint64_t l_val = l.get_uint64();
    const uint64_t r_val = r.get_uint64();
    cmp_ret = (l_val > r_val) ? 1 : ((l_val < r_val) ? -1 : 0);
  } else {
    if (OB_ISNULL(l.get_compact_row()) || OB_ISNULL(r.get_compact_row())) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected nullptr to compact row", K(ret), K(l), K(r));
    }
    for (int64_t i = 0; OB_SUCC(ret) && cmp_ret == 0 && i < exprs_.count(); ++i) {
      const ObDatum &l_datum = l.get_compact_row()->get_datum(meta_, i);
      const ObDatum &r_datum = r.get_compact_row()->get_datum(meta_, i);
      if (OB_FAIL(exprs_.at(i)->basic_funcs_->null_last_cmp_(l_datum, r_datum, cmp_ret))) {
        SQL_LOG(WARN, "failed to compare datums", K(ret), K(i), K(l_datum), K(r_datum));
      }
    }
  }
  return ret;
}

struct ObDASSearchCost
{
public:
  ObDASSearchCost(): cost_(OB_INVALID_SEARCH_COST) {}
  explicit ObDASSearchCost(int64_t cost) : cost_(cost) {}
  ~ObDASSearchCost() {}
  bool is_valid() const { return cost_ >= 0; }
  int64_t cost() const { return cost_; }
  void reset() { cost_ = OB_INVALID_SEARCH_COST; }
  OB_INLINE void operator=(const ObDASSearchCost &other) { cost_ = other.cost_; }

  TO_STRING_KV(K(cost_));

  static int min(const ObDASSearchCost &a, const ObDASSearchCost &b, ObDASSearchCost &result)
  {
    int ret = OB_SUCCESS;
    if (!a.is_valid() || !b.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid cost", KR(ret), K(a), K(b));
    } else {
      result = ObDASSearchCost(std::min(a.cost(), b.cost()));
    }
    return ret;
  }

private:
  static const int64_t OB_INVALID_SEARCH_COST = -1;
  int64_t cost_;
};

class ObIDASSearchOp;

struct ObIDASSearchCtDef : ObDASAttachCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObIDASSearchCtDef(common::ObIAllocator &alloc, const ObDASOpType &op_type)
    : ObDASAttachCtDef(alloc, op_type),
      is_scoring_(false),
      is_top_level_scoring_(false)
  { }
  virtual void set_is_scoring(const bool is_scoring) { is_scoring_ = is_scoring; }
  OB_INLINE void set_is_top_level_scoring(bool is_top_level_scoring) { is_top_level_scoring_ = is_top_level_scoring; }
  OB_INLINE bool is_top_level_scoring() const { return is_top_level_scoring_; }
  OB_INLINE bool is_scoring() const { return is_scoring_; }
  virtual bool is_search_ctdef() const override { return true; }

private:
  // whether the query need contribute score.
  bool is_scoring_;
  // whether the query is the top-level scoring query, which means the query will drive
  // the top-k max score dynamic pruning.
  bool is_top_level_scoring_;
};

struct ObIDASSearchRtDef : ObDASAttachRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObIDASSearchRtDef(const ObDASOpType &op_type)
    : ObDASAttachRtDef(op_type),
      cost_()
  { }

public:
  int get_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost);
  virtual int generate_op(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op) = 0;
  virtual int compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost) = 0;
  virtual int can_pushdown_filter_to_bmm(bool &can_pushdown);
  virtual void set_pushdown_filter(const bool query_optional, ObIDASSearchOp *filter_op);

private:
  // the estimated cost of this query, which is, the upper bound of the number of matching docs.
  ObDASSearchCost cost_;
};

/*
 * Support multi-path vector hybrid search, where all other non-vector queries will be
 * processed via a search operator tree. Fusion CtDef/RtDef is used to merge the results of these
 * query paths and perform reranking.
*/
struct ObDASFusionCtDef : ObDASAttachCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASFusionCtDef(common::ObIAllocator &alloc)
    : ObDASAttachCtDef(alloc, DAS_OP_FUSION_QUERY),
      search_index_(-1),
      rowid_exprs_(alloc),
      score_exprs_(alloc),
      rank_exprs_(alloc),
      weight_exprs_(alloc),
      path_top_k_limit_exprs_(alloc),
      size_expr_(nullptr),
      offset_expr_(nullptr),
      rank_window_size_expr_(nullptr),
      rank_constant_expr_(nullptr),
      min_score_expr_(nullptr),
      has_search_subquery_(false),
      has_vector_subquery_(false),
      is_top_k_query_(true),
      fusion_method_(ObFusionMethod::WEIGHT_SUM),
      has_hybrid_fusion_op_(false)
  {}
  ~ObDASFusionCtDef() {}
  INHERIT_TO_STRING_KV("ObDASFusionCtDef", ObDASAttachCtDef, K(search_index_), K(has_search_subquery_), K(has_vector_subquery_), K(rowid_exprs_), K(score_exprs_), K(rank_exprs_), K(weight_exprs_), KPC_(size_expr), KPC_(offset_expr), KPC_(rank_window_size_expr), KPC_(rank_constant_expr), KPC_(min_score_expr), K(fusion_method_));

  int init(
    int64_t search_index,
    bool has_search_subquery,
    bool has_vector_subquery,
    bool is_top_k_query,
    ObFusionMethod fusion_method,
    bool has_hybrid_fusion_op,
    ObExpr *size_expr,
    ObExpr *offset_expr,
    ObExpr *rank_window_size_expr,
    ObExpr *rank_constant_expr,
    ObExpr *min_score_expr,
    const common::ObIArray<ObExpr *> &rowid_exprs,
    const common::ObIArray<ObExpr *> &score_exprs,
    const common::ObIArray<ObExpr *> &result_output_exprs,
    const common::ObIArray<ObExpr *> &weight_exprs,
    const common::ObIArray<ObExpr *> &path_top_k_limit_exprs);

  // Getter methods for backward compatibility
  OB_INLINE ObExpr *get_search_score_expr() const
  {
    return search_index_ >= 0 && search_index_ < score_exprs_.count() ? score_exprs_.at(search_index_) : nullptr;
  }
  OB_INLINE ObExpr *get_fusion_score_expr() const
  {
    return (score_exprs_.count() > 0) ? score_exprs_.at(score_exprs_.count() - 1) : nullptr;
  }

  OB_INLINE const ExprFixedArray &get_score_exprs() const { return score_exprs_; }

  OB_INLINE bool need_search_score() const { return get_search_score_expr() != nullptr; }
  OB_INLINE bool has_search_subquery() const { return has_search_subquery_; }
  OB_INLINE bool has_vector_subquery() const { return has_vector_subquery_; }
  OB_INLINE bool need_topk() const { return need_search_score() && nullptr != size_expr_ && nullptr != offset_expr_; }

  OB_INLINE int64_t get_search_idx() const { return search_index_; }
  OB_INLINE void set_search_index(int64_t index) { search_index_ = index; }

  OB_INLINE bool is_search_index(int64_t index) const { return has_search_subquery_ && index == search_index_; }
  OB_INLINE bool is_vector_index(int64_t index) const { return has_vector_subquery_ && index != search_index_; }

  const ObIDASSearchCtDef *get_search_ctdef() const
  {
    const ObIDASSearchCtDef *search_ctdef = nullptr;
    int64_t search_idx = get_search_idx();
    if (search_idx >= 0 && search_idx < children_cnt_ && children_ != nullptr) {
      search_ctdef = static_cast<const ObIDASSearchCtDef *>(children_[search_idx]);
    }
    return search_ctdef;
  }

public:
  int64_t search_index_;
  ExprFixedArray rowid_exprs_;
  ExprFixedArray score_exprs_;
  ExprFixedArray rank_exprs_;
  ExprFixedArray weight_exprs_;
  ExprFixedArray path_top_k_limit_exprs_;
  ObExpr *size_expr_;
  ObExpr *offset_expr_;
  ObExpr *rank_window_size_expr_;
  ObExpr *rank_constant_expr_;
  ObExpr *min_score_expr_;
  bool has_search_subquery_;
  bool has_vector_subquery_;
  bool is_top_k_query_;
  ObFusionMethod fusion_method_;
  bool has_hybrid_fusion_op_;
};

struct ObDASFusionRtDef : ObDASAttachRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASFusionRtDef()
    : ObDASAttachRtDef(DAS_OP_FUSION_QUERY)
  {}
  ~ObDASFusionRtDef() {}

  ObIDASSearchRtDef *get_search_rtdef()
  {
    ObIDASSearchRtDef *search_rtdef = nullptr;
    if (OB_NOT_NULL(ctdef_)) {
      const ObDASFusionCtDef *ctdef = static_cast<const ObDASFusionCtDef *>(ctdef_);
      int64_t search_idx = ctdef->get_search_idx();
      if (search_idx >= 0 && search_idx < children_cnt_ && children_ != nullptr) {
        search_rtdef = static_cast<ObIDASSearchRtDef *>(children_[search_idx]);
      }
    }
    return search_rtdef;
  }
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_SEARCH_DEFINE_H_
