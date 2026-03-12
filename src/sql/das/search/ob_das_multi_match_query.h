/**
 * Copyright (c) 2025 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SQL_OB_DAS_MULTI_MATCH_QUERY_H_
#define OCEANBASE_SQL_OB_DAS_MULTI_MATCH_QUERY_H_

#include "sql/das/ob_das_ir_define.h"
#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/engine/expr/ob_expr.h"
#include "lib/container/ob_fixed_array.h"
#include "storage/retrieval/ob_block_max_iter.h"

namespace oceanbase
{
namespace sql
{

struct ObDASMultiMatchCtDef : ObIDASSearchCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASMultiMatchCtDef(ObIAllocator &alloc)
    : ObIDASSearchCtDef(alloc, DAS_OP_MULTI_MATCH_QUERY),
      query_text_(nullptr),
      boost_(nullptr),
      match_operator_(nullptr),
      minimum_should_match_(nullptr),
      type_(nullptr),
      field_boosts_(alloc),
      ir_ctdef_indices_(alloc)
  {
    set_is_scoring(true);
  }
  virtual ~ObDASMultiMatchCtDef() {}
  INHERIT_TO_STRING_KV("ObIDASSearchCtDef", ObIDASSearchCtDef,
      KPC_(query_text),
      KPC_(boost),
      KPC_(match_operator),
      KPC_(minimum_should_match),
      KPC_(type),
      K_(ir_ctdef_indices),
      K_(field_boosts));

  ObDASIRScanCtDef *get_ir_ctdef(const int64_t idx) const
  {
    return (idx >= 0 && idx < ir_ctdef_indices_.count())
        ? static_cast<ObDASIRScanCtDef *>(children_[ir_ctdef_indices_.at(idx)])
        : nullptr;
  }

public:
  ObExpr *query_text_;
  ObExpr *boost_;
  ObExpr *match_operator_;
  ObExpr *minimum_should_match_;
  ObExpr *type_;
  ExprFixedArray field_boosts_;
  ObFixedArray<int64_t, ObIAllocator> ir_ctdef_indices_;
};

struct ObDASMultiMatchRtDef : ObIDASSearchRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASMultiMatchRtDef()
    : ObIDASSearchRtDef(DAS_OP_MULTI_MATCH_QUERY),
      allocator_(ObMemAttr(MTL_ID(), "DASMMatchQ")),
      block_max_params_(&allocator_),
      pushdown_filter_op_(nullptr),
      block_max_params_initialized_(false),
      query_optional_(false)
  {}
  int compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost) override
  { return OB_SUCCESS; }
  int generate_op(
      ObDASSearchCost lead_cost,
      ObDASSearchCtx &search_ctx,
      ObIDASSearchOp *&op) override;
  int can_pushdown_filter_to_bmm(bool &can_pushdown) override;
  void set_pushdown_filter(const bool query_optional, ObIDASSearchOp *filter_op) override
  {
    pushdown_filter_op_ = filter_op;
    query_optional_ = query_optional;
  }
private:
  ObDASIRScanRtDef *get_ir_rtdef(const int64_t idx) const
  {
    ObDASIRScanRtDef *pret = nullptr;
    const ObDASMultiMatchCtDef *ctdef = static_cast<const ObDASMultiMatchCtDef *>(ctdef_);
    if (OB_NOT_NULL(ctdef) && OB_LIKELY(idx >= 0 && idx < ctdef->ir_ctdef_indices_.count())) {
      const int64_t child_idx = ctdef->ir_ctdef_indices_.at(idx);
      if (child_idx >= 0 && child_idx < children_cnt_) {
        pret = static_cast<ObDASIRScanRtDef *>(children_[child_idx]);
      }
    }
    return pret;
  }
  int init_block_max_params(const ObDASMultiMatchCtDef &ctdef, const int64_t field_cnt);
public:
  ObArenaAllocator allocator_; // op-level lifetime
  ObFixedArray<ObBlockMaxScoreIterParam, ObIAllocator> block_max_params_;
  ObIDASSearchOp *pushdown_filter_op_;
  bool block_max_params_initialized_;
  bool query_optional_;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_DAS_MULTI_MATCH_QUERY_H_
