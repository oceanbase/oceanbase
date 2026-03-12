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

#ifndef OCEANBASE_SQL_OB_DAS_TEXT_RETRIEVAL_DEFINE_H_
#define OCEANBASE_SQL_OB_DAS_TEXT_RETRIEVAL_DEFINE_H_

#include "sql/das/ob_das_ir_define.h"
#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/search/ob_das_search_context.h"
#include "storage/retrieval/ob_block_max_iter.h"

namespace oceanbase
{
namespace sql
{

// Execution meta data for Match Query:
//   Run disjunctive / conjunctive retrieval on tokens tokenized from query text
struct ObDASMatchCtDef : ObIDASSearchCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASMatchCtDef(common::ObIAllocator &alloc)
    : ObIDASSearchCtDef(alloc, DAS_OP_MATCH_QUERY),
      query_text_(nullptr),
      boost_(nullptr),
      match_operator_(nullptr),
      minimum_should_match_(nullptr),
      ir_ctdef_idx_(-1),
      pushdown_must_filter_op_ctdef_idx_(-1)
  {
    set_is_scoring(true);
  }
  ObDASIRScanCtDef *get_ir_ctdef() const
  {
    return ir_ctdef_idx_ >= 0 ? static_cast<ObDASIRScanCtDef *>(children_[ir_ctdef_idx_]) : nullptr;
  }
  ObIDASSearchCtDef *get_pushdown_must_filter_op_ctdef() const
  {
    return pushdown_must_filter_op_ctdef_idx_ >= 0
        ? static_cast<ObIDASSearchCtDef *>(children_[pushdown_must_filter_op_ctdef_idx_]) : nullptr;
  }
  virtual ~ObDASMatchCtDef() {}
  INHERIT_TO_STRING_KV("ObIDASSearchCtDef", ObIDASSearchCtDef,
      KPC_(query_text),
      KPC_(boost),
      KPC_(match_operator),
      KPC_(minimum_should_match),
      K_(ir_ctdef_idx),
      K_(pushdown_must_filter_op_ctdef_idx));

  ObExpr *query_text_;
  ObExpr *boost_;
  ObExpr *match_operator_;
  ObExpr *minimum_should_match_;
  int64_t ir_ctdef_idx_;
  int64_t pushdown_must_filter_op_ctdef_idx_;
};

struct ObDASMatchRtDef : ObIDASSearchRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASMatchRtDef()
    : ObIDASSearchRtDef(DAS_OP_MATCH_QUERY),
      allocator_(ObMemAttr(MTL_ID(), "DASMatchQ")),
      block_max_param_(),
      pushdown_filter_op_(nullptr),
      block_max_param_initialized_(false),
      query_optional_(false)
  {}
  virtual ~ObDASMatchRtDef() {}
  // TODO: implement cost computing
  virtual int compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost) override { return OB_SUCCESS; }
  virtual int generate_op(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op) override;
  virtual int can_pushdown_filter_to_bmm(bool &can_pushdown) override;
  virtual void set_pushdown_filter(const bool query_optional, ObIDASSearchOp *filter_op) override
  {
    pushdown_filter_op_ = filter_op;
    query_optional_ = query_optional;
  }
  ObDASIRScanRtDef *get_ir_rtdef() const
  {
    const int64_t ir_rtdef_idx = static_cast<const ObDASMatchCtDef *>(ctdef_)->ir_ctdef_idx_;
    return ir_rtdef_idx >= 0 ? static_cast<ObDASIRScanRtDef *>(children_[ir_rtdef_idx]) : nullptr;
  }
  ObIDASSearchRtDef *get_pushdown_must_filter_op_rtdef() const
  {
    const int64_t pushdown_must_filter_op_rtdef_idx
        = static_cast<const ObDASMatchCtDef *>(ctdef_)->pushdown_must_filter_op_ctdef_idx_;
    return pushdown_must_filter_op_rtdef_idx >= 0
        ? static_cast<ObIDASSearchRtDef *>(children_[pushdown_must_filter_op_rtdef_idx]) : nullptr;
  }
private:
  const ObDASMatchCtDef * get_ctdef() const { return static_cast<const ObDASMatchCtDef *>(ctdef_); }
public:
  ObArenaAllocator allocator_; // op-level lifetime
  storage::ObBlockMaxScoreIterParam block_max_param_;
  ObIDASSearchOp *pushdown_filter_op_;
  bool block_max_param_initialized_;
  bool query_optional_;
};


}
}
#endif // OCEANBASE_SQL_OB_DAS_TEXT_RETRIEVAL_DEFINE_H_