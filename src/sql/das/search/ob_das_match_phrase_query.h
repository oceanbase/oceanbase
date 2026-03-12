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

#ifndef OCEANBASE_SQL_OB_DAS_MATCH_PHRASE_QUERY_H_
#define OCEANBASE_SQL_OB_DAS_MATCH_PHRASE_QUERY_H_

#include "sql/das/ob_das_ir_define.h"
#include "sql/das/search/ob_i_das_search_op.h"
#include "storage/retrieval/ob_block_max_iter.h"

namespace oceanbase
{
namespace sql
{

struct ObDASMatchPhraseCtDef : ObIDASSearchCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASMatchPhraseCtDef(ObIAllocator &alloc)
    : ObIDASSearchCtDef(alloc, DAS_OP_MATCH_PHRASE_QUERY),
      query_text_(nullptr),
      boost_(nullptr),
      slop_(nullptr),
      ir_ctdef_idx_(-1)
  {
    set_is_scoring(true);
  }
  ObDASIRScanCtDef *get_ir_ctdef() const
  {
    return ir_ctdef_idx_ >= 0 ? static_cast<ObDASIRScanCtDef *>(children_[ir_ctdef_idx_]) : nullptr;
  }
  virtual ~ObDASMatchPhraseCtDef() {}
  INHERIT_TO_STRING_KV("ObIDASSearchCtDef", ObIDASSearchCtDef,
      KPC_(query_text),
      KPC_(boost),
      KPC_(slop),
      K_(ir_ctdef_idx));
public:
  ObExpr *query_text_;
  ObExpr *boost_;
  ObExpr *slop_;
  int64_t ir_ctdef_idx_;
};

struct ObDASMatchPhraseRtDef : ObIDASSearchRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASMatchPhraseRtDef()
    : ObIDASSearchRtDef(DAS_OP_MATCH_PHRASE_QUERY),
      allocator_(ObMemAttr(MTL_ID(), "DASMatchPQ")),
      block_max_param_(),
      block_max_param_initialized_(false)
  {}
  int compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost) override { return OB_SUCCESS; }
  int generate_op(
      ObDASSearchCost lead_cost,
      ObDASSearchCtx &search_ctx,
      ObIDASSearchOp *&op) override;
  ObDASIRScanRtDef *get_ir_rtdef() const
  {
    const int64_t ir_rtdef_idx = static_cast<const ObDASMatchPhraseCtDef *>(ctdef_)->ir_ctdef_idx_;
    return ir_rtdef_idx >= 0 ? static_cast<ObDASIRScanRtDef *>(children_[ir_rtdef_idx]) : nullptr;
  }
public:
  ObArenaAllocator allocator_; // op-level lifetime
  storage::ObBlockMaxScoreIterParam block_max_param_;
  bool block_max_param_initialized_;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_DAS_MATCH_PHRASE_QUERY_H_
