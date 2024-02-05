/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OBDEV_SRC_SQL_DAS_OB_TEXT_RETRIEVAL_OP_H_
#define OBDEV_SRC_SQL_DAS_OB_TEXT_RETRIEVAL_OP_H_

#include "sql/das/ob_das_task.h"
#include "sql/das/ob_das_scan_op.h"

namespace oceanbase
{
namespace sql
{
static const int64_t OB_MAX_TEXT_RETRIEVAL_TOKEN_CNT = 64;

// Lots of meta data in ctdef / rtdef for inverted index / forward index scan are redundant here
//  Should we make constant meta datas shared between iterators ?
struct ObDASIRCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASIRCtDef(common::ObIAllocator &alloc)
    : allocator_(alloc),
      search_text_(nullptr),
      inv_scan_doc_id_col_(nullptr),
      relevance_expr_(nullptr),
      match_filter_(nullptr),
      relevance_proj_col_(nullptr),
      inv_idx_scan_ctdef_(alloc),
      inv_idx_loc_meta_(alloc),
      inv_idx_agg_ctdef_(nullptr),
      doc_id_idx_whole_agg_ctdef_(nullptr),
      fwd_idx_scan_ctdef_(nullptr),
      fwd_idx_loc_meta_(nullptr),
      need_relevance_(false) {}
  virtual ~ObDASIRCtDef() {}
  ObExpr *get_inv_scan_doc_id() const { return inv_scan_doc_id_col_; }
  ObExpr *get_relevance_expr() const { return relevance_expr_; }
  ObExpr *get_relevance_proj_expr() const { return relevance_proj_col_; }
  bool need_proj_relevance_score() const { return nullptr != relevance_proj_col_; }
  TO_STRING_KV(
      KPC_(search_text),
      KPC_(inv_scan_doc_id_col),
      KPC_(relevance_expr),
      KPC_(match_filter),
      KPC_(relevance_proj_col),
      K_(inv_idx_scan_ctdef),
      K_(inv_idx_loc_meta),
      KPC_(inv_idx_agg_ctdef),
      KPC_(doc_id_idx_whole_agg_ctdef),
      KPC_(fwd_idx_scan_ctdef),
      KPC_(fwd_idx_loc_meta),
      K_(need_relevance));

  common::ObIAllocator &allocator_;
  ObExpr *search_text_;
  ObExpr *inv_scan_doc_id_col_;
  ObExpr *relevance_expr_;
  ObExpr *match_filter_;
  ObExpr *relevance_proj_col_;
  ObDASScanCtDef inv_idx_scan_ctdef_;
  ObDASTableLocMeta inv_idx_loc_meta_;
  ObDASScanCtDef *inv_idx_agg_ctdef_;
  ObDASScanCtDef *doc_id_idx_whole_agg_ctdef_; // aggregate on inverted index with whole range
  ObDASScanCtDef *fwd_idx_scan_ctdef_;
  ObDASTableLocMeta *fwd_idx_loc_meta_;
  bool need_relevance_;
};

struct ObDASIRRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASIRRtDef(common::ObIAllocator &alloc)
    : allocator_(alloc),
      inv_idx_scan_rtdef_(),
      inv_idx_agg_rtdef_(nullptr),
      doc_id_idx_whole_agg_rtdef_(nullptr),
      fwd_idx_rtdef_(nullptr),
      eval_ctx_(nullptr),
      need_relevance_(false) {}
  virtual ~ObDASIRRtDef() {}

  TO_STRING_KV(K_(inv_idx_scan_rtdef), KPC_(inv_idx_agg_rtdef), KPC_(doc_id_idx_whole_agg_rtdef),
      KPC_(fwd_idx_rtdef), KPC_(eval_ctx), K_(need_relevance));
  common::ObIAllocator &allocator_;
  ObDASScanRtDef inv_idx_scan_rtdef_;
  ObDASScanRtDef *inv_idx_agg_rtdef_;
  ObDASScanRtDef *doc_id_idx_whole_agg_rtdef_;
  ObDASScanRtDef *fwd_idx_rtdef_;
  ObEvalCtx *eval_ctx_;
  bool need_relevance_;
};

} // namespace sql
} // namespace oceanbase

#endif
