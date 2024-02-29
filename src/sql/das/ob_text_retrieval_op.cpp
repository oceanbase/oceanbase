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

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/ob_text_retrieval_op.h"

namespace oceanbase
{
namespace sql
{

OB_DEF_SERIALIZE_SIZE(ObDASIRCtDef)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      inv_idx_scan_ctdef_,
      search_text_,
      inv_scan_doc_id_col_,
      match_filter_,
      need_relevance_);
  if (need_relevance_) {
    if (OB_NOT_NULL(inv_idx_agg_ctdef_)
        && OB_NOT_NULL(fwd_idx_scan_ctdef_)
        && OB_NOT_NULL(doc_id_idx_whole_agg_ctdef_)) {
      LST_DO_CODE(OB_UNIS_ADD_LEN,
          relevance_expr_,
          relevance_proj_col_,
          *inv_idx_agg_ctdef_,
          *doc_id_idx_whole_agg_ctdef_,
          *fwd_idx_scan_ctdef_);
      }
  }
  return len;
}

OB_DEF_SERIALIZE(ObDASIRCtDef)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
      inv_idx_scan_ctdef_,
      search_text_,
      inv_scan_doc_id_col_,
      match_filter_,
      need_relevance_);
  if (OB_SUCC(ret) && need_relevance_) {
    if (OB_ISNULL(inv_idx_agg_ctdef_)
        || OB_ISNULL(fwd_idx_scan_ctdef_)
        || OB_ISNULL(doc_id_idx_whole_agg_ctdef_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected", K(ret), KP_(inv_idx_agg_ctdef), KP_(fwd_idx_scan_ctdef), KP_(doc_id_idx_whole_agg_ctdef));
    } else {
      LST_DO_CODE(OB_UNIS_ENCODE,
          relevance_expr_,
          relevance_proj_col_,
          *inv_idx_agg_ctdef_,
          *doc_id_idx_whole_agg_ctdef_,
          *fwd_idx_scan_ctdef_);
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDASIRCtDef)
{
  int ret = OB_SUCCESS;
  need_relevance_ = false;
  LST_DO_CODE(OB_UNIS_DECODE,
      inv_idx_scan_ctdef_,
      search_text_,
      inv_scan_doc_id_col_,
      match_filter_,
      need_relevance_);
  if (OB_SUCC(ret) && need_relevance_) {
    if (OB_ISNULL(inv_idx_agg_ctdef_ = OB_NEWx(ObDASScanCtDef, &allocator_, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ctdef", K(ret));
    } else if (OB_ISNULL(fwd_idx_scan_ctdef_ = OB_NEWx(ObDASScanCtDef, &allocator_, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ctdef", K(ret));
    } else if (OB_ISNULL(doc_id_idx_whole_agg_ctdef_ = OB_NEWx(ObDASScanCtDef, &allocator_, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ctdef", K(ret));
    } else {
      LST_DO_CODE(OB_UNIS_DECODE,
          relevance_expr_,
          relevance_proj_col_,
          *inv_idx_agg_ctdef_,
          *doc_id_idx_whole_agg_ctdef_,
          *fwd_idx_scan_ctdef_);
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDASIRRtDef)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      inv_idx_scan_rtdef_,
      need_relevance_);
  if (need_relevance_) {
    if (OB_NOT_NULL(inv_idx_agg_rtdef_) && OB_NOT_NULL(doc_id_idx_whole_agg_rtdef_) && OB_NOT_NULL(fwd_idx_rtdef_)) {
      LST_DO_CODE(OB_UNIS_ADD_LEN,
          *inv_idx_agg_rtdef_,
          *doc_id_idx_whole_agg_rtdef_,
          *fwd_idx_rtdef_);
    }
  }
  return len;
}

OB_DEF_SERIALIZE(ObDASIRRtDef)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
      inv_idx_scan_rtdef_,
      need_relevance_);
  if (OB_SUCC(ret) && need_relevance_) {
    if (OB_ISNULL(inv_idx_agg_rtdef_) || OB_ISNULL(doc_id_idx_whole_agg_rtdef_) || OB_ISNULL(fwd_idx_rtdef_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null rtdef", K(ret), KP_(inv_idx_agg_rtdef), KP_(fwd_idx_rtdef), KP_(doc_id_idx_whole_agg_rtdef));
    } else {
      LST_DO_CODE(OB_UNIS_ENCODE,
          *inv_idx_agg_rtdef_,
          *doc_id_idx_whole_agg_rtdef_,
          *fwd_idx_rtdef_);
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDASIRRtDef)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
      inv_idx_scan_rtdef_,
      need_relevance_);
  if (OB_SUCC(ret) && need_relevance_) {
    if (OB_ISNULL(inv_idx_agg_rtdef_ = OB_NEWx(ObDASScanRtDef, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for rtdef", K(ret));
    } else if (OB_ISNULL(fwd_idx_rtdef_ = OB_NEWx(ObDASScanRtDef, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for rtdef", K(ret));
    } else if (OB_ISNULL(doc_id_idx_whole_agg_rtdef_ = OB_NEWx(ObDASScanRtDef, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for rtdef", K(ret));
    } else {
      LST_DO_CODE(OB_UNIS_DECODE,
          *inv_idx_agg_rtdef_,
          *doc_id_idx_whole_agg_rtdef_,
          *fwd_idx_rtdef_);
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
