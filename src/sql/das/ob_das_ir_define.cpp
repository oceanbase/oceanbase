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
#include "ob_das_ir_define.h"

namespace oceanbase
{
namespace sql
{

ObTextBlockMaxSpec::ObTextBlockMaxSpec(common::ObIAllocator &alloc)
  : col_types_(alloc),
    col_store_idxes_(alloc),
    scan_col_proj_(alloc),
    min_id_idx_(-1),
    max_id_idx_(-1),
    token_freq_idx_(-1),
    doc_length_idx_(-1) {}

OB_SERIALIZE_MEMBER(ObTextBlockMaxSpec,
    col_types_,
    col_store_idxes_,
    scan_col_proj_,
    min_id_idx_,
    max_id_idx_,
    token_freq_idx_,
    doc_length_idx_);

bool ObTextBlockMaxSpec::is_valid() const
{
  return 4 == col_types_.count()
      && 4 == col_store_idxes_.count()
      && 4 == scan_col_proj_.count()
      && 0 == min_id_idx_
      && 1 == max_id_idx_
      && 2 == token_freq_idx_
      && 3 == doc_length_idx_;
}

OB_DEF_SERIALIZE(ObDASIRScanCtDef)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASAttachCtDef::serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize ObDASAttachCtDef", K(ret));
  }
  LST_DO_CODE(OB_UNIS_ENCODE,
    flags_,
    search_text_,
    inv_scan_domain_id_col_, // FARM COMPAT WHITELIST
    inv_scan_doc_length_col_,
    match_filter_,
    relevance_expr_,
    relevance_proj_col_,
    estimated_total_doc_cnt_,
    mode_flag_,
    topk_limit_expr_,
    topk_offset_expr_,
    token_col_,
    field_boost_expr_);
  if (OB_SUCC(ret) && has_block_max_scan_) {
    OB_UNIS_ENCODE(block_max_spec_);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDASIRScanCtDef)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASAttachCtDef::deserialize(buf, data_len, pos))) {
    LOG_WARN("failed to deserialize ObDASAttachCtDef", K(ret));
  }
  LST_DO_CODE(OB_UNIS_DECODE,
    flags_,
    search_text_,
    inv_scan_domain_id_col_, // FARM COMPAT WHITELIST
    inv_scan_doc_length_col_,
    match_filter_,
    relevance_expr_,
    relevance_proj_col_,
    estimated_total_doc_cnt_,
    mode_flag_,
    topk_limit_expr_,
    topk_offset_expr_,
    token_col_,
    field_boost_expr_);
  if (OB_SUCC(ret) && has_block_max_scan_) {
    OB_UNIS_DECODE(block_max_spec_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDASIRScanCtDef)
{
  int64_t len = 0;
  len += ObDASAttachCtDef::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN,
    flags_,
    search_text_,
    inv_scan_domain_id_col_, // FARM COMPAT WHITELIST
    inv_scan_doc_length_col_,
    match_filter_,
    relevance_expr_,
    relevance_proj_col_,
    estimated_total_doc_cnt_,
    mode_flag_,
    topk_limit_expr_,
    topk_offset_expr_,
    token_col_,
    field_boost_expr_);
  if (has_block_max_scan_) {
    OB_UNIS_ADD_LEN(block_max_spec_);
  }
  return len;
}

OB_SERIALIZE_MEMBER(ObDASIRScanRtDef);

OB_SERIALIZE_MEMBER((ObDASIRAuxLookupCtDef, ObDASAttachCtDef),
                    relevance_proj_col_);

OB_SERIALIZE_MEMBER((ObDASIRAuxLookupRtDef, ObDASAttachRtDef));

OB_SERIALIZE_MEMBER((ObDASFuncLookupCtDef, ObDASAttachCtDef),
    main_lookup_cnt_,
    doc_id_lookup_cnt_,
    func_lookup_cnt_,
    lookup_domain_id_expr_ // FARM COMPAT WHITELIST
    );

OB_SERIALIZE_MEMBER((ObDASFuncLookupRtDef, ObDASAttachRtDef));

} // sql
} // oceanbase
