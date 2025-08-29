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

OB_SERIALIZE_MEMBER((ObDASIREsMatchCtDef, ObDASAttachCtDef),
                    relevance_proj_col_,
                    inv_scan_domain_id_col_,
                    es_param_text_expr_);

OB_SERIALIZE_MEMBER((ObDASIREsMatchRtDef, ObDASAttachRtDef));

OB_SERIALIZE_MEMBER((ObDASIREsScoreCtDef, ObDASAttachCtDef));

OB_SERIALIZE_MEMBER((ObDASIREsScoreRtDef, ObDASAttachRtDef));

ObDocIdExt::ObDocIdExt()
  : buf_{}, datum_(buf_, 0, false)
{
}

ObDocIdExt::ObDocIdExt(const ObDocIdExt &other)
  : buf_{}, datum_(other.datum_)
{
  MEMCPY(buf_, other.buf_, OB_DOC_ID_EXT_SIZE);
  datum_.ptr_ = buf_;
}

void ObDocIdExt::reset()
{
  MEMSET(buf_, 0, OB_DOC_ID_EXT_SIZE);
  datum_ = ObDatum(buf_, 0, false);
}

int ObDocIdExt::hash(uint64_t &hash_val) const
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(datum_.ptr_)) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(datum_.ptr_));
  } else {
    hash_val = murmurhash(datum_.ptr_, datum_.len_, 0);
  }
  return ret;
}

const ObDatum &ObDocIdExt::get_datum() const
{
  return datum_;
}

int ObDocIdExt::from_obj(const ObObj &obj)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(datum_.from_obj(obj))) {
    LOG_WARN("failed to copy obj to doc id ext", K(ret));
  }
  return ret;
}

int ObDocIdExt::from_datum(const ObDatum &datum)
{
  int ret = common::OB_SUCCESS;
  int64_t dummy_pos = 0;
  if (OB_FAIL(datum_.deep_copy(datum, buf_, OB_DOC_ID_EXT_SIZE, dummy_pos))) {
    LOG_WARN("failed to copy datum to doc id ext", K(ret));
  }
  return ret;
}

ObDocIdExt &ObDocIdExt::operator=(const ObDocIdExt &other)
{
  if (this != &other) {
    MEMCPY(buf_, other.buf_, OB_DOC_ID_EXT_SIZE);
    datum_ = other.datum_;
    datum_.ptr_ = buf_;
  }
  return *this;
}

bool ObDocIdExt::operator==(const ObDocIdExt &other) const
{
  return ObDatum::binary_equal(datum_, other.datum_);
}

bool ObDocIdExt::operator!=(const ObDocIdExt &other) const
{
  return !(*this == other);
}

} // sql
} // oceanbase
