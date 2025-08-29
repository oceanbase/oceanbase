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

#ifndef OB_DAS_IR_DEFINE_H_
#define OB_DAS_IR_DEFINE_H_

#include "ob_das_attach_define.h"

namespace oceanbase
{
namespace sql
{

struct ObTextBlockMaxSpec
{
  OB_UNIS_VERSION(1);
public:
  ObTextBlockMaxSpec(common::ObIAllocator &alloc);
  virtual ~ObTextBlockMaxSpec() {}

  bool is_valid() const;
  TO_STRING_KV(K_(col_types), K_(col_store_idxes),
      K_(scan_col_proj), K_(min_id_idx), K_(max_id_idx), K_(token_freq_idx),
      K_(doc_length_idx));
  ObFixedArray<ObSkipIndexColType, ObIAllocator> col_types_;
  ObFixedArray<int32_t, ObIAllocator> col_store_idxes_;
  ObFixedArray<int32_t, ObIAllocator> scan_col_proj_;
  int32_t min_id_idx_;
  int32_t max_id_idx_;
  int32_t token_freq_idx_;
  int32_t doc_length_idx_;
};

struct ObDASIRScanCtDef : ObDASAttachCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASIRScanCtDef(common::ObIAllocator &alloc)
    : ObDASAttachCtDef(alloc, DAS_OP_IR_SCAN),
      search_text_(nullptr),
      inv_scan_domain_id_col_(nullptr),
      inv_scan_doc_length_col_(nullptr),
      match_filter_(nullptr),
      relevance_expr_(nullptr),
      relevance_proj_col_(nullptr),
      estimated_total_doc_cnt_(0),
      topk_limit_expr_(nullptr),
      topk_offset_expr_(nullptr),
      token_col_(nullptr),
      block_max_spec_(alloc),
      mode_flag_(NATURAL_LANGUAGE_MODE),
      flags_(0),
      field_boost_expr_(nullptr) {}
  bool need_calc_relevance() const { return nullptr != relevance_expr_; }
  bool need_proj_relevance_score() const { return nullptr != relevance_proj_col_; }
  bool need_fwd_idx_agg() const { return has_fwd_agg_ && need_calc_relevance(); }
  bool need_inv_idx_agg() const { return has_inv_agg_ && need_calc_relevance(); }
  bool need_block_max_scan() const { return has_block_max_scan_; }
  bool has_pushdown_topk() const { return nullptr != topk_limit_expr_; }
  bool is_block_scan_valid() const
  {
    return has_block_max_scan_
        && nullptr != token_col_
        && nullptr != topk_limit_expr_
        && block_max_spec_.is_valid();
  }
  const ObDASScanCtDef *get_inv_idx_scan_ctdef() const
  {
    const ObDASScanCtDef *idx_scan_ctdef = nullptr;
    if (children_cnt_ > 0 && children_ != nullptr) {
      idx_scan_ctdef = static_cast<const ObDASScanCtDef*>(children_[get_inv_scan_idx()]);
    }
    return idx_scan_ctdef;
  }
  const ObDASScanCtDef *get_inv_idx_agg_ctdef() const
  {
    const ObDASScanCtDef *idx_agg_ctdef = nullptr;
    const int64_t ctdef_idx = get_inv_agg_idx();
    if (children_cnt_ > ctdef_idx && ctdef_idx > 0 && children_ != nullptr) {
      const ObDASScanCtDef *child = static_cast<const ObDASScanCtDef *>(children_[ctdef_idx]);
      if (child->ir_scan_type_ == ObTSCIRScanType::OB_IR_INV_IDX_AGG) {
        idx_agg_ctdef = child;
      }
    }
    return idx_agg_ctdef;
  }
  const ObDASScanCtDef *get_doc_agg_ctdef() const
  {
    const ObDASScanCtDef *doc_agg_ctdef = nullptr;
    const int64_t ctdef_idx = get_doc_agg_idx();
    if (children_cnt_ > ctdef_idx && ctdef_idx > 0 && children_ != nullptr) {
      const ObDASScanCtDef *child = static_cast<const ObDASScanCtDef *>(children_[ctdef_idx]);
      if (child->ir_scan_type_ == ObTSCIRScanType::OB_IR_DOC_ID_IDX_AGG) {
        doc_agg_ctdef = child;
      }
    }
    return doc_agg_ctdef;
  }
  const ObDASScanCtDef *get_fwd_idx_agg_ctdef() const
  {
    const ObDASScanCtDef *fwd_idx_agg_ctdef = nullptr;
    const int64_t ctdef_idx = get_fwd_agg_idx();
    if (children_cnt_ > ctdef_idx && ctdef_idx > 0 && children_ != nullptr) {
      const ObDASScanCtDef *child = static_cast<const ObDASScanCtDef *>(children_[ctdef_idx]);
      if (child->ir_scan_type_ == ObTSCIRScanType::OB_IR_FWD_IDX_AGG) {
        fwd_idx_agg_ctdef = child;
      }
    }
    return fwd_idx_agg_ctdef;
  }
  const ObDASScanCtDef *get_block_max_scan_ctdef() const
  {
    const ObDASScanCtDef *block_max_scan_ctdef = nullptr;
    const int64_t ctdef_idx = get_block_max_scan_idx();
    if (children_cnt_ > ctdef_idx && ctdef_idx > 0 && children_ != nullptr) {
      const ObDASScanCtDef *child = static_cast<const ObDASScanCtDef *>(children_[ctdef_idx]);
      if (child->ir_scan_type_ == ObTSCIRScanType::OB_IR_BLOCK_MAX_SCAN) {
        block_max_scan_ctdef = child;
      }
    }
    return block_max_scan_ctdef;
  }
  int64_t get_inv_scan_idx() const { return 0; }
  int64_t get_inv_agg_idx() const { return has_inv_agg_ ? 1 : -1; }
  int64_t get_doc_agg_idx() const { return has_doc_id_agg_ ? (1 + has_inv_agg_) : -1; }
  int64_t get_fwd_agg_idx() const { return has_fwd_agg_ ? (1 + has_inv_agg_ + has_doc_id_agg_) : -1; }
  int64_t get_block_max_scan_idx() const
  {
    return has_block_max_scan_ ? (1 + has_inv_agg_ + has_doc_id_agg_ + has_fwd_agg_) : -1;
  }
  bool need_estimate_total_doc_cnt() const { return 0 != estimated_total_doc_cnt_; }

  INHERIT_TO_STRING_KV("ObDASBaseCtDef", ObDASBaseCtDef,
                       K_(flags),
                       KPC_(search_text),
                       KPC_(inv_scan_domain_id_col),
                       KPC_(inv_scan_doc_length_col),
                       KPC_(match_filter),
                       KPC_(relevance_expr),
                       KPC_(relevance_proj_col),
                       K_(estimated_total_doc_cnt),
                       KPC_(topk_limit_expr),
                       KPC_(topk_offset_expr),
                       K_(token_col),
                       K_(block_max_spec),
                       K_(mode_flag),
                       KPC_(field_boost_expr));

  ObExpr *search_text_;
  ObExpr *inv_scan_domain_id_col_;
  ObExpr *inv_scan_doc_length_col_;
  ObExpr *match_filter_;
  ObExpr *relevance_expr_;
  ObExpr *relevance_proj_col_;
  int64_t estimated_total_doc_cnt_;
  ObExpr *topk_limit_expr_;
  ObExpr *topk_offset_expr_;
  ObExpr *token_col_;
  ObTextBlockMaxSpec block_max_spec_;
  ObMatchAgainstMode mode_flag_; // for MySQL search mode flag
  union
  {
    uint8_t flags_;
    struct
    {
      uint8_t has_inv_agg_:1;
      uint8_t has_doc_id_agg_:1;
      uint8_t has_fwd_agg_:1;
      uint8_t has_block_max_scan_:1;
      uint8_t reserved_:4;
    };
  };
  ObExpr *field_boost_expr_;
};

struct ObDASIRScanRtDef : ObDASAttachRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASIRScanRtDef()
    : ObDASAttachRtDef(DAS_OP_IR_SCAN),
      fts_idx_(OB_INVALID_INDEX),
      minimum_should_match_(0) {}

  virtual ~ObDASIRScanRtDef() {}

  ObDASScanRtDef *get_inv_idx_scan_rtdef()
  {
    const ObDASIRScanCtDef *ctdef = static_cast<const ObDASIRScanCtDef *>(ctdef_);
    const int64_t rtdef_idx = ctdef->get_inv_scan_idx();
    ObDASScanRtDef *idx_scan_rtdef = nullptr;
    if (children_cnt_ > rtdef_idx && children_ != nullptr) {
      idx_scan_rtdef = static_cast<ObDASScanRtDef*>(children_[rtdef_idx]);
    }
    return idx_scan_rtdef;
  }
  ObDASScanRtDef *get_inv_idx_agg_rtdef()
  {
    const ObDASIRScanCtDef *ctdef = static_cast<const ObDASIRScanCtDef *>(ctdef_);
    const int64_t rtdef_idx = ctdef->get_inv_agg_idx();
    ObDASScanRtDef *idx_agg_rtdef = nullptr;
    if (children_cnt_ > rtdef_idx && rtdef_idx > 0 && children_ != nullptr) {
      idx_agg_rtdef = static_cast<ObDASScanRtDef*>(children_[rtdef_idx]);
    }
    return idx_agg_rtdef;
  }
  ObDASScanRtDef *get_doc_agg_rtdef()
  {
    const ObDASIRScanCtDef *ctdef = static_cast<const ObDASIRScanCtDef *>(ctdef_);
    const int64_t rtdef_idx = ctdef->get_doc_agg_idx();
    ObDASScanRtDef *doc_id_idx_agg_rtdef = nullptr;
    if (children_cnt_ > rtdef_idx && rtdef_idx > 0 && children_ != nullptr) {
      doc_id_idx_agg_rtdef = static_cast<ObDASScanRtDef*>(children_[rtdef_idx]);
    }
    return doc_id_idx_agg_rtdef;
  }
  ObDASScanRtDef *get_fwd_idx_agg_rtdef() const
  {
    const ObDASIRScanCtDef *ctdef = static_cast<const ObDASIRScanCtDef *>(ctdef_);
    const int64_t rtdef_idx = ctdef->get_fwd_agg_idx();
    ObDASScanRtDef *fwd_idx_agg_rtdef = nullptr;
    if (children_cnt_ > rtdef_idx && rtdef_idx > 0 && children_ != nullptr) {
      fwd_idx_agg_rtdef = static_cast<ObDASScanRtDef*>(children_[rtdef_idx]);
    }
    return fwd_idx_agg_rtdef;
  }
  ObDASScanRtDef *get_block_max_scan_rtdef() const
  {
    const ObDASIRScanCtDef *ctdef = static_cast<const ObDASIRScanCtDef *>(ctdef_);
    const int64_t rtdef_idx = ctdef->get_block_max_scan_idx();
    ObDASScanRtDef *block_max_scan_rtdef = nullptr;
    if (children_cnt_ > rtdef_idx && rtdef_idx > 0 && children_ != nullptr) {
      block_max_scan_rtdef = static_cast<ObDASScanRtDef*>(children_[rtdef_idx]);
    }
    return block_max_scan_rtdef;
  }

  // currently a query could involve multiple fts indexes, such as index merge or func lookup,
  // and fts_idx_ serves as a unique identifier for each fts index, such as locating the corresponding
  // fts tablet ids in ObDASRelatedTabletID.
  // fts_idx_ is dynamically generated during execution based on the rtdef tree and does not need to be serialized.
  int64_t fts_idx_;
  int64_t minimum_should_match_;
};

struct ObDASIRAuxLookupCtDef : ObDASAttachCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASIRAuxLookupCtDef(common::ObIAllocator &alloc)
    : ObDASAttachCtDef(alloc, DAS_OP_IR_AUX_LOOKUP),
      relevance_proj_col_(nullptr) {}

  const ObDASBaseCtDef *get_doc_id_scan_ctdef() const
  {
    OB_ASSERT(2 == children_cnt_ && children_ != nullptr);
    return children_[0];
  }
  const ObDASScanCtDef *get_lookup_scan_ctdef() const
  {
    OB_ASSERT(children_cnt_ == 2 && children_ != nullptr);
    return static_cast<const ObDASScanCtDef*>(children_[1]);
  }

  ObExpr *relevance_proj_col_;
};

struct ObDASIRAuxLookupRtDef : ObDASAttachRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASIRAuxLookupRtDef()
    : ObDASAttachRtDef(DAS_OP_IR_AUX_LOOKUP) {}

  virtual ~ObDASIRAuxLookupRtDef() {}

  ObDASBaseRtDef *get_doc_id_scan_rtdef()
  {
    OB_ASSERT(2 == children_cnt_ && children_ != nullptr);
    return children_[0];
  }
  ObDASScanRtDef *get_lookup_scan_rtdef()
  {
    OB_ASSERT(children_cnt_ == 2 && children_ != nullptr);
    return static_cast<ObDASScanRtDef*>(children_[1]);
  }
};

struct ObDASFuncLookupCtDef : ObDASAttachCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASFuncLookupCtDef(common::ObIAllocator &alloc)
    : ObDASAttachCtDef(alloc, DAS_OP_FUNC_LOOKUP),
      main_lookup_cnt_(0),
      doc_id_lookup_cnt_(0),
      func_lookup_cnt_(0),
      lookup_domain_id_expr_(nullptr) {}

  virtual ~ObDASFuncLookupCtDef() {}

  bool has_main_table_lookup() const { return main_lookup_cnt_ > 0; }
  bool has_doc_id_lookup() const { return doc_id_lookup_cnt_ > 0; }
  int64_t get_func_lookup_scan_idx(const int64_t idx) const
  {
    OB_ASSERT(children_cnt_ == (main_lookup_cnt_ + doc_id_lookup_cnt_ + func_lookup_cnt_));
    return (idx < func_lookup_cnt_) ? (idx + doc_id_lookup_cnt_ + main_lookup_cnt_) : -1;
  }

  int64_t get_doc_id_lookup_scan_idx() const
  {
    OB_ASSERT(children_cnt_ == (main_lookup_cnt_ + doc_id_lookup_cnt_ + func_lookup_cnt_));
    const int64_t ret_idx = has_doc_id_lookup() ? (main_lookup_cnt_) : -1;
    return ret_idx;
  }

  int64_t get_main_lookup_scan_idx() const
  {
    OB_ASSERT(children_cnt_ == (main_lookup_cnt_ + doc_id_lookup_cnt_ + func_lookup_cnt_));
    const int64_t ret_idx = has_main_table_lookup() ? 0 : -1;
    return ret_idx;
  }

  const ObDASBaseCtDef *get_func_lookup_scan_ctdef(const int64_t idx) const
  {
    const ObDASBaseCtDef *ctdef = nullptr;
    const int64_t children_idx = get_func_lookup_scan_idx(idx);
    if (children_idx >= 0 && children_idx < children_cnt_ && nullptr != children_) {
      ctdef = children_[children_idx];
    }
    return ctdef;
  }

  // need to check the return pointer
  const ObDASBaseCtDef *get_doc_id_lookup_scan_ctdef() const
  {
    ObDASBaseCtDef *doc_id_lookup_scan_ctdef = nullptr;
    const int64_t children_idx = get_doc_id_lookup_scan_idx();
    if (children_idx >= 0 && children_idx < children_cnt_ && nullptr != children_) {
      doc_id_lookup_scan_ctdef = children_[children_idx];
    }
    return doc_id_lookup_scan_ctdef;
  }

  const ObDASBaseCtDef *get_main_lookup_scan_ctdef() const
  {
    ObDASBaseCtDef *main_lookup_ctdef = nullptr;
    const int64_t children_idx = get_main_lookup_scan_idx();
    if (children_idx >= 0 && children_idx < children_cnt_ && nullptr != children_) {
      main_lookup_ctdef = children_[children_idx];
    }
    return main_lookup_ctdef;
  }

  int64_t main_lookup_cnt_;
  int64_t doc_id_lookup_cnt_;
  int64_t func_lookup_cnt_;
  ObExpr *lookup_domain_id_expr_; // when the docid column is not exist, the expr is the rowkey expr
};

struct ObDASFuncLookupRtDef : ObDASAttachRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASFuncLookupRtDef()
    : ObDASAttachRtDef(DAS_OP_FUNC_LOOKUP) {}

  virtual ~ObDASFuncLookupRtDef() {}

  int64_t get_func_lookup_count() const
  {
    return static_cast<const ObDASFuncLookupCtDef *>(ctdef_)->func_lookup_cnt_;
  }

  ObDASBaseRtDef *get_func_lookup_scan_rtdef(const int64_t idx) const
  {
    const ObDASFuncLookupCtDef *ctdef = static_cast<const ObDASFuncLookupCtDef *>(ctdef_);
    ObDASBaseRtDef *rtdef = nullptr;
    const int64_t children_idx = ctdef->get_func_lookup_scan_idx(idx);
    if (children_idx >= 0 && children_idx < children_cnt_ && nullptr != children_) {
      rtdef = children_[children_idx];
    }
    return rtdef;
  }

  ObDASBaseRtDef *get_doc_id_lookup_scan_rtdef() const
  {
    const ObDASFuncLookupCtDef *ctdef = static_cast<const ObDASFuncLookupCtDef *>(ctdef_);
    ObDASBaseRtDef *rtdef = nullptr;
    const int64_t children_idx = ctdef->get_doc_id_lookup_scan_idx();
    if (children_idx >= 0 && children_idx < children_cnt_ && nullptr != children_) {
      rtdef = children_[children_idx];
    }
    return rtdef;
  }

  ObDASBaseRtDef *get_main_lookup_scan_rtdef() const
  {
    const ObDASFuncLookupCtDef *ctdef = static_cast<const ObDASFuncLookupCtDef *>(ctdef_);
    ObDASBaseRtDef *rtdef = nullptr;
    const int64_t children_idx = ctdef->get_main_lookup_scan_idx();
    if (children_idx >= 0 && children_idx < children_cnt_ && nullptr != children_) {
      rtdef = children_[children_idx];
    }
    return rtdef;
  }
};

struct ObDASIREsMatchCtDef : ObDASAttachCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASIREsMatchCtDef(common::ObIAllocator &alloc)
    : ObDASAttachCtDef(alloc, DAS_OP_IR_ES_MATCH),
      relevance_proj_col_(nullptr),
      inv_scan_domain_id_col_(nullptr),
      es_param_text_expr_(nullptr) {}
  virtual ~ObDASIREsMatchCtDef() {}

  INHERIT_TO_STRING_KV("ObDASBaseCtDef", ObDASBaseCtDef,
                       KPC_(relevance_proj_col),
                       KPC_(inv_scan_domain_id_col),
                       KPC_(es_param_text_expr));
  ObExpr *relevance_proj_col_;
  ObExpr *inv_scan_domain_id_col_;
  ObExpr *es_param_text_expr_;
};

struct ObDASIREsMatchRtDef : ObDASAttachRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASIREsMatchRtDef()
    : ObDASAttachRtDef(DAS_OP_IR_ES_MATCH),
      match_boost_(0),
      match_operator_(ObMatchOperator::MATCH_OPERATOR_OR),
      score_norm_function_(ObMatchScoreNorm::SCORE_NORM_NONE),
      match_fields_type_(ObMatchFiledsType::MATCH_MOST_FIELDS) {}

  virtual ~ObDASIREsMatchRtDef() {}
  double match_boost_;
  ObMatchOperator match_operator_;
  ObMatchScoreNorm score_norm_function_;
  ObMatchFiledsType match_fields_type_;
};

struct ObDASIREsScoreCtDef : ObDASAttachCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASIREsScoreCtDef(common::ObIAllocator &alloc)
    : ObDASAttachCtDef(alloc, DAS_OP_IR_ES_SCORE)
    {}
  virtual ~ObDASIREsScoreCtDef() {}
};

struct ObDASIREsScoreRtDef : ObDASAttachRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASIREsScoreRtDef()
    : ObDASAttachRtDef(DAS_OP_IR_ES_SCORE) {}

  virtual ~ObDASIREsScoreRtDef() {}
};

class ObDocIdExt final
{
public:
  ObDocIdExt();
  ObDocIdExt(const ObDocIdExt &other);
  ~ObDocIdExt() = default;
  void reset();

  int hash(uint64_t &hash_val) const;
  const ObDatum &get_datum() const;
  int from_datum(const ObDatum &datum);
  int from_obj(const ObObj &obj);

  ObDocIdExt &operator=(const ObDocIdExt &other);
  bool operator==(const ObDocIdExt &other) const;
  bool operator!=(const ObDocIdExt &other) const;

  TO_STRING_KV(KP_(buf), K_(datum));
private:
  static const int64_t OB_DOC_ID_EXT_SIZE = 40;
  char buf_[OB_DOC_ID_EXT_SIZE];
  ObDatum datum_;
};

} // namespace sql
} // namespace oceanbase

#endif
