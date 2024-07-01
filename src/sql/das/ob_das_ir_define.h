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

struct ObDASIRScanCtDef : ObDASAttachCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASIRScanCtDef(common::ObIAllocator &alloc)
    : ObDASAttachCtDef(alloc, DAS_OP_IR_SCAN),
      search_text_(nullptr),
      inv_scan_doc_id_col_(nullptr),
      inv_scan_doc_length_col_(nullptr),
      match_filter_(nullptr),
      relevance_expr_(nullptr),
      relevance_proj_col_(nullptr),
      estimated_total_doc_cnt_(0),
      flags_(0)
  {
  }
  bool need_calc_relevance() const { return nullptr != relevance_expr_; }
  bool need_proj_relevance_score() const { return nullptr != relevance_proj_col_; }
  bool need_fwd_idx_agg() const { return has_fwd_agg_ && need_calc_relevance(); }
  bool need_inv_idx_agg() const { return has_inv_agg_ && need_calc_relevance(); }
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
  const ObDASScanCtDef *get_doc_id_idx_agg_ctdef() const
  {
    const ObDASScanCtDef *doc_id_idx_agg_ctdef = nullptr;
    const int64_t ctdef_idx = get_doc_agg_idx();
    if (children_cnt_ > ctdef_idx && ctdef_idx > 0 && children_ != nullptr) {
      const ObDASScanCtDef *child = static_cast<const ObDASScanCtDef *>(children_[ctdef_idx]);
      if (child->ir_scan_type_ == ObTSCIRScanType::OB_IR_DOC_ID_IDX_AGG) {
        doc_id_idx_agg_ctdef = child;
      }
    }
    return doc_id_idx_agg_ctdef;
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
  int64_t get_inv_scan_idx() const { return 0; }
  int64_t get_inv_agg_idx() const { return has_inv_agg_ ? 1 : -1; }
  int64_t get_doc_agg_idx() const { return has_doc_id_agg_ ? (1 + has_inv_agg_) : -1; }
  int64_t get_fwd_agg_idx() const { return has_fwd_agg_ ? (1 + has_inv_agg_ + has_doc_id_agg_) : -1; }
  bool need_do_total_doc_cnt() const { return need_calc_relevance() && 0 == estimated_total_doc_cnt_; }

  INHERIT_TO_STRING_KV("ObDASBaseCtDef", ObDASBaseCtDef,
                       K_(flags),
                       KPC_(search_text),
                       KPC_(inv_scan_doc_id_col),
                       KPC_(inv_scan_doc_length_col),
                       KPC_(match_filter),
                       KPC_(relevance_expr),
                       KPC_(relevance_proj_col),
                       K_(estimated_total_doc_cnt));

  ObExpr *search_text_;
  ObExpr *inv_scan_doc_id_col_;
  ObExpr *inv_scan_doc_length_col_;
  ObExpr *match_filter_;
  ObExpr *relevance_expr_;
  ObExpr *relevance_proj_col_;
  int64_t estimated_total_doc_cnt_;
  union
  {
    uint8_t flags_;
    struct
    {
      uint8_t has_inv_agg_:1;
      uint8_t has_doc_id_agg_:1;
      uint8_t has_fwd_agg_:1;
      uint8_t reserved_:5;
    };
  };
};

struct ObDASIRScanRtDef : ObDASAttachRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASIRScanRtDef()
    : ObDASAttachRtDef(DAS_OP_IR_SCAN) {}

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
  ObDASScanRtDef *get_doc_id_idx_agg_rtdef()
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
};

struct ObDASIRAuxLookupCtDef : ObDASAttachCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASIRAuxLookupCtDef(common::ObIAllocator &alloc)
    : ObDASAttachCtDef(alloc, DAS_OP_IR_AUX_LOOKUP),
      relevance_proj_col_(nullptr)
  { }

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
    : ObDASAttachRtDef(DAS_OP_IR_AUX_LOOKUP)
  {}

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

} // namespace sql
} // namespace oceanbase

#endif
