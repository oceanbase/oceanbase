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

#ifndef OB_DAS_VEC_DEFINE_H_
#define OB_DAS_VEC_DEFINE_H_

#include "ob_das_attach_define.h"
#include "src/sql/optimizer/ob_join_order.h"
#include "src/share/vector_index/ob_vector_index_util.h"

namespace oceanbase
{
namespace sql
{

struct ObDASVecAuxScanCtDef : ObDASAttachCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASVecAuxScanCtDef(common::ObIAllocator &alloc)
    : ObDASAttachCtDef(alloc, DAS_OP_VEC_SCAN),
      inv_scan_vec_id_col_(nullptr),
      vec_index_param_(),
      dim_(0),
      access_pk_(false) {}

  bool is_pre_filter() const { return ObVecIndexType::VEC_INDEX_PRE == vec_type_; }
  bool is_post_filter() const { return ObVecIndexType::VEC_INDEX_POST == vec_type_; }
  bool is_brute_force_filter() const { return is_pre_filter() && false && row_count_ * selectivity_ < 100000; }

  int64_t get_inv_scan_idx() const { return ObVecAuxTableIdx::VALID_VID_SCAN_IDX; }
  int64_t get_delta_tbl_idx() const { return ObVecAuxTableIdx::FIRST_VEC_AUX_TBL_IDX; }
  int64_t get_index_id_tbl_idx() const { return ObVecAuxTableIdx::SECOND_VEC_AUX_TBL_IDX; }
  int64_t get_snapshot_tbl_idx() const { return ObVecAuxTableIdx::THIRD_VEC_AUX_TBL_IDX; }
  int64_t get_com_aux_tbl_idx() const { return ObVecAuxTableIdx::COM_AUX_TBL_IDX; }
  int64_t get_rowkey_vid_tbl_idx() const { return ObVecAuxTableIdx::FOURTH_VEC_AUX_TBL_IDX; }

  // IVF
  int64_t get_ivf_centroid_tbl_idx() const { return ObVecAuxTableIdx::FIRST_VEC_AUX_TBL_IDX; }
  // for pq, is ivf_pq_code
  int64_t get_ivf_cid_vec_tbl_idx() const { return ObVecAuxTableIdx::SECOND_VEC_AUX_TBL_IDX; }
  // for pq, is ivf_pq_rowkey_cid
  int64_t get_ivf_rowkey_cid_tbl_idx() const { return ObVecAuxTableIdx::THIRD_VEC_AUX_TBL_IDX; }
  int64_t get_ivf_sq_meta_tbl_idx() const { return ObVecAuxTableIdx::FOURTH_VEC_AUX_TBL_IDX ; }
  int64_t get_ivf_pq_id_tbl_idx() const { return ObVecAuxTableIdx::FOURTH_VEC_AUX_TBL_IDX; }

  const ObDASBaseCtDef *get_inv_idx_scan_ctdef() const
  {
    const  ObDASBaseCtDef *idx_scan_ctdef = nullptr;
    if (children_cnt_ > 0 && children_ != nullptr) {
      idx_scan_ctdef = children_[get_inv_scan_idx()];
    }
    return idx_scan_ctdef;
  }

  const ObDASScanCtDef *get_vec_aux_tbl_ctdef(int64_t ctdef_idx, ObTSCIRScanType scan_type) const
  {
    const ObDASScanCtDef *aux_tbl_ctdef = nullptr;
    if (children_cnt_ > ctdef_idx && ctdef_idx > 0 && children_ != nullptr) {
      const ObDASScanCtDef *child = static_cast<const ObDASScanCtDef *>(children_[ctdef_idx]);
      if (child->ir_scan_type_ == scan_type) {
        aux_tbl_ctdef = child;
      }
    }
    return aux_tbl_ctdef;
  }

  INHERIT_TO_STRING_KV("ObDASBaseCtDef", ObDASBaseCtDef,
                       KPC_(inv_scan_vec_id_col), K_(vec_index_param), K_(dim),
                       K_(vec_type), K_(algorithm_type), K_(selectivity), K_(row_count));

  ObExpr *inv_scan_vec_id_col_;
  ObString vec_index_param_;
  int64_t dim_;
  ObVecIndexType vec_type_;
  ObVectorIndexAlgorithmType algorithm_type_;
  double selectivity_;
  double row_count_;
  bool access_pk_;
};

struct ObDASVecAuxScanRtDef : ObDASAttachRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASVecAuxScanRtDef()
    : ObDASAttachRtDef(DAS_OP_VEC_SCAN) {}

  virtual ~ObDASVecAuxScanRtDef() {}

  ObDASBaseRtDef *get_inv_idx_scan_rtdef()
  {
    const ObDASVecAuxScanCtDef *ctdef = static_cast<const ObDASVecAuxScanCtDef *>(ctdef_);
    const int64_t rtdef_idx = ctdef->get_inv_scan_idx();
    ObDASBaseRtDef *idx_scan_rtdef = nullptr;
    if (children_cnt_ > rtdef_idx && children_ != nullptr) {
      idx_scan_rtdef = children_[rtdef_idx];
    }
    return idx_scan_rtdef;
  }

  ObDASScanRtDef *get_vec_aux_tbl_rtdef(int64_t rtdef_idx) const
  {
    const ObDASVecAuxScanCtDef *ctdef = static_cast<const ObDASVecAuxScanCtDef *>(ctdef_);
    ObDASScanRtDef *rtdef = nullptr;
    if (children_cnt_ > rtdef_idx && children_ != nullptr) {
      rtdef = static_cast<ObDASScanRtDef*>(children_[rtdef_idx]);
    }
    return rtdef;
  }
};

} // namespace sql
} // namespace oceanbase

#endif
