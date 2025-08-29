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
#include "share/vector_index/ob_vector_index_param.h"
#include "src/sql/das/ob_das_ir_define.h"

namespace oceanbase
{
namespace sql
{

struct ObSPIVBlockMaxSpec
{
  OB_UNIS_VERSION(1);
public:
  ObSPIVBlockMaxSpec(common::ObIAllocator &alloc);
  virtual ~ObSPIVBlockMaxSpec() {}

  bool is_valid() const;
  TO_STRING_KV(K_(col_types), K_(col_store_idxes),
      K_(scan_col_proj), K_(min_id_idx), K_(max_id_idx), K_(value_idx));

  static const int64_t SKIP_INDEX_COUNT = 3;
  static const int32_t MIN_ID_IDX = 0;
  static const int32_t MAX_ID_IDX = 1;
  static const int32_t VALUE_IDX = 2;
  ObFixedArray<ObSkipIndexColType, ObIAllocator> col_types_;
  ObFixedArray<int32_t, ObIAllocator> col_store_idxes_;
  ObFixedArray<int32_t, ObIAllocator> scan_col_proj_;
  int32_t min_id_idx_;
  int32_t max_id_idx_;
  int32_t value_idx_;
};

struct ObDASVecAuxScanCtDef : ObDASAttachCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASVecAuxScanCtDef(common::ObIAllocator &alloc)
    : ObDASAttachCtDef(alloc, DAS_OP_VEC_SCAN),
      inv_scan_vec_id_col_(nullptr),
      vec_index_param_(),
      dim_(0),
      vec_type_(ObVecIndexType::VEC_INDEX_INVALID),
      algorithm_type_(ObVectorIndexAlgorithmType::VIAT_MAX),
      selectivity_(0.0),
      row_count_(0),
      access_pk_(false),
      can_use_vec_pri_opt_(false),
      extra_column_count_(0),
      spiv_scan_docid_col_(nullptr),
      spiv_scan_value_col_(nullptr),
      vector_index_param_(),
      vec_query_param_(),
      adaptive_try_path_(ObVecIdxAdaTryPath::VEC_PATH_UNCHOSEN),
      is_multi_value_index_(false),
      is_spatial_index_(false),
      can_extract_range_(false),
      block_max_spec_(alloc),
      relevance_col_cnt_(0),
      is_hybrid_(false) {}

  inline bool is_pre_filter() const { return ObVecIndexType::VEC_INDEX_PRE == vec_type_;  }
  inline bool is_vec_adaptive_scan() const { return ObVecIndexType::VEC_INDEX_ADAPTIVE_SCAN == vec_type_ && ObVecIdxAdaTryPath::VEC_PATH_UNCHOSEN != adaptive_try_path_; }
  inline bool is_post_filter() const { return ObVecIndexType::VEC_INDEX_POST_WITHOUT_FILTER == vec_type_ || ObVecIndexType::VEC_INDEX_POST_ITERATIVE_FILTER == vec_type_; }
  inline bool is_iter_filter() const { return ObVecIndexType::VEC_INDEX_POST_ITERATIVE_FILTER == vec_type_; }
  inline bool filter_in_hnsw_iter() const { return ObVecIndexType::VEC_INDEX_PRE == vec_type_ || ObVecIndexType::VEC_INDEX_POST_ITERATIVE_FILTER == vec_type_; }
  inline void set_can_use_vec_pri_opt(bool can_use_vec_pri_opt) {can_use_vec_pri_opt_ = can_use_vec_pri_opt;}
  inline bool can_use_vec_pri_opt() const { return can_use_vec_pri_opt_; }

  int64_t get_inv_scan_idx() const { return ObVecAuxTableIdx::VALID_VID_SCAN_IDX; }
  int64_t get_delta_tbl_idx() const { return ObVecAuxTableIdx::FIRST_VEC_AUX_TBL_IDX; }
  int64_t get_index_id_tbl_idx() const { return ObVecAuxTableIdx::SECOND_VEC_AUX_TBL_IDX; }
  int64_t get_snapshot_tbl_idx() const { return ObVecAuxTableIdx::THIRD_VEC_AUX_TBL_IDX; }
  int64_t get_com_aux_tbl_idx() const { return ObVecAuxTableIdx::FOURTH_VEC_AUX_TBL_IDX; }
  int64_t get_rowkey_vid_tbl_idx() const { return ObVecAuxTableIdx::FIFTH_VEC_AUX_TBL_IDX; }
  const ObVectorIndexParam& get_vec_index_param() const { return vector_index_param_; }
  int64_t get_functial_lookup_idx() const { return children_cnt_ - 1; }

  // IVF
  int64_t get_ivf_centroid_tbl_idx() const { return ObVecAuxTableIdx::FIRST_VEC_AUX_TBL_IDX; }
  // for pq, is ivf_pq_code
  int64_t get_ivf_cid_vec_tbl_idx() const { return ObVecAuxTableIdx::SECOND_VEC_AUX_TBL_IDX; }
  // for pq, is ivf_pq_rowkey_cid
  int64_t get_ivf_rowkey_cid_tbl_idx() const { return ObVecAuxTableIdx::THIRD_VEC_AUX_TBL_IDX; }
  int64_t get_ivf_sq_meta_tbl_idx() const { return ObVecAuxTableIdx::FOURTH_VEC_AUX_TBL_IDX ; }
  int64_t get_ivf_pq_id_tbl_idx() const { return ObVecAuxTableIdx::FOURTH_VEC_AUX_TBL_IDX; }
  int64_t get_ivf_brute_tbl_idx() const
  {
    return algorithm_type_ == ObVectorIndexAlgorithmType::VIAT_IVF_FLAT ? ObVecAuxTableIdx::FOURTH_VEC_AUX_TBL_IDX
                                                                        : ObVecAuxTableIdx::FIFTH_VEC_AUX_TBL_IDX;
  }

  // SPIV
  int64_t get_spiv_scan_idx() const { return ObVecAuxTableIdx::FIRST_VEC_AUX_TBL_IDX; }
  int64_t get_spiv_rowkey_docid_tbl_idx() const { return ObVecAuxTableIdx::SECOND_VEC_AUX_TBL_IDX; }
  int64_t get_spiv_aux_data_tbl_idx() const { return ObVecAuxTableIdx::THIRD_VEC_AUX_TBL_IDX; }
  int64_t get_spiv_block_max_scan_idx() const { return ObVecAuxTableIdx::FOURTH_VEC_AUX_TBL_IDX; }


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

  const ObDASFuncLookupCtDef *get_functional_lookup_ctdef() const
  {
    const ObDASBaseCtDef *func_ctdef = nullptr;
    if (children_cnt_ > 0 && children_ != nullptr) {
      func_ctdef = children_[get_functial_lookup_idx()];
      if (OB_ISNULL(func_ctdef) || DAS_OP_FUNC_LOOKUP != func_ctdef->op_type_ ) {
        func_ctdef = nullptr;
      }
    }
    return static_cast<const ObDASFuncLookupCtDef*>(func_ctdef);
  }

  INHERIT_TO_STRING_KV("ObDASBaseCtDef", ObDASBaseCtDef,
                       KPC_(inv_scan_vec_id_col), K_(vec_index_param), K_(dim),
                       K_(vec_type), K_(algorithm_type), K_(selectivity), K_(row_count),
                       K_(extra_column_count), K_(vector_index_param), K_(vec_query_param),
                       K_(vector_index_param), K_(adaptive_try_path), K_(is_multi_value_index),
                       K_(is_spatial_index), K_(can_extract_range));

  ObExpr *inv_scan_vec_id_col_;
  ObString vec_index_param_;
  int64_t dim_;
  ObVecIndexType vec_type_;
  ObVectorIndexAlgorithmType algorithm_type_;
  double selectivity_;
  double row_count_;
  bool access_pk_;
  bool can_use_vec_pri_opt_;
  int64_t extra_column_count_; // for hnsw
  ObExpr *spiv_scan_docid_col_;
  ObExpr *spiv_scan_value_col_;
  ObVectorIndexParam vector_index_param_;
  ObVectorIndexQueryParam vec_query_param_;
  ObVecIdxAdaTryPath adaptive_try_path_;
  bool is_multi_value_index_;
  bool is_spatial_index_;
  bool can_extract_range_;
  ObSPIVBlockMaxSpec block_max_spec_;
  int64_t relevance_col_cnt_;
  bool is_hybrid_;
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

  ObDASFuncLookupRtDef* get_functional_lookup_rtdef()
  {
    const ObDASVecAuxScanCtDef *ctdef = static_cast<const ObDASVecAuxScanCtDef *>(ctdef_);
    const int64_t rtdef_idx = ctdef->get_functial_lookup_idx();
    ObDASFuncLookupRtDef* rtdef = nullptr;
    if (children_cnt_ > rtdef_idx && children_ != nullptr && OB_NOT_NULL(ctdef->get_functional_lookup_ctdef())) {
      rtdef = static_cast<ObDASFuncLookupRtDef*>(children_[rtdef_idx]);
    }
    return rtdef;
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
