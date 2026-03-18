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

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_VEC_INDEX_DRIVER_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_VEC_INDEX_DRIVER_ITER_H_

#include "sql/das/iter/ob_das_iter.h"
#include "sql/das/iter/ob_das_vec_index_scan_iter.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

enum class ObVecFilterMode {
  VEC_FILTER_MODE_INVALID = 0,
  VEC_FILTER_MODE_PRE_FILTER,
  VEC_FILTER_MODE_SEARCH_DRIVER_FILTER,
  VEC_FILTER_MODE_EXPR_FILTER
};

struct ObDASVecIndexDriverCtDef : ObDASAttachCtDef
{
  OB_UNIS_VERSION(1);
public:
ObDASVecIndexDriverCtDef(common::ObIAllocator &alloc)
    : ObDASAttachCtDef(alloc, DAS_OP_VEC_INDEX_DRIVER),
      sort_expr_(nullptr),
      limit_expr_(nullptr),
      offset_expr_(nullptr),
      vec_index_param_(),
      query_param_(),
      vector_index_param_(),
      algorithm_type_(ObVectorIndexAlgorithmType::VIAT_MAX),
      vec_type_(ObVecIndexType::VEC_INDEX_INVALID),
      row_count_(0),
      dim_(0),
      filter_mode_(ObKnnFilterMode::INVALID_KNN_FILTER_MODE) {}

  virtual ~ObDASVecIndexDriverCtDef() = default;

  INHERIT_TO_STRING_KV("ObDASBaseCtDef", ObDASBaseCtDef,
                       K_(sort_expr), K_(limit_expr), K_(offset_expr), K_(vec_index_param),
                       K_(dim), K_(vec_type), K_(algorithm_type), K_(row_count),
                       K_(vector_index_param), K_(query_param));

  ObExpr *sort_expr_;
  ObExpr *limit_expr_;
  ObExpr *offset_expr_;

  ObString vec_index_param_;
  ObVectorIndexQueryParam query_param_;
  ObVectorIndexParam vector_index_param_;
  ObVectorIndexAlgorithmType algorithm_type_;
  ObVecIndexType vec_type_;
  int64_t row_count_;
  int64_t dim_;
  ObKnnFilterMode filter_mode_;
};

struct ObDASVecIndexDriverRtDef : ObDASAttachRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASVecIndexDriverRtDef()
    : ObDASAttachRtDef(DAS_OP_VEC_INDEX_DRIVER) {}

  virtual ~ObDASVecIndexDriverRtDef() = default;
};

struct ObDASVecIndexDriverIterParam : public ObDASIterParam
{
  OB_UNIS_VERSION(1);
public:
  ObDASVecIndexDriverIterParam()
    : ObDASIterParam(ObDASIterType::DAS_ITER_VEC_INDEX_DRIVER),
      ls_id_(),
      tx_desc_(nullptr),
      snapshot_(nullptr),
      vec_index_scan_iter_(nullptr),
      filter_iter_(nullptr),
      vec_index_driver_ctdef_(nullptr),
      vec_index_driver_rtdef_(nullptr),
      vec_index_type_(ObVecIndexType::VEC_INDEX_INVALID),
      vec_idx_try_path_(ObVecIdxAdaTryPath::VEC_PATH_UNCHOSEN),
      search_ctx_(nullptr),
      sort_expr_(nullptr),
      limit_expr_(nullptr),
      offset_expr_(nullptr),
      score_expr_(nullptr),
      dim_(0),
      filter_mode_(ObVecFilterMode::VEC_FILTER_MODE_INVALID),
      scalar_scan_ctdef_(nullptr),
      scalar_scan_rtdef_(nullptr) {}

  virtual ~ObDASVecIndexDriverIterParam() {}

  virtual bool is_valid() const override
  {
    bool valid = nullptr != vec_index_driver_ctdef_ &&
                 nullptr != vec_index_driver_rtdef_ &&
                 nullptr != vec_index_scan_iter_ &&
                 nullptr != search_ctx_ &&
                 nullptr != sort_expr_ &&
                 nullptr != limit_expr_;
    return valid;
  }

  TO_STRING_KV(K_(vec_index_driver_ctdef),
              K_(vec_index_driver_rtdef),
              K_(filter_iter),
              K_(vec_index_scan_iter),
              K_(vec_index_type),
              K_(vec_idx_try_path),
              K_(filter_mode));

  share::ObLSID ls_id_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;

  ObDASVecIndexScanIter *vec_index_scan_iter_;
  ObDASIter *filter_iter_;

  const ObDASVecIndexDriverCtDef *vec_index_driver_ctdef_;
  ObDASVecIndexDriverRtDef *vec_index_driver_rtdef_;

  ObVecIndexType vec_index_type_;
  ObVecIdxAdaTryPath vec_idx_try_path_;

  ObDASSearchCtx *search_ctx_;
  ObExpr* sort_expr_;
  ObExpr* limit_expr_;
  ObExpr* offset_expr_;
  ObExpr* score_expr_;

  int64_t dim_;

  // 表达式过滤模式所需参数
  ObVecFilterMode filter_mode_;
  const ObDASScalarScanCtDef *scalar_scan_ctdef_;
  ObDASScalarScanRtDef *scalar_scan_rtdef_;
};


class ObDASVecIndexDriverIter : public ObDASIter
{
public:
  ObDASVecIndexDriverIter()
    : ObDASIter(ObDASIterType::DAS_ITER_VEC_INDEX_DRIVER),
      ls_id_(ObLSID::INVALID_LS_ID),
      tx_desc_(nullptr),
      snapshot_(nullptr),
      mem_context_(nullptr),
      vec_op_alloc_("VecIdxDriver", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      vec_index_scan_iter_(nullptr),
      filter_iter_(nullptr),
      bitmap_(nullptr),
      vec_index_driver_ctdef_(nullptr),
      vec_index_driver_rtdef_(nullptr),
      vec_index_type_(ObVecIndexType::VEC_INDEX_INVALID),
      vec_idx_try_path_(ObVecIdxAdaTryPath::VEC_PATH_UNCHOSEN),
      search_ctx_(nullptr),
      sort_expr_(nullptr),
      limit_expr_(nullptr),
      offset_expr_(nullptr),
      score_expr_(nullptr),
      limit_param_(),
      dim_(0),
      delta_buf_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      index_id_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      snapshot_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      com_aux_vec_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      ready_to_output_(false),
      query_cond_(),
      search_vec_(nullptr),
      search_param_(),
      distance_threshold_(FLT_MAX),
      adaptor_vid_iter_(nullptr),
      iter_unfiltered_vid_cnt_(0),
      iter_added_cnt_(0),
      iter_scan_total_num_(0),
      vid_to_distance_(),
      filter_mode_(ObVecFilterMode::VEC_FILTER_MODE_INVALID),
      scalar_scan_ctdef_(nullptr),
      scalar_scan_rtdef_(nullptr),
      profile_(nullptr) {}

  virtual ~ObDASVecIndexDriverIter() {}

  virtual int do_table_scan() override;
  virtual int rescan() override;
  virtual void clear_evaluated_flag() override;

  bool is_hnsw_bq() const
  {
    return OB_NOT_NULL(vec_index_driver_ctdef_) &&
           vec_index_driver_ctdef_->algorithm_type_ == ObVectorIndexAlgorithmType::VIAT_HNSW_BQ;
  }

  int set_vec_index_param(ObString vec_index_param) { return ob_write_string(vec_op_alloc_, vec_index_param, vec_index_param_); }
  void set_related_tablet_ids(const ObDASRelatedTabletID &related_tablet_ids)
  {
    delta_buf_tablet_id_ = related_tablet_ids.delta_buf_tablet_id_;
    index_id_tablet_id_ = related_tablet_ids.index_id_tablet_id_;
    snapshot_tablet_id_ = related_tablet_ids.snapshot_tablet_id_;
    com_aux_vec_tablet_id_ = related_tablet_ids.lookup_tablet_id_;
  }

  ObDASVecIndexScanIter *get_vec_index_scan_iter() const { return vec_index_scan_iter_; }

  static const uint64_t VSAG_MAX_EF_SEARCH = 160000;
  static constexpr double FIXED_MAGNIFICATION_RATIO = 2.0;
  static constexpr double FIXED_MAGNIFICATION_RATIO_EACH_ITERATIVE = 10.0;
  static constexpr double ITER_CONSIDER_LAST_SEARCH_SELETIVITY = 0.05;

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;

private:
  int get_reorder_count(const int64_t ef_search, const int64_t topK, const ObVectorIndexParam& param);
  int set_vector_query_condition(ObVectorQueryConditions &query_cond);
  int init_limit_param();
  int init_sort();
  int get_ob_hnsw_ef_search(uint64_t &ob_hnsw_ef_search);
  int process_pre_filter_mode(share::ObPluginVectorIndexAdaptor* adaptor, int64_t &count, int64_t capacity);
  int process_iterative_filter_mode(share::ObPluginVectorIndexAdaptor* adaptor, int64_t &count, int64_t capacity);
  int process_post_filter_mode(share::ObPluginVectorIndexAdaptor* adaptor, int64_t &count, int64_t capacity);
  int save_vec_socre_expr_result(ObNewRow *row, int64_t size);
  int distance_to_score(double distance, double &score);

  int build_bitmap_from_filter_iter(share::ObPluginVectorIndexAdaptor* adaptor);
  int set_bitmap_to_filter_iter();
  int adjust_vector_query_condition(bool first_search);
  bool can_be_last_search(int64_t old_ef, int64_t need_cnt_next, float select_ratio);
  int fetch_vids_from_vec_index(share::ObPluginVectorIndexAdaptor* adaptor, int64_t &count, int64_t capacity);
  int post_query_vid_with_filter();
  int fill_results_to_eval_ctx(share::ObVectorQueryVidIterator *adaptor_vid_iter, int64_t &count, int64_t capacity);

  int post_query_vid_with_expr_filter();
  int filter_by_index_back(int64_t vid, float distance, bool &filter_res);
  int set_rowkey_by_vid(int64_t vid);
  int get_single_row_from_filter_iter(bool is_vectorized);

  int init_adaptor_vid_iter_if_null();

  OB_INLINE bool is_pre_filter() const { return vec_index_type_ == ObVecIndexType::VEC_INDEX_PRE; }
  OB_INLINE bool is_iter_filter() const { return vec_index_type_ == ObVecIndexType::VEC_INDEX_POST_ITERATIVE_FILTER; }
  OB_INLINE bool is_post_filter() const { return is_post_without_filter() || is_post_with_filter(); }
  OB_INLINE bool is_post_without_filter() const { return vec_index_type_ == ObVecIndexType::VEC_INDEX_POST_WITHOUT_FILTER; }
  OB_INLINE bool is_post_with_filter() const
  {
    return vec_index_type_ == ObVecIndexType::VEC_INDEX_ADAPTIVE_SCAN &&
           vec_idx_try_path_ == ObVecIdxAdaTryPath::VEC_INDEX_POST_FILTER;
  }

  share::ObLSID ls_id_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;

  lib::MemoryContext mem_context_;  // for temporary allocation
  ObArenaAllocator vec_op_alloc_;

  ObDASVecIndexScanIter *vec_index_scan_iter_;
  ObDASIter *filter_iter_;
  ObVecIndexBitmap bitmap_;

  const ObDASVecIndexDriverCtDef *vec_index_driver_ctdef_;
  ObDASVecIndexDriverRtDef *vec_index_driver_rtdef_;

  ObString vec_index_param_;
  ObVecIndexType vec_index_type_;
  ObVecIdxAdaTryPath vec_idx_try_path_;

  ObDASSearchCtx *search_ctx_;
  ObExpr* sort_expr_;
  ObExpr* limit_expr_;
  ObExpr* offset_expr_;
  ObExpr* score_expr_;
  ObLimitParam limit_param_;
  int64_t dim_;

  ObTabletID delta_buf_tablet_id_;
  ObTabletID index_id_tablet_id_;
  ObTabletID snapshot_tablet_id_;
  ObTabletID com_aux_vec_tablet_id_;

  bool ready_to_output_;

  ObVectorQueryConditions query_cond_;
  ObExpr* search_vec_;
  ObVectorIndexParam search_param_;
  float distance_threshold_;

  ObVectorQueryVidIterator* adaptor_vid_iter_;

  // for iterative filter mode: use index merge filter
  int64_t iter_unfiltered_vid_cnt_;
  int64_t iter_added_cnt_;
  int64_t iter_scan_total_num_;
  hash::ObHashMap<int64_t, float> vid_to_distance_;

  // for iterative filter mode： use expr filter
  ObVecFilterMode filter_mode_;
  const ObDASScalarScanCtDef *scalar_scan_ctdef_;
  ObDASScalarScanRtDef *scalar_scan_rtdef_;
  common::ObObj vid_obj_for_lookup_;
  common::ObOpProfile<common::ObMetric> *profile_;
};

} // namespace sql
} // namespace oceanbase

#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_VEC_INDEX_DRIVER_ITER_H_ */
