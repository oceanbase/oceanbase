/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_HNSW_SCAN_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_HNSW_SCAN_ITER_H_

#include "sql/das/iter/ob_das_iter.h"
#include "sql/das/iter/ob_das_scan_iter.h"
#include "src/share/vector_index/ob_plugin_vector_index_service.h"
#include "sql/engine/expr/ob_expr_vector.h"
#include "sql/das/iter/ob_das_vec_scan_utils.h"
#include "src/share/vector_index/ob_vector_index_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

enum ObVidAdaLookupStatus
{
  STATES_INIT,
  QUERY_INDEX_ID_TBL,
  QUERY_SNAPSHOT_TBL, 
  QUERY_ROWKEY_VEC,
  STATES_SET_RESULT,
  STATES_ERROR,
  STATES_FINISH, 
  STATES_REFRESH,
};

enum ObAdaptorIterRowIdx
{
  DISTANCE_OBJ_IDX = 0,
  VID_OBJ_IDX = 1,
  ROWKEY_START_IDX = 2, 
};

struct ObDASHNSWScanIterParam : public ObDASIterParam
{
public:
  ObDASHNSWScanIterParam()
    : ObDASIterParam(ObDASIterType::DAS_ITER_HNSW_SCAN),
      ls_id_(),
      tx_desc_(nullptr),
      snapshot_(nullptr),
      inv_idx_scan_iter_(nullptr),
      delta_buf_iter_(nullptr),
      index_id_iter_(nullptr),
      snapshot_iter_(nullptr),
      vid_rowkey_iter_(nullptr),
      com_aux_vec_iter_(nullptr),
      rowkey_vid_iter_(nullptr),
      data_filter_iter_(nullptr),
      pre_scan_param_(nullptr),
      vec_aux_ctdef_(nullptr),
      vec_aux_rtdef_(nullptr),
      vid_rowkey_ctdef_(nullptr),
      vid_rowkey_rtdef_(nullptr),
      data_filter_ctdef_(nullptr),
      data_filter_rtdef_(nullptr),
      sort_ctdef_(nullptr),
      sort_rtdef_(nullptr),
      vec_index_type_(ObVecIndexType::VEC_INDEX_INVALID),
      vec_idx_try_path_(ObVecIdxAdaTryPath::VEC_PATH_UNCHOSEN),
      can_extract_range_(false),
      is_primary_index_(false) {}

  virtual bool is_valid() const override
  {
    return ls_id_.is_valid() &&
           nullptr != tx_desc_ &&
           nullptr != snapshot_ && 
           nullptr != delta_buf_iter_ && 
           nullptr != index_id_iter_ && 
           nullptr != snapshot_iter_ &&
           nullptr != vid_rowkey_iter_ &&
           nullptr != com_aux_vec_iter_ &&
           nullptr != pre_scan_param_ &&
           nullptr != vec_aux_ctdef_ && 
           nullptr != vec_aux_rtdef_ &&
           nullptr != vid_rowkey_ctdef_ &&
           nullptr != vid_rowkey_rtdef_ &&
           (!((is_adaptive_plan() || is_pre_plan()) &&
           nullptr == rowkey_vid_iter_)) &&
           (!((is_adaptive_plan() || is_iter_plan()) && 
           nullptr == data_filter_iter_));
  }
  bool is_adaptive_plan() const { return vec_index_type_ == ObVecIndexType::VEC_INDEX_ADAPTIVE_SCAN;}
  bool is_pre_plan() const { return vec_index_type_ == ObVecIndexType::VEC_INDEX_PRE;}
  bool is_iter_plan() const { return vec_index_type_ == ObVecIndexType::VEC_INDEX_POST_ITERATIVE_FILTER;}
  TO_STRING_KV(K_(vec_aux_ctdef),
               K_(vid_rowkey_ctdef),
               K_(data_filter_ctdef),
               K_(vec_index_type),
               K_(vec_idx_try_path),
               K_(can_extract_range),
               K_(is_primary_index));

  share::ObLSID ls_id_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;

  ObDASIter *inv_idx_scan_iter_;
  ObDASScanIter *delta_buf_iter_;
  ObDASScanIter *index_id_iter_;
  ObDASScanIter *snapshot_iter_;
  ObDASScanIter *vid_rowkey_iter_;
  ObDASScanIter *com_aux_vec_iter_;
  ObDASScanIter *rowkey_vid_iter_;
  ObDASScanIter *data_filter_iter_;
  ObTableScanParam* pre_scan_param_;
  const ObDASVecAuxScanCtDef *vec_aux_ctdef_;
  ObDASVecAuxScanRtDef *vec_aux_rtdef_;
  const ObDASScanCtDef *vid_rowkey_ctdef_;
  ObDASScanRtDef *vid_rowkey_rtdef_;
  const ObDASScanCtDef *data_filter_ctdef_;
  ObDASScanRtDef *data_filter_rtdef_;
  const ObDASSortCtDef *sort_ctdef_;
  ObDASSortRtDef *sort_rtdef_; 
  ObVecIndexType vec_index_type_;
  ObVecIdxAdaTryPath vec_idx_try_path_;
  bool can_extract_range_;
  bool is_primary_index_;
};
class ObSimpleMaxHeap;

class ObHnswSimpleCmpInfo
{
  public:
  ObHnswSimpleCmpInfo() 
  : inited_(false), 
    filter_arg_(),
    filter_expr_(nullptr) {}
  
  ~ObHnswSimpleCmpInfo() {};
  void reset() { 
    inited_ = false; 
    filter_expr_ = nullptr;
  }
  
  bool inited_;
  ObObj filter_arg_;
  ObExpr* filter_expr_;
};

class ObHnswAadaptiveCtx
{
  public:
  ObHnswAadaptiveCtx() 
  : selectivity_(0.0),
    row_count_(0),
    retry_times_(0),
    pre_total_scan_rows_(0),
    iter_times_(0),
    iter_filter_row_cnt_(0),
    iter_res_row_cnt_(0),
    pre_scan_row_cnt_(0),
    can_extract_range_(false),
    is_primary_index_(false),
    with_extra_info_(false),
    only_rowkey_filter_(false) {}
  TO_STRING_KV(K_(selectivity),
               K_(row_count),
               K_(pre_total_scan_rows),
               K_(iter_times),
               K_(iter_filter_row_cnt),
               K_(iter_res_row_cnt),
               K_(pre_scan_row_cnt),
               K_(can_extract_range),
               K_(is_primary_index),
               K_(with_extra_info),
               K_(only_rowkey_filter));
  void reset() {
    selectivity_ = 0;
    row_count_ = 0;
    retry_times_ = 0;
    pre_total_scan_rows_ = 0;
    iter_times_ = 0;
    iter_filter_row_cnt_ = 0;
    iter_res_row_cnt_ = 0;
    pre_scan_row_cnt_ = 0;
    can_extract_range_ = false;
    is_primary_index_ = false;
    with_extra_info_ = false;
    only_rowkey_filter_ = false;
  }
  double selectivity_;
  int64_t row_count_;
  int64_t retry_times_;
  int64_t pre_total_scan_rows_;
  int64_t iter_times_;
  int64_t iter_filter_row_cnt_;
  int64_t iter_res_row_cnt_;
  int64_t pre_scan_row_cnt_;
  bool can_extract_range_;
  bool is_primary_index_;
  bool with_extra_info_;
  bool only_rowkey_filter_;
};

class ObDASHNSWScanIter : public ObDASIter
{
public:
    ObDASHNSWScanIter()
    : ObDASIter(ObDASIterType::DAS_ITER_HNSW_SCAN),
      mem_context_(nullptr),
      vec_op_alloc_("HNSW", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      ls_id_(),
      tx_desc_(nullptr),
      snapshot_(nullptr),
      inv_idx_scan_iter_(nullptr),
      delta_buf_iter_(nullptr),
      index_id_iter_(nullptr),
      snapshot_iter_(nullptr),
      vid_rowkey_iter_(nullptr),
      com_aux_vec_iter_(nullptr),
      rowkey_vid_iter_(nullptr),
      delta_buf_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      index_id_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      snapshot_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      vid_rowkey_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      com_aux_vec_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      rowkey_vid_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      pre_scan_param_(nullptr),
      delta_buf_scan_param_(),
      index_id_scan_param_(),
      snapshot_scan_param_(),
      vid_rowkey_scan_param_(),
      com_aux_vec_scan_param_(),
      rowkey_vid_scan_param_(),
      delta_buf_iter_first_scan_(true),
      index_id_iter_first_scan_(true),
      snapshot_iter_first_scan_(true),
      vid_rowkey_iter_first_scan_(true),
      com_aux_vec_iter_first_scan_(true),
      rowkey_vid_iter_first_scan_(true),
      data_filter_iter_first_scan_(true),
      vec_aux_ctdef_(nullptr),
      vec_aux_rtdef_(nullptr),
      vid_rowkey_ctdef_(nullptr),
      vid_rowkey_rtdef_(nullptr),
      data_filter_ctdef_(nullptr),
      data_filter_rtdef_(nullptr),
      sort_ctdef_(nullptr),
      sort_rtdef_(nullptr),
      adaptor_vid_iter_(nullptr),
      tmp_adaptor_vid_iter_(nullptr),
      limit_param_(),
      vec_index_param_(),
      search_param_(),
      query_cond_(),
      dim_(0),
      search_vec_(nullptr),
      distance_calc_(nullptr),
      is_primary_pre_with_rowkey_with_filter_(false),
      go_brute_force_(false),
      only_complete_data_(false),
      extra_column_count_(0),
      simple_cmp_info_(),
      vec_index_type_(ObVecIndexType::VEC_INDEX_INVALID),
      vec_idx_try_path_(ObVecIdxAdaTryPath::VEC_PATH_UNCHOSEN),
      adaptive_ctx_(),
      can_retry_(false),
      idx_iter_first_scan_(true),
      distance_threshold_(FLT_MAX) {
        extra_in_rowkey_idxs_.set_attr(ObMemAttr(MTL_ID(), "ExtraIdx"));
      }
  
  virtual ~ObDASHNSWScanIter() {}

  virtual int do_table_scan() override;
  virtual int rescan() override;
  virtual void clear_evaluated_flag() override;

  ObDASIter *get_inv_idx_scan_iter() { return inv_idx_scan_iter_; }

  void set_related_tablet_ids(const ObDASRelatedTabletID &related_tablet_ids) 
  {
    delta_buf_tablet_id_ = related_tablet_ids.delta_buf_tablet_id_;
    index_id_tablet_id_ = related_tablet_ids.index_id_tablet_id_;
    snapshot_tablet_id_ = related_tablet_ids.snapshot_tablet_id_;
    vid_rowkey_tablet_id_ = related_tablet_ids.doc_rowkey_tablet_id_;
    com_aux_vec_tablet_id_ = related_tablet_ids.lookup_tablet_id_;
    rowkey_vid_tablet_id_ = related_tablet_ids.rowkey_vid_tablet_id_;
  }

  void set_ls_id(const share::ObLSID &ls_id) { ls_id_ = ls_id; }
  uint64_t adjust_batch_count(bool is_vectored, uint64_t batch_count);
  bool enable_using_simplified_scan() { return need_save_distance_result(); }
protected:
  int save_distance_expr_result(const ObObj& dist_obj);
  int save_distance_expr_result(ObNewRow *row, int64_t size);
  
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;

private:
  int reuse_pre_filter_by_type();
  int build_rowkey_vid_range();
  bool is_hnsw_bq() const { return OB_NOT_NULL(vec_aux_ctdef_) && vec_aux_ctdef_->algorithm_type_ == ObVectorIndexAlgorithmType::VIAT_HNSW_BQ;}
  bool need_save_distance_result() { 
    return distance_calc_ != nullptr && ! is_hnsw_bq();
  }
  int process_adaptor_state(bool is_vectorized);
  int inner_process_adaptor_state(bool is_vectorized);
  int process_adaptor_state_brute_force(ObIAllocator &allocator, bool is_vectorized);
  int process_adaptor_state_hnsw(ObIAllocator &allocator, bool is_vectorized);
  int process_adaptor_state_pre_filter(ObVectorQueryAdaptorResultContext *ada_ctx, ObPluginVectorIndexAdaptor* adaptor, bool is_vectorized);
  int process_adaptor_state_pre_filter_with_idx_filter(ObVectorQueryAdaptorResultContext *ada_ctx, ObPluginVectorIndexAdaptor* adaptor, int64_t *&vids,
                        int& brute_cnt, bool is_vectorized);
  int process_adaptor_state_pre_filter_with_rowkey(ObVectorQueryAdaptorResultContext *ada_ctx, ObPluginVectorIndexAdaptor* adaptor, int64_t *&vids,
                        int& brute_cnt, bool is_vectorized);
  int process_adaptor_state_pre_filter_brute_force(ObVectorQueryAdaptorResultContext *ada_ctx, ObPluginVectorIndexAdaptor* adaptor,
                                                    int64_t *&brute_vids, int& brute_cnt, bool& need_complete_data, 
                                                    bool check_need_complete_data = true);
  int process_adaptor_state_pre_filter_brute_force_not_bq(ObVectorQueryAdaptorResultContext *ada_ctx, ObPluginVectorIndexAdaptor* adaptor,
                                                    int64_t *&brute_vids, int& brute_cnt, bool& need_complete_data, 
                                                    bool check_need_complete_data = true);                                                  
  int process_adaptor_state_pre_filter_brute_force_bq(ObVectorQueryAdaptorResultContext *ada_ctx, ObPluginVectorIndexAdaptor* adaptor,
                                                    int64_t *&brute_vids, int& brute_cnt, bool& need_complete_data, 
                                                    bool check_need_complete_data = true);                                                  
  int set_rowkey_by_vid(ObNewRow *row);
  int post_query_vid_with_filter(ObVectorQueryAdaptorResultContext *ada_ctx, 
                                ObPluginVectorIndexAdaptor* adaptor, 
                                int64_t hnsw_max_iter_scan_nums,
                                int64_t& iter_scan_total_num, 
                                bool is_vectorized);
  int process_adaptor_state_post_filter(ObVectorQueryAdaptorResultContext *ada_ctx, ObPluginVectorIndexAdaptor* adaptor, bool is_vectorized);
  int process_adaptor_state_post_filter_once(ObVectorQueryAdaptorResultContext *ada_ctx, ObPluginVectorIndexAdaptor* adaptor);
  int get_single_row_from_data_filter_iter(bool is_vectorized);
  
  int prepare_state(const ObVidAdaLookupStatus& cur_state, ObVectorQueryAdaptorResultContext &ada_ctx);
  int call_pva_interface(const ObVidAdaLookupStatus& cur_state,
                         ObVectorQueryAdaptorResultContext& ada_ctx,
                         ObPluginVectorIndexAdaptor &adaptor);
  int next_state(ObVidAdaLookupStatus& cur_state, ObVectorQueryAdaptorResultContext& ada_ctx);
  int get_ob_hnsw_ef_search(uint64_t &ob_hnsw_ef_search);
  int set_vector_query_condition(ObVectorQueryConditions &query_cond);
  int prepare_complete_vector_data(ObVectorQueryAdaptorResultContext& ada_ctx);

  int get_vid_from_rowkey_vid_table(int64_t &vid);
  int get_vid_from_rowkey_vid_table(ObRowkey& rowkey, int64_t &vid);
  int get_rowkey_from_vid_rowkey_table(ObIAllocator &allocator, ObRowkey& vid, ObRowkey *&rowkey);
  int get_vector_from_com_aux_vec_table(ObIAllocator &allocator, ObRowkey *rowkey, ObString &vector);
  int get_vector_from_com_aux_vec_table(ObIAllocator &allocator, ObString &vector);

  int do_delta_buf_table_scan();
  int do_index_id_table_scan();
  int do_snapshot_table_scan();
  int do_com_aux_vec_table_scan();
  int do_vid_rowkey_table_scan();
  int do_rowkey_vid_table_scan();
  
  int do_aux_table_scan_need_reuse(bool &first_scan,
                                   ObTableScanParam &scan_param,
                                   const ObDASScanCtDef *ctdef,
                                   ObDASScanRtDef *rtdef,
                                   ObDASScanIter *iter,
                                   ObTabletID &tablet_id,
                                   bool is_get = false);
  int do_aux_table_scan(bool &first_scan, ObTableScanParam &scan_param, const ObDASScanCtDef *ctdef, ObDASScanRtDef *rtdef, ObDASScanIter *iter, ObTabletID &tablet_id);
  int reuse_vid_rowkey_iter() { return ObDasVecScanUtils::reuse_iter(ls_id_, vid_rowkey_iter_, vid_rowkey_scan_param_, vid_rowkey_tablet_id_); };
  int reuse_rowkey_vid_iter() { return ObDasVecScanUtils::reuse_iter(ls_id_, rowkey_vid_iter_, rowkey_vid_scan_param_, rowkey_vid_tablet_id_); };
  int reuse_com_aux_vec_iter() { return ObDasVecScanUtils::reuse_iter(ls_id_, com_aux_vec_iter_, com_aux_vec_scan_param_, com_aux_vec_tablet_id_); };
  int reuse_filter_data_table_iter() { return ObDasVecScanUtils::reuse_iter(ls_id_, data_filter_iter_, data_filter_scan_param_, com_aux_vec_tablet_id_); };

  int get_rowkey(ObIAllocator &allocator, ObRowkey *&rowkey) {
    const ObDASScanCtDef * ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_rowkey_vid_tbl_idx(), ObTSCIRScanType::OB_VEC_ROWKEY_VID_SCAN);
    ObDASScanRtDef *rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_rowkey_vid_tbl_idx());
    return ObDasVecScanUtils::get_rowkey(allocator, ctdef, rtdef, rowkey);
  }
  int get_from_vid_rowkey(ObIAllocator &allocator, ObRowkey *&rowkey);

  int init_sort(const ObDASVecAuxScanCtDef *ir_ctdef, ObDASVecAuxScanRtDef *ir_rtdef);
  int set_vec_index_param(ObString vec_index_param) { return ob_write_string(vec_op_alloc_, vec_index_param, vec_index_param_); }
  int get_extra_info_by_vids(ObPluginVectorIndexAdaptor *adaptor, const ObSimpleMaxHeap &heap,
                             ObVecExtraInfoPtr &extra_info_ptr);
  int do_get_extra_info_by_vids(ObPluginVectorIndexAdaptor *adaptor, const ObSimpleMaxHeap &heap, int64_t *vids_idx,
                                int64_t *vids, bool get_snap, ObVecExtraInfoPtr &extra_info_ptr);

  int get_extra_idx_in_outexprs(ObIArray<int64_t> &extra_in_rowkey_idxs);
  bool can_be_last_search(int64_t old_ef, int64_t need_cnt_next, float select_ratio);
  int init_pre_filter(ObPluginVectorIndexAdaptor *adaptor, ObVectorQueryAdaptorResultContext *ada_ctx);
  int check_is_simple_cmp_filter();
  int get_simple_cmp_filter_res(ObNewRow *row, bool& res);
  int build_rowkey_obj_from_extra_info(ObObj *extra_info_objs, ObObj *&rowkey_objs);
  int build_extra_info_obj_from_rowkey(const ObObj *rowkey_objs, ObObj *&extra_info_objs);
  int prepare_extra_objs(ObIAllocator &allocator, ObObj *&objs);
  int build_extra_info_rowkey(const ObRowkey &rowkey, ObRowkey &extra_rowkey);
  int build_extra_info_range(const ObNewRange &range, const ObNewRange *&const_extra_range);
  int64_t get_reorder_count(const int64_t ef_search, const int64_t topK, const ObVectorIndexParam& param);
  int64_t get_reorder_count_for_brute_force(const int64_t ef_search, const int64_t topK, const ObVectorIndexParam& param);
  inline bool is_pre_filter() { return vec_index_type_ == ObVecIndexType::VEC_INDEX_PRE 
                      || (vec_index_type_ == ObVecIndexType::VEC_INDEX_ADAPTIVE_SCAN && vec_idx_try_path_ == ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER);}
  inline bool is_in_filter() { return vec_index_type_ == ObVecIndexType::VEC_INDEX_ADAPTIVE_SCAN && vec_idx_try_path_ == ObVecIdxAdaTryPath::VEC_INDEX_IN_FILTER;}
  inline bool is_post_filter() { return vec_index_type_ == ObVecIndexType::VEC_INDEX_POST_WITHOUT_FILTER 
                      ||  vec_index_type_ == ObVecIndexType::VEC_INDEX_POST_ITERATIVE_FILTER
                      || (vec_index_type_ == ObVecIndexType::VEC_INDEX_ADAPTIVE_SCAN && vec_idx_try_path_ == ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER);}
  inline bool is_adaptive_filter() { return vec_index_type_ == ObVecIndexType::VEC_INDEX_ADAPTIVE_SCAN;}
  inline bool is_iter_filter() { return vec_index_type_ == ObVecIndexType::VEC_INDEX_POST_ITERATIVE_FILTER
                      || (vec_index_type_ == ObVecIndexType::VEC_INDEX_ADAPTIVE_SCAN && vec_idx_try_path_ == ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER);}
  inline bool check_if_can_retry() { return is_adaptive_filter() && (vec_idx_try_path_ == ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER 
                                                                 || vec_idx_try_path_ == ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER);}
  int check_iter_filter_need_retry();
  int check_pre_filter_need_retry();
  int reset_filter_path();
  int updata_vec_exec_ctx(ObPlanStat* plan_stat);
  int prepare_follower_query_cond(ObVectorQueryConditions &query_cond);
private:
  static const uint64_t MAX_VSAG_QUERY_RES_SIZE = 16384;
  static const uint64_t VSAG_MAX_EF_SEARCH = 160000;
  static constexpr double FIXED_MAGNIFICATION_RATIO = 2.0;
  static constexpr double ITER_CONSIDER_LAST_SEARCH_SELETIVITY = 0.05;
  static const uint64_t MAX_OPTIMIZE_BATCH_COUNT = 16;
  static const uint64_t MAX_HNSW_BRUTE_FORCE_SIZE = 20000;
  static const int32_t CHANGE_PATH_WINDOW_SIZE = 30;
  static constexpr double DECAY_FACTOR = 0.5;
  static const uint64_t MIN_BQ_REORDER_SIZE_FOR_BRUTE_FORCE = 100;

private:
  lib::MemoryContext mem_context_;
  ObArenaAllocator vec_op_alloc_;
  share::ObLSID ls_id_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;

  ObDASIter *inv_idx_scan_iter_;
  ObDASScanIter *delta_buf_iter_;
  ObDASScanIter *index_id_iter_;
  ObDASScanIter *snapshot_iter_;
  ObDASScanIter *vid_rowkey_iter_;
  ObDASScanIter *com_aux_vec_iter_;
  ObDASScanIter *rowkey_vid_iter_;
  ObDASScanIter *data_filter_iter_;

  ObTabletID delta_buf_tablet_id_;
  ObTabletID index_id_tablet_id_;
  ObTabletID snapshot_tablet_id_;
  ObTabletID vid_rowkey_tablet_id_;
  ObTabletID com_aux_vec_tablet_id_;
  ObTabletID rowkey_vid_tablet_id_;

  ObTableScanParam* pre_scan_param_;
  ObTableScanParam delta_buf_scan_param_;
  ObTableScanParam index_id_scan_param_;
  ObTableScanParam snapshot_scan_param_;
  ObTableScanParam vid_rowkey_scan_param_;
  ObTableScanParam com_aux_vec_scan_param_; 
  ObTableScanParam rowkey_vid_scan_param_;
  ObTableScanParam data_filter_scan_param_; 

  bool delta_buf_iter_first_scan_;
  bool index_id_iter_first_scan_;
  bool snapshot_iter_first_scan_;
  bool vid_rowkey_iter_first_scan_;
  bool com_aux_vec_iter_first_scan_;
  bool rowkey_vid_iter_first_scan_;
  bool data_filter_iter_first_scan_;
  
  const ObDASVecAuxScanCtDef *vec_aux_ctdef_;
  ObDASVecAuxScanRtDef *vec_aux_rtdef_;
  const ObDASScanCtDef *vid_rowkey_ctdef_;
  ObDASScanRtDef *vid_rowkey_rtdef_;
  const ObDASScanCtDef *data_filter_ctdef_;
  ObDASScanRtDef *data_filter_rtdef_;
  const ObDASSortCtDef *sort_ctdef_;
  ObDASSortRtDef *sort_rtdef_;

  ObVectorQueryVidIterator* adaptor_vid_iter_;
  ObVectorQueryVidIterator* tmp_adaptor_vid_iter_;
  common::ObLimitParam limit_param_; 
  
  ObString vec_index_param_;
  ObVectorIndexParam search_param_;
  ObVectorQueryConditions query_cond_;
  int64_t dim_;
  ObExpr* search_vec_;
  ObExpr* distance_calc_;
  bool is_primary_pre_with_rowkey_with_filter_;
  bool go_brute_force_;
  bool only_complete_data_;
  int64_t extra_column_count_;
  ObHnswSimpleCmpInfo simple_cmp_info_;
  // extra_info idx to rowkey idx, because of extra_info is sort by column id
  // if extra_column_count_ <= 0, extra_in_rowkey_idxs_ is empty
  ObSEArray<int64_t, 4> extra_in_rowkey_idxs_;
  ObVecIndexType vec_index_type_;
  ObVecIdxAdaTryPath vec_idx_try_path_;
  ObHnswAadaptiveCtx adaptive_ctx_;
  bool can_retry_;
  bool idx_iter_first_scan_;
  float distance_threshold_;

private:

  struct BruteForceContext {
    ObString search_vec;
    uint64_t limit;
    ObExprVectorDistance::ObVecDisType dis_type;

    BruteForceContext() : limit(0), dis_type(ObExprVectorDistance::ObVecDisType::MAX_TYPE) {}
  };

  struct DistanceResult {
    const float* distances_inc;
    const float* distances_snap;
    int brute_cnt;

    DistanceResult() : distances_inc(nullptr), distances_snap(nullptr), brute_cnt(0) {}
  };

  int init_brute_force_params(ObVectorQueryAdaptorResultContext *ada_ctx,
                              ObPluginVectorIndexAdaptor* adaptor,
                              BruteForceContext& ctx);

  int query_brute_force_distances(ObPluginVectorIndexAdaptor* adaptor,
                                  const ObString& search_vec,
                                  int64_t* brute_vids,
                                  int brute_cnt,
                                  DistanceResult& dist_result);

  int merge_and_sort_brute_force_results_bq(const DistanceResult& dist_result,
                                            int64_t* brute_vids,
                                            int brute_cnt,
                                            ObSimpleMaxHeap& snap_heap,
                                            ObSimpleMaxHeap& incr_heap,
                                            bool& need_complete_data,
                                            bool check_need_complete_data);

  int build_brute_force_result_iterator_bq(ObPluginVectorIndexAdaptor* adaptor,
                                           const ObSimpleMaxHeap& snap_heap,
                                           const ObSimpleMaxHeap& incr_heap,
                                           ObVectorQueryVidIterator*& result_iter);

  void release_brute_force_distance_memory(ObPluginVectorIndexAdaptor* adaptor,
                                          const DistanceResult& dist_result);
};


class ObSimpleMaxHeap {
public:
  ObSimpleMaxHeap(ObIAllocator* allocator, uint64_t capacity_): 
      allocator_(allocator), 
      heap_(nullptr),
      capacity_(capacity_), 
      size_(0), 
      init_(false) {}  
  ~ObSimpleMaxHeap() {}
  int init();
  int release();
  // is_snap true: vid is geted by snap in adapter
  //         false: vid is geted by inc in adapter
  void push(int64_t vid, double distiance, bool is_snap);
  void max_heap_sort();
  int64_t at(uint64_t idx) const;
  double value_at(uint64_t idx) const;
  bool is_snap(uint64_t idx) const;
  uint64_t get_size() const { return size_; }

private:
  struct ObSortItem {
    int64_t vid_;
    double distance_;
    bool is_snap_;
    ObSortItem(int64_t vid, double distance, bool is_snap) : vid_(vid), distance_(distance), is_snap_(is_snap) {}

    bool operator<(const ObSortItem& other) const {
        return distance_ < other.distance_;
    }
    bool operator>(const ObSortItem& other) const {
        return distance_ > other.distance_;
    }
  };

  void heapify_up(int idx);
  void heapify_down(int idx);

  ObIAllocator* allocator_;
  ObSortItem* heap_;
  uint64_t capacity_;
  uint64_t size_;
  bool init_;
};


}  // namespace sql
}  // namespace oceanbase



#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_HNSW_SCAN_ITER_H_ */
