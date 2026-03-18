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

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_VEC_INDEX_HNSW_SCAN_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_VEC_INDEX_HNSW_SCAN_ITER_H_

#include "sql/das/iter/ob_das_iter.h"
#include "sql/das/iter/ob_das_scan_iter.h"
#include "sql/das/iter/ob_das_vec_index_scan_iter.h"
#include "src/share/vector_index/ob_plugin_vector_index_service.h"
#include "sql/das/iter/ob_das_vec_scan_utils.h"
#include "src/share/vector_index/ob_vector_index_util.h"
#include "share/vector_index/ob_plugin_vector_index_util.h"
#include "share/vector_index/ob_plugin_vector_index_adaptor.h"
#include "sql/das/iter/ob_das_hnsw_scan_iter.h"
#include "sql/das/iter/ob_das_vec_index_scan_iter.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

class ObSimpleMaxHeap;
class ObHnswSimpleCmpInfo;
class ObHnswAadaptiveCtx;

struct ObDASVecIndexHNSWScanIterParam : public ObDASIterParam
{
  OB_UNIS_VERSION(1);
public:
  ObDASVecIndexHNSWScanIterParam()
    : ObDASIterParam(ObDASIterType::DAS_ITER_HNSW_SCAN),
      ls_id_(),
      tx_desc_(nullptr),
      snapshot_(nullptr),
      delta_buf_iter_(nullptr),
      index_id_iter_(nullptr),
      snapshot_iter_(nullptr),
      com_aux_vec_iter_(nullptr),
      vec_index_scan_ctdef_(nullptr),
      vec_index_scan_rtdef_(nullptr),
      vec_index_type_(ObVecIndexType::VEC_INDEX_INVALID),
      sort_expr_(nullptr),
      search_ctx_(nullptr) {}

  virtual bool is_valid() const override
  {
    bool valid = ls_id_.is_valid() &&
          nullptr != tx_desc_ &&
          nullptr != snapshot_ &&
          nullptr != delta_buf_iter_ &&
          nullptr != index_id_iter_ &&
          nullptr != snapshot_iter_ &&
          nullptr != com_aux_vec_iter_ &&
          nullptr != vec_index_scan_ctdef_ &&
          nullptr != vec_index_scan_rtdef_;
    return valid;
  }

  TO_STRING_KV(K_(vec_index_scan_ctdef),
              K_(vec_index_scan_rtdef),
              K_(vec_index_type));

  share::ObLSID ls_id_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;

  ObDASScanIter *delta_buf_iter_;
  ObDASScanIter *index_id_iter_;
  ObDASScanIter *snapshot_iter_;
  ObDASScanIter *com_aux_vec_iter_;

  const ObDASVecIndexHNSWScanCtDef *vec_index_scan_ctdef_;
  ObDASVecIndexHNSWScanRtDef *vec_index_scan_rtdef_;

  ObVecIndexType vec_index_type_;
  ObExpr* sort_expr_;
  ObDASSearchCtx *search_ctx_;
};

class ObDASVecIndexHNSWScanIter : public ObDASVecIndexScanIter
{
public:
  ObDASVecIndexHNSWScanIter()
    : ObDASVecIndexScanIter(ObDASIterType::DAS_ITER_HNSW_SCAN),
      mem_context_(nullptr),
      vec_op_alloc_("HNSW", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      ls_id_(),
      tx_desc_(nullptr),
      snapshot_(nullptr),
      delta_buf_iter_(nullptr),
      index_id_iter_(nullptr),
      snapshot_iter_(nullptr),
      com_aux_vec_iter_(nullptr),
      delta_buf_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      index_id_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      snapshot_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      com_aux_vec_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      delta_buf_scan_param_(),
      index_id_scan_param_(),
      snapshot_scan_param_(),
      com_aux_vec_scan_param_(),
      delta_buf_iter_first_scan_(true),
      index_id_iter_first_scan_(true),
      snapshot_iter_first_scan_(true),
      com_aux_vec_iter_first_scan_(true),
      vec_index_scan_ctdef_(nullptr),
      vec_index_scan_rtdef_(nullptr),
      adaptor_vid_iter_(nullptr),
      search_param_(),
      query_cond_(nullptr),
      sort_expr_(nullptr),
      search_vec_(nullptr),
      distance_calc_(nullptr),
      distance_threshold_(FLT_MAX),
      adaptor_(nullptr),
      ada_ctx_(MTL_ID(), 0, &vec_op_alloc_, nullptr),
      first_post_filter_search_(true) {}

  virtual ~ObDASVecIndexHNSWScanIter() {}

  virtual void set_adaptor(share::ObPluginVectorIndexAdaptor *adaptor) { adaptor_ = adaptor; }
  virtual void set_vector_query_condition(ObVectorQueryConditions *query_cond) { query_cond_ = query_cond; }
  virtual share::ObVectorQueryVidIterator* get_adaptor_vid_iter() override { return adaptor_vid_iter_; }
  void reset_adaptor_vid_iter_for_next_iteration();

  virtual void set_related_tablet_ids(const ObDASRelatedTabletID &related_tablet_ids)
  {
    delta_buf_tablet_id_ = related_tablet_ids.delta_buf_tablet_id_;
    index_id_tablet_id_ = related_tablet_ids.index_id_tablet_id_;
    snapshot_tablet_id_ = related_tablet_ids.snapshot_tablet_id_;
    com_aux_vec_tablet_id_ = related_tablet_ids.lookup_tablet_id_;
  }

  void set_ls_id(const share::ObLSID &ls_id) { ls_id_ = ls_id; }
  uint64_t adjust_batch_count(bool is_vectored, uint64_t batch_count);
  bool enable_using_simplified_scan() { return need_save_distance_result(); }

  static const uint64_t MAX_VSAG_QUERY_RES_SIZE = 16384;
  static const uint64_t VSAG_MAX_EF_SEARCH = 160000;
  static constexpr double SPARSE_FIXED_MAGNIFICATION_RATIO = 50.0;
  static constexpr double ITER_CONSIDER_LAST_SEARCH_SELETIVITY = 0.05;
  static const uint64_t MAX_OPTIMIZE_BATCH_COUNT = 16;
  static const uint64_t MAX_HNSW_BRUTE_FORCE_SIZE = 20000;
  static const int32_t CHANGE_PATH_WINDOW_SIZE = 30;
  static constexpr double DECAY_FACTOR = 0.5;
  static const uint64_t MIN_BQ_REORDER_SIZE_FOR_BRUTE_FORCE = 100;

protected:
  int save_distance_expr_result(ObNewRow *row, int64_t size);
  int save_vec_socre_expr_result(ObNewRow *row, int64_t size);
  int distance_to_score(double distance, double &score);

  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;

private:
  bool is_hnsw_bq() const { return OB_NOT_NULL(vec_index_scan_ctdef_) && vec_index_scan_ctdef_->algorithm_type_ == ObVectorIndexAlgorithmType::VIAT_HNSW_BQ; }
  bool need_save_distance_result() { return distance_calc_ != nullptr && !is_hnsw_bq(); }

  int process_adaptor_state(bool is_vectorized);
  int process_adaptor_state_brute_force(ObIAllocator &allocator, bool is_vectorized);
  int process_adaptor_state_pre_filter(ObVectorQueryAdaptorResultContext *ada_ctx, ObPluginVectorIndexAdaptor* adaptor, bool is_vectorized);
  int process_adaptor_state_pre_filter_brute_force(ObVectorQueryAdaptorResultContext *ada_ctx, ObPluginVectorIndexAdaptor* adaptor,
                                                    int64_t *&brute_vids, int64_t& brute_cnt, bool& need_complete_data,
                                                    bool check_need_complete_data = true);
  int process_adaptor_state_pre_filter_brute_force_not_bq(ObVectorQueryAdaptorResultContext *ada_ctx, ObPluginVectorIndexAdaptor* adaptor,
                                                    int64_t *&brute_vids, int64_t& brute_cnt, bool& need_complete_data,
                                                    bool check_need_complete_data = true);
  int process_adaptor_state_pre_filter_brute_force_bq(ObVectorQueryAdaptorResultContext *ada_ctx, ObPluginVectorIndexAdaptor* adaptor,
                                                    int64_t *&brute_vids, int64_t& brute_cnt, bool& need_complete_data,
                                                    bool check_need_complete_data = true);
  int process_adaptor_state_post_filter(ObVectorQueryAdaptorResultContext *ada_ctx, ObPluginVectorIndexAdaptor* adaptor, bool is_vectorized);
  int process_adaptor_state_post_filter_once(ObVectorQueryAdaptorResultContext *ada_ctx, ObPluginVectorIndexAdaptor* adaptor);

  int prepare_state(const ObVidAdaLookupStatus& cur_state, ObVectorQueryAdaptorResultContext &ada_ctx);
  int call_pva_interface(const ObVidAdaLookupStatus& cur_state,
                        ObVectorQueryAdaptorResultContext& ada_ctx,
                        ObPluginVectorIndexAdaptor &adaptor);
  int next_state(ObVidAdaLookupStatus& cur_state, ObVectorQueryAdaptorResultContext& ada_ctx);
  int get_ob_hnsw_ef_search(uint64_t &ob_hnsw_ef_search);
  int prepare_complete_vector_data(ObVectorQueryAdaptorResultContext& ada_ctx);

  int get_vector_from_com_aux_vec_table(ObIAllocator &allocator, ObRowkey *rowkey, ObString &vector);
  int get_vector_from_com_aux_vec_table(ObIAllocator &allocator, ObString &vector);

  int do_delta_buf_table_scan();
  int do_index_id_table_scan();
  int do_snapshot_table_scan();
  int do_com_aux_vec_table_scan();
  int do_snapshot_table_scan_need_reuse(bool &first_scan,
                                  ObTableScanParam &scan_param,
                                  const ObDASScanCtDef *ctdef,
                                  ObDASScanRtDef *rtdef,
                                  ObDASScanIter *iter,
                                  ObTabletID &tablet_id,
                                  bool is_get = false);
  int do_aux_table_scan_need_reuse(bool &first_scan,
                                  ObTableScanParam &scan_param,
                                  const ObDASScanCtDef *ctdef,
                                  ObDASScanRtDef *rtdef,
                                  ObDASScanIter *iter,
                                  ObTabletID &tablet_id,
                                  bool is_get = false,
                                  bool need_reverse = false);
  int do_aux_table_scan(bool &first_scan, ObTableScanParam &scan_param, const ObDASScanCtDef *ctdef, ObDASScanRtDef *rtdef, ObDASScanIter *iter, ObTabletID &tablet_id);
  int reuse_com_aux_vec_iter() { return ObDasVecScanUtils::reuse_iter(ls_id_, com_aux_vec_iter_, com_aux_vec_scan_param_, com_aux_vec_tablet_id_); }

  int init_sort();
  int init_pre_filter(ObVectorQueryAdaptorResultContext *ada_ctx);

  int64_t get_reorder_count(const int64_t ef_search, const int64_t topK, const ObVectorIndexParam& param);
  int64_t get_reorder_count_for_brute_force(const int64_t ef_search, const int64_t topK, const ObVectorIndexParam& param);

  OB_INLINE bool is_pre_filter() const { return vec_index_type_ == ObVecIndexType::VEC_INDEX_PRE; }
  bool is_parallel_with_block_granule();
  int prepare_follower_query_cond(ObVectorQueryConditions &query_cond);

  struct BruteForceContext {
    ObString search_vec;
    uint64_t limit;

    BruteForceContext() : limit(0) {}
  };

  int init_brute_force_params(ObVectorQueryAdaptorResultContext *ada_ctx,
                              ObPluginVectorIndexAdaptor* adaptor,
                              BruteForceContext& ctx);
  int query_brute_force_distances(ObPluginVectorIndexAdaptor* adaptor,
                                  const ObString& search_vec,
                                  int64_t* brute_vids,
                                  int64_t brute_cnt,
                                  ObVecIdxQueryResult& dist_result);
  int merge_and_sort_brute_force_results_bq(const ObVecIdxQueryResult& dist_result,
                                            int64_t* brute_vids,
                                            int64_t brute_cnt,
                                            ObSimpleMaxHeap& snap_heap,
                                            ObSimpleMaxHeap& incr_heap,
                                            bool& need_complete_data,
                                            bool check_need_complete_data);
  int build_brute_force_result_iterator_bq(ObPluginVectorIndexAdaptor* adaptor,
                                          const ObSimpleMaxHeap& snap_heap,
                                          const ObSimpleMaxHeap& incr_heap,
                                          ObVecIdxQueryResult &dist_result,
                                          ObVectorQueryVidIterator*& result_iter);

  lib::MemoryContext mem_context_;
  ObArenaAllocator vec_op_alloc_;
  share::ObLSID ls_id_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;

  ObDASScanIter *delta_buf_iter_;
  ObDASScanIter *index_id_iter_;
  ObDASScanIter *snapshot_iter_;
  ObDASScanIter *com_aux_vec_iter_;

  ObTabletID delta_buf_tablet_id_;
  ObTabletID index_id_tablet_id_;
  ObTabletID snapshot_tablet_id_;
  ObTabletID com_aux_vec_tablet_id_;

  ObTableScanParam delta_buf_scan_param_;
  ObTableScanParam index_id_scan_param_;
  ObTableScanParam snapshot_scan_param_;
  ObTableScanParam com_aux_vec_scan_param_;

  bool delta_buf_iter_first_scan_;
  bool index_id_iter_first_scan_;
  bool snapshot_iter_first_scan_;
  bool com_aux_vec_iter_first_scan_;

  const ObDASVecIndexHNSWScanCtDef *vec_index_scan_ctdef_;
  ObDASVecIndexHNSWScanRtDef *vec_index_scan_rtdef_;

  ObVectorQueryVidIterator* adaptor_vid_iter_;

  ObVectorIndexParam search_param_;
  ObVectorQueryConditions *query_cond_;

  ObExpr* sort_expr_;
  ObExpr* search_vec_;
  ObExpr* distance_calc_;

  bool only_complete_data_;
  float distance_threshold_;
  share::ObPluginVectorIndexAdaptor* adaptor_;
  ObVectorQueryAdaptorResultContext ada_ctx_;
  bool first_post_filter_search_;
};

}  // namespace sql
}  // namespace oceanbase


#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_VEC_INDEX_HNSW_SCAN_ITER_H_ */
