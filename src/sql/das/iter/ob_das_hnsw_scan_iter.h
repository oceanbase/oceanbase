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
      vec_aux_ctdef_(nullptr),
      vec_aux_rtdef_(nullptr),
      vid_rowkey_ctdef_(nullptr),
      vid_rowkey_rtdef_(nullptr),
      sort_ctdef_(nullptr),
      sort_rtdef_(nullptr) {}

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
           nullptr != rowkey_vid_iter_ &&
           nullptr != vec_aux_ctdef_ &&
           nullptr != vec_aux_rtdef_ &&
           nullptr != vid_rowkey_ctdef_ &&
           nullptr != vid_rowkey_rtdef_;
  }

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
  const ObDASVecAuxScanCtDef *vec_aux_ctdef_;
  ObDASVecAuxScanRtDef *vec_aux_rtdef_;
  const ObDASScanCtDef *vid_rowkey_ctdef_;
  ObDASScanRtDef *vid_rowkey_rtdef_;
  const ObDASSortCtDef *sort_ctdef_;
  ObDASSortRtDef *sort_rtdef_;
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
      vec_aux_ctdef_(nullptr),
      vec_aux_rtdef_(nullptr),
      vid_rowkey_ctdef_(nullptr),
      vid_rowkey_rtdef_(nullptr),
      sort_ctdef_(nullptr),
      sort_rtdef_(nullptr),
      adaptor_vid_iter_(nullptr),
      limit_param_(),
      vec_index_param_(),
      dim_(0),
      search_vec_(nullptr),
      distance_calc_(nullptr) {
        saved_rowkeys_.set_attr(ObMemAttr(MTL_ID(), "VecIdxKeyRanges"));
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
  bool need_save_distance_result() {
    return distance_calc_ != nullptr;
  }
  int process_adaptor_state(bool is_vectorized);
  int process_adaptor_state_brute_force(ObIAllocator &allocator, bool is_vectorized);
  int process_adaptor_state_hnsw(ObIAllocator &allocator, bool is_vectorized);
  int process_adaptor_state_pre_filter(ObVectorQueryAdaptorResultContext *ada_ctx, ObPluginVectorIndexAdaptor* adaptor, bool is_vectorized);
  int process_adaptor_state_post_filter(ObVectorQueryAdaptorResultContext *ada_ctx, ObPluginVectorIndexAdaptor* adaptor);
  int get_next_single_row(bool is_vectorized);

  int prepare_state(const ObVidAdaLookupStatus& cur_state, ObVectorQueryAdaptorResultContext &ada_ctx);
  int call_pva_interface(const ObVidAdaLookupStatus& cur_state,
                         ObVectorQueryAdaptorResultContext& ada_ctx,
                         ObPluginVectorIndexAdaptor &adaptor);
  int next_state(ObVidAdaLookupStatus& cur_state, ObVectorQueryAdaptorResultContext& ada_ctx);
  int get_ob_hnsw_ef_search(uint64_t &ob_hnsw_ef_search);
  int set_vector_query_condition(ObVectorQueryConditions &query_cond);
  int prepare_complete_vector_data(ObVectorQueryAdaptorResultContext& ada_ctx);

  int get_vid_from_rowkey_vid_table(ObRowkey& rowkey, int64_t &vid);
  int get_rowkey_from_vid_rowkey_table(ObIAllocator &allocator, ObRowkey& vid, ObRowkey& rowkey);
  int get_vector_from_com_aux_vec_table(ObIAllocator &allocator, ObRowkey& rowkey, ObString &vector);

  int do_delta_buf_table_scan();
  int do_index_id_table_scan();
  int do_snapshot_table_scan();
  int do_com_aux_vec_table_scan();

  int do_aux_table_scan_need_reuse(bool &first_scan,
                                   ObTableScanParam &scan_param,
                                   const ObDASScanCtDef *ctdef,
                                   ObDASScanRtDef *rtdef,
                                   ObDASScanIter *iter,
                                   ObTabletID &tablet_id,
                                   bool is_get = false);
  int do_aux_table_scan(bool &first_scan, ObTableScanParam &scan_param, const ObDASScanCtDef *ctdef, ObDASScanRtDef *rtdef, ObDASScanIter *iter, ObTabletID &tablet_id);

  int reuse_vid_rowkey_iter() { return reuse_iter(vid_rowkey_scan_param_, vid_rowkey_tablet_id_, vid_rowkey_iter_); };
  int reuse_rowkey_vid_iter() { return reuse_iter(rowkey_vid_scan_param_, rowkey_vid_tablet_id_, rowkey_vid_iter_); };
  int reuse_com_aux_vec_iter() { return reuse_iter(com_aux_vec_scan_param_, com_aux_vec_tablet_id_, com_aux_vec_iter_); };
  int reuse_iter(ObTableScanParam &scan_param, ObTabletID &tablet_id, ObDASScanIter *iter);

  int get_rowkey(ObIAllocator &allocator, ObRowkey &rowkey);

  int init_sort(const ObDASVecAuxScanCtDef *ir_ctdef, ObDASVecAuxScanRtDef *ir_rtdef);
  int set_vec_index_param(ObString vec_index_param) { return ob_write_string(vec_op_alloc_, vec_index_param, vec_index_param_); }
private:
  static const uint64_t MAX_VSAG_QUERY_RES_SIZE = 16384;
  static const uint64_t MAX_BRUTE_FORCE_SIZE = 10000;
  static const uint64_t MAX_OPTIMIZE_BATCH_COUNT = 16;

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

  ObTabletID delta_buf_tablet_id_;
  ObTabletID index_id_tablet_id_;
  ObTabletID snapshot_tablet_id_;
  ObTabletID vid_rowkey_tablet_id_;
  ObTabletID com_aux_vec_tablet_id_;
  ObTabletID rowkey_vid_tablet_id_;

  ObTableScanParam delta_buf_scan_param_;
  ObTableScanParam index_id_scan_param_;
  ObTableScanParam snapshot_scan_param_;
  ObTableScanParam vid_rowkey_scan_param_;
  ObTableScanParam com_aux_vec_scan_param_;
  ObTableScanParam rowkey_vid_scan_param_;

  bool delta_buf_iter_first_scan_;
  bool index_id_iter_first_scan_;
  bool snapshot_iter_first_scan_;
  bool vid_rowkey_iter_first_scan_;
  bool com_aux_vec_iter_first_scan_;
  bool rowkey_vid_iter_first_scan_;

  const ObDASVecAuxScanCtDef *vec_aux_ctdef_;
  ObDASVecAuxScanRtDef *vec_aux_rtdef_;
  const ObDASScanCtDef *vid_rowkey_ctdef_;
  ObDASScanRtDef *vid_rowkey_rtdef_;
  const ObDASSortCtDef *sort_ctdef_;
  ObDASSortRtDef *sort_rtdef_;

  ObVectorQueryVidIterator* adaptor_vid_iter_;
  common::ObLimitParam limit_param_;

  ObString vec_index_param_;
  int64_t dim_;
  double selectivity_;

  ObExpr* search_vec_;

  common::ObSEArray<common::ObRowkey, 16> saved_rowkeys_;
  ObExpr* distance_calc_;
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
  void push(ObRowkey &rowkey, double distiance);
  void max_heap_sort();
  ObRowkey* at(uint64_t idx);
  double value_at(uint64_t idx);
  uint64_t get_size() const { return size_; }

private:
  struct ObSortItem {
    ObRowkey rowkey_;
    double distance_;
    ObSortItem(const ObRowkey& rowkey, double distance) : rowkey_(rowkey), distance_(distance) {}

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
