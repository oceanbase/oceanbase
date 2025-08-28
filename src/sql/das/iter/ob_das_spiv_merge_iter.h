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

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_SPIV_MERGE_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_SPIV_MERGE_ITER_H_

#include "sql/das/iter/ob_das_iter.h"
#include "sql/das/iter/ob_das_scan_iter.h"
#include "sql/das/iter/ob_das_spiv_scan_iter.h"
#include "sql/engine/expr/ob_expr_vector.h"
#include "lib/hash/ob_hashset.h"
#include "share/vector_index/ob_plugin_vector_index_util.h"
#include "share/vector_type/ob_vector_common_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
class ObDasSPIVScanIter;

struct ObSPIVItem
{
  ObSPIVItem();
  ~ObSPIVItem() = default;

  float value_;
  ObString docid_;
  int64_t iter_idx_;
  bool operator<(const ObSPIVItem& other) const {
    return docid_ < other.docid_;
  }
  bool operator>(const ObSPIVItem& other) const {
    return docid_ > other.docid_;
  }
  TO_STRING_KV(K_(value), K_(docid), K_(iter_idx));
};



struct ObDASSPIVMergeIterParam : public ObDASIterParam
{
public:
  ObDASSPIVMergeIterParam()
    : ObDASIterParam(ObDASIterType::DAS_ITER_SPIV_MERGE),
      ls_id_(),
      tx_desc_(nullptr),
      snapshot_(nullptr),
      inv_idx_scan_iter_(nullptr),
      rowkey_docid_iter_(nullptr),
      aux_data_iter_(nullptr),
      vec_aux_ctdef_(nullptr),
      vec_aux_rtdef_(nullptr),
      spiv_scan_ctdef_(nullptr),
      spiv_scan_rtdef_(nullptr),
      sort_ctdef_(nullptr),
      sort_rtdef_(nullptr) {}

  virtual bool is_valid() const override
  {
    return ls_id_.is_valid() &&
           nullptr != tx_desc_ &&
           nullptr != snapshot_ && 
           nullptr != vec_aux_ctdef_ &&
           nullptr != vec_aux_rtdef_;
  }

  share::ObLSID ls_id_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;

  ObDASIter *inv_idx_scan_iter_;
  ObDASScanIter *rowkey_docid_iter_;
  ObDASScanIter *aux_data_iter_;

  const ObDASVecAuxScanCtDef *vec_aux_ctdef_;
  ObDASVecAuxScanRtDef *vec_aux_rtdef_;
  const ObDASScanCtDef *spiv_scan_ctdef_;
  ObDASScanRtDef *spiv_scan_rtdef_;
  const ObDASSortCtDef *sort_ctdef_;
  ObDASSortRtDef *sort_rtdef_;  
};

class ObDASSPIVMergeIter : public ObDASIter
{
public:
    ObDASSPIVMergeIter()
    : ObDASIter(ObDASIterType::DAS_ITER_SPIV_MERGE),
      mem_context_(nullptr),
      allocator_(lib::ObMemAttr(MTL_ID(), "SPIVMergeIter"), OB_MALLOC_NORMAL_BLOCK_SIZE), 
      ls_id_(),
      tx_desc_(nullptr),
      snapshot_(nullptr),
      inv_idx_scan_iter_(nullptr),
      rowkey_docid_iter_(nullptr),
      aux_data_iter_(nullptr),
      aux_data_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      rowkey_docid_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      aux_data_scan_param_(), 
      rowkey_docid_scan_param_(), 
      aux_data_table_first_scan_(true),
      rowkey_docid_table_first_scan_(true),
      is_inited_(false),
      is_cursors_inited_(false),
      vec_aux_ctdef_(nullptr),
      vec_aux_rtdef_(nullptr),
      sort_ctdef_(nullptr),
      sort_rtdef_(nullptr),
      spiv_scan_ctdef_(nullptr),
      spiv_scan_rtdef_(nullptr),
      qvec_expr_(nullptr),
      qvec_(nullptr),
      result_docids_curr_iter_(OB_INVALID_INDEX_INT64),
      distance_calc_(nullptr) {
        saved_rowkeys_.set_attr(ObMemAttr(MTL_ID(), "VecIdxKeyRanges"));
      }
  
  virtual ~ObDASSPIVMergeIter() {}

  virtual int do_table_scan() override;
  virtual int rescan() override;
  virtual void clear_evaluated_flag() override;

  ObDASIter *get_inv_idx_scan_iter() { return inv_idx_scan_iter_; }

  void set_related_tablet_ids(const ObDASRelatedTabletID &related_tablet_ids) 
  {
    rowkey_docid_tablet_id_ = related_tablet_ids.rowkey_doc_tablet_id_;
    aux_data_tablet_id_ = related_tablet_ids.lookup_tablet_id_;
  }

  void set_ls_id(const share::ObLSID &ls_id) { ls_id_ = ls_id; }
  void set_iters_ls_tablet_id(const share::ObLSID &ls_id, const ObDASRelatedTabletID &related_tablet_ids)
  {
    for(int i = 0; i < iters_.count(); i++) {
      iters_.at(i)->set_ls_tablet_id(ls_id, related_tablet_ids.dim_docid_value_tablet_id_);
    }
  }
  ObMapType* get_qvec() { return qvec_; };
  int set_iters(const ObIArray<ObDASIter *> &iters);
  int push_iter_and_cursor(ObDASSPIVScanIter *iter) { 
    int ret = OB_SUCCESS;
    if (OB_FAIL(iters_.push_back(iter))) {
    } else if (OB_FAIL(cursors_.push_back(nullptr))){
    }
    return ret;
}


protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;

private:
  int init_query_vector(const ObDASVecAuxScanCtDef *ir_ctdef,
                              ObDASVecAuxScanRtDef *ir_rtdef,
                              const ObDASSortCtDef *sort_ctdef,
                              ObDASSortRtDef *sort_rtdef,
                              const common::ObLimitParam &limit_param,
                              ObExpr *&search_vec,
                              ObExpr *&distance_calc);
  void set_algo();
  
  int get_ob_sparse_drop_ratio_search(uint64_t &drop_ratio);

  int get_docid_from_rowkey_docid_table(ObString &docid);
  int get_vector_from_aux_data_table(ObString &vector);

  int do_aux_data_table_scan();
  int do_rowkey_docid_table_scan();
  int reuse_aux_data_iter() { return ObDasVecScanUtils::reuse_iter(ls_id_, aux_data_iter_, aux_data_scan_param_, aux_data_tablet_id_); }
  int reuse_rowkey_docid_iter() { return ObDasVecScanUtils::reuse_iter(ls_id_, rowkey_docid_iter_, rowkey_docid_scan_param_, rowkey_docid_tablet_id_); }
  
  int get_rowkey_pre_filter(ObIAllocator &allocator, bool is_vectorized, int64_t batch_count);
  int get_rowkey_and_set_docids(ObIAllocator &allocator, bool is_vectorized, int64_t batch_count);
  int process(bool is_vectorized);
  int process_pre_filter(ObIAllocator &allocator, bool is_vectorized);
  int process_post_filter(ObIAllocator &allocator, bool is_vectorized);
  int do_brute_force(ObIAllocator &allocator, bool is_vectorized, int64_t batch_count);
  int process_algo(ObIAllocator &allocator, bool is_pre_filter, bool is_vectorized);

  int daat_naive(ObIAllocator &allocator, bool is_pre_filter, bool is_vectorized, int64_t capacity);

  int set_valid_docids_with_rowkeys(ObIAllocator &allocator, int64_t batch_count);
  int fill_spiv_item(ObIAllocator &allocator, ObDASSPIVScanIter &iter, const int64_t iter_idx, ObSPIVItem *&item);
  int make_cursors(ObIAllocator &allocator, bool is_pre_filter, bool is_vectorized);
  int single_iter_get_next_row(ObIAllocator &allocator, const int64_t iter_idx, ObSPIVItem *&item, bool &is_iter_end, bool is_vectorized);

  int random_partition_by_key(uint32_t *keys, float *values, int l, int r);
  int random_partition_by_value(uint32_t *keys, float *values, int l, int r);
  void sort_by_key(uint32_t *keys, float *values, int l, int r);
  void sort_by_value(uint32_t *keys, float *values, int l, int r);
private:
  static const uint64_t MAX_SPIV_BRUTE_FORCE_SIZE = 20000;
  static const int64_t OB_DEFAULT_SPIV_SCAN_ITER_CNT = 16;

  typedef ObSEArray<ObDASSPIVScanIter *, OB_DEFAULT_SPIV_SCAN_ITER_CNT> ObDASSPIVScanIterArray;
  ObDASSPIVScanIterArray iters_;

  lib::MemoryContext mem_context_;
  ObArenaAllocator allocator_;
  share::ObLSID ls_id_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;

  ObDASIter *inv_idx_scan_iter_;
  ObDASScanIter *rowkey_docid_iter_;
  ObDASScanIter *aux_data_iter_;

  ObTabletID aux_data_tablet_id_;
  ObTabletID rowkey_docid_tablet_id_;

  ObTableScanParam aux_data_scan_param_; 
  ObTableScanParam rowkey_docid_scan_param_; 

  bool aux_data_table_first_scan_;
  bool rowkey_docid_table_first_scan_;
  bool is_inited_;
  bool is_cursors_inited_;
  
  const ObDASVecAuxScanCtDef *vec_aux_ctdef_;
  ObDASVecAuxScanRtDef *vec_aux_rtdef_;
  const ObDASSortCtDef *sort_ctdef_;
  ObDASSortRtDef *sort_rtdef_;
  const ObDASScanCtDef *spiv_scan_ctdef_;
  ObDASScanRtDef *spiv_scan_rtdef_;

  common::ObLimitParam limit_param_; 
  
  ObExpr *qvec_expr_;
  ObMapType *qvec_;
  
  common::ObSEArray<ObString, 16> result_docids_;
  int64_t result_docids_curr_iter_;

  common::hash::ObHashSet<ObString> valid_docid_set_;
  
  ObDocidScoreItemCmp docid_score_cmp_;
  ObRowkeyScoreItemCmp rowkey_score_cmp_;

  common::ObSEArray<common::ObRowkey *, 16> saved_rowkeys_;
  ObExpr* distance_calc_;
  enum SPIVAlgo{
    DAAT_NAIVE,
    WAND,
    DAAT_MAX_SCORE,
    TAAT_NAIVE
  };
  SPIVAlgo algo_;

  ObSEArray<ObSPIVItem *, OB_DEFAULT_SPIV_SCAN_ITER_CNT> cursors_; 
  ObExprVectorDistance::ObVecDisType dis_type_;
  double selectivity_;
};

}  // namespace sql
}  // namespace oceanbase

#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_SPIV_MERGE_ITER_H_ */
