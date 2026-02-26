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
#include "sql/das/ob_das_ir_define.h"
#include "sql/engine/expr/ob_expr_vector.h"
#include "lib/hash/ob_hashset.h"
#include "share/vector_index/ob_plugin_vector_index_util.h"
#include "share/vector_type/ob_vector_common_util.h"
#include "storage/retrieval/ob_spiv_dim_iter.h"
#include "storage/retrieval/ob_spiv_daat_iter.h"
#include "storage/retrieval/ob_block_max_iter.h"

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
  ObDocIdExt docid_;
  int64_t iter_idx_;
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
      sort_rtdef_(nullptr),
      block_max_scan_ctdef_(nullptr),
      block_max_scan_rtdef_(nullptr) {}

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
  const ObDASScanCtDef *block_max_scan_ctdef_;
  ObDASScanRtDef *block_max_scan_rtdef_;
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
      vec_aux_ctdef_(nullptr),
      vec_aux_rtdef_(nullptr),
      sort_ctdef_(nullptr),
      sort_rtdef_(nullptr),
      spiv_scan_ctdef_(nullptr),
      spiv_scan_rtdef_(nullptr),
      qvec_expr_(nullptr),
      qvec_(nullptr),
      result_docids_curr_iter_(OB_INVALID_INDEX_INT64),
      distance_calc_(nullptr),
      algo_(SPIVAlgo::BLOCK_MAX_WAND),
      set_datum_func_(nullptr),
      docid_lt_func_(nullptr),
      docid_gt_func_(nullptr),
      spiv_iter_(nullptr),
      is_pre_processed_(false)
      {
        result_docids_.set_attr(ObMemAttr(MTL_ID(), "SPIVResultDocid"));
        saved_rowkeys_.set_attr(ObMemAttr(MTL_ID(), "VecIdxKeyRanges"));
      }

  virtual ~ObDASSPIVMergeIter() {}

  virtual int do_table_scan() override;
  virtual int rescan() override;
  virtual void clear_evaluated_flag() override;

  bool is_use_docid() { return nullptr != rowkey_docid_iter_; }

  ObDASIter *get_inv_idx_scan_iter() { return inv_idx_scan_iter_; }

  void set_related_tablet_ids(const ObDASRelatedTabletID &related_tablet_ids)
  {
    rowkey_docid_tablet_id_ = related_tablet_ids.rowkey_doc_tablet_id_;
    aux_data_tablet_id_ = related_tablet_ids.lookup_tablet_id_;
    dim_docid_value_tablet_id_ = related_tablet_ids.dim_docid_value_tablet_id_;
  }

  void set_ls_id(const share::ObLSID &ls_id) { ls_id_ = ls_id; }
  ObMapType* get_qvec() { return qvec_; };
  int get_aux_data_tbl_idx() {
    int idx = vec_aux_ctdef_->get_spiv_aux_data_tbl_idx();
    if (!is_use_docid()) {
      idx -= 1;
    }
    return idx;
  }
  int push_inv_scan_iter(ObDASScanIter *iter)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(inv_dim_scan_iters_.push_back(iter))) {
      LOG_WARN("failed to push inv scan iter");
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

  int get_docid_from_rowkey_docid_table(ObDocIdExt &docid);
  int get_vector_from_aux_data_table(ObString &vector);

  int do_aux_data_table_scan();
  int do_rowkey_docid_table_scan();
  int reuse_aux_data_iter() { return ObDasVecScanUtils::reuse_iter(ls_id_, aux_data_iter_, aux_data_scan_param_, aux_data_tablet_id_); }
  int reuse_rowkey_docid_iter() { return ObDasVecScanUtils::reuse_iter(ls_id_, rowkey_docid_iter_, rowkey_docid_scan_param_, rowkey_docid_tablet_id_); }

  int get_ctdef_with_rowkey_exprs(const ObDASScanCtDef *&ctdef, ObDASScanRtDef *&rtdef);
  int get_rowkey_pre_filter(ObIAllocator &allocator, bool is_vectorized, int64_t batch_count);
  int get_rowkey_and_set_docids(ObIAllocator &allocator, bool is_vectorized, int64_t batch_count);
  int do_brute_force(ObIAllocator &allocator, bool is_vectorized, int64_t batch_count);
  int set_valid_docids_with_rowkeys(ObIAllocator &allocator, int64_t batch_count);
  int random_partition_by_key(uint32_t *keys, float *values, int l, int r);
  int random_partition_by_value(uint32_t *keys, float *values, int l, int r);
  void sort_by_key(uint32_t *keys, float *values, int l, int r);
  void sort_by_value(uint32_t *keys, float *values, int l, int r);

  int rowkey2docid(ObRowkey &rowkey, ObDocIdExt &docid);
  inline static void set_datum_int(ObDatum &datum, const ObDocIdExt &docid)
  {
    datum.set_int(docid.get_datum().get_int());
  }
  inline static void set_datum_shallow(ObDatum &datum, const ObDocIdExt &docid)
  {
    datum.set_datum(docid.get_datum());
  }
  inline static bool docid_lt_int(const ObDocIdExt &left, const ObDocIdExt &right) {
    return left.get_datum().get_uint64() < right.get_datum().get_uint64();
  }
  inline static bool docid_lt_string(const ObDocIdExt &left, const ObDocIdExt &right) {
    return left.get_datum().get_string() < right.get_datum().get_string();
  }

  inline static bool docid_gt_int(const ObDocIdExt &left, const ObDocIdExt &right) {
    return left.get_datum().get_uint64() > right.get_datum().get_uint64();
  }
  inline static bool docid_gt_string(const ObDocIdExt &left, const ObDocIdExt &right) {
    return left.get_datum().get_string() > right.get_datum().get_string();
  }
  int init_dim_iter_param(ObSPIVDimIterParam &dim_param, int64_t idx);
  int create_dim_iters();
  int init_spiv_merge_param(ObSPIVDaaTParam &iter_param);
  int create_spiv_merge_iter();
  int pre_process(bool is_vectorized);
  int project_brute_result(int64_t &count, int64_t capacity);
  int build_inv_scan_range(ObNewRange &range, uint64_t table_id, uint32_t dim);
  int init_block_max_iter_param();
  int set_inv_scan_range_key();

private:
  static const uint64_t MAX_SPIV_BRUTE_FORCE_SIZE = 20000;
  static const int64_t OB_DEFAULT_SPIV_SCAN_ITER_CNT = 16;
  static const int64_t INV_IDX_ROWKEY_COL_CNT = 2;

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

  const ObDASVecAuxScanCtDef *vec_aux_ctdef_;
  ObDASVecAuxScanRtDef *vec_aux_rtdef_;
  const ObDASSortCtDef *sort_ctdef_;
  ObDASSortRtDef *sort_rtdef_;
  const ObDASScanCtDef *spiv_scan_ctdef_;
  ObDASScanRtDef *spiv_scan_rtdef_;
  const ObDASScanCtDef *block_max_scan_ctdef_;
  ObDASScanRtDef *block_max_scan_rtdef_;

  common::ObLimitParam limit_param_;

  ObExpr *qvec_expr_;
  ObMapType *qvec_;

  common::ObSEArray<ObDocIdExt, 16> result_docids_;
  int64_t result_docids_curr_iter_;

  common::hash::ObHashSet<ObDocIdExt> valid_docid_set_;

  ObDocidScoreItemCmp docid_score_cmp_;
  ObRowkeyScoreItemCmp rowkey_score_cmp_;

  common::ObSEArray<common::ObRowkey *, 16> saved_rowkeys_;
  ObExpr* distance_calc_;
  enum SPIVAlgo{
    DAAT_NAIVE = 0,
    WAND,
    BLOCK_MAX_WAND,
    DAAT_MAX_SCORE,
    BLOCK_MAX_MAX_SCORE,
    TAAT_NAIVE
  };
  SPIVAlgo algo_;

  ObExprVectorDistance::ObVecDisType dis_type_;
  double selectivity_;
  void (*set_datum_func_)(ObDatum &, const ObDocIdExt &);
  bool (*docid_lt_func_)(const ObDocIdExt &, const ObDocIdExt &);
  bool (*docid_gt_func_)(const ObDocIdExt &, const ObDocIdExt &);
  ObSEArray<ObDASScanIter *, OB_DEFAULT_SPIV_SCAN_ITER_CNT> inv_dim_scan_iters_;
  ObSEArray<ObISRDaaTDimIter *, OB_DEFAULT_SPIV_SCAN_ITER_CNT> dim_iters_;
  ObISparseRetrievalMergeIter* spiv_iter_;
  common::ObTabletID dim_docid_value_tablet_id_;
  ObSparseRetrievalMergeParam base_param_;
  ObFixedArray<ObTableScanParam *, ObIAllocator> inv_scan_params_;
  ObFixedArray<ObTableScanParam *, ObIAllocator> block_max_scan_params_;
  ObBlockMaxScoreIterParam block_max_iter_param_;
  bool is_pre_processed_;
};

}  // namespace sql
}  // namespace oceanbase

#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_SPIV_MERGE_ITER_H_ */
