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

#ifndef OB_DAS_TR_MERGE_ITER_H_
#define OB_DAS_TR_MERGE_ITER_H_

#include "sql/das/iter/ob_das_iter.h"
#include "sql/das/ob_das_ir_define.h"
#include "storage/retrieval/ob_i_sparse_retrieval_iter.h"
#include "storage/retrieval/ob_text_daat_iter.h"
#include "storage/retrieval/ob_text_taat_iter.h"
#include "storage/retrieval/ob_sparse_lookup_iter.h"

namespace oceanbase
{
namespace sql
{
struct ObDASIRScanCtDef;
struct ObDASIRScanRtDef;
class ObDASTextRetrievalIterator;
class ObFtsEvalNode;
class ObDASTextRetrievalIter;
class ObDocIdExt;

struct ObDASTRMergeIterParam : public ObDASIterParam
{
  ObDASTRMergeIterParam()
    : ObDASIterParam(DAS_ITER_TEXT_RETRIEVAL_MERGE),
      ir_ctdef_(nullptr),
      ir_rtdef_(nullptr),
      tx_desc_(nullptr),
      snapshot_(nullptr),
      query_tokens_(),
      dim_weights_(),
      max_batch_size_(0),
      boolean_compute_node_(nullptr),
      flags_(0)
  {}

  virtual bool is_valid() const override
  {
    return nullptr != ir_ctdef_ && nullptr != ir_rtdef_;
  }

  const ObDASIRScanCtDef *ir_ctdef_;
  ObDASIRScanRtDef *ir_rtdef_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  ObArray<ObString> query_tokens_;
  ObArray<double> dim_weights_;
  int64_t max_batch_size_;
  ObFtsEvalNode *boolean_compute_node_;
  union {
    struct {
      uint32_t function_lookup_mode_  : 1;
      uint32_t topk_mode_             : 1;
      uint32_t daat_mode_             : 1;
      uint32_t taat_mode_             : 1;
      uint32_t reserve                : 28;
    };
    uint32_t flags_;
  };
};

class ObDASTRMergeIter : public ObDASIter
{
public:
  ObDASTRMergeIter();
  virtual ~ObDASTRMergeIter() {}
  virtual int do_table_scan() override;
  virtual int rescan() override;

  INHERIT_TO_STRING_KV("ObDASIter", ObDASIter, K_(ir_ctdef), K_(ir_rtdef), K_(tx_desc),
      K_(snapshot), K_(query_tokens), K_(sr_iter_param), K_(sparse_retrieval_iter), K_(dim_iters),
      K_(ls_id), K_(total_doc_cnt_tablet_id), K_(inv_idx_tablet_id), K_(fwd_idx_tablet_id),
      K_(flags), K_(check_rangekey_inited), K_(is_inited));
protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;
public:
  int set_related_tablet_ids(
      const ObLSID &ls_id,
      const ObDASFTSTabletID &related_tablet_ids)
  {
    int ret = OB_SUCCESS;
    ls_id_ = ls_id;
    total_doc_cnt_tablet_id_ = related_tablet_ids.domain_id_idx_tablet_id_;
    if (inv_idx_tablet_id_.is_valid() && inv_idx_tablet_id_ != related_tablet_ids.inv_idx_tablet_id_) {
      inv_idx_tablet_switched_ = true;
    }
    inv_idx_tablet_id_ = related_tablet_ids.inv_idx_tablet_id_;
    fwd_idx_tablet_id_ = related_tablet_ids.fwd_idx_tablet_id_;
    return ret;
  }
  static int build_query_tokens(
      const ObDASIRScanCtDef *ir_ctdef,
      ObDASIRScanRtDef *ir_rtdef,
      common::ObIAllocator &alloc,
      ObArray<ObString> &query_tokens,
      ObArray<double> &boost_values,
      ObFtsEvalNode *&root_node,
      bool &has_duplicate_boolean_tokens);
  int set_children_iter_rangekey(const common::ObIArray<std::pair<ObDocIdExt, int>> &virtual_rangekeys, const int64_t batch_size);
  bool is_taat_mode() { return taat_mode_; }
  int get_query_max_score(double &score)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(sparse_retrieval_iter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(ret));
    } else {
      ret = sparse_retrieval_iter_->get_query_max_score(score);
    }
    return ret;
  }
  int preset_top_k_threshold(const double threshold)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(sparse_retrieval_iter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(ret));
    } else {
      ret = sparse_retrieval_iter_->preset_top_k_threshold(threshold);
    }
    return ret;
  }
  int is_topk_mode() const { return topk_mode_; }
  void set_topk_limit(const int64_t limit) { topk_limit_ = limit; }
private:
  int init_das_iter_scan_params();
  static int init_das_iter_scan_param(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const ObDASScanCtDef *ctdef,
      ObDASScanRtDef *rtdef,
      transaction::ObTxDesc *tx_desc,
      transaction::ObTxReadSnapshot *snapshot,
      common::ObArenaAllocator &allocator,
      ObTableScanParam &scan_param);
  static int reuse_das_iter_scan_param(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      ObTableScanParam &scan_param);
  int create_dim_iters();
  int init_dim_iter_param(ObTextRetrievalScanIterParam &iter_param, const int64_t &idx);
  int create_sparse_retrieval_iter();
  int init_daat_iter_param(ObTextDaaTParam &iter_param);
  int init_taat_iter_param(ObTextTaaTParam &iter_param);
  int set_children_iter_rangekey();
  int gen_inv_idx_scan_default_range(const ObString &query_token, ObNewRange &scan_range);
  int gen_inv_idx_scan_one_range(const ObString &query_token, const ObDocIdExt &doc_id, ObNewRange &scan_range);
  int gen_fwd_idx_scan_feak_range(ObNewRange &scan_range);
  int init_topk_limit();
  int init_block_max_iter_param();
  int init_doc_length_est_param();
private:
  static const int64_t FWD_IDX_ROWKEY_COL_CNT = 2;
  static const int64_t INV_IDX_ROWKEY_COL_CNT = 2;
private:
  lib::MemoryContext mem_context_;  // clean after release or reuse
  common::ObArenaAllocator myself_allocator_; // clean after release
  const ObDASIRScanCtDef *ir_ctdef_;
  ObDASIRScanRtDef *ir_rtdef_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  ObArray<ObString> query_tokens_;
  ObArray<double> dim_weights_;
  ObSparseRetrievalMergeParam sr_iter_param_;
  storage::ObISparseRetrievalMergeIter *sparse_retrieval_iter_;
  static const int64_t OB_DEFAULT_QUERY_TOKEN_ITER_CNT = 4;
  typedef ObSEArray<ObISRDaaTDimIter *, OB_DEFAULT_QUERY_TOKEN_ITER_CNT> ObDASTokenRetrievalIterArray;
  ObDASTokenRetrievalIterArray dim_iters_;
  ObISparseRetrievalDimIter *dim_iter_;
  ObTableScanParam *total_doc_cnt_scan_param_;
  ObFixedArray<ObTableScanParam *, ObIAllocator> inv_scan_params_;
  ObFixedArray<ObTableScanParam *, ObIAllocator> inv_agg_params_;
  ObFixedArray<ObTableScanParam *, ObIAllocator> fwd_scan_params_;
  ObFtsEvalNode *boolean_compute_node_;
  ObFixedArray<ObTableScanParam *, ObIAllocator> block_max_scan_params_;
  ObBlockMaxScoreIterParam block_max_iter_param_;
  ObBlockStatScanParam doc_length_est_param_;
  ObFixedArray<ObSkipIndexColMeta, ObIAllocator> doc_length_est_stat_cols_;
  int64_t topk_limit_;
  ObLSID ls_id_;
  ObTabletID total_doc_cnt_tablet_id_;
  ObTabletID inv_idx_tablet_id_;
  ObTabletID fwd_idx_tablet_id_;
  union {
    struct {
      uint32_t function_lookup_mode_  : 1;
      uint32_t topk_mode_             : 1;
      uint32_t daat_mode_             : 1;
      uint32_t taat_mode_             : 1;
      uint32_t reserve                : 28;
    };
    uint32_t flags_;
  };
  bool check_rangekey_inited_;
  bool inv_idx_tablet_switched_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDASTRMergeIter);
};

} // namespace sql
} // namespace oceanbase

#endif // OB_DAS_TR_MERGE_ITER_H_
