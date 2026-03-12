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

#ifndef OB_DAS_MULTI_MATCH_ITER_H_
#define OB_DAS_MULTI_MATCH_ITER_H_

#include "sql/das/iter/ob_das_iter.h"
#include "sql/das/ob_das_ir_define.h"
#include "storage/retrieval/ob_i_sparse_retrieval_iter.h"
#include "storage/retrieval/ob_text_daat_iter.h"
#include "storage/retrieval/ob_disjunctive_max_iter.h"

namespace oceanbase
{
namespace sql
{
struct ObDASIRScanCtDef;
struct ObDASIRScanRtDef;
class ObDocIdExt;

struct ObDASMultiMatchIterParam : public ObDASIterParam
{
  ObDASMultiMatchIterParam()
    : ObDASIterParam(DAS_ITER_ES_MATCH),
      es_match_ctdef_(nullptr),
      es_match_rtdef_(nullptr),
      ir_scan_ctdefs_(),
      ir_scan_rtdefs_(),
      tx_desc_(nullptr),
      snapshot_(nullptr),
      query_tokens_(),
      token_weights_(),
      max_batch_size_(0)
  {}

  virtual bool is_valid() const override
  {
    bool bret = nullptr != es_match_ctdef_ && nullptr != es_match_rtdef_
        && !ir_scan_ctdefs_.empty() && ir_scan_ctdefs_.count() == ir_scan_rtdefs_.count()
        && query_tokens_.count() == token_weights_.count();
    for (int64_t i = 0; bret && i < ir_scan_ctdefs_.count(); ++i) {
      bret = bret && nullptr != ir_scan_ctdefs_.at(i) && nullptr != ir_scan_rtdefs_.at(i)
          && nullptr != ir_scan_rtdefs_.at(i)->eval_ctx_
          && (ir_scan_ctdefs_.at(i)->need_inv_idx_agg()
              || !ir_scan_ctdefs_.at(i)->need_fwd_idx_agg());
    }
    return bret;
  }

  const ObDASIREsMatchCtDef *es_match_ctdef_;
  ObDASIREsMatchRtDef *es_match_rtdef_;
  ObSEArray<const ObDASIRScanCtDef *, 4> ir_scan_ctdefs_;
  ObSEArray<ObDASIRScanRtDef *, 4> ir_scan_rtdefs_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  ObArray<ObString> query_tokens_;
  ObArray<double> token_weights_;
  int64_t max_batch_size_;
};

class ObDASMultiMatchIter : public ObDASIter
{
public:
  ObDASMultiMatchIter();
  virtual ~ObDASMultiMatchIter() {}
  virtual int do_table_scan() override;
  virtual int rescan() override;

  INHERIT_TO_STRING_KV("ObDASIter", ObDASIter, K_(es_match_ctdef), K_(es_match_rtdef),
      K_(ir_scan_ctdefs), K_(ir_scan_rtdefs), K_(tx_desc), K_(snapshot),
      K_(query_tokens), K_(token_weights), K_(max_batch_size), K_(is_inited));
protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;
public:
  static int parse_match_params(const ObDASIREsMatchCtDef &es_match_ctdef,
                                ObDASIREsMatchRtDef &es_match_rtdef,
                                ObIAllocator &alloc);
  static int parse_match_tokens(const ObDASIRScanCtDef &ir_scan_ctdef,
                                ObDASIRScanRtDef &ir_scan_rtdef,
                                ObIAllocator &alloc,
                                const bool compact_duplicate_tokens,
                                ObArray<ObString> &query_tokens,
                                ObArray<double> &token_weights);
  int set_related_tablet_ids(
      const ObLSID &ls_id,
      const ObIArray<ObDASFTSTabletID> &fts_tablet_ids)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(fts_tablet_ids_.count() != fts_tablet_ids.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected fts tablet id count", K(ret));
    } else {
      ls_id_ = ls_id;
      for (int64_t i = 0; i < fts_tablet_ids.count(); ++i) {
        fts_tablet_ids_.at(i) = fts_tablet_ids.at(i);
      }
    }
    return ret;
  }
private:
  int init_das_iter_scan_params();
  int create_dim_iters();
  int init_dim_iter_param(ObTextRetrievalScanIterParam &iter_param, const int64_t idx);
  int create_sparse_retrieval_iter();
  int init_daat_iter_param(ObTextDaaTParam &iter_param, const int64_t idx);
  int set_children_iter_rangekey();
  int gen_inv_idx_scan_default_range(const ObString &query_token, ObNewRange &scan_range);
  int gen_fwd_idx_scan_feak_range(ObNewRange &scan_range);
private:
  int64_t field_cnt() const { return ir_scan_ctdefs_.count(); }
  int64_t token_cnt() const { return query_tokens_.count(); }
  int64_t dim_cnt() const { return ir_scan_ctdefs_.count() * query_tokens_.count(); }
  int64_t sr_cnt() const { return is_two_level_merge() ? field_cnt() : 1; }
  bool is_topk_mode() const { return ir_scan_ctdefs_.at(0)->has_pushdown_topk(); }
  bool is_two_level_merge() const { return is_topk_mode()
      && ObMatchFieldsType::MATCH_BEST_FIELDS == es_match_rtdef_->match_fields_type_; }
  bool is_trivially_empty() const { return 0 == token_cnt()
      || es_match_rtdef_->minimum_should_match_ > token_cnt()
      || (is_topk_mode() && 0 == topk_limit_); }
private:
  lib::MemoryContext mem_context_;  // clean after release or reuse
  common::ObArenaAllocator myself_allocator_; // clean after release
  const ObDASIREsMatchCtDef *es_match_ctdef_;
  ObDASIREsMatchRtDef *es_match_rtdef_;
  ObFixedArray<const ObDASIRScanCtDef *, ObIAllocator> ir_scan_ctdefs_;
  ObFixedArray<ObDASIRScanRtDef *, ObIAllocator> ir_scan_rtdefs_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  ObFixedArray<ObString, ObIAllocator> query_tokens_;
  ObFixedArray<double, ObIAllocator> token_weights_;
  ObFixedArray<double, ObIAllocator> field_boosts_;
  int64_t max_batch_size_;
  ObFixedArray<ObSparseRetrievalMergeParam, ObIAllocator> sr_iter_params_;
  ObISparseRetrievalMergeIter *sparse_retrieval_iter_;
  ObFixedArray<ObFixedArray<ObISRDaaTDimIter *, ObIAllocator>, ObIAllocator> dim_iters_;
  ObFixedArray<ObTableScanParam *, ObIAllocator> inv_scan_params_;
  ObFixedArray<ObTableScanParam *, ObIAllocator> inv_agg_params_;
  ObFixedArray<ObTableScanParam *, ObIAllocator> fwd_scan_params_;
  ObFixedArray<ObTableScanParam *, ObIAllocator> block_max_scan_params_;
  ObFixedArray<ObTableScanParam *, ObIAllocator> total_doc_cnt_scan_params_;
  ObFixedArray<ObBM25ParamMultiEstCtx, ObIAllocator> bm25_param_est_ctxs_;
  ObBlockMaxScoreIterParam block_max_iter_param_;
  int64_t topk_limit_;
  ObLSID ls_id_;
  ObFixedArray<ObDASFTSTabletID, ObIAllocator> fts_tablet_ids_;
  bool check_rangekey_inited_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDASMultiMatchIter);
};

} // namespace sql
} // namespace oceanbase

#endif // OB_DAS_MULTI_MATCH_ITER_H_
