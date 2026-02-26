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

#ifndef OB_TEXT_RETRIEVAL_TOKEN_ITER_H_
#define OB_TEXT_RETRIEVAL_TOKEN_ITER_H_

#include "lib/container/ob_se_array.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/das/iter/ob_das_scan_iter.h"
#include "sql/das/ob_das_ir_define.h"
#include "ob_block_max_iter.h"
#include "ob_i_sparse_retrieval_iter.h"
namespace oceanbase
{
namespace storage
{
class ObTableScanParam;
class ObBlockMaxScoreIterator;
struct ObTextRetrievalScanIterParam
{
  ObTextRetrievalScanIterParam()
    : allocator_(nullptr),
      mem_context_(nullptr),
      inv_idx_scan_param_(nullptr),
      inv_idx_agg_param_(nullptr),
      fwd_idx_scan_param_(nullptr),
      inv_idx_scan_iter_(nullptr),
      inv_idx_agg_iter_(nullptr),
      fwd_idx_agg_iter_(nullptr),
      inv_idx_agg_expr_(nullptr),
      fwd_idx_agg_expr_(nullptr),
      eval_ctx_(nullptr),
      relevance_expr_(nullptr),
      inv_scan_doc_length_col_(nullptr),
      inv_scan_domain_id_col_(nullptr),
      inv_idx_agg_cache_mode_(false)
  {}

  ObArenaAllocator *allocator_;
  lib::MemoryContext mem_context_;
  ObTableScanParam *inv_idx_scan_param_;
  ObTableScanParam *inv_idx_agg_param_;
  ObTableScanParam *fwd_idx_scan_param_;
  sql::ObDASScanIter *inv_idx_scan_iter_;
  sql::ObDASScanIter *inv_idx_agg_iter_;
  sql::ObDASScanIter *fwd_idx_agg_iter_;
  sql::ObExpr *inv_idx_agg_expr_;
  sql::ObExpr *fwd_idx_agg_expr_;
  sql::ObEvalCtx *eval_ctx_;
  sql::ObExpr *relevance_expr_;
  sql::ObExpr *inv_scan_doc_length_col_;
  sql::ObExpr *inv_scan_domain_id_col_;
  bool inv_idx_agg_cache_mode_;
};

class ObTextRetrievalTokenIter final : public ObISparseRetrievalDimIter
{
public:
  ObTextRetrievalTokenIter();
  virtual ~ObTextRetrievalTokenIter() {}

  int init(const ObTextRetrievalScanIterParam &iter_param);
  void reset();
  void reuse();

  // Sparse Retrieval Dimension Iter Interfaces
  virtual int get_next_row() override;
  virtual int get_next_batch(const int64_t capacity, int64_t &count) override;
  virtual int advance_to(const ObDatum &id_datum) override;
  virtual int update_scan_param(const ObString &token, common::ObArenaAllocator &allocator);
public:
  sql::ObBitVector *get_skip() { return skip_; }
  int get_token_doc_cnt(int64_t &token_doc_cnt) const;
  double get_max_token_relevance() const { return max_token_relevance_; }
private:
  int init_calc_exprs_in_relevance_expr();
  void clear_batch_wise_evaluated_flag(const int64_t count);
  void clear_row_wise_evaluated_flag();
  int get_next_doc_token_cnt(const bool use_fwd_idx_agg);
  int get_inv_idx_scan_doc_id(ObDocIdExt &doc_id);
  int gen_fwd_idx_scan_range(const ObDocIdExt &doc_id, ObNewRange &scan_range);
  int do_token_cnt_agg(const ObDocIdExt &doc_id);
  int fill_token_cnt_with_doc_len();
  int batch_fill_token_cnt_with_doc_len(const int64_t count);
  int fill_token_doc_cnt();
  int eval_relevance_expr();
  int batch_eval_relevance_expr(const int64_t count);
  int estimate_token_doc_cnt();
  inline bool need_inv_idx_agg() { return inv_idx_agg_iter_ != nullptr; }
  inline bool need_fwd_idx_agg() { return fwd_idx_agg_iter_ != nullptr; }
  inline bool need_calc_relevance() { return inv_idx_agg_iter_ != nullptr; }
  // tools method
  // In ivector2.0, need use the size which is created by precision to alloc the memeory.
  static int set_decimal_int_by_precision(ObDatum &result_datum, const uint64_t decint, const ObPrecision precision);
public:
  static const int64_t FWD_IDX_ROWKEY_COL_CNT = 2;
  static const int64_t INV_IDX_ROWKEY_COL_CNT = 2;
private:
  lib::MemoryContext mem_context_;
  ObArenaAllocator *allocator_;
  ObTableScanParam *inv_idx_scan_param_;
  ObTableScanParam *inv_idx_agg_param_;
  ObTableScanParam *fwd_idx_scan_param_;
  sql::ObDASScanIter *inv_idx_scan_iter_;
  sql::ObDASScanIter *inv_idx_agg_iter_;
  sql::ObDASScanIter *fwd_idx_agg_iter_;
  sql::ObExpr *inv_idx_agg_expr_;
  sql::ObExpr *fwd_idx_agg_expr_;
  sql::ObEvalCtx *eval_ctx_;
  sql::ObExpr *relevance_expr_;
  sql::ObExpr *inv_scan_doc_length_col_; // read from inv_scan_table
  sql::ObExpr *inv_scan_domain_id_col_; // read from inv_scan_table
  sql::ObExpr *doc_token_cnt_expr_; // fill the expr from another expr, and use the expr to calc the relevance_expr_
  common::ObSEArray<sql::ObExpr *, 2> relevance_calc_exprs_;
  sql::ObBitVector *skip_;
  ObObj *fwd_range_objs_;
  int64_t max_batch_size_;
  int64_t token_doc_cnt_;
  double max_token_relevance_;
  ObDocIdExt advance_doc_id_;
  bool token_doc_cnt_calculated_;
  bool inv_idx_agg_cache_mode_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObTextRetrievalTokenIter);
};

class ObTextRetrievalDaaTTokenIter final : public ObISRDaaTDimIter
{
public:
  ObTextRetrievalDaaTTokenIter();
  virtual ~ObTextRetrievalDaaTTokenIter() {}
  int init(const ObTextRetrievalScanIterParam &iter_param);
  void reuse();
  void reset();
  virtual int get_next_row() override;
  virtual int get_next_batch(const int64_t capacity, int64_t &count) override;
  virtual int advance_to(const ObDatum &id_datum) override;
  virtual int get_curr_score(double &score) const override;
  virtual int get_curr_id(const ObDatum *&id_datum) const override;
  virtual bool iter_end() const override { return iter_end_; }
public:
  int get_token_doc_cnt(int64_t &token_doc_cnt) const { return token_iter_->get_token_doc_cnt(token_doc_cnt); }
  virtual int get_dim_max_score(double &score) override {
    int ret = OB_SUCCESS;
    score = token_iter_->get_max_token_relevance();
    if (OB_UNLIKELY(score < 0.0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected max token relevance", K(ret), K(score));
    }
    return ret;
  }
private:
  int save_relevances_and_docids();
  int save_docids();
  ObArenaAllocator *allocator_;
  ObTextRetrievalTokenIter *token_iter_;
  sql::ObEvalCtx *eval_ctx_;
  sql::ObExpr *relevance_expr_;
  sql::ObExpr *inv_scan_domain_id_col_;
  int64_t max_batch_size_;
  int64_t cur_idx_;
  int64_t count_;
  ObFixedArray<double, ObIAllocator> relevance_; // when ~ObFixedArray(), wikll destory itself
  ObFixedArray<ObDocIdExt, ObIAllocator> doc_id_;
  common::ObDatumCmpFuncType cmp_func_;
  bool is_inited_;
  bool iter_end_;
  DISALLOW_COPY_AND_ASSIGN(ObTextRetrievalDaaTTokenIter);
};

class ObTextRetrievalBlockMaxIter final : public ObISRDimBlockMaxIter
{
public:
  ObTextRetrievalBlockMaxIter();
  ~ObTextRetrievalBlockMaxIter() {}
  int init(
      const ObTextRetrievalScanIterParam &iter_param,
      const ObBlockMaxScoreIterParam &block_max_iter_param,
      ObTableScanParam &scan_param);
  void reset();
  void reuse();

  virtual int get_next_row() override;
  virtual int get_next_batch(const int64_t capacity, int64_t &count) override;
  virtual int advance_to(const ObDatum &id_datum) override;

  virtual int get_curr_score(double &score) const override;
  virtual int get_curr_id(const ObDatum *&id_datum) const override;
  virtual int get_dim_max_score(double &score) override;
  virtual int advance_shallow(const ObDatum &id_datum, const bool inclusive) override;
  virtual int get_curr_block_max_info(const ObMaxScoreTuple *&max_score_tuple) override;
  virtual bool in_shallow_status() const override;
  virtual bool iter_end() const override { return block_max_iter_end_ || token_iter_.iter_end(); }
  // currently, for text retrieval, total_doc_cnt and token_doc_cnt is required before block max calculation
  int init_block_max_iter(const int64_t total_doc_cnt, const double avg_doc_token_cnt);
private:
  int calc_dim_max_score(
      const ObBlockMaxScoreIterParam &block_max_iter_param,
      const ObBlockMaxBM25RankingParam &ranking_param,
      ObTableScanParam &scan_param);
private:
  ObTextRetrievalDaaTTokenIter token_iter_;
  ObBlockMaxScoreIterator block_max_iter_;
  const ObBlockMaxScoreIterParam *block_max_iter_param_;
  ObTableScanParam *block_max_scan_param_;
  ObBlockMaxBM25RankingParam ranking_param_;
  const ObDatum *curr_id_;
  const ObMaxScoreTuple *max_score_tuple_;
  double dim_max_score_;
  bool block_max_inited_;
  bool block_max_iter_end_;
  bool in_shallow_status_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObTextRetrievalBlockMaxIter);
};

} // namespace storage
} // namespace oceanbase


#endif
