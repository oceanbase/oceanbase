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

#include "sql/das/search/ob_i_das_search_op.h"
#include "ob_inv_idx_param_estimator.h"

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
      inv_scan_pos_list_col_(nullptr),
      reuse_inv_idx_agg_res_(false),
      use_rich_format_(false)
  {}

  ObArenaAllocator *allocator_; // long-term allocator, lifetime larger than iter
  ObTableScanParam *inv_idx_scan_param_;
  ObTableScanParam *inv_idx_agg_param_; // reserved for potential stable ranking feature
  ObTableScanParam *fwd_idx_scan_param_; // TODO: remove after barrier version between 43x
  sql::ObDASScanIter *inv_idx_scan_iter_;
  sql::ObDASScanIter *inv_idx_agg_iter_; // reserved for potential stable ranking feature
  sql::ObDASScanIter *fwd_idx_agg_iter_; // TODO: remove after barrier version between 43x
  sql::ObExpr *inv_idx_agg_expr_; // reserved for potential stable ranking feature
  sql::ObExpr *fwd_idx_agg_expr_; // TODO: remove after barrier version between 43x
  sql::ObEvalCtx *eval_ctx_;
  sql::ObExpr *relevance_expr_;
  sql::ObExpr *inv_scan_doc_length_col_;
  sql::ObExpr *inv_scan_domain_id_col_;
  sql::ObExpr *inv_scan_pos_list_col_;
  bool reuse_inv_idx_agg_res_;
  bool use_rich_format_;
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
  int get_token_doc_cnt(int64_t &token_doc_cnt);
  int get_or_calculate_max_token_relevance(double &max_token_relevance);
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
  int fill_token_weight();
  int eval_relevance_expr();
  int batch_eval_relevance_expr(const int64_t count);
  int estimate_token_doc_cnt();
  inline bool need_inv_idx_agg() { return inv_idx_agg_iter_ != nullptr; }
  inline bool need_fwd_idx_agg() { return fwd_idx_agg_iter_ != nullptr; }
  inline bool need_calc_relevance() { return inv_idx_agg_iter_ != nullptr; }
  inline bool need_fill_token_cnt() { return nullptr != relevance_expr_ && !sql::ObExprBM25::use_new_version(*relevance_expr_); }
  inline bool need_fill_token_weight() { return !need_fill_token_cnt(); }
  // tools method
  // In ivector2.0, need use the size which is created by precision to alloc the memeory.
  // TODO: remove this decimal int set functions after using doc_length expr as bm25 arg expr directly
  static int set_decimal_int_by_precision(ObDatum &result_datum, const uint64_t decint, const ObPrecision precision);
  static int set_decimal_int_by_precision(
      const uint64_t &value,
      const ObPrecision precision,
      const int64_t idx,
      ObIVector &vec);
public:
  static const int64_t FWD_IDX_ROWKEY_COL_CNT = 2;
  static const int64_t INV_IDX_ROWKEY_COL_CNT = 2;
private:
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
  sql::ObExpr *token_weight_expr_; // fill the expr from with max_token_relevance_
  common::ObSEArray<sql::ObExpr *, 2> relevance_calc_exprs_;
  sql::ObBitVector *skip_;
  ObObj *fwd_range_objs_;
  int64_t max_batch_size_;
  int64_t token_doc_cnt_;
  double max_token_relevance_;
  ObDocIdExt advance_doc_id_;
  bool use_doc_length_col_as_agg_;
  bool token_doc_cnt_calculated_;
  bool reuse_inv_idx_agg_res_;
  bool use_rich_format_;
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
  int get_curr_doc_length(int64_t &length) const;
  int get_curr_pos_list(ObString &pos_list) const;
  virtual bool iter_end() const override { return iter_end_; }
public:
  int get_token_doc_cnt(int64_t &token_doc_cnt) { return token_iter_.get_token_doc_cnt(token_doc_cnt); }
  virtual int get_dim_max_score(double &score) override {
    int ret = OB_SUCCESS;
    if (OB_FAIL(token_iter_.get_or_calculate_max_token_relevance(score))) {
      LOG_WARN("failed to get or calculate max token relevance", K(ret));
    } else if (OB_UNLIKELY(score < 0.0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected max token relevance", K(ret), K(score));
    }
    return ret;
  }
  void set_filter_threshold(const double threshold);
private:
  int do_expr_materialization();
  int do_expr_materialization_with_threshold();
  int try_refresh_max_batch_size();
private:
  static constexpr int64_t MIN_BATCH_SIZE = 4;
  static constexpr double SKIPPED_ROWS_RATIO = 0.9;
  ObArenaAllocator *allocator_;
  ObTextRetrievalTokenIter token_iter_;
  sql::ObEvalCtx *eval_ctx_;
  sql::ObExpr *relevance_expr_;
  sql::ObExpr *inv_scan_domain_id_col_;
  sql::ObExpr *inv_scan_doc_length_col_;
  sql::ObExpr *inv_scan_pos_list_col_;
  int64_t max_batch_size_;
  int64_t initial_max_batch_size_;
  int64_t cur_idx_;
  int64_t count_;
  int64_t skipped_rows_in_advance_;
  double filter_threshold_;
  ObFixedArray<double, ObIAllocator> relevance_; // when ~ObFixedArray(), wikll destory itself
  ObFixedArray<ObDocIdExt, ObIAllocator> doc_id_;
  ObFixedArray<int64_t, ObIAllocator> doc_length_;
  ObFixedArray<ObString, ObIAllocator> pos_list_;
  common::ObDatumCmpFuncType cmp_func_;
  bool use_rich_format_;
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
  int get_curr_doc_length(int64_t &length) const;
  int get_curr_pos_list(ObString &pos_list) const;
  int get_max_token_relevance(double &max_token_relevance)
  { return token_iter_.get_dim_max_score(max_token_relevance); }
  virtual int get_dim_max_score(double &score) override;
  virtual int advance_shallow(const ObDatum &id_datum, const bool inclusive) override;
  virtual int get_curr_block_max_info(const ObMaxScoreTuple *&max_score_tuple) override;
  virtual bool in_shallow_status() const override;
  virtual bool iter_end() const override { return block_max_iter_end_ || token_iter_.iter_end(); }
  // currently, for text retrieval, total_doc_cnt and token_doc_cnt is required before block max calculation
  int init_block_max_iter(const int64_t total_doc_cnt, const double avg_doc_token_cnt);
  // threshold = topK_threshold - other_dim_max_score
  virtual void set_filter_threshold(const double threshold) override { token_iter_.set_filter_threshold(threshold); }
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
  ObDomainIdCmp domain_id_cmp_;
  const ObDatum *curr_id_;
  const ObMaxScoreTuple *max_score_tuple_;
  double dim_max_score_;
  bool block_max_inited_;
  bool block_max_iter_end_;
  bool in_shallow_status_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObTextRetrievalBlockMaxIter);
};


/***************************************** Vec 2.0 ************************************************/


} // namespace storage
} // namespace oceanbase


#endif
