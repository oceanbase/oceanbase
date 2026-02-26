/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_SPIV_DIM_ITER_H_
#define OB_SPIV_DIM_ITER_H_

#include "sql/das/iter/ob_das_scan_iter.h"
#include "ob_i_sparse_retrieval_iter.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/das/ob_das_ir_define.h"
#include "sql/das/iter/ob_das_vec_scan_utils.h"
#include "ob_block_max_iter.h"

namespace oceanbase
{
namespace storage
{
class ObTableScanParam;

struct ObSPIVDimIterParam
{
  ObSPIVDimIterParam()
      : mem_context_(nullptr),
        allocator_(nullptr),
        inv_idx_scan_param_(nullptr),
        inv_idx_scan_iter_(nullptr),
        inv_idx_agg_param_(nullptr),
        inv_idx_agg_iter_(nullptr),
        inv_idx_agg_expr_(nullptr),
        inv_scan_domain_id_expr_(nullptr),
        inv_scan_score_expr_(nullptr),
        inv_idx_scan_ctdef_(nullptr),
        inv_idx_scan_rtdef_(nullptr),
        eval_ctx_(nullptr),
        query_value_(0.0)
  {}
  lib::MemoryContext mem_context_;
  ObArenaAllocator *allocator_;
  ObTableScanParam *inv_idx_scan_param_;
  sql::ObDASScanIter *inv_idx_scan_iter_;
  ObTableScanParam *inv_idx_agg_param_;
  sql::ObDASScanIter *inv_idx_agg_iter_;
  sql::ObExpr *inv_idx_agg_expr_;
  sql::ObExpr *inv_scan_domain_id_expr_;
  sql::ObExpr *inv_scan_score_expr_;
  const ObDASScanCtDef *inv_idx_scan_ctdef_;
  ObDASScanRtDef *inv_idx_scan_rtdef_;
  sql::ObEvalCtx *eval_ctx_;
  share::ObLSID ls_id_;
  ObTabletID dim_docid_value_tablet_id_;
  double query_value_;
  uint32_t dim_;
};

class ObSPIVDaaTDimIter : public ObISRDaaTDimIter
{
public:
  ObSPIVDaaTDimIter()
      : mem_context_(nullptr),
        allocator_(nullptr),
        inv_idx_scan_param_(nullptr),
        inv_idx_agg_param_(nullptr),
        inv_idx_scan_iter_(nullptr),
        inv_idx_agg_iter_(nullptr),
        inv_idx_agg_expr_(nullptr),
        inv_scan_domain_id_expr_(nullptr),
        inv_scan_score_expr_(nullptr),
        eval_ctx_(nullptr),
        ls_id_(),
        dim_docid_value_tablet_id_(),
        is_inited_(false),
        max_batch_size_(1),
        cur_idx_(-1),
        count_(0),
        query_value_(0.0),
        dim_(-1),
        max_score_(0.0),
        max_score_cached_(false),
        iter_end_(false)
  {}
  virtual ~ObSPIVDaaTDimIter()
  {}

  int init(const ObSPIVDimIterParam &iter_param);
  void reset();
  void reuse();

  bool need_inv_agg()
  {
    return inv_idx_agg_iter_ != nullptr;
  }

  virtual int get_next_row() override;
  virtual int advance_to(const ObDatum &id_datum) override;

  // return inner product, support other distance?
  virtual int get_curr_score(double &score) const override;
  virtual int get_curr_id(const ObDatum *&datum) const override;
  // interface for plain dynamic pruning algorithms such as WAND and MaxScore
  virtual int get_dim_max_score(double &score) override;
  virtual bool iter_end() const override { return iter_end_; }

private:
  int save_docids();
  int update_scan_param(const ObDatum &id_datum);
  // int init_scan_param();
  // int build_range(ObNewRange &range, uint64_t table_id);

  static const int64_t INV_IDX_ROWKEY_COL_CNT = 2;

  lib::MemoryContext mem_context_;
  ObArenaAllocator *allocator_;
  ObTableScanParam *inv_idx_scan_param_;  // scan param for inv idx scan, we might need to
                                          // modify query range and rescan
  ObTableScanParam *inv_idx_agg_param_;
  sql::ObDASScanIter *inv_idx_scan_iter_;
  sql::ObDASScanIter *inv_idx_agg_iter_;
  sql::ObExpr *inv_idx_agg_expr_;
  sql::ObExpr *inv_scan_domain_id_expr_;
  sql::ObExpr *inv_scan_score_expr_;
  const ObDASScanCtDef *inv_idx_scan_ctdef_;
  ObDASScanRtDef *inv_idx_scan_rtdef_;
  sql::ObEvalCtx *eval_ctx_;
  share::ObLSID ls_id_;
  ObTabletID dim_docid_value_tablet_id_;
  bool is_inited_;
  int64_t max_batch_size_;
  int64_t cur_idx_;
  int64_t count_;
  // for inner product
  float query_value_;
  uint32_t dim_;
  ObFixedArray<float, ObIAllocator> scores_;  // when ~ObFixedArray(), will destory itself
  ObFixedArray<ObDocIdExt, ObIAllocator> doc_ids_;
  common::ObDatumCmpFuncType cmp_func_;
  float max_score_;
  bool max_score_cached_;
  bool iter_end_;
  DISALLOW_COPY_AND_ASSIGN(ObSPIVDaaTDimIter);
};

class ObSPIVBlockMaxDimIter final : public ObISRDimBlockMaxIter
{
public:
  ObSPIVBlockMaxDimIter()
      : ObISRDimBlockMaxIter(),
        dim_iter_(),
        block_max_iter_(),
        block_max_iter_param_(nullptr),
        block_max_scan_param_(nullptr),
        ranking_param_(),
        curr_id_(nullptr),
        max_score_tuple_(nullptr),
        dim_max_score_(0),
        block_max_inited_(false),
        block_max_iter_end_(false),
        in_shallow_status_(false),
        is_inited_(false)
  {}
  ~ObSPIVBlockMaxDimIter() {}
  int init(
      const ObSPIVDimIterParam &iter_param,
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
  // currently, for text retrieval, total_doc_cnt and token_doc_cnt is required before block max calculation
  virtual bool iter_end() const override { return block_max_iter_end_ || dim_iter_.iter_end(); }
  int init_block_max_iter();

private:
  int calc_dim_max_score(
      const ObBlockMaxScoreIterParam &block_max_iter_param,
      const ObBlockMaxIPRankingParam &ranking_param,
      ObTableScanParam &scan_param);
private:
  ObSPIVDaaTDimIter dim_iter_;
  ObBlockMaxScoreIterator block_max_iter_;
  const ObBlockMaxScoreIterParam *block_max_iter_param_;
  ObTableScanParam *block_max_scan_param_;
  ObBlockMaxIPRankingParam ranking_param_;
  const ObDatum *curr_id_;
  const ObMaxScoreTuple *max_score_tuple_;
  double dim_max_score_;
  bool block_max_inited_;
  bool block_max_iter_end_;
  bool in_shallow_status_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSPIVBlockMaxDimIter);
};

}  // namespace storage
}  // namespace oceanbase

#endif
