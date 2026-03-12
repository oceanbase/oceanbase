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

#ifndef OB_INV_IDX_PARAM_ESTIMATOR_H_
#define OB_INV_IDX_PARAM_ESTIMATOR_H_

#include "share/ob_define.h"
#include "lib/number/ob_number_v2.h"
#include "ob_block_stat_iter.h"
#include "sql/das/ob_das_ir_define.h"
#include "ob_inv_idx_param_cache.h"

namespace oceanbase
{
namespace sql
{
class ObExpr;
class ObEvalCtx;
class ObDASSearchCtx;
class ObDASScanIter;
}

namespace storage
{

struct ObTextAvgDocLenEstimator
{
  ObTextAvgDocLenEstimator(const int64_t total_doc_cnt, ObBlockStatScanParam &doc_length_est_param)
    : total_doc_cnt_(total_doc_cnt),
      doc_length_est_param_(doc_length_est_param) {}
  int estimate_avg_doc_len(sql::ObExpr &avg_doc_token_cnt_expr, sql::ObEvalCtx &eval_ctx, double &result);
  int cast_number_to_double(const number::ObNumber &num, double &result);
  int64_t total_doc_cnt_;
  ObBlockStatScanParam &doc_length_est_param_;
};


// bm25 param estimator for one single index on demand
class ObBM25IndexParamEstimator
{
public:
  ObBM25IndexParamEstimator();
  virtual ~ObBM25IndexParamEstimator() { reset(); }
  int init(
      const sql::ObDASIRScanCtDef &ir_scan_ctdef,
      const common::ObTabletID &inv_idx_tablet_id,
      ObDASIRScanRtDef &ir_scan_rtdef,
      ObTableScanParam &inv_scan_param,
      ObInvIdxParamCache &param_cache);
  void reset();
  // do total doc cnt and avg doc token cnt estimation on demand
  int do_estimation(sql::ObDASSearchCtx &search_ctx);
  int get_bm25_param(uint64_t &total_doc_cnt, double &avg_doc_token_cnt) const;
private:
  int do_total_doc_cnt_estimation_on_demand(sql::ObDASSearchCtx &search_ctx);
  int do_avg_doc_token_cnt_estimation_on_demand(sql::ObEvalCtx &eval_ctx);
private:
  const sql::ObDASIRScanCtDef *ir_scan_ctdef_;
  ObDASIRScanRtDef *ir_scan_rtdef_;
  common::ObTabletID tablet_id_;
  ObTableScanParam *inv_scan_param_;
  ObInvIdxParamCache *param_cache_;
  uint64_t total_doc_cnt_;
  double avg_doc_token_cnt_;
  bool can_est_by_sum_skip_index_;
  bool need_est_avg_doc_token_cnt_;
  bool estimated_;
  bool is_inited_;
};

struct ObBM25ParamMultiEstCtx
{
  ObBM25ParamMultiEstCtx();
  virtual ~ObBM25ParamMultiEstCtx() { reset(); }
  int init(
      const ObIArray<const sql::ObDASIRScanCtDef *> &ir_scan_ctdefs,
      ObIArray<ObTableScanParam *> &inv_scan_params,
      ObIAllocator *alloc);
  void reset();
  TO_STRING_KV(K_(estimated_total_doc_cnt), KP_(total_doc_cnt_iter),
      K_(total_doc_cnt_exprs), K_(avg_doc_token_cnt_exprs), K_(doc_length_est_params),
      K_(can_est_by_sum_skip_index), K_(need_est_avg_doc_token_cnt));
  int64_t estimated_total_doc_cnt_;
  sql::ObDASScanIter *total_doc_cnt_iter_;
  ObFixedArray<sql::ObExpr *, ObIAllocator> total_doc_cnt_exprs_;
  ObFixedArray<sql::ObExpr *, ObIAllocator> avg_doc_token_cnt_exprs_;
  ObFixedArray<ObBlockStatScanParam, ObIAllocator> doc_length_est_params_;
  ObFixedArray<ObSEArray<ObSkipIndexColMeta, 1>, ObIAllocator> doc_length_est_stat_cols_;
  ObFixedArray<bool, ObIAllocator> can_est_by_sum_skip_index_;
  bool need_est_avg_doc_token_cnt_;
};

class ObBM25ParamMultiEstimator
{
public:
  ObBM25ParamMultiEstimator();
  virtual ~ObBM25ParamMultiEstimator() { reset(); }

  int init(ObBM25ParamMultiEstCtx *est_ctx, ObIAllocator *alloc);
  void reset();
  void reuse(const bool switch_tablet);
  int do_estimation(sql::ObEvalCtx &eval_ctx);
  int64_t get_total_doc_cnt() const { return total_doc_cnt_; }
  const ObIArray<double> &get_avg_doc_token_cnts() const { return avg_doc_token_cnts_; }
  bool is_estimated() const { return estimated_; }
private:
  ObBM25ParamMultiEstCtx *est_ctx_;
  int64_t total_doc_cnt_;
  ObFixedArray<double, ObIAllocator> avg_doc_token_cnts_;
  bool estimated_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase

#endif // OB_INV_IDX_PARAM_ESTIMATOR_H_