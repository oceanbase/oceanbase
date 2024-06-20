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

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_JOIN_FILTER_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_JOIN_FILTER_H_
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/px/ob_px_bloom_filter.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_share_info.h"
namespace oceanbase
{
namespace sql
{

class ObP2PDatahubMsgBase;
struct ObRowWithHash;
enum RuntimeFilterType
{
  NOT_INIT_RUNTIME_FILTER_TYPE = 0,
  BLOOM_FILTER = 1,
  RANGE = 2,
  IN = 3
};

using ObRuntimeFilterParams = common::ObSEArray<common::ObDatum, 4>;

class ObExprJoinFilter : public ObExprOperator
{
public:
  class ObExprJoinFilterContext : public ObExprOperatorCtx
  {
    public:
      ObExprJoinFilterContext() : ObExprOperatorCtx(),
          rf_msg_(nullptr), rf_key_(), hash_funcs_(), cmp_funcs_(), start_time_(0),
          filter_count_(0), total_count_(0), check_count_(0),
          n_times_(0), ready_ts_(0), next_check_start_pos_(0),
          window_cnt_(0), window_size_(0),
          partial_filter_count_(0), partial_total_count_(0),
          cur_pos_(total_count_), need_reset_sample_info_(false), flag_(0),
          cur_row_(), cur_row_with_hash_(nullptr), skip_vector_(nullptr),
          is_partition_wise_jf_(false)
        {
          cur_row_.set_attr(ObMemAttr(MTL_ID(), "RfCurRow"));
          need_wait_rf_ = true;
          is_first_ = true;
        }
      virtual ~ObExprJoinFilterContext();
    public:
      bool is_ready() { return is_ready_; }
      bool need_wait_ready() { return need_wait_rf_; }
      bool dynamic_disable() {  return dynamic_disable_; }
      void collect_monitor_info(
          const int64_t filtered_rows_count,
          const int64_t check_rows_count,
          const int64_t total_rows_count);
      void inc_partial_rows_count(
          const int64_t filtered_rows_count,
          const int64_t total_rows_count)
      {
        if (!dynamic_disable()) {
          partial_filter_count_ += filtered_rows_count;
          partial_total_count_ += total_rows_count;
        }
      }
      void reset_monitor_info();
    public:
      ObP2PDatahubMsgBase *rf_msg_;
      ObP2PDhKey rf_key_;
      ObHashFuncs hash_funcs_;
      ObCmpFuncs cmp_funcs_;
      int64_t start_time_;
      int64_t filter_count_;
      int64_t total_count_;
      int64_t check_count_;
      int64_t n_times_;
      int64_t ready_ts_;

      // for adaptive bloom filter
      int64_t next_check_start_pos_;
      int64_t window_cnt_;
      int64_t window_size_;
      int64_t partial_filter_count_;
      int64_t partial_total_count_;
      int64_t &cur_pos_;
      bool need_reset_sample_info_;
      union {
        uint64_t flag_;
        struct {
          bool need_wait_rf_:1;
          bool is_ready_:1;
          bool dynamic_disable_:1;
          bool is_first_:1;
          int32_t max_wait_time_ms_:32;
          int32_t reserved_:28;
        };
      };
      ObTMArray<ObDatum> cur_row_;
      ObRowWithHash *cur_row_with_hash_; // used in ObRFInFilterVecMsg, for probe
      // used in ObRFInFilterVecMsg/ObRFBloomFilterMsg, for probe in single row interface
      ObBitVector *skip_vector_;
      // used in ObRFInFilterVecMsg/ObRFBloomFilterMsg, cal probe data's hash value
      uint64_t *right_hash_vals_;

      // for runtime filter pushdown, if is partition wise join, we need to reset
      // pushdown filter parameters
      bool is_partition_wise_jf_;
  };
  ObExprJoinFilter();
  explicit ObExprJoinFilter(common::ObIAllocator& alloc);
  virtual ~ObExprJoinFilter();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx)
                                const override;
  static int eval_bloom_filter(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);

  static int eval_range_filter(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);

  static int eval_in_filter(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);

  static int eval_bloom_filter_batch(
             const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size);
  static int eval_range_filter_batch(
             const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size);
  static int eval_in_filter_batch(
             const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size);

  static int eval_bloom_filter_vector(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const EvalBound &bound);
  static int eval_range_filter_vector(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const EvalBound &bound);
  static int eval_in_filter_vector(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const EvalBound &bound);

  static int eval_filter_internal(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);


  static int eval_filter_batch_internal(
             const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size);

  static int eval_filter_vector_internal(
             const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
  // hard code seed, 32 bit max prime number
  static const int64_t JOIN_FILTER_SEED = 4294967279;
  static void collect_sample_info(
    ObExprJoinFilter::ObExprJoinFilterContext *join_filter_ctx,
    bool is_match);
  static void collect_sample_info_batch(
    ObExprJoinFilter::ObExprJoinFilterContext &join_filter_ctx,
    int64_t filter_count, int64_t total_count);
  static int prepare_storage_white_filter_data(
      const ObExpr &expr,
      ObDynamicFilterExecutor &dynamic_filter,
      ObEvalCtx &eval_ctx,
      ObRuntimeFilterParams &params,
      bool &is_data_prepared);
private:
  static int check_rf_ready(
    ObExecContext &exec_ctx,
    ObExprJoinFilter::ObExprJoinFilterContext *join_filter_ctx);

  static void check_need_dynamic_diable_bf(
      ObExprJoinFilter::ObExprJoinFilterContext *join_filter_ctx);
  static void check_need_dynamic_diable_bf_batch(
      ObExprJoinFilter::ObExprJoinFilterContext &join_filter_ctx);
private:
  static const int64_t CHECK_TIMES = 127;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprJoinFilter);
};

}
}





#endif
