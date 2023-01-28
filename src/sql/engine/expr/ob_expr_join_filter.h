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
namespace oceanbase
{
namespace sql
{

class ObExprJoinFilter : public ObExprOperator
{
public:
  class ObExprJoinFilterContext : public ObExprOperatorCtx
  {
    public:
      ObExprJoinFilterContext() : ObExprOperatorCtx(),
          bloom_filter_ptr_(NULL), bf_key_(), filter_count_(0), total_count_(0), check_count_(0),
          n_times_(0), ready_ts_(0), next_check_start_pos_(0), window_cnt_(0), window_size_(0),
          partial_filter_count_(0), partial_total_count_(0),
          cur_pos_(total_count_), flag_(0) {}
      virtual ~ObExprJoinFilterContext() {}
    public:
      bool is_ready() { return is_ready_; }
      bool need_wait_ready() { return need_wait_bf_; }
      bool dynamic_disable() {  return dynamic_disable_; }
      void reset_monitor_info();
    public:
      ObPxBloomFilter *bloom_filter_ptr_;
      ObPXBloomFilterHashWrapper bf_key_;
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
      union {
        uint64_t flag_;
        struct {
          bool need_wait_bf_:1;
          bool is_ready_:1;
          bool dynamic_disable_:1;
          uint64_t reserved_:61;
        };
      };
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
  static int eval_bloom_filter_batch(
             const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
  // hard code seed, 32 bit max prime number
  static const int64_t JOIN_FILTER_SEED = 4294967279;
private:
  static int check_bf_ready(
    ObExecContext &exec_ctx,
    ObExprJoinFilter::ObExprJoinFilterContext *join_filter_ctx);
  static int collect_sample_info(
    ObExprJoinFilter::ObExprJoinFilterContext *join_filter_ctx,
    bool is_match);
  static int check_need_dynamic_diable_bf(
      ObExprJoinFilter::ObExprJoinFilterContext *join_filter_ctx);
private:
  static const int64_t CHECK_TIMES = 127;
  DISALLOW_COPY_AND_ASSIGN(ObExprJoinFilter);
};

}
}





#endif
