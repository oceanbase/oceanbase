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
          n_times_(0), ready_ts_(0), is_ready_(false), wait_ready_(false) {}
      virtual ~ObExprJoinFilterContext() {} 
      void reset_monitor_info();
      ObPxBloomFilter *bloom_filter_ptr_;
      ObPXBloomFilterHashWrapper bf_key_;
      int64_t filter_count_;
      int64_t total_count_;
      int64_t check_count_;
      int64_t n_times_;
      int64_t ready_ts_;
      bool is_ready_;
      bool wait_ready_;
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
  static const int64_t CHECK_TIMES = 127;
  DISALLOW_COPY_AND_ASSIGN(ObExprJoinFilter);
};

}
}





#endif
