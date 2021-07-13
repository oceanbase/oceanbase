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

#ifndef OCEANBASE_SQL_OB_LOG_TOPK_H
#define OCEANBASE_SQL_OB_LOG_TOPK_H
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_set.h"
namespace oceanbase {
namespace sql {
class ObLogTopk : public ObLogicalOperator {
public:
  ObLogTopk(ObLogPlan& plan)
      : ObLogicalOperator(plan),
        minimum_row_count_(0),
        topk_precision_(0),
        topk_limit_count_(NULL),
        topk_limit_offset_(NULL)
  {}
  virtual ~ObLogTopk()
  {}
  inline ObRawExpr* get_topk_limit_count() const
  {
    return topk_limit_count_;
  }
  inline ObRawExpr* get_topk_limit_offset() const
  {
    return topk_limit_offset_;
  }
  int set_topk_params(
      ObRawExpr* limit_count, ObRawExpr* limit_offset, int64_t minimum_row_cuont, int64_t topk_precision);
  int set_topk_size();
  inline int64_t get_minimum_row_count() const
  {
    return minimum_row_count_;
  }
  inline int64_t get_topk_precision() const
  {
    return topk_precision_;
  }
  virtual int allocate_exchange_post(AllocExchContext* ctx) override;
  virtual int allocate_exchange(AllocExchContext* ctx, bool parts_order);

  virtual uint64_t hash(uint64_t seed) const;
  virtual int copy_without_child(ObLogicalOperator*& out);
  int check_output_dep_specific(ObRawExprCheckDep& checker);
  virtual int print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type);
  virtual int inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const;

private:
  int64_t minimum_row_count_;
  int64_t topk_precision_;
  ObRawExpr* topk_limit_count_;
  ObRawExpr* topk_limit_offset_;
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_OB_LOG_TOPK_H
