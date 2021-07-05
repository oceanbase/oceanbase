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

#ifndef OCEANBASE_SQL_OB_LOG_LIMIT_H
#define OCEANBASE_SQL_OB_LOG_LIMIT_H
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_set.h"
namespace oceanbase {
namespace sql {
class ObLogLimit : public ObLogicalOperator {
public:
  ObLogLimit(ObLogPlan& plan)
      : ObLogicalOperator(plan),
        is_calc_found_rows_(false),
        has_union_child_(false),
        is_top_limit_(false),
        is_fetch_with_ties_(false),
        limit_count_(NULL),
        limit_offset_(NULL),
        limit_percent_(NULL)
  {}
  virtual ~ObLogLimit()
  {}
  virtual int allocate_expr_pre(ObAllocExprContext& ctx) override;
  inline ObRawExpr* get_limit_count() const
  {
    return limit_count_;
  }
  inline ObRawExpr* get_limit_offset() const
  {
    return limit_offset_;
  }
  inline ObRawExpr* get_limit_percent() const
  {
    return limit_percent_;
  }
  inline void set_limit_count(ObRawExpr* limit_count)
  {
    limit_count_ = limit_count;
  }
  inline void set_limit_offset(ObRawExpr* limit_offset)
  {
    limit_offset_ = limit_offset;
  }
  inline void set_limit_percent(ObRawExpr* limit_percent)
  {
    limit_percent_ = limit_percent;
  }
  void set_calc_found_rows(bool found_rows)
  {
    is_calc_found_rows_ = found_rows;
  }
  int need_calc_found_rows(bool& need_calc)
  {
    int ret = common::OB_SUCCESS;
    need_calc = false;
    if (is_top_limit_ && is_calc_found_rows_) {
      need_calc = true;
    } else if (is_top_limit_ && has_union_child_) {
      need_calc = true;
    } else { /* Do nothing */
    }
    return ret;
  }
  void set_has_union_child(bool has_union_child)
  {
    has_union_child_ = has_union_child;
  }
  void set_top_limit(bool is_top_limit)
  {
    is_top_limit_ = is_top_limit;
  }
  inline bool is_top_limit()
  {
    return is_top_limit_;
  }
  bool has_union_child()
  {
    return has_union_child_;
  }
  virtual int est_cost() override;
  virtual int allocate_exchange_post(AllocExchContext* ctx);
  virtual int transmit_op_ordering() override;
  virtual int re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est) override;

  virtual uint64_t hash(uint64_t seed) const;
  virtual int copy_without_child(ObLogicalOperator*& out);
  int check_output_dep_specific(ObRawExprCheckDep& checker);
  virtual int print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type);
  virtual int inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const;
  void set_fetch_with_ties(bool is_fetch_with_ties)
  {
    is_fetch_with_ties_ = is_fetch_with_ties;
  }
  inline bool is_fetch_with_ties()
  {
    return is_fetch_with_ties_;
  }

private:
  bool is_calc_found_rows_;
  bool has_union_child_;
  bool is_top_limit_;
  bool is_fetch_with_ties_;
  ObRawExpr* limit_count_;
  ObRawExpr* limit_offset_;
  ObRawExpr* limit_percent_;
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_OB_LOG_LIMIT_H
