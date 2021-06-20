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

#ifndef _OB_LOG_VALUES_H
#define _OB_LOG_VALUES_H 1
#include "common/row/ob_row_store.h"
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/resolver/ob_stmt.h"
#include "sql/optimizer/ob_log_plan.h"

namespace oceanbase {
namespace sql {
/**
 * ObLogValues is currently being used as 'explain' and 'help' operator.
 */
class ObLogValues : public ObLogicalOperator {
public:
  static const int64_t MAX_EXPLAIN_BUFFER_SIZE = 1024 * 1024;
  ObLogValues(ObLogPlan& plan)
      : ObLogicalOperator(plan), explain_plan_(NULL), stmt_(NULL), row_store_(plan.get_allocator())
  {}
  virtual ~ObLogValues()
  {}
  virtual int allocate_exchange_post(AllocExchContext* ctx)
  {
    UNUSED(ctx);
    return common::OB_SUCCESS;
  }
  virtual int copy_without_child(ObLogicalOperator*& out)
  {
    out = NULL;
    return common::OB_SUCCESS;
  }
  ObLogPlan* get_explain_plan() const
  {
    return explain_plan_;
  }
  ObStmt* get_stmt() const
  {
    return stmt_;
  }
  void set_explain_plan(ObLogPlan* const plan)
  {
    explain_plan_ = plan;
  }
  void set_stmt(ObStmt* stmt)
  {
    stmt_ = stmt;
  }
  int add_row(const common::ObNewRow& row)
  {
    return row_store_.add_row(row);
  }
  common::ObRowStore& get_row_store()
  {
    return row_store_;
  }
  uint64_t hash(uint64_t seed) const
  {
    if (NULL != explain_plan_) {
      seed = do_hash(*explain_plan_, seed);
    }
    seed = ObLogicalOperator::hash(seed);

    return seed;
  }

  int set_row_store(const common::ObRowStore& row_store)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(row_store_.assign(row_store))) {
      SQL_OPT_LOG(WARN, "fail to assign row store, ret=%d", K(ret));
    }
    return ret;
  }
  int64_t get_col_count() const
  {
    return row_store_.get_col_count();
  }

  virtual int inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const override
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(raw_exprs.append(output_exprs_))) {
      SQL_LOG(WARN, "array append failed", K(ret));
    }
    return ret;
  }

private:
  ObLogPlan* explain_plan_;
  ObStmt* stmt_;
  common::ObRowStore row_store_;
  DISALLOW_COPY_AND_ASSIGN(ObLogValues);
};
}  // namespace sql
}  // namespace oceanbase
#endif
