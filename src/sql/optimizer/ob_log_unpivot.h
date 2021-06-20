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

#ifndef _OB_LOG_UNPIVOT_H_
#define _OB_LOG_UNPIVOT_H_
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/resolver/dml/ob_dml_stmt.h"

namespace oceanbase {
namespace sql {
class ObLogUnpivot : public ObLogicalOperator {
public:
  ObLogUnpivot(ObLogPlan& plan)
      : ObLogicalOperator(plan), subquery_id_(common::OB_INVALID_ID), subquery_name_(), access_exprs_()
  {}

  ~ObLogUnpivot()
  {}

  virtual int copy_without_child(ObLogicalOperator*& out);
  virtual int allocate_expr_post(ObAllocExprContext& ctx);
  virtual int print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type);
  int allocate_exchange_post(AllocExchContext* ctx) override;

  int calc_cost();
  virtual int est_cost() override;
  virtual int re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est) override;
  int gen_filters();
  int gen_output_columns();
  int set_properties();
  void set_subquery_id(uint64_t subquery_id)
  {
    subquery_id_ = subquery_id;
  }
  inline const uint64_t& get_subquery_id() const
  {
    return subquery_id_;
  }
  inline common::ObString& get_subquery_name()
  {
    return subquery_name_;
  }
  inline const common::ObIArray<ObRawExpr*>& get_access_exprs() const
  {
    return access_exprs_;
  }
  inline common::ObIArray<ObRawExpr*>& get_access_exprs()
  {
    return access_exprs_;
  }
  virtual int transmit_op_ordering();
  virtual int transmit_local_ordering();

  virtual int compute_op_ordering() override;
  virtual int compute_fd_item_set() override;
  virtual int compute_one_row_info() override;

public:
  uint64_t subquery_id_;
  common::ObString subquery_name_;
  common::ObArray<ObRawExpr*, common::ModulePageAllocator, true> access_exprs_;
  ObUnpivotInfo unpivot_info_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogUnpivot);
};

}  // end of namespace sql
}  // end of namespace oceanbase

#endif /* OB_LOG_SUBQUERY_SCAN_H_ */
