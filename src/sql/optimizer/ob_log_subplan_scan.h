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

#ifndef _OB_LOG_SUBQUERY_SCAN_H_
#define _OB_LOG_SUBQUERY_SCAN_H_
#include "sql/optimizer/ob_logical_operator.h"

namespace oceanbase {
namespace sql {
class ObLogSubPlanScan : public ObLogicalOperator {
public:
  ObLogSubPlanScan(ObLogPlan& plan)
      : ObLogicalOperator(plan), subquery_id_(common::OB_INVALID_ID), subquery_name_(), access_exprs_()
  {}

  ~ObLogSubPlanScan(){};
  int calc_cost();
  virtual int est_cost() override;
  virtual int re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est) override;

  virtual int copy_without_child(ObLogicalOperator*& out);
  int allocate_expr_post(ObAllocExprContext& ctx);
  int allocate_exchange_post(AllocExchContext* ctx) override;
  int update_weak_part_exprs(AllocExchContext* ctx);
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
  virtual int print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type);
  virtual int transmit_op_ordering();
  virtual int transmit_local_ordering();
  virtual int inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const;
  virtual int generate_link_sql_pre(GenLinkStmtContext& link_ctx) override;

private:
  virtual int print_operator_for_outline(planText& plan_text);
  virtual int is_used_join_type_hint(JoinAlgo join_algo, bool& is_used);
  virtual int is_used_in_leading_hint(bool& is_used);

private:
  uint64_t subquery_id_;
  common::ObString subquery_name_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> access_exprs_;
  DISALLOW_COPY_AND_ASSIGN(ObLogSubPlanScan);
};

}  // end of namespace sql
}  // end of namespace oceanbase

#endif /* OB_LOG_SUBQUERY_SCAN_H_ */
