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

#ifndef OCEANBASE_SQL_OB_LOG_SUBPLAN_FILTER_H_
#define OCEANBASE_SQL_OB_LOG_SUBPLAN_FILTER_H_

#include "sql/optimizer/ob_logical_operator.h"

namespace oceanbase {
namespace sql {
class ObBasicCostInfo;
class ObLogSubPlanFilter : public ObLogicalOperator {
public:
  ObLogSubPlanFilter(ObLogPlan& plan)
      : ObLogicalOperator(plan),
        exec_params_(),
        onetime_exprs_(),
        init_plan_idxs_(),
        one_time_idxs_(),
        update_set_(false)
  {}
  ~ObLogSubPlanFilter()
  {}
  virtual int copy_without_child(ObLogicalOperator*& out);
  int allocate_exchange_post(AllocExchContext* ctx) override;
  int check_if_match_partition_wise(const AllocExchContext& ctx, bool& is_partition_wise);
  int has_serial_child(bool& has_serial_child);
  int get_equal_key(const AllocExchContext& ctx, ObLogicalOperator* child, common::ObIArray<ObRawExpr*>& left_key,
      common::ObIArray<ObRawExpr*>& right_key);

  int inner_get_equal_key(ObLogicalOperator* child, common::ObIArray<ObRawExpr*>& filters,
      common::ObIArray<ObRawExpr*>& left_key, common::ObIArray<ObRawExpr*>& right_key);

  int extract_correlated_keys(const ObLogicalOperator* op, common::ObIArray<ObRawExpr*>& left_key,
      common::ObIArray<ObRawExpr*>& right_key, common::ObIArray<ObRawExpr*>& nullsafe_left_key,
      common::ObIArray<ObRawExpr*>& nullsafe_right_key);
  int gen_filters();
  int gen_output_columns();
  virtual int est_cost() override;
  virtual int re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est) override;
  virtual int transmit_op_ordering();
  virtual int transmit_local_ordering();

  /**
   *  Get the exec params
   */
  inline const common::ObIArray<std::pair<int64_t, ObRawExpr*> >& get_exec_params()
  {
    return exec_params_;
  }

  /**
   *  Set the exec params
   */
  inline int add_exec_params(const common::ObIArray<std::pair<int64_t, ObRawExpr*> >& params)
  {
    return append(exec_params_, params);
  }

  inline const common::ObIArray<std::pair<int64_t, ObRawExpr*> >& get_onetime_exprs() const
  {
    return onetime_exprs_;
  }

  inline common::ObIArray<std::pair<int64_t, ObRawExpr*> >& get_onetime_exprs()
  {
    return onetime_exprs_;
  }

  inline int add_onetime_exprs(const common::ObIArray<std::pair<int64_t, ObRawExpr*> >& onetime_exprs)
  {
    return append(onetime_exprs_, onetime_exprs);
  }

  inline const common::ObBitSet<>& get_onetime_idxs()
  {
    return one_time_idxs_;
  }

  inline int add_onetime_idxs(const common::ObBitSet<>& one_time_idxs)
  {
    return one_time_idxs_.add_members(one_time_idxs);
  }

  inline const common::ObBitSet<>& get_initplan_idxs()
  {
    return init_plan_idxs_;
  }

  inline int add_initplan_idxs(const common::ObBitSet<>& init_plan_idxs)
  {
    return init_plan_idxs_.add_members(init_plan_idxs);
  }

  virtual int allocate_expr_pre(ObAllocExprContext& ctx) override;
  bool should_be_produced_by_subplan_filter(const ObRawExpr* subexpr, const ObRawExpr* expr);
  virtual int inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const override;
  bool is_my_subquery_expr(const ObQueryRefRawExpr* query_expr);
  bool is_my_onetime_expr(const ObRawExpr* expr);

  int get_subquery_exprs(ObIArray<ObRawExpr*>& subquery_exprs);

  virtual int check_output_dep_specific(ObRawExprCheckDep& checker);

  virtual int print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type);
  virtual int re_calc_cost();
  int get_children_cost_info(common::ObIArray<ObBasicCostInfo>& children_cost_info);
  uint64_t hash(uint64_t seed) const;
  void set_update_set(bool update_set)
  {
    update_set_ = update_set;
  }
  bool is_update_set()
  {
    return update_set_;
  }
  int allocate_granule_pre(AllocGIContext& ctx);
  int allocate_granule_post(AllocGIContext& ctx);
  virtual int compute_one_row_info() override;

protected:
  common::ObSEArray<std::pair<int64_t, ObRawExpr*>, 8, common::ModulePageAllocator, true> exec_params_;
  common::ObSEArray<std::pair<int64_t, ObRawExpr*>, 8, common::ModulePageAllocator, true> onetime_exprs_;
  common::ObBitSet<> init_plan_idxs_;
  common::ObBitSet<> one_time_idxs_;
  bool update_set_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogSubPlanFilter);
};
}  // end of namespace sql
}  // end of namespace oceanbase

#endif  // OCEANBASE_SQL_OB_LOG_SUBPLAN_FILTER_H_
