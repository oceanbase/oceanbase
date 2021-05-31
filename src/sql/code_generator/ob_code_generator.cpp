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

#define USING_LOG_PREFIX SQL_CG

#include "sql/code_generator/ob_code_generator.h"
#include "sql/code_generator/ob_code_generator_impl.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "sql/code_generator/ob_static_engine_cg.h"

namespace oceanbase {
namespace sql {

int ObCodeGenerator::generate(const ObLogPlan& log_plan, ObPhysicalPlan& phy_plan)
{
  int ret = OB_SUCCESS;
  if (!use_static_typing_engine_) {
    if (OB_FAIL(generate_old_plan(log_plan, phy_plan))) {
      LOG_WARN("fail to generate old plan", K(ret));
    }
  } else {
    if (OB_FAIL(generate_exprs(log_plan, phy_plan))) {
      LOG_WARN("fail to get all raw exprs", K(ret));
    } else if (OB_FAIL(generate_operators(log_plan, phy_plan))) {
      LOG_WARN("fail to generate plan", K(ret));
    }
  }

  return ret;
}

int ObCodeGenerator::generate_old_plan(const ObLogPlan& log_plan, ObPhysicalPlan& phy_plan)
{
  int ret = OB_SUCCESS;
  ObCodeGeneratorImpl code_generator(min_cluster_version_);
  if (OB_FAIL(code_generator.generate(log_plan, phy_plan))) {
    LOG_WARN("fail to code generate", K(ret));
  }

  return ret;
}

int ObCodeGenerator::generate_exprs(const ObLogPlan& log_plan, ObPhysicalPlan& phy_plan)
{
  int ret = OB_SUCCESS;
  ObStaticEngineExprCG expr_cg(phy_plan.get_allocator(), param_store_);
  // init ctx for operator cg
  expr_cg.init_operator_cg_ctx(log_plan.get_optimizer_context().get_exec_ctx());
  ObRawExprUniqueSet all_raw_exprs(phy_plan.get_allocator());
  if (OB_FAIL(all_raw_exprs.init())) {
    LOG_WARN("fail to create hash set", K(ret));
  } else if (OB_FAIL(get_plan_all_exprs(log_plan, all_raw_exprs))) {
    LOG_WARN("get all exprs failed", K(ret));
  } else if (OB_FAIL(expr_cg.generate(all_raw_exprs, phy_plan.get_expr_frame_info()))) {
    LOG_WARN("fail to generate expr", K(ret));
  } else {
    phy_plan.get_next_expr_id() = phy_plan.get_expr_frame_info().need_ctx_cnt_;
  }

  return ret;
}

int ObCodeGenerator::generate_operators(const ObLogPlan& log_plan, ObPhysicalPlan& phy_plan)
{
  int ret = OB_SUCCESS;
  ObStaticEngineCG static_engin_cg(min_cluster_version_);
  if (OB_FAIL(static_engin_cg.generate(log_plan, phy_plan))) {
    LOG_WARN("fail to code generate", K(ret));
  }

  return ret;
}

int ObCodeGenerator::get_plan_all_exprs(const ObLogPlan& plan, ObRawExprUniqueSet& exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObLogPlan*, 16> plans;
  OZ(get_all_log_plan(&plan, plans));
  FOREACH_CNT_X(p, plans, OB_SUCC(ret))
  {
    if (NULL != *p) {
      OZ(exprs.append((*p)->get_all_exprs().get_expr_array()));
    }
  }
  return ret;
}

int ObCodeGenerator::get_all_log_plan(const ObLogPlan* root, ObIArray<const ObLogPlan*>& plans)
{
  int ret = OB_SUCCESS;
  OZ(check_stack_overflow());
  const int64_t by_op_start = plans.count();
  if (NULL != root) {
    OZ(get_all_log_plan(root->get_plan_root(), plans));
  }
  const int64_t by_op_end = plans.count();
  for (int64_t i = by_op_start; OB_SUCC(ret) && i < by_op_end; i++) {
    FOREACH_CNT_X(sub, plans.at(i)->get_subplans(), OB_SUCC(ret))
    {
      if (NULL != *sub && NULL != (*sub)->subplan_) {
        OZ(add_var_to_array_no_dup(plans, const_cast<const ObLogPlan*>((*sub)->subplan_)));
      }
    }
  }
  const int64_t by_subplan_info_end = plans.count();
  for (int64_t i = by_op_end; OB_SUCC(ret) && i < by_subplan_info_end; i++) {
    OZ(get_all_log_plan(plans.at(i), plans));
  }
  return ret;
}

int ObCodeGenerator::get_all_log_plan(const ObLogicalOperator* op, ObIArray<const ObLogPlan*>& plans)
{
  int ret = OB_SUCCESS;
  if (NULL != op) {
    OZ(check_stack_overflow());
    if (NULL != op->get_plan()) {
      OZ(add_var_to_array_no_dup(plans, op->get_plan()));
      for (int64_t i = 0; OB_SUCC(ret) && i < op->get_num_of_child(); i++) {
        OZ(get_all_log_plan(op->get_child(i), plans));
      }
    }
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
