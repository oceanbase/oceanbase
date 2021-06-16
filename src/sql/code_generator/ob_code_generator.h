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

#ifndef OCEANBASE_SQL_CODE_GENERATOR_OB_CODE_GENERATOR_
#define OCEANBASE_SQL_CODE_GENERATOR_OB_CODE_GENERATOR_

#include "sql/engine/expr/ob_expr.h"
#include "lib/container/ob_iarray.h"

namespace oceanbase {
namespace common {
class ObIAllocator;
}

namespace sql {
class ObCodeGeneratorImpl;
class ObPhysicalPlan;
class ObLogPlan;
class ObRawExpr;
class ObLogicalOperator;
class ObRawExprUniqueSet;

class ObCodeGenerator {
public:
  ObCodeGenerator(
      bool use_jit, bool use_static_typing_engine, uint64_t min_cluster_version, DatumParamStore* param_store)
      : use_jit_(use_jit),
        use_static_typing_engine_(use_static_typing_engine),
        min_cluster_version_(min_cluster_version),
        param_store_(param_store)
  {}
  virtual ~ObCodeGenerator()
  {}

  int generate(const ObLogPlan& log_plan, ObPhysicalPlan& phy_plan);

private:
  int generate_old_plan(const ObLogPlan& log_plan, ObPhysicalPlan& phy_plan);
  int generate_exprs(const ObLogPlan& log_plan, ObPhysicalPlan& phy_plan);

  int generate_operators(const ObLogPlan& log_plan, ObPhysicalPlan& phy_plan);

  // get all raw exprs of logical plan (include the subplans)
  int get_plan_all_exprs(const ObLogPlan& plan, ObRawExprUniqueSet& exprs);

  // get all plans, referenced in subplan_infos_ or referenced by logical operators.
  // subplans are referenced in two ways:
  //   1. ObLogicalPlan::subplan_infos_:
  //      subplan of subquery ref is add to subplan_infos_, e.g:
  //      select * from t1 where c1 < (select distinct c2 from t2);
  //   2. ObLogicalOperator::my_plan_:
  //      subplan subplan scan is referenced in this way, we traverse all logical operators
  //      get all subplans. e.g.:
  //
  //        Hash Join (my_plan_: root)
  //          Table Scan (my_plan_: root)
  //          Subplan Scan (my_plan_: root)
  //            Table Scan (my_plan_: subplan)
  //
  int get_all_log_plan(const ObLogPlan* plan, common::ObIArray<const ObLogPlan*>& plans);

  // traverse logical operator tree, get all plans referenced by logical operators.
  int get_all_log_plan(const ObLogicalOperator* op, common::ObIArray<const ObLogPlan*>& plans);

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObCodeGenerator);

private:
  bool use_jit_;
  bool use_static_typing_engine_;
  uint64_t min_cluster_version_;
  DatumParamStore* param_store_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_CODE_GENERATOR_OB_CODE_GENERATOR_ */
