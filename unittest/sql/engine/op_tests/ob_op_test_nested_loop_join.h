/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TESTS_OB_OP_TEST_NESTED_LOOP_JOIN_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TESTS_OB_OP_TEST_NESTED_LOOP_JOIN_H_

#include "lib/ob_errno.h"
#define USING_LOG_PREFIX SQL
#include "sql/engine/join/ob_join_vec_op.h"
#include "sql/engine/join/ob_nested_loop_join_vec_op.h"
#include "sql/engine/join/ob_nested_loop_join_op.h"
#include "unittest/sql/engine/op_tests/ob_op_test_join_base.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include <set>

namespace oceanbase
{
namespace sql
{

class NestedLoopJoinTestSpec : public JoinSpecBuilder<NestedLoopJoinTestSpec>
{
public:
  NestedLoopJoinTestSpec() = default;
  ~NestedLoopJoinTestSpec() = default;

  ObOpSpec *create_spec(common::ObIAllocator &alloc,
                        MockDataSourceSpec *left_mock_spec,
                        MockDataSourceSpec *right_mock_spec,
                        const ExprFixedArray &output_exprs,
                        bool use_rich_format)
  {
    if (use_rich_format) {
      return create_spec_impl<ObNestedLoopJoinVecSpec, ObNestedLoopJoinVecOp>(
          alloc, left_mock_spec, right_mock_spec, output_exprs, use_rich_format, PHY_VEC_NESTED_LOOP_JOIN);
    } else {
      return create_spec_impl<ObNestedLoopJoinSpec, ObNestedLoopJoinOp>(
          alloc, left_mock_spec, right_mock_spec, output_exprs, use_rich_format, PHY_NESTED_LOOP_JOIN);
    }
  }

  ObOperator *create_op(ObExecContext &ctx, ObOpSpec &spec,
                        ObOperator *left_op, ObOperator *right_op)
  {
    if (spec.type_ == PHY_VEC_NESTED_LOOP_JOIN) {
      return default_create_join_op<ObNestedLoopJoinVecOp>(ctx, spec, left_op, right_op);
    } else {
      return default_create_join_op<ObNestedLoopJoinOp>(ctx, spec, left_op, right_op);
    }
  }

  ObOpInput *create_input(ObExecContext &ctx, ObOpSpec &spec)
  {
    return nullptr;
  }

private:
  ObExpr *find_rt_expr(ObRawExpr *raw)
  {
    if (OB_ISNULL(raw)) return nullptr;
    return ObStaticEngineExprCG::get_rt_expr(*raw);
  }

private:
  template <typename SpecType, typename OpType>
  SpecType *create_spec_impl(common::ObIAllocator &alloc,
                             MockDataSourceSpec *left_mock_spec,
                             MockDataSourceSpec *right_mock_spec,
                             const ExprFixedArray &output_exprs,
                             bool use_rich_format,
                             ObPhyOperatorType op_type)
  {
    int ret = OB_SUCCESS;
    FatalErrorChecker error_checker(ret);

    void *mem = alloc.alloc(sizeof(SpecType));
    if (OB_ISNULL(mem)) {
      LOG_WARN("alloc nlj spec failed", K(ret));
      return nullptr;
    }
    SpecType *nlj_spec = new (mem) SpecType(alloc, op_type);

    nlj_spec->plan_ = left_mock_spec->plan_;
    nlj_spec->max_batch_size_ = left_mock_spec->max_batch_size_;
    nlj_spec->use_rich_format_ = use_rich_format;
    nlj_spec->output_ = output_exprs;
    nlj_spec->join_type_ = join_type_;
    nlj_spec->group_rescan_ = false;
    nlj_spec->group_size_ = 0;

    void *child_spec_mem = alloc.alloc(sizeof(ObOpSpec *) * 2);
    if (OB_ISNULL(child_spec_mem)) {
      LOG_WARN("alloc children array failed", K(ret));
      return nullptr;
    }
    ObOpSpec **children = reinterpret_cast<ObOpSpec **>(child_spec_mem);
    children[0] = left_mock_spec;
    children[1] = right_mock_spec;
    if (OB_FAIL(nlj_spec->set_children_pointer(children, 2))) {
      LOG_WARN("set children pointer failed", K(ret));
      return nullptr;
    }

    // Collect all join conditions from ON clause
    const common::ObIArray<JoinedTable *> &joined_tables = resolved_stmt_->get_joined_tables();
    common::ObSEArray<ObRawExpr *, 8> all_join_conds;
    for (int64_t i = 0; i < joined_tables.count(); ++i) {
      JoinedTable *jt = joined_tables.at(i);
      if (OB_NOT_NULL(jt)) {
        const common::ObIArray<ObRawExpr *> &join_conds = jt->get_join_conditions();
        for (int64_t j = 0; j < join_conds.count(); ++j) {
          ObRawExpr *raw_expr = join_conds.at(j);
          if (OB_NOT_NULL(raw_expr)) {
            if (OB_FAIL(all_join_conds.push_back(raw_expr))) {
              LOG_WARN("push join cond failed", K(ret));
              return nullptr;
            }
          }
        }
      }
    }

    // Also check condition_exprs (WHERE clause) for additional conditions
    const common::ObIArray<ObRawExpr *> &cond_exprs = resolved_stmt_->get_condition_exprs();
    for (int64_t i = 0; i < cond_exprs.count(); ++i) {
      ObRawExpr *raw_expr = cond_exprs.at(i);
      if (OB_NOT_NULL(raw_expr)) {
        bool exists = false;
        for (int64_t j = 0; j < all_join_conds.count(); ++j) {
          if (all_join_conds.at(j) == raw_expr) {
            exists = true;
            break;
          }
        }
        if (!exists) {
          if (OB_FAIL(all_join_conds.push_back(raw_expr))) {
            LOG_WARN("push join cond failed", K(ret));
            return nullptr;
          }
        }
      }
    }

    int64_t cond_cnt = all_join_conds.count();
    if (cond_cnt > 0) {
      if (OB_FAIL(nlj_spec->other_join_conds_.init(cond_cnt)) ||
          OB_FAIL(nlj_spec->other_join_conds_.prepare_allocate(cond_cnt))) {
        LOG_WARN("init other_join_conds failed", K(ret));
        return nullptr;
      }
      // Collect condition expressions for calc_exprs_ so that clear_evaluated_flag()
      // properly clears their evaluated flags between left row iterations.
      // Without this, condition results are cached and reused for all left rows.
      constexpr int64_t MAX_CALC_EXPRS = 128;
      ExprFixedArray calc_exprs_arr(alloc);
      std::set<ObExpr *> visited;
      ExprFixedArray child_output(alloc);
      // Collect left and right child output as child_output (to exclude from calc_exprs)
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(calc_exprs_arr.init(MAX_CALC_EXPRS))) {
        LOG_WARN("init calc_exprs_arr failed", K(ret));
      } else if (OB_FAIL(child_output.init(
          left_mock_spec->output_.count() + right_mock_spec->output_.count()))) {
        LOG_WARN("init child_output failed", K(ret));
      } else {
        for (int64_t i = 0; i < left_mock_spec->output_.count() && OB_SUCC(ret); ++i) {
          if (OB_FAIL(child_output.push_back(left_mock_spec->output_.at(i)))) {
            LOG_WARN("push left output failed", K(ret));
          }
        }
        for (int64_t i = 0; i < right_mock_spec->output_.count() && OB_SUCC(ret); ++i) {
          if (OB_FAIL(child_output.push_back(right_mock_spec->output_.at(i)))) {
            LOG_WARN("push right output failed", K(ret));
          }
        }
      }
      for (int64_t i = 0; i < cond_cnt && OB_SUCC(ret); ++i) {
        ObRawExpr *raw = all_join_conds.at(i);
        ObExpr *rt = find_rt_expr(raw);
        if (OB_ISNULL(rt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("find rt expr failed", K(ret));
          return nullptr;
        }
        nlj_spec->other_join_conds_.at(i) = rt;
        // Collect the condition expression and its non-column-ref sub-exprs
        if (OB_FAIL(collect_calc_exprs(rt, child_output, visited, calc_exprs_arr))) {
          LOG_WARN("collect calc exprs from join cond failed", K(ret));
        }
      }
      // Set calc_exprs_ on the spec
      if (OB_SUCC(ret) && calc_exprs_arr.count() > 0) {
        if (OB_FAIL(nlj_spec->calc_exprs_.init(calc_exprs_arr.count()))) {
          LOG_WARN("init nlj calc_exprs failed", K(ret));
        } else if (OB_FAIL(nlj_spec->calc_exprs_.prepare_allocate(calc_exprs_arr.count()))) {
          LOG_WARN("prepare_allocate nlj calc_exprs failed", K(ret));
        } else {
          for (int64_t i = 0; i < calc_exprs_arr.count(); ++i) {
            nlj_spec->calc_exprs_.at(i) = calc_exprs_arr.at(i);
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("create nlj spec failed", K(ret));
      return nullptr;
    }
    return nlj_spec;
  }
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TESTS_OB_OP_TEST_NESTED_LOOP_JOIN_H_
