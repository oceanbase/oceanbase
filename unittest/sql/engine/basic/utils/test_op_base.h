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
#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_BASIC_UTILS_TEST_OP_BASE_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_BASIC_UTILS_TEST_OP_BASE_H_

#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_malloc.h"
#include "sql/engine/ob_batch_rows.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/session/ob_sql_session_info.h"
#include "expr_maker.h"
#include "mock_child.h"

namespace oceanbase
{
namespace sql
{

/**
 * How to use TestOpBase in operator UTs:
 *
 * 1) Build expression makers that describe input/output vector frames.
 * 2) Call init_exec_env(...) to allocate frames and initialize mock child/plan.
 * 3) Build operators/specs and open the root operator.
 * 4) Validate outputs with:
 *    - validate_data<T>() for one batch value checking
 *    - validate_brs() for size/end/skip checking
 *    - get_next_all_and_validate<T>() for full-stream checking
 *
 * Minimal example:
 *   FixedGenExprMaker<int64_t> child_expr(4, {{1, 2}, {3}});
 *   init_exec_env({&child_expr}, {}, parent_spec, 4);
 *   MockChildSpec *child = child_utils_.spec();
 *   // ... init operator tree and open root op ...
 *   get_next_all_and_validate<int64_t>(
 *       root_op, 4, {1, 2, 3}, {false, false, false});
 */
// Common fixture utilities shared by basic operator UTs:
// - SQL execution context/session initialization
// - expression frame allocation
// - batch value/skip validation helpers
class TestOpBase : public ::testing::Test
{
public:
  explicit TestOpBase(const char *name);

protected:
  // Initializes session/tenant context used by SQL engine operators in tests.
  void SetUp() override;
  // Initializes full execution environment for operator tests:
  // 1) allocate eval frames for gen + extra makers
  // 2) initialize mock child using gen makers
  // 3) bind shared physical plan to parent and child specs
  // @param gen_expr_makers child output generators
  // @param extra_expr_makers additional makers (for example, limit/offset constants)
  // @param parent_spec parent operator spec to bind with shared plan
  // @param batch_size max batch size used by spec/child
  // @param phy_op_count physical operator slots reserved in exec_ctx
  void init_exec_env(const std::vector<GenExprMaker *> &gen_expr_makers,
                     const std::vector<ExprMaker *> &extra_expr_makers,
                     ObOpSpec &parent_spec,
                     int64_t batch_size,
                     const int64_t phy_op_count = 2);
  // Attaches prepared mock child under parent operator.
  void attach_mock_child(ObOpSpec &parent_spec, ObOperator &parent_op);

  // Validate active row values match expected. Uses brs.skip to determine active rows.
  // @param expr evaluated output expression
  // @param brs batch metadata (size/skip/all_rows_active)
  // @param expected active-row values only; skipped rows are excluded
  // Example:
  //   brs.skip={F,T,F}, expected={10,30}
  template <typename T>
  void validate_data(ObExpr *expr,
                    const ObBatchRows *brs,
                    const std::vector<std::optional<T>> &expected);

  // Fetch all batches from one operator and validate skip/data continuously.
  // @param op root operator
  // @param batch_size max rows requested per get_next_batch() call
  // @param expected_data flattened active-row values across all batches
  // @param skip_vec optional flattened skip bits; true means skipped
  // Example:
  //   get_next_all_and_validate<std::string>(op, 4,
  //       {"a", "b", "c"}, {false, true, false, false});
  template <typename T>
  void get_next_all_and_validate(ObOperator *op, int64_t batch_size,
                            const std::vector<std::optional<T>> &expected_data,
                            const std::vector<bool> &skip_vec = {});

  // Validate brs size, end, and skip pattern.
  // skip_vec: true means skip; empty {} means skip pattern not checked.
  void validate_brs(const ObBatchRows *brs,
                    const std::vector<bool> &skip_vec,
                    int64_t size,
                    bool is_end);

  // Use ObMalloc-backed allocator so exec_ctx/operator allocations are tracked
  // in sanity mode with the same memory domain.
  common::ObMalloc allocator_;
  ObSQLSessionInfo session_;
  ObExecContext exec_ctx_;
  ObEvalCtx eval_ctx_;
  MockChildUtils child_utils_;
  ObPhysicalPlan phy_plan_;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_UNITTEST_SQL_ENGINE_BASIC_UTILS_TEST_OP_BASE_H_
