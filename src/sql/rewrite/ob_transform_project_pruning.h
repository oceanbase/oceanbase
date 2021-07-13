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

#ifndef OCEANBASE_SQL_REWRITE_OB_TRANSFORM_PROJECT_PRUNING_
#define OCEANBASE_SQL_REWRITE_OB_TRANSFORM_PROJECT_PRUNING_
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_bit_set.h"
#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"
namespace oceanbase {
namespace sql {

class ObTransformProjectPruning : public ObTransformRule {
public:
  explicit ObTransformProjectPruning(ObTransformerCtx* ctx) : ObTransformRule(ctx, TransMethod::PRE_ORDER)
  {}
  virtual ~ObTransformProjectPruning()
  {}
  virtual int transform_one_stmt(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened) override;

private:
  int project_pruning(const uint64_t table_id, ObSelectStmt& child_stmt, ObDMLStmt& upper_stmt, bool& trans_happened);

  int is_const_expr(ObRawExpr* expr, bool& is_const);

  int check_transform_validity(const ObSelectStmt& stmt, bool& is_valid);
  int check_need_remove(ObSelectStmt* stmt, const int64_t idx, bool& need_remove);

private:
  DISALLOW_COPY_AND_ASSIGN(ObTransformProjectPruning);
};

}  // namespace sql
}  // namespace oceanbase

#endif
