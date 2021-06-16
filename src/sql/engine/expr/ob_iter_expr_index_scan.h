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

#ifndef OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_ITER_EXPR_INDEX_SCAN_H_
#define OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_ITER_EXPR_INDEX_SCAN_H_
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase {
namespace sql {
class ObIndexScanIterExpr : public ObIterExprOperator {
public:
  ObIndexScanIterExpr() : ObIterExprOperator(), iter_idx_(common::OB_INVALID_INDEX)
  {}
  inline void set_iter_idx(int64_t iter_idx)
  {
    iter_idx_ = iter_idx;
  }
  int get_next_row(ObIterExprCtx& expr_ctx, const common::ObNewRow*& result) const;

private:
  int64_t iter_idx_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_ITER_EXPR_INDEX_SCAN_H_ */
