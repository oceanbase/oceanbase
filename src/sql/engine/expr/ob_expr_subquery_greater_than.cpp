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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_subquery_greater_than.h"
#include "common/row/ob_row.h"
#include "common/object/ob_obj_compare.h"
#include "sql/engine/expr/ob_expr_greater_than.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObExprSubQueryGreaterThan::ObExprSubQueryGreaterThan(ObIAllocator& alloc)
    : ObSubQueryRelationalExpr(alloc, T_OP_SQ_GT, N_SQ_GREATER_THAN, 2, NOT_ROW_DIMENSION)
{}

ObExprSubQueryGreaterThan::~ObExprSubQueryGreaterThan()
{}

int ObExprSubQueryGreaterThan::compare_single_row(
    const ObNewRow& left_row, const ObNewRow& right_row, ObExprCtx& expr_ctx, ObObj& result) const
{
  int ret = OB_SUCCESS;
  int32_t cmp = 0;
  if (OB_UNLIKELY(left_row.get_count() != right_row.get_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("right and left row is not equal", K(ret));
  } else {
    const ObIArray<ObExprCalcType>& cmp_types = result_type_.get_row_calc_cmp_types();
    for (int64_t i = 0; OB_SUCC(ret) && i < left_row.get_count(); ++i) {
      const ObObj& left_param = left_row.get_cell(i);
      const ObObj& right_param = right_row.get_cell(i);
      if (OB_FAIL(ObSubQueryRelationalExpr::compare_obj(
              expr_ctx, left_param, right_param, cmp_types.at(i), false, result))) {
        LOG_WARN("compare subquery obj failed", K(ret));
      } else if (OB_UNLIKELY(result.is_null())) {
        break;
      } else if (OB_FAIL(result.get_int32(cmp))) {
        LOG_WARN("get int value from result failed", K(ret), K(result));
      } else if (0 != cmp) {
        result.set_int32(cmp > 0);
        break;
      } else if (i == left_row.get_count() - 1) {
        result.set_int32(false);
        break;
      }
    }
  }

  return ret;
}
}  // namespace sql
}  // namespace oceanbase
