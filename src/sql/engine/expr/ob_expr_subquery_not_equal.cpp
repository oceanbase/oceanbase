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
#include "sql/engine/expr/ob_expr_subquery_not_equal.h"
#include "common/row/ob_row.h"
#include "common/object/ob_obj_compare.h"
#include "sql/engine/expr/ob_expr_not_equal.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObExprSubQueryNotEqual::ObExprSubQueryNotEqual(ObIAllocator& alloc)
    : ObSubQueryRelationalExpr(alloc, T_OP_SQ_NE, N_SQ_NOT_EQUAL, 2, NOT_ROW_DIMENSION)
{}

ObExprSubQueryNotEqual::~ObExprSubQueryNotEqual()
{}

int ObExprSubQueryNotEqual::compare_single_row(
    const ObNewRow& left_row, const ObNewRow& right_row, ObExprCtx& expr_ctx, ObObj& result) const
{
  int ret = OB_SUCCESS;
  bool cnt_null = false;
  if (OB_UNLIKELY(left_row.get_count() != right_row.get_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("right and left row is not equal", K(ret));
  } else {
    const ObIArray<ObExprCalcType>& cmp_types = result_type_.get_row_calc_cmp_types();
    EXPR_DEFINE_CMP_CTX(result_type_.get_calc_meta(), false, expr_ctx);
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_WARN_ON_FAIL);
    for (int64_t i = 0; OB_SUCC(ret) && i < left_row.get_count(); ++i) {
      const ObObj& left_param = left_row.get_cell(i);
      const ObObj& right_param = right_row.get_cell(i);
      cmp_ctx.cmp_type_ = cmp_types.at(i).get_type();
      cmp_ctx.cmp_cs_type_ = cmp_types.at(i).get_collation_type();
      if (OB_FAIL(ObExprNotEqual::calc(result, left_param, right_param, cmp_ctx, cast_ctx))) {
        LOG_WARN("Compare expression failed", K(ret));
      } else if (result.is_true()) {
        break;
      } else if (result.is_null()) {
        cnt_null = true;
      }
    }
    if (OB_SUCC(ret) && cnt_null && result.is_false()) {
      result.set_null();
    }
  }

  return ret;
}
}  // namespace sql
}  // namespace oceanbase
