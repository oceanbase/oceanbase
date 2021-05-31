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
#include "sql/engine/expr/ob_expr_subquery_equal.h"
#include "common/object/ob_obj_compare.h"
#include "sql/engine/expr/ob_expr_equal.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObExprSubQueryEqual::ObExprSubQueryEqual(ObIAllocator& alloc)
    : ObSubQueryRelationalExpr(alloc, T_OP_SQ_EQ, N_SQ_EQUAL, 2, NOT_ROW_DIMENSION)
{}

ObExprSubQueryEqual::~ObExprSubQueryEqual()
{}

int ObExprSubQueryEqual::compare_single_row(
    const ObNewRow& left_row, const ObNewRow& right_row, ObExprCtx& expr_ctx, ObObj& result) const
{
  int ret = OB_SUCCESS;
  bool cnt_null = false;
  if (OB_UNLIKELY(left_row.get_count() != right_row.get_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("right and left row is not equal", K(ret));
  } else {
    const ObIArray<ObExprCalcType>& cmp_types = result_type_.get_row_calc_cmp_types();
    ObCompareCtx cmp_ctx(ObMaxType, CS_TYPE_INVALID, false, expr_ctx.tz_offset_, default_null_pos());
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_WARN_ON_FAIL);
    for (int64_t i = 0; OB_SUCC(ret) && i < left_row.get_count(); ++i) {
      const ObObj& left_param = left_row.get_cell(i);
      const ObObj& right_param = right_row.get_cell(i);
      cmp_ctx.cmp_type_ = cmp_types.at(i).get_type();
      cmp_ctx.cmp_cs_type_ = cmp_types.at(i).get_collation_type();
      if (OB_FAIL(ObExprEqual::calc(result, left_param, right_param, cmp_ctx, cast_ctx))) {
        LOG_WARN("Compare expression failed", K(ret));
      } else if (result.is_false()) {
        break;
      } else if (result.is_null()) {
        cnt_null = true;
      }
      LOG_TRACE("hualong debug", K(result), "class", result.get_type());
    }
    if (OB_SUCC(ret) && cnt_null && result.is_true()) {
      result.set_null();
    }
  }

  return ret;
}
}  // namespace sql
}  // namespace oceanbase
