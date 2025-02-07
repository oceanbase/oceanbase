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
 * This file contains implementation for array_range expression.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_array_range.h"
#include "lib/udt/ob_collection_type.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

namespace oceanbase
{
namespace sql
{
ObExprArrayRange::ObExprArrayRange(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUNC_SYS_ARRAY_RANGE, N_ARRAY_RANGE, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprArrayRange::~ObExprArrayRange()
{
}

int ObExprArrayRange::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        int64_t param_num,
                                        ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  ObExecContext *exec_ctx = NULL;
  ObString res_type_info = "ARRAY(BIGINT)";
  uint16_t subschema_id;
  bool is_null_res = false;

  if (OB_ISNULL(session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSQLSessionInfo is null", K(ret));
  } else if (OB_ISNULL(exec_ctx = session->get_cur_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObExecContext is null", K(ret));
  } else if (param_num > 3) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param num is not correct", K(ret), K(param_num));
  }

  for (int64_t i = 0; i < param_num && OB_SUCC(ret) && !is_null_res; i++) {
    if (types_stack[i].is_null()) {
      is_null_res = true;
    } else if (!ob_is_numeric_type(types_stack[i].get_type())
           && !ob_is_varchar_or_char(types_stack[i].get_type(), types_stack[i].get_collation_type())) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("invalid data type", K(ret), K(types_stack[i].get_type()));
    } else {
      types_stack[i].set_calc_type(ObIntType);
    }
  } // end for

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(exec_ctx->get_subschema_id_by_type_string(res_type_info, subschema_id))) {
    LOG_WARN("failed get subschema id", K(ret), K(res_type_info));
  } else {
    type.set_collection(subschema_id);
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObCollectionSQLType]).get_length());
  }

  return ret;
}

int ObExprArrayRange::eval_array_range(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();

  ObDatum *datum = NULL;
  int64_t param_start = 0;
  int64_t param_end = 0;
  int64_t param_step = 1;
  int32_t arr_size = 0;
  char *arr_buf = NULL;
  uint32_t arr_buf_len = 0;
  int64_t *arr_data = NULL;
  bool is_null_res = false;

  for (int64_t i = 0; i < expr.arg_cnt_ && OB_SUCC(ret) && !is_null_res; i++) {
    datum = NULL;
    if (OB_FAIL(expr.args_[i]->eval(ctx, datum))) {
      LOG_WARN("failed to eval args", K(ret));
    } else if (datum->is_null()) {
      is_null_res = true;
    } else if (expr.arg_cnt_ == 1) {
      param_end = datum->get_int();
      if (param_end < 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("param_end less than 0", K(ret), K(param_end));
      }
    } else if (i == 0) {
      param_start = datum->get_int();
    } else if (i == 1) {
      param_end = datum->get_int();
    } else if (i == 2) {
      param_step = datum->get_int();
      if (param_step == 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("step can not be 0", K(ret), K(param_step));
      }
    }
  } // end for

  if (OB_FAIL(ret) || is_null_res) {
  } else if ((param_step > 0 && param_start >= param_end)
             || (param_step < 0 && param_start <= param_end)) {
    // empty array
    arr_buf_len = sizeof(uint32_t);
  } else {
    /* int64 array binary: | length(uint32_t) | null_bitmaps(size * uint8_t) | arr_data( size * int64_t ) | */
    uint64_t interval = 0;
    if (param_start >= 0 && param_end < 0) {
      if (param_end == INT64_MIN) {
        interval = static_cast<uint64_t>(param_start) + static_cast<uint64_t>(param_end);
      } else {
        interval = static_cast<uint64_t>(param_start) + static_cast<uint64_t>(abs(param_end));
      }
    } else if (param_start < 0 && param_end >= 0) {
      if (param_start == INT64_MIN) {
        interval = static_cast<uint64_t>(param_start) + static_cast<uint64_t>(param_end);
      } else {
        interval = static_cast<uint64_t>(abs(param_start)) + static_cast<uint64_t>(param_end);
      }
    } else {
      interval = abs(param_end - param_start);
    }
    arr_size = (interval - 1) / abs(param_step) + 1;
    arr_buf_len = sizeof(uint32_t) + arr_size * (sizeof(uint8_t) + sizeof(int64_t));
    if (arr_size > MAX_ARRAY_ELEMENT_SIZE) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("array element size exceed max", K(ret), K(arr_size), K(MAX_ARRAY_ELEMENT_SIZE));
    }
  }

  if (OB_FAIL(ret) || is_null_res) {
  } else if (OB_ISNULL(arr_buf = static_cast<char *>(tmp_allocator.alloc(arr_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(arr_buf_len));
  } else if (arr_size == 0) {
    *(reinterpret_cast<uint32_t *>(arr_buf)) = 0;
  } else {
    MEMSET(arr_buf, 0, arr_buf_len);
    // set the array binary length
    *(reinterpret_cast<uint32_t *>(arr_buf)) = arr_size;
    // fill array data
    arr_data = reinterpret_cast<int64_t *>(arr_buf + sizeof(uint32_t) + arr_size * sizeof(uint8_t));
    for (int32_t i = 0; i < arr_size; ++i) {
      arr_data[i] = param_start + i * param_step;
    } // end for
  }

  if (OB_FAIL(ret)) {
  } else if (is_null_res) {
    res.set_null();
  } else {
    ObString res_str;
    if (OB_FAIL(ObArrayExprUtils::set_array_res(NULL, arr_buf_len, expr, ctx, res_str, arr_buf))) {
      LOG_WARN("get array binary string failed", K(ret));
    } else {
      res.set_string(res_str);
    }
  }

  return ret;
}

int ObExprArrayRange::cg_expr(ObExprCGCtx &expr_cg_ctx,
                         const ObRawExpr &raw_expr,
                         ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_array_range;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
