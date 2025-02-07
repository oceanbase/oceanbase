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
 * This file contains implementation for array_to_string expression.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_array_to_string.h"
#include "lib/udt/ob_collection_type.h"
#include "lib/udt/ob_array_type.h"
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
ObExprArrayToString::ObExprArrayToString(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUNC_SYS_ARRAY_TO_STRING, N_ARRAY_TO_STRING, TWO_OR_THREE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprArrayToString::~ObExprArrayToString()
{
}

int ObExprArrayToString::calc_result_typeN(ObExprResType &type,
                                           ObExprResType *types,
                                           int64_t param_num,
                                           common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  ObExecContext *exec_ctx = NULL;
  ObExprResType *array_type = &types[0];
  ObExprResType *delimiter_type = &types[1];
  ObSubSchemaValue arr_meta;

  if (OB_ISNULL(session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSQLSessionInfo is null", K(ret));
  } else if (OB_ISNULL(exec_ctx = session->get_cur_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObExecContext is null", K(ret));
  } else if (ob_is_null(array_type->get_type())) {
    // do nothing
  } else if (!ob_is_collection_sql_type(array_type->get_type())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "ARRAY", ob_obj_type_str(array_type->get_type()));
  } else if (OB_FAIL(exec_ctx->get_sqludt_meta_by_subschema_id(array_type->get_subschema_id(), arr_meta))) {
    LOG_WARN("failed to get elem meta.", K(ret), K(array_type->get_subschema_id()));
  } else if (arr_meta.type_ != ObSubSchemaType::OB_SUBSCHEMA_COLLECTION_TYPE) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid subschema type", K(ret), K(arr_meta.type_));
  }
  if (OB_FAIL(ret) || ob_is_null(delimiter_type->get_type())) {
    // do nothing
  } else if (ob_is_varchar_char_type(delimiter_type->get_type(), delimiter_type->get_collation_type())) {
    delimiter_type->set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "VARCHAR", ob_obj_type_str(delimiter_type->get_type()));
  }
  if (OB_SUCC(ret) && param_num == 3) {
    ObExprResType *null_str_type = &types[2];
    if (ob_is_null(null_str_type->get_type())) {
      // do nothing
    } else if (ob_is_varchar_char_type(null_str_type->get_type(), null_str_type->get_collation_type())) {
      null_str_type->set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    } else {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "VARCHAR", ob_obj_type_str(null_str_type->get_type()));
    }
  }

  if (OB_SUCC(ret)) {
    type.set_type(ObLongTextType);
    type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObLongTextType]);
  }

  return ret;
}

int ObExprArrayToString::eval_array_to_string(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  ObDatum *arr_datum = NULL;
  ObDatum *delimiter_datum = NULL;
  ObDatum *null_str_datum = NULL;
  bool is_null_res = false;
  ObIArrayType *arr_obj = NULL;
  ObString delimiter;
  ObString null_str;
  bool has_null_str = false;

  if (OB_FAIL(expr.args_[0]->eval(ctx, arr_datum))) {
    LOG_WARN("failed to eval source array arg", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, delimiter_datum))) {
    LOG_WARN("failed to eval delimiter string arg", K(ret));
  } else if (expr.arg_cnt_ > 2 && OB_FAIL(expr.args_[2]->eval(ctx, null_str_datum))) {
    LOG_WARN("failed to eval null string arg", K(ret));
  } else if (arr_datum->is_null() || delimiter_datum->is_null()) {
    is_null_res = true;
  } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, subschema_id, arr_datum->get_string(), arr_obj))) {
    LOG_WARN("construct array obj failed", K(ret));
  } else if (OB_FALSE_IT(delimiter = delimiter_datum->get_string())) {
  } else if (expr.arg_cnt_ > 2 && !null_str_datum->is_null()) {
    has_null_str = true;
    null_str = null_str_datum->get_string();
  }

  if (OB_FAIL(ret)) {
  } else if (is_null_res) {
    res.set_null();
  } else {
    ObStringBuffer res_buf(&tmp_allocator);
    ObTextStringDatumResult str_result(expr.datum_meta_.type_, &expr, &ctx, &res);
    if (OB_FAIL(arr_obj->print_element(res_buf, 0, 0, delimiter, has_null_str, null_str))) {
      LOG_WARN("failed to format array", K(ret));
    } else if (OB_FAIL(str_result.init(res_buf.length()))) {
      LOG_WARN("failed to init result", K(ret), K(res_buf.length()));
    } else if (OB_FAIL(str_result.append(res_buf.ptr(), res_buf.length()))) {
      LOG_WARN("failed to append realdata", K(ret), K(res_buf));
    } else {
      str_result.set_result();
    }
  }
  return ret;
}

int ObExprArrayToString::eval_array_to_string_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                                    const ObBitVector &skip, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  ObIArrayType *arr_obj = NULL;

  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval source array failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval delimiter string failed", K(ret));
  } else if (OB_FAIL(expr.arg_cnt_ > 2 && expr.args_[2]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval null string failed", K(ret));
  } else {
    ObDatumVector arr_array = expr.args_[0]->locate_expr_datumvector(ctx);
    ObDatumVector delimiter_array = expr.args_[1]->locate_expr_datumvector(ctx);
    ObDatumVector null_str_array = expr.arg_cnt_ > 2 ? expr.args_[2]->locate_expr_datumvector(ctx) : ObDatumVector();
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
      bool is_null_res = false;
      ObString delimiter;
      ObString null_str;
      bool has_null_str = false;
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      eval_flags.set(j);
      if (arr_array.at(j)->is_null() || delimiter_array.at(j)->is_null()) {
        is_null_res = true;
      } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, subschema_id, arr_array.at(j)->get_string(), arr_obj))) {
        LOG_WARN("construct array obj failed", K(ret));
      } else if (OB_FALSE_IT(delimiter = delimiter_array.at(j)->get_string())) {
      } else if (expr.arg_cnt_ > 2 && !null_str_array.at(j)->is_null()) {
        has_null_str = true;
        null_str = null_str_array.at(j)->get_string();
      }
      if (OB_FAIL(ret)) {
      } else if (is_null_res) {
        res_datum.at(j)->set_null();
      } else {
        ObStringBuffer res_buf(&tmp_allocator);
        ObTextStringDatumResult str_result(expr.datum_meta_.type_, &expr, &ctx, res_datum.at(j));
        if (OB_FAIL(arr_obj->print_element(res_buf, 0, 0, delimiter, has_null_str, null_str))) {
          LOG_WARN("failed to format array", K(ret));
        } else if (OB_FAIL(str_result.init_with_batch_idx(res_buf.length(), j))) {
          LOG_WARN("failed to init result", K(ret), K(res_buf.length()), K(j));
        } else if (OB_FAIL(str_result.append(res_buf.ptr(), res_buf.length()))) {
          LOG_WARN("failed to append realdata", K(ret), K(res_buf));
        } else {
          str_result.set_result();
        }
      }
    } // end for
  }
  return ret;
}

int ObExprArrayToString::eval_array_to_string_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                                     const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  ObIArrayType *arr_obj = NULL;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval source array failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval delimiter string failed", K(ret));
  } else if (OB_FAIL(expr.arg_cnt_ > 2 && expr.args_[2]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval null string failed", K(ret));
  } else {
    ObIVector *arr_vec = expr.args_[0]->get_vector(ctx);
    VectorFormat arr_format = arr_vec->get_format();
    ObIVector *delimiter_vec = expr.args_[1]->get_vector(ctx);
    ObIVector *null_str_vec = expr.arg_cnt_ > 2 ? expr.args_[2]->get_vector(ctx) : NULL;
    ObIVector *res_vec = expr.get_vector(ctx);
    ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      bool is_null_res = false;
      ObString delimiter;
      ObString null_str;
      bool has_null_str = false;
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else if (arr_vec->is_null(idx) || delimiter_vec->is_null(idx)) {
        is_null_res = true;
      } else if (arr_format == VEC_UNIFORM || arr_format == VEC_UNIFORM_CONST) {
        ObString arr_str = arr_vec->get_string(idx);
        if (OB_FAIL(ObNestedVectorFunc::construct_param(tmp_allocator, ctx, subschema_id, arr_str, arr_obj))) {
          LOG_WARN("construct array obj failed", K(ret));
        }
      } else if (OB_FAIL(ObNestedVectorFunc::construct_attr_param(
                     tmp_allocator, ctx, *expr.args_[0], subschema_id, idx, arr_obj))) {
        LOG_WARN("construct array obj failed", K(ret));
      }
      if (OB_FAIL(ret) || is_null_res) {
      } else if (OB_FALSE_IT(delimiter = delimiter_vec->get_string(idx))) {
      } else if (expr.arg_cnt_ > 2 && !null_str_vec->is_null(idx)) {
        has_null_str = true;
        null_str = null_str_vec->get_string(idx);
      }
      if (OB_FAIL(ret)) {
      } else if (is_null_res) {
        res_vec->set_null(idx);
        eval_flags.set(idx);
      } else {
        ObStringBuffer res_buf(&tmp_allocator);
        if (OB_FAIL(arr_obj->print_element(res_buf, 0, 0, delimiter, has_null_str, null_str))) {
          LOG_WARN("failed to format array", K(ret));
        } else {
          if (res_format == VEC_DISCRETE) {
            if (OB_FAIL(set_text_res<ObDiscreteFormat>(res_buf, expr, ctx, static_cast<ObDiscreteFormat *>(res_vec), idx))) {
              LOG_WARN("set array res failed", K(ret));
            }
          } else if (res_format == VEC_UNIFORM) {
            if (OB_FAIL(set_text_res<ObUniformFormat<false>>(res_buf, expr, ctx, static_cast<ObUniformFormat<false> *>(res_vec), idx))) {
              LOG_WARN("set array res failed", K(ret));
            }
          } else if (OB_FAIL(set_text_res<ObVectorBase>(res_buf, expr, ctx, static_cast<ObVectorBase *>(res_vec), idx))) {
            LOG_WARN("set array res failed", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          eval_flags.set(idx);
        }
      }
    } // end for
  }
  return ret;
}

int ObExprArrayToString::cg_expr(ObExprCGCtx &expr_cg_ctx,
                         const ObRawExpr &raw_expr,
                         ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_array_to_string;
  rt_expr.eval_batch_func_ = eval_array_to_string_batch;
  rt_expr.eval_vector_func_ = eval_array_to_string_vector;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
