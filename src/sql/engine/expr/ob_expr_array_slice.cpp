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
 * This file contains implementation for array_slice.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_array_slice.h"
#include "lib/udt/ob_array_type.h"
#include "lib/udt/ob_collection_type.h"
#include "sql/engine/expr/ob_array_cast.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

namespace oceanbase
{
namespace sql
{

ObExprArraySlice::ObExprArraySlice(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,
          T_FUNC_SYS_ARRAY_SLICE,
          N_ARRAY_SLICE,
          TWO_OR_THREE,
          VALID_FOR_GENERATED_COL,
          NOT_ROW_DIMENSION)
{
}

ObExprArraySlice::~ObExprArraySlice() {}

int ObExprArraySlice::calc_result_typeN(ObExprResType &type,
                          ObExprResType *types,
                          int64_t param_num,
                          ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  ObExecContext *exec_ctx = NULL;
  ObExprResType *arr_type = &types[0];
  ObExprResType *offset_type = &types[1];
  bool is_null = false;
  uint16_t subschema_id = arr_type->get_subschema_id();

  if (OB_ISNULL(session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSQLSessionInfo is null", K(ret));
  } else if (OB_ISNULL(exec_ctx = session->get_cur_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObExecContext is null", K(ret));
  } else if (ob_is_null(arr_type->get_type())) {
    is_null = true;
  } else if (!ob_is_collection_sql_type(arr_type->get_type())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "ARRAY", ob_obj_type_str(arr_type->get_type()));
  } else if (ob_is_null(offset_type->get_type())) {
    is_null = true;
  } else if (!ob_is_numeric_type(offset_type->get_type())
         && !ob_is_varchar_or_char(offset_type->get_type(), offset_type->get_collation_type())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "OFFSET", ob_obj_type_str(offset_type->get_type()));
  } else if (param_num == 3) {
    ObExprResType *len_type = &types[2];
    if (ob_is_null(len_type->get_type())) {
      is_null = true;
    } else if (!ob_is_numeric_type(len_type->get_type())
           && !ob_is_varchar_or_char(len_type->get_type(), len_type->get_collation_type())) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "LEN", ob_obj_type_str(len_type->get_type()));
    } else {
      len_type->set_calc_type(ObIntType);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (is_null) {
    type.set_null();
  } else {
    offset_type->set_calc_type(ObIntType);
    type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
    type.set_collection(subschema_id);
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObCollectionSQLType]).get_length());
  }

  return ret;
}

int ObExprArraySlice::eval_array_slice(const ObExpr &expr,
                          ObEvalCtx &ctx,
                          ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  uint16_t subschema_id = expr.obj_meta_.get_subschema_id();
  ObDatum *arr_datum = NULL;
  ObDatum *offset_datum = NULL;
  ObDatum *len_datum = NULL;
  ObIArrayType *src_arr = NULL;
  ObIArrayType *res_arr = NULL;

  if (OB_FAIL(expr.args_[0]->eval(ctx, arr_datum))) {
    LOG_WARN("eval source array failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, offset_datum))) {
    LOG_WARN("eval offset failed", K(ret));
  } else if (expr.arg_cnt_ > 2 && OB_FAIL(expr.args_[2]->eval(ctx, len_datum))) {
    LOG_WARN("eval len failed", K(ret));
  } else if (arr_datum->is_null() || offset_datum->is_null() ||
             (expr.arg_cnt_ > 2 && len_datum->is_null())) {
    res.set_null();
  } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator,
                                          ctx,
                                          subschema_id,
                                          arr_datum->get_string(),
                                          src_arr))) {
    LOG_WARN("construct array obj failed", K(ret));
  } else if (OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_allocator,
                                          ctx,
                                          subschema_id,
                                          res_arr,
                                          false))) {
    LOG_WARN("construct array obj failed", K(ret));
  } else {
    uint32_t arr_len = src_arr->size();
    int64_t offset = offset_datum->get_int();
    int64_t len = 0;
    bool has_len = false;
    if (expr.arg_cnt_ > 2) {
      has_len = true;
      len = len_datum->get_int();
    }
    if (OB_FAIL(get_subarray(res_arr, src_arr, offset, len, has_len))) {
      LOG_WARN("failed to get subarray", K(ret));
    } else {
      ObString res_str;
      if (OB_FAIL(ObArrayExprUtils::set_array_res(res_arr,
                                        res_arr->get_raw_binary_len(),
                                        expr,
                                        ctx,
                                        res_str))) {
        LOG_WARN("get array binary string failed", K(ret));
      } else {
        res.set_string(res_str);
      }
    }
  }

  return ret;
}

int ObExprArraySlice::eval_array_slice_batch(const ObExpr &expr,
                          ObEvalCtx &ctx,
                          const ObBitVector &skip,
                          const int64_t batch_size)
{
  int ret = OB_SUCCESS;

  ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.obj_meta_.get_subschema_id();
  ObIArrayType *src_arr = NULL;
  ObIArrayType *res_arr = NULL;

  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval source array failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval offset failed", K(ret));
  } else if (expr.arg_cnt_ > 2 && OB_FAIL(expr.args_[2]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval len failed", K(ret));
  } else {
    ObDatumVector arr_array = expr.args_[0]->locate_expr_datumvector(ctx);
    ObDatumVector offset_array = expr.args_[1]->locate_expr_datumvector(ctx);
    ObDatumVector len_array =
        expr.arg_cnt_ > 2 ? expr.args_[2]->locate_expr_datumvector(ctx) : ObDatumVector();
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      eval_flags.set(j);
      if (arr_array.at(j)->is_null() || offset_array.at(j)->is_null() ||
          (expr.arg_cnt_ > 2 && len_array.at(j)->is_null())) {
        res_datum.at(j)->set_null();
      } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator,
                                              ctx,
                                              subschema_id,
                                              arr_array.at(j)->get_string(),
                                              src_arr))) {
        LOG_WARN("construct array obj failed", K(ret));
      } else if (OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_allocator,
                                              ctx,
                                              subschema_id,
                                              res_arr,
                                              false))) {
        LOG_WARN("construct array obj failed", K(ret));
      } else {
        uint32_t arr_len = src_arr->size();
        int64_t offset = offset_array.at(j)->get_int();
        int64_t len = 0;
        bool has_len = false;
        if (expr.arg_cnt_ > 2) {
          has_len = true;
          len = len_array.at(j)->get_int();
        }
        if (OB_FAIL(get_subarray(res_arr, src_arr, offset, len, has_len))) {
          LOG_WARN("failed to get subarray", K(ret));
        } else {
          int32_t res_size = res_arr->get_raw_binary_len();
          char *res_buf = nullptr;
          int64_t res_buf_len = 0;
          ObTextStringDatumResult output_result(expr.datum_meta_.type_, &expr, &ctx, res_datum.at(j));
          if (OB_FAIL(output_result.init_with_batch_idx(res_size, j))) {
            LOG_WARN("fail to init result", K(ret), K(res_size));
          } else if (OB_FAIL(output_result.get_reserved_buffer(res_buf, res_buf_len))) {
            LOG_WARN("fail to get reserver buffer", K(ret));
          } else if (res_buf_len < res_size) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get invalid res buf len", K(ret), K(res_buf_len), K(res_size));
          } else if (OB_FAIL(res_arr->get_raw_binary(res_buf, res_buf_len))) {
            LOG_WARN("get array raw binary failed", K(ret), K(res_buf_len), K(res_size));
          } else if (OB_FAIL(output_result.lseek(res_size, 0))) {
            LOG_WARN("failed to lseek res.", K(ret), K(output_result), K(res_size));
          } else {
            output_result.set_result();
          }
        }
      }
    } // end for
  }

  return ret;
}

int ObExprArraySlice::eval_array_slice_vector(const ObExpr &expr,
                          ObEvalCtx &ctx,
                          const ObBitVector &skip,
                          const EvalBound &bound)
{
  int ret = OB_SUCCESS;

  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.obj_meta_.get_subschema_id();
  ObIArrayType *src_arr = NULL;
  ObIArrayType *res_arr = NULL;

  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval source array failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval offset failed", K(ret));
  } else if (expr.arg_cnt_ > 2 && OB_FAIL(expr.args_[2]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval len failed", K(ret));
  } else {
    ObIVector *arr_vec = expr.args_[0]->get_vector(ctx);
    VectorFormat arr_format = arr_vec->get_format();
    ObIVector *offset_vec = expr.args_[1]->get_vector(ctx);
    ObIVector *len_vec = expr.arg_cnt_ > 2 ? expr.args_[2]->get_vector(ctx) : NULL;
    ObIVector *res_vec = expr.get_vector(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

    for (int64_t j = bound.start(); OB_SUCC(ret) && j < bound.end(); ++j) {
      bool is_null_res = false;
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      eval_flags.set(j);
      if (arr_vec->is_null(j) || offset_vec->is_null(j) ||
          (expr.arg_cnt_ > 2 && len_vec->is_null(j))) {
        is_null_res = true;
      } else if (OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_allocator,
                                              ctx,
                                              subschema_id,
                                              res_arr,
                                              false))) {
        LOG_WARN("construct array obj failed", K(ret));
      } else if (arr_format == VEC_UNIFORM || arr_format == VEC_UNIFORM_CONST) {
        ObString arr_string = arr_vec->get_string(j);
        if (OB_FAIL(ObNestedVectorFunc::construct_param(tmp_allocator,
                                            ctx,
                                            subschema_id,
                                            arr_string,
                                            src_arr))) {
          LOG_WARN("construct array obj failed", K(ret));
        }
      } else if (OB_FAIL(ObNestedVectorFunc::construct_attr_param(
                     tmp_allocator, ctx, *expr.args_[0], subschema_id, j, src_arr))) {
        LOG_WARN("construct array obj failed", K(ret));
      }

      if (OB_FAIL(ret)) {
      } else if (is_null_res) {
        res_vec->set_null(j);
      } else {
        uint32_t arr_len = src_arr->size();
        int64_t offset = offset_vec->get_int(j);
        int64_t len = 0;
        bool has_len = false;
        if (expr.arg_cnt_ > 2) {
          has_len = true;
          len = len_vec->get_int(j);
        }
        if (OB_FAIL(get_subarray(res_arr, src_arr, offset, len, has_len))) {
          LOG_WARN("failed to get subarray", K(ret));
        } else {
          if (res_format == VEC_DISCRETE) {
            if (OB_FAIL(ObArrayExprUtils::set_array_res<ObDiscreteFormat>(
                    res_arr, expr, ctx, static_cast<ObDiscreteFormat *>(res_vec), j))) {
              LOG_WARN("set array res failed", K(ret));
            }
          } else if (res_format == VEC_UNIFORM) {
            if (OB_FAIL(ObArrayExprUtils::set_array_res<ObUniformFormat<false>>(
                    res_arr, expr, ctx, static_cast<ObUniformFormat<false> *>(res_vec), j))) {
              LOG_WARN("set array res failed", K(ret));
            }
          } else if (OB_FAIL(ObArrayExprUtils::set_array_res<ObVectorBase>(
                         res_arr, expr, ctx, static_cast<ObVectorBase *>(res_vec), j))) {
            LOG_WARN("set array res failed", K(ret));
          }
        }
      }
    } // end for
  } // end if

  return ret;
}

int ObExprArraySlice::get_subarray(ObIArrayType *&res_arr,
                          ObIArrayType *src_arr,
                          int64_t offset,
                          int64_t len,
                          bool has_len)
{
  int ret = OB_SUCCESS;

  int64_t arr_len = src_arr->size();
  int64_t left = offset > 0 ? offset : max(1, arr_len + offset + 1);
  int64_t right = 0;
  if (offset == 0 || offset > arr_len || (has_len && len < 0 && arr_len + len <= 0)) {
    // do nothing
  } else {
    if (has_len) {
      if (len < 0) {
        right = arr_len + len + 1;
      } else {
        if (offset >= 0) {
          if (len > INT64_MAX - offset) {
            right = arr_len + 1;
          } else {
            right = offset + len;
          }
        } else {
          right = left + max(0, offset + arr_len >= 0 ? len : arr_len + offset + len);
        }
        right = right > arr_len + 1 ? arr_len + 1 : right;
      }
    } else {
      right = arr_len + 1;
    }
  }
  if (left > right) {
    left = 1, right = 1;
  }
  if (OB_FAIL(res_arr->insert_from(*src_arr, left - 1, right - left))) {
    LOG_WARN("failed to insert_from", K(ret));
  }

  return ret;
}

int ObExprArraySlice::cg_expr(ObExprCGCtx &expr_cg_ctx,
                          const ObRawExpr &raw_expr,
                          ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_array_slice;
  rt_expr.eval_batch_func_ = eval_array_slice_batch;
  rt_expr.eval_vector_func_ = eval_array_slice_vector;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase