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
 * This file contains implementation for array_difference expression.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_array_difference.h"
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
ObExprArrayDifference::ObExprArrayDifference(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUNC_SYS_ARRAY_DIFFERENCE, N_ARRAY_DIFFERENCE, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprArrayDifference::~ObExprArrayDifference()
{
}

int ObExprArrayDifference::calc_result_type1(ObExprResType &type,
                                     ObExprResType &type1,
                                     ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  ObExecContext *exec_ctx = NULL;
  ObDataType src_elem_type;
  uint32_t depth = 0;
  bool is_vec = false;
  ObObjType coll_calc_type = ObMaxType;
  ObExprResType calc_type;
  bool is_null_res = false;

  if (OB_ISNULL(session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSQLSessionInfo is null", K(ret));
  } else if (OB_ISNULL(exec_ctx = session->get_cur_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObExecContext is null", K(ret));
  } else if (ob_is_null(type1.get_type())) {
    is_null_res = true;
  } else if (!ob_is_collection_sql_type(type1.get_type())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "ARRAY", ob_obj_type_str(type1.get_type()));
  } else if (OB_FAIL(ObArrayExprUtils::get_array_element_type(exec_ctx, type1.get_subschema_id(), src_elem_type, depth, is_vec))) {
    LOG_WARN("failed to get array element type", K(ret));
  } else if (depth != 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "array_difference with multi-dimension array");
    LOG_WARN("not supported array dimension", K(ret), K(depth));
  } else if (ob_is_int_uint_tc(src_elem_type.get_obj_type())) {
    coll_calc_type = ObObjType::ObIntType;
  } else if (ob_is_float_tc(src_elem_type.get_obj_type())) {
    coll_calc_type = ObObjType::ObFloatType;
  } else if (ob_is_double_tc(src_elem_type.get_obj_type())) {
    coll_calc_type = ObObjType::ObDoubleType;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "array_difference with non-numeric type");
    LOG_WARN("not supported array data type", K(ret), K(src_elem_type.get_obj_type()));
  }
  if (OB_FAIL(ret)) {
  } else if (is_null_res) {
    type.set_null();
  } else {
    uint16_t subschema_id;
    ObDataType elem_data;
    elem_data.set_obj_type(coll_calc_type);
    const int MAX_LEN = 256;
    char type_name[MAX_LEN] = {0};
    ObString type_info;
    if (OB_FAIL(ObArrayUtil::get_type_name(ObNestedType::OB_ARRAY_TYPE, elem_data, type_name, MAX_LEN, depth))) {
      LOG_WARN("failed to convert len to string", K(ret));
    } else if (FALSE_IT(type_info.assign_ptr(type_name, strlen(type_name)))) {
    } else if (OB_FAIL(exec_ctx->get_subschema_id_by_type_string(type_info, subschema_id))) {
      LOG_WARN("failed get subschema id", K(ret), K(type_info));
    } else {
      calc_type.set_collection(subschema_id);
      type.set_collection(subschema_id);
    }
  }
  return ret;
}

int ObExprArrayDifference::calc_difference(ObIArrayType *src_arr, ObIArrayType *res_arr)
{
  int ret = OB_SUCCESS;
  ObObj this_elem;
  ObObj last_elem;
  ObCollectionBasicType *elem_type = static_cast<ObCollectionBasicType *>(src_arr->get_array_type()->element_type_);
  ObObjType obj_type = elem_type->basic_meta_.get_obj_type();

  for (int64_t i = 0; i < src_arr->size() && OB_SUCC(ret); i++) {
    last_elem = this_elem;
    if (OB_FAIL(src_arr->elem_at(i, this_elem))) {
      LOG_WARN("failed to get element", K(ret), K(i));
    } else if (i == 0 && OB_FALSE_IT(last_elem = this_elem)) {
    } else if (ob_is_int_uint_tc(obj_type)) {
      ObArrayFixedSize<int64_t> *array_obj = static_cast<ObArrayFixedSize<int64_t> *>(res_arr);
      if (OB_FAIL(ObArrayExprUtils::raw_check_minus<int64_t>(
                      this_elem.get_int() - last_elem.get_int(),
                      this_elem.get_int(),
                      last_elem.get_int()))) {
        LOG_WARN("array_difference overflow", K(ret));
      } else if (OB_FAIL(array_obj->push_back(this_elem.get_int() - last_elem.get_int()))) {
        LOG_WARN("failed to push back value", K(ret));
      }
    } else if (ob_is_float_tc(obj_type)) {
      ObArrayFixedSize<float> *array_obj = static_cast<ObArrayFixedSize<float> *>(res_arr);
      if (OB_FAIL(ObArrayExprUtils::raw_check_minus<float>(
                      this_elem.get_float() - last_elem.get_float(),
                      this_elem.get_float(),
                      last_elem.get_float()))) {
        LOG_WARN("array_difference overflow", K(ret));
      } else if (OB_FAIL(array_obj->push_back(this_elem.get_float() - last_elem.get_float()))) {
        LOG_WARN("failed to push back value", K(ret));
      }
    } else if (ob_is_double_tc(obj_type)) {
      ObArrayFixedSize<double> *array_obj = static_cast<ObArrayFixedSize<double> *>(res_arr);
      if (OB_FAIL(ObArrayExprUtils::raw_check_minus<double>(
                      this_elem.get_double() - last_elem.get_double(),
                      this_elem.get_double(),
                      last_elem.get_double()))) {
        LOG_WARN("array_difference overflow", K(ret));
      } else if (OB_FAIL(array_obj->push_back(this_elem.get_double() - last_elem.get_double()))) {
        LOG_WARN("failed to push back value", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "unexpected result element type", K(ret), K(obj_type));
    }
  } // end for
  return ret;
}
int ObExprArrayDifference::eval_array_difference(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  const uint16_t res_subschema_id = expr.obj_meta_.get_subschema_id();
  ObDatum *arr_datum = NULL;
  ObIArrayType *src_arr = NULL;
  ObIArrayType *res_arr = NULL;

  if (OB_FAIL(expr.args_[0]->eval(ctx, arr_datum))) {
    LOG_WARN("failed to eval source array arg", K(ret));
  } else if (arr_datum->is_null()) {
    res.set_null();
  } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, subschema_id, arr_datum->get_string(), src_arr))) {
    LOG_WARN("construct array obj failed", K(ret));
  } else if (src_arr->contain_null()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "array_difference with array contains null element");
  } else if (OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_allocator, ctx, res_subschema_id, res_arr, false))) {
    LOG_WARN("construct array obj failed", K(ret));
  } else if (OB_FAIL(calc_difference(src_arr, res_arr))) {
    LOG_WARN("calc array difference failed", K(ret));
  } else {
    ObString res_str;
    if (OB_FAIL(ObArrayExprUtils::set_array_res(res_arr, res_arr->get_raw_binary_len(), expr, ctx, res_str))) {
      LOG_WARN("get array binary string failed", K(ret));
    } else {
      res.set_string(res_str);
    }
  }
  return ret;
}

int ObExprArrayDifference::eval_array_difference_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                                       const ObBitVector &skip, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  const uint16_t res_subschema_id = expr.obj_meta_.get_subschema_id();
  ObIArrayType *src_arr = NULL;
  ObIArrayType *res_arr = NULL;

  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval source array failed", K(ret));
  } else {
    ObDatumVector arr_array = expr.args_[0]->locate_expr_datumvector(ctx);
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      eval_flags.set(j);
      if (arr_array.at(j)->is_null()) {
        res_datum.at(j)->set_null();
      } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, subschema_id, arr_array.at(j)->get_string(), src_arr))) {
        LOG_WARN("construct array obj failed", K(ret));
      } else if (src_arr->contain_null()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "array_difference with array contains null element");
      } else if (OB_NOT_NULL(res_arr) && OB_FALSE_IT(res_arr->clear())) {
      } else if (OB_ISNULL(res_arr) && OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_allocator, ctx, res_subschema_id, res_arr, false))) {
        LOG_WARN("construct array obj failed", K(ret));
      } else if (OB_FAIL(calc_difference(src_arr, res_arr))) {
        LOG_WARN("calc array difference failed", K(ret));
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
    } // end for
  }
  return ret;
}

int ObExprArrayDifference::eval_array_difference_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                                        const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  const uint16_t res_subschema_id = expr.obj_meta_.get_subschema_id();
  ObIArrayType *src_arr = NULL;
  ObIArrayType *res_arr = NULL;

  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval source array failed", K(ret));
  } else {
    ObIVector *arr_vec = expr.args_[0]->get_vector(ctx);
    VectorFormat arr_format = arr_vec->get_format();
    ObIVector *res_vec = expr.get_vector(ctx);
    ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      bool is_null_res = false;
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      }
      eval_flags.set(idx);
      if (arr_vec->is_null(idx)) {
        is_null_res = true;
      } else if (arr_format == VEC_UNIFORM || arr_format == VEC_UNIFORM_CONST) {
        ObString arr_str = arr_vec->get_string(idx);
        if (OB_FAIL(ObNestedVectorFunc::construct_param(tmp_allocator, ctx, subschema_id, arr_str, src_arr))) {
          LOG_WARN("construct array obj failed", K(ret));
        }
      } else if (OB_FAIL(ObNestedVectorFunc::construct_attr_param(
                     tmp_allocator, ctx, *expr.args_[0], subschema_id, idx, src_arr))) {
        LOG_WARN("construct array obj failed", K(ret));
      }

      if (OB_FAIL(ret)) {
      } else if (is_null_res) {
        res_vec->set_null(idx);
      } else if (src_arr->contain_null()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "array_difference with array contains null element");
      } else if (OB_NOT_NULL(res_arr) && OB_FALSE_IT(res_arr->clear())) {
      } else if (OB_ISNULL(res_arr) && OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_allocator,ctx, res_subschema_id, res_arr, false))) {
        LOG_WARN("construct child array obj failed", K(ret));
      } else if (OB_FAIL(calc_difference(src_arr, res_arr))) {
        LOG_WARN("calc array difference failed", K(ret));
      } else if (res_format == VEC_DISCRETE) {
        if (OB_FAIL(ObArrayExprUtils::set_array_res<ObDiscreteFormat>(res_arr, expr, ctx, static_cast<ObDiscreteFormat *>(res_vec), idx))) {
          LOG_WARN("set array res failed", K(ret));
        }
      } else if (res_format == VEC_UNIFORM) {
        if (OB_FAIL(ObArrayExprUtils::set_array_res<ObUniformFormat<false>>(res_arr, expr, ctx, static_cast<ObUniformFormat<false> *>(res_vec), idx))) {
          LOG_WARN("set array res failed", K(ret));
        }
      } else if (OB_FAIL(ObArrayExprUtils::set_array_res<ObVectorBase>(res_arr, expr, ctx, static_cast<ObVectorBase *>(res_vec), idx))) {
        LOG_WARN("set array res failed", K(ret));
      }
    } // end for
  }
  return ret;
}

int ObExprArrayDifference::cg_expr(ObExprCGCtx &expr_cg_ctx,
                         const ObRawExpr &raw_expr,
                         ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_array_difference;
  rt_expr.eval_batch_func_ = eval_array_difference_batch;
  rt_expr.eval_vector_func_ = eval_array_difference_vector;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
