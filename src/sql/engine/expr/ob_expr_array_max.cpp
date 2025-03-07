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
 * This file contains implementation for array_max expression.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_array_max.h"
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
ObExprArrayExtreme::ObExprArrayExtreme(common::ObIAllocator &alloc, ObExprOperatorType type, const char *name)
    : ObFuncExprOperator(alloc, type, name, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprArrayExtreme::~ObExprArrayExtreme()
{
}

int ObExprArrayExtreme::calc_result_type1(ObExprResType &type,
                                          ObExprResType &type1,
                                          ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  ObExecContext *exec_ctx = NULL;
  ObSubSchemaValue arr_meta;
  const ObSqlCollectionInfo *coll_info = NULL;
  ObCollectionArrayType *arr_type = NULL;
  ObDataType src_elem_type;

  if (OB_ISNULL(session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSQLSessionInfo is null", K(ret));
  } else if (OB_ISNULL(exec_ctx = session->get_cur_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObExecContext is null", K(ret));
  } else if (ob_is_null(type1.get_type())) {
    type.set_utinyint(); // default type
  } else if (!ob_is_collection_sql_type(type1.get_type())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "ARRAY", ob_obj_type_str(type1.get_type()));
  } else if (OB_FAIL(exec_ctx->get_sqludt_meta_by_subschema_id(type1.get_subschema_id(), arr_meta))) {
    LOG_WARN("failed to get elem meta.", K(ret), K(type1.get_subschema_id()));
  } else if (OB_ISNULL(coll_info = reinterpret_cast<const ObSqlCollectionInfo *>(arr_meta.value_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSqlCollectionInfo is null", K(ret));
  } else if (OB_ISNULL(arr_type = static_cast<ObCollectionArrayType *>(coll_info->collection_meta_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObCollectionArrayType is null", K(ret));
  } else if (arr_type->element_type_->type_id_ == ObNestedType::OB_BASIC_TYPE) {
    ObCollectionBasicType *elem_type = static_cast<ObCollectionBasicType *>(arr_type->element_type_);
    type.set_meta(elem_type->basic_meta_.get_meta_type());
    type.set_accuracy(elem_type->basic_meta_.get_accuracy());
  } else if (arr_type->element_type_->type_id_ == ObNestedType::OB_ARRAY_TYPE
             || arr_type->element_type_->type_id_ == ObNestedType::OB_VECTOR_TYPE) {
    // TODO: support array of array
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "array_max with nested array");
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ObNestedType type", K(ret), K(arr_type->element_type_->type_id_));
  }
  return ret;
}

int ObExprArrayExtreme::calc_extreme(ObIArrayType* src_arr, ObObj &res_obj, bool is_max)
{
  int ret = OB_SUCCESS;
  ObCollectionBasicType *elem_type = NULL;
  res_obj.set_null();

  if (OB_ISNULL(elem_type = static_cast<ObCollectionBasicType *>(src_arr->get_array_type()->element_type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("source array collection element type is null", K(ret));
  } else if (src_arr->is_nested_array()) {
    // TODO: support array of array
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "array_max with nested array");
  } else {
    for (uint32_t i = 0; i < src_arr->size() && OB_SUCC(ret); ++i) {
      ObObj elem_obj;
      int cmp = 0;
      if (src_arr->is_null(i)) {
        // do nothing
      } else if (OB_FAIL(src_arr->elem_at(i, elem_obj))) {
        LOG_WARN("failed to get element", K(ret), K(i));
      } else if (res_obj.is_null()) {
        res_obj = elem_obj;
      } else if (elem_obj.is_varchar()) {
        if (is_max && elem_obj.get_string() > res_obj.get_string()) {
          res_obj = elem_obj;
        } else if (!is_max && elem_obj.get_string() < res_obj.get_string()) {
          res_obj = elem_obj;
        }
      } else {
        if (is_max && elem_obj > res_obj) {
          res_obj = elem_obj;
        } else if (!is_max && elem_obj < res_obj) {
          res_obj = elem_obj;
        }
      }
    } // end for
  }
  return ret;
}

int ObExprArrayExtreme::eval_array_extreme(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res, bool is_max)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  ObDatum *arr_datum = NULL;
  ObIArrayType *src_arr = NULL;
  ObObj res_obj;

  if (OB_FAIL(expr.args_[0]->eval(ctx, arr_datum))) {
    LOG_WARN("failed to eval source array arg", K(ret));
  } else if (arr_datum->is_null()) {
    res.set_null();
  } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, subschema_id, arr_datum->get_string(), src_arr))) {
    LOG_WARN("construct array obj failed", K(ret));
  } else if (OB_FAIL(calc_extreme(src_arr, res_obj, is_max))) {
    LOG_WARN("calc array extreme value failed", K(ret));
  } else {
    res.from_obj(res_obj);
  }
  return ret;
}

int ObExprArrayExtreme::eval_array_extreme_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                                 const ObBitVector &skip, const int64_t batch_size,
                                                 bool is_max)
{
  int ret = OB_SUCCESS;
  ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  ObIArrayType *src_arr = NULL;
  ObObj res_obj;

  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval source array failed", K(ret));
  } else {
    ObDatumVector arr_array = expr.args_[0]->locate_expr_datumvector(ctx);
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
      int64_t idx = 0;
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      eval_flags.set(j);
      if (arr_array.at(j)->is_null()) {
        res_datum.at(j)->set_null();
      } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, subschema_id, arr_array.at(j)->get_string(), src_arr))) {
        LOG_WARN("construct array obj failed", K(ret));
      } else if (OB_FAIL(calc_extreme(src_arr, res_obj, is_max))) {
        LOG_WARN("calc array extreme value failed", K(ret));
      } else {
        res_datum.at(j)->from_obj(res_obj);
      }
    } // end for
  }
  return ret;
}

int ObExprArrayExtreme::eval_array_extreme_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                            const ObBitVector &skip, const EvalBound &bound,
                                            bool is_max)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  ObIArrayType *src_arr = NULL;

  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval source array failed", K(ret));
  } else {
    ObIVector *arr_vec = expr.args_[0]->get_vector(ctx);
    VectorFormat arr_format = arr_vec->get_format();
    ObIVector *res_vec = expr.get_vector(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    ObObj res_obj;
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      bool is_null_res = false;
      int64_t arr_idx = 0;
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
      } else if (OB_FAIL(calc_extreme(src_arr, res_obj, is_max))) {
        LOG_WARN("calc array extreme value failed", K(ret));
      } else if (OB_FAIL(ObArrayExprUtils::set_obj_to_vector(res_vec, idx, res_obj))) {
        LOG_WARN("failed to set object value to result vector", K(ret), K(idx), K(res_obj));
      }
    } // end for
  }
  return ret;
}

ObExprArrayMax::ObExprArrayMax(common::ObIAllocator &alloc)
    : ObExprArrayExtreme(alloc, T_FUNC_SYS_ARRAY_MAX, N_ARRAY_MAX)
{
}

ObExprArrayMax::~ObExprArrayMax()
{
}

int ObExprArrayMax::eval_array_max(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  return ObExprArrayExtreme::eval_array_extreme(expr, ctx, res, true);
}

int ObExprArrayMax::eval_array_max_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                         const ObBitVector &skip, const int64_t batch_size)
{
  return ObExprArrayExtreme::eval_array_extreme_batch(expr, ctx, skip, batch_size, true);
}

int ObExprArrayMax::eval_array_max_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                          const ObBitVector &skip, const EvalBound &bound)

{
  return ObExprArrayExtreme::eval_array_extreme_vector(expr, ctx, skip, bound, true);
}

int ObExprArrayMax::cg_expr(ObExprCGCtx &expr_cg_ctx,
                            const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_array_max;
  rt_expr.eval_batch_func_ = eval_array_max_batch;
  rt_expr.eval_vector_func_ = eval_array_max_vector;
  return OB_SUCCESS;
}

ObExprArrayMin::ObExprArrayMin(common::ObIAllocator &alloc)
    : ObExprArrayExtreme(alloc, T_FUNC_SYS_ARRAY_MIN, N_ARRAY_MIN)
{
}

ObExprArrayMin::~ObExprArrayMin()
{
}

int ObExprArrayMin::eval_array_min(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  return ObExprArrayExtreme::eval_array_extreme(expr, ctx, res, false);
}

int ObExprArrayMin::eval_array_min_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                         const ObBitVector &skip, const int64_t batch_size)
{
  return ObExprArrayExtreme::eval_array_extreme_batch(expr, ctx, skip, batch_size, false);
}

int ObExprArrayMin::eval_array_min_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                          const ObBitVector &skip, const EvalBound &bound)

{
  return ObExprArrayExtreme::eval_array_extreme_vector(expr, ctx, skip, bound, false);
}

int ObExprArrayMin::cg_expr(ObExprCGCtx &expr_cg_ctx,
                            const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_array_min;
  rt_expr.eval_batch_func_ = eval_array_min_batch;
  rt_expr.eval_vector_func_ = eval_array_min_vector;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
