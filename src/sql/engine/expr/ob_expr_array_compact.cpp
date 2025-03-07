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
 * This file contains implementation for array.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_array_compact.h"
#include "lib/udt/ob_collection_type.h"
#include "lib/udt/ob_array_type.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/expr/ob_expr_array.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

namespace oceanbase
{
namespace sql
{
ObExprArrayCompact::ObExprArrayCompact(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUNC_SYS_ARRAY_COMPACT, N_ARRAY_COMPACT, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprArrayCompact::ObExprArrayCompact(ObIAllocator &alloc, ObExprOperatorType type,
                                       const char *name, int32_t param_num, int32_t dimension)
    : ObFuncExprOperator(alloc, type, name, param_num, VALID_FOR_GENERATED_COL, dimension)
{
}

ObExprArrayCompact::~ObExprArrayCompact()
{
}

int ObExprArrayCompact::calc_result_type1(ObExprResType &type,
                                          ObExprResType &type1,
                                          ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (ob_is_null(type1.get_type())) {
    type.set_null();
  } else if (!ob_is_collection_sql_type(type1.get_type())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "ARRAY", ob_obj_type_str(type1.get_type()));
  } else {
    type.set_collection(type1.get_subschema_id());
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObCollectionSQLType]).get_length());
  }
  return ret;
}

int ObExprArrayCompact::eval_array_compact(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  ObIArrayType *src_arr = NULL;
  ObIArrayType *res_arr = NULL;
  ObDatum *arr_datum = NULL;

  if (!ob_is_null(expr.obj_meta_.get_type()) && subschema_id != expr.obj_meta_.get_subschema_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subschema id is not equal", K(ret), K(subschema_id), K(expr.obj_meta_.get_subschema_id()));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, arr_datum))) {
    LOG_WARN("failed to eval source array", K(ret));
  } else if (arr_datum->is_null()) {
    res.set_null();
  } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, subschema_id, arr_datum->get_string(), src_arr))) {
    LOG_WARN("construct array obj failed", K(ret));
  } else if (OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_allocator,ctx, subschema_id, res_arr, false))) {
    LOG_WARN("construct result array obj failed", K(ret));
  } else if (OB_FAIL(eval_compact(tmp_allocator, ctx, src_arr, res_arr))) {
    LOG_WARN("failed to evalue compat", K(ret));
  } else {
    ObString res_str;
    if (OB_FAIL(ObArrayExprUtils::set_array_res(
            res_arr, res_arr->get_raw_binary_len(), expr, ctx, res_str))) {
      LOG_WARN("get array binary string failed", K(ret));
    } else {
      res.set_string(res_str);
    }
  }
  return ret;
}

int ObExprArrayCompact::eval_array_compact_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                                 const ObBitVector &skip, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  ObIArrayType *src_arr = NULL;
  ObIArrayType *res_arr = NULL;
  ObDatum *arr_datum = NULL;

  if (!ob_is_null(expr.obj_meta_.get_type()) && subschema_id != expr.obj_meta_.get_subschema_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subschema id is not equal", K(ret), K(subschema_id), K(expr.obj_meta_.get_subschema_id()));
  } else if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("failed to eval source array", K(ret));
  } else {
    ObDatumVector arr_datum = expr.args_[0]->locate_expr_datumvector(ctx);
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      eval_flags.set(j);
      if (arr_datum.at(j)->is_null()) {
        res_datum.at(j)->set_null();
      } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, subschema_id,
                                                         arr_datum.at(j)->get_string(), src_arr))) {
        LOG_WARN("construct array obj failed", K(ret));
      } else if (OB_NOT_NULL(res_arr) && OB_FALSE_IT(res_arr->clear())) {
      } else if (OB_ISNULL(res_arr) && OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_allocator,ctx, subschema_id, res_arr, false))) {
        LOG_WARN("construct result array obj failed", K(ret));
      } else if (OB_FAIL(eval_compact(tmp_allocator, ctx, src_arr, res_arr))) {
        LOG_WARN("failed to evalue compat", K(ret));
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

int ObExprArrayCompact::eval_array_compact_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                                const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  ObIArrayType *src_arr = NULL;
  ObIArrayType *res_arr = NULL;
  ObDatum *arr_datum = NULL;

  if (!ob_is_null(expr.obj_meta_.get_type()) && subschema_id != expr.obj_meta_.get_subschema_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subschema id is not equal", K(ret), K(subschema_id), K(expr.obj_meta_.get_subschema_id()));
  } else if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval source array failed", K(ret));
  } else {
    ObIVector *arr_vec = expr.args_[0]->get_vector(ctx);
    ObIVector *res_vec = expr.get_vector(ctx);
    ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      bool is_null_res = false;
      int64_t arr_idx = 0;
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      }
      if (arr_vec->is_null(idx)) {
        is_null_res = true;
      } else if (arr_vec->get_format() == VEC_UNIFORM || arr_vec->get_format() == VEC_UNIFORM_CONST) {
        ObString arr_str = arr_vec->get_string(idx);
        if (OB_FAIL(ObNestedVectorFunc::construct_param(tmp_allocator, ctx, subschema_id, arr_str, src_arr))) {
          LOG_WARN("construct array obj failed", K(ret));
        }
      } else if (OB_FAIL(ObNestedVectorFunc::construct_attr_param(
                     tmp_allocator, ctx, *expr.args_[0], subschema_id, idx, src_arr))) {
        LOG_WARN("construct array obj failed", K(ret));
      }
      if (OB_FAIL(ret) || is_null_res) {
      } else if (OB_NOT_NULL(res_arr) && OB_FALSE_IT(res_arr->clear())) {
      } else if (OB_ISNULL(res_arr) && OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_allocator,ctx, subschema_id, res_arr, false))) {
        LOG_WARN("construct child array obj failed", K(ret));
      } else if (OB_FAIL(eval_compact(tmp_allocator, ctx, src_arr, res_arr))) {
        LOG_WARN("failed to evalue compat", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (is_null_res) {
        res_vec->set_null(idx);
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
    } //end for
  }
  return ret;
}

int ObExprArrayCompact::eval_compact(ObIAllocator &tmp_allocator, ObEvalCtx &ctx,
                                     ObIArrayType *src_arr, ObIArrayType *res_arr)
{
  int ret = OB_SUCCESS;
  if (src_arr->is_nested_array()) {
    ObArrayNested *nest_array = static_cast<ObArrayNested *>(src_arr);
    ObIArrayType *child_type = NULL;
    ObIArrayType *last_child = NULL;
    ObIArrayType *this_child = NULL;
    bool is_last_null = false;
    if (OB_UNLIKELY(OB_ISNULL(nest_array))) {
      ret = OB_ERR_ARRAY_TYPE_MISMATCH;
      LOG_WARN("unexpected status: invalid argument", K(ret), K(nest_array));
    } else if (OB_ISNULL(child_type = nest_array->get_child_array())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid child array type", K(ret), K(child_type));
    } else if (OB_FAIL(child_type->clone_empty(tmp_allocator, last_child, false))) {
      OB_LOG(WARN, "clone empty failed", K(ret));
    } else if (OB_FAIL(child_type->clone_empty(tmp_allocator, this_child, false))) {
      OB_LOG(WARN, "clone empty failed", K(ret));
    }
    for (int64_t i = 0; i < src_arr->size() && OB_SUCC(ret); i++) {
      int cmp_ret = 1;
      if (src_arr->is_null(i)) {
        if (i > 0 && is_last_null) {
          // do nothing
        } else if (OB_FAIL(res_arr->push_null())) {
          LOG_WARN("failed to push null to array", K(ret), K(i));
        } else {
          is_last_null = true;
        }
      } else if (OB_FALSE_IT(this_child->clear())) {
      } else if (OB_FAIL(src_arr->at(i, *this_child))) {
        LOG_WARN("failed to get element", K(ret), K(i));
      } else if (i > 0 && !is_last_null && OB_FAIL(last_child->compare(*this_child, cmp_ret))) {
        LOG_WARN("failed to compare", K(ret));
      } else if (cmp_ret == 0) {
        // do nothing
      } else if (OB_FAIL(static_cast<ObArrayNested *>(res_arr)->push_back(*this_child))) {
        LOG_WARN("failed to push back array", K(ret));
      } else {
        ObIArrayType *tmp_child = last_child;
        last_child = this_child;
        this_child = tmp_child;
        is_last_null = false;
      }
    } // end for
  } else {
    const ObCollectionBasicType *src_type = dynamic_cast<const ObCollectionBasicType *>(src_arr->get_array_type()->element_type_);
    ObObj last_elem;
    ObObj this_elem;
    bool is_last_null = false;
    if (OB_UNLIKELY(!src_type)) {
      ret = OB_ERR_ARRAY_TYPE_MISMATCH;
      LOG_WARN("unexpected status: invalid argument", K(ret), KP(src_type));
    } else {
      this_elem.set_meta_type(src_type->basic_meta_.get_meta_type());
    }
    for (int64_t i = 0; i < src_arr->size() && OB_SUCC(ret); i++) {
      if (src_arr->is_null(i)) {
        if (i > 0 && is_last_null) {
          // do nothing
        } else if (OB_FAIL(res_arr->push_null())) {
          LOG_WARN("failed to push null to array", K(ret), K(i));
        } else {
          is_last_null = true;
        }
      } else if (OB_FAIL(src_arr->elem_at(i, this_elem))) {
          LOG_WARN("failed to get element", K(ret), K(i));
      } else if (i > 0 && !is_last_null && this_elem == last_elem) {
        // do nothing
      } else if (OB_FAIL(res_arr->insert_from(*src_arr, i))) {
        LOG_WARN("failed to insert element to result array", K(ret), K(i));
      } else {
        is_last_null = false;
        last_elem = this_elem;
      }
    } // end for
  }

  return ret;
}

int ObExprArrayCompact::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_array_compact;
  rt_expr.eval_batch_func_ = eval_array_compact_batch;
  rt_expr.eval_vector_func_ = eval_array_compact_vector;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
