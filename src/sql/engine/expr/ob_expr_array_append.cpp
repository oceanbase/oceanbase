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
#include "sql/engine/expr/ob_expr_array_append.h"
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
ObExprArrayAppendCommon::ObExprArrayAppendCommon(common::ObIAllocator &alloc, ObExprOperatorType type, const char *name)
    : ObFuncExprOperator(alloc, type, name, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprArrayAppendCommon::ObExprArrayAppendCommon(ObIAllocator &alloc, ObExprOperatorType type,
                                                     const char *name, int32_t param_num, int32_t dimension)
    : ObFuncExprOperator(alloc, type, name, param_num, VALID_FOR_GENERATED_COL, dimension)
{
}

ObExprArrayAppendCommon::~ObExprArrayAppendCommon()
{
}

int ObExprArrayAppendCommon::calc_result_type2(ObExprResType &type,
                                               ObExprResType &type1,
                                               ObExprResType &type2,
                                               common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
  uint16_t subschema_id = type1.get_subschema_id();
  bool is_null_res = false;

  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec ctx is null", K(ret));
  } else if (type1.is_null()) {
    is_null_res = true;
  } else if (!ob_is_collection_sql_type(type1.get_type())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "ARRAY", ob_obj_type_str(type1.get_type()));
  } else if (type2.is_null()) {
    // do nothing
  } else if (OB_FAIL(ObArrayExprUtils::deduce_array_type(exec_ctx, type1, type2, subschema_id))) {
    LOG_WARN("failed to get result array type subschema id", K(ret));
  }
  // set result type
  if (OB_FAIL(ret)) {
  } else if (is_null_res) {
    type.set_null();
  } else {
    type.set_collection(subschema_id);
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObCollectionSQLType]).get_length());
  }
  return ret;
}

int ObExprArrayAppendCommon::eval_append(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res, bool is_prepend)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  const uint16_t val_subschema_id = expr.args_[1]->obj_meta_.get_subschema_id();
  ObIArrayType *src_arr = NULL;
  ObIArrayType *val_arr = NULL;
  ObIArrayType *res_arr = NULL;
  ObDatum *arr_datum = NULL;
  ObDatum *val_datum = NULL;
  bool is_null_res = false;
  if (!ob_is_null(expr.obj_meta_.get_type()) && subschema_id != expr.obj_meta_.get_subschema_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subschema id not match", K(ret), K(subschema_id), K(expr.obj_meta_.get_subschema_id()));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, arr_datum))) {
    LOG_WARN("failed to eval args", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, val_datum))) {
    LOG_WARN("failed to eval args", K(ret));
  } else if (arr_datum->is_null()) {
    is_null_res = true;
  } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, subschema_id, arr_datum->get_string(), src_arr))) {
    LOG_WARN("construct array obj failed", K(ret));
  } else if (OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_allocator, ctx, subschema_id, res_arr, false))) {
    LOG_WARN("construct result array obj failed", K(ret));
  } else if (!is_prepend) {
    if (OB_FAIL(res_arr->insert_from(*src_arr))) {
      LOG_WARN("failed to insert elements from array", K(ret));
    } else if (OB_FAIL(append_elem(tmp_allocator, ctx, val_datum, val_subschema_id, val_arr, res_arr))) {
      LOG_WARN("failed to append element", K(ret));
    }
  } else {
    if (OB_FAIL(append_elem(tmp_allocator, ctx, val_datum, val_subschema_id, val_arr, res_arr))) {
      LOG_WARN("failed to append element", K(ret));
    } else if (OB_FAIL(res_arr->insert_from(*src_arr))) {
      LOG_WARN("failed to insert elements from array", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (is_null_res) {
    res.set_null();
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

int ObExprArrayAppendCommon::eval_append_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                               const ObBitVector &skip, const int64_t batch_size,
                                               bool is_prepend)
{
  int ret = OB_SUCCESS;
  ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  const uint16_t val_subschema_id = expr.args_[1]->obj_meta_.get_subschema_id();
  ObIArrayType *src_arr = NULL;
  ObIArrayType *val_arr = NULL;
  ObIArrayType* res_arr = NULL;

  if (!ob_is_null(expr.obj_meta_.get_type()) && subschema_id != expr.obj_meta_.get_subschema_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subschema id not match", K(ret), K(subschema_id), K(expr.obj_meta_.get_subschema_id()));
  } else if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval source array failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval index failed", K(ret));
  } else {
    ObDatumVector arr_datum = expr.args_[0]->locate_expr_datumvector(ctx);
    ObDatumVector val_datum = expr.args_[1]->locate_expr_datumvector(ctx);
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
      bool is_null_res = false;
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      eval_flags.set(j);
      if (arr_datum.at(j)->is_null()) {
        is_null_res = true;
      } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, subschema_id,
                                                         arr_datum.at(j)->get_string(), src_arr))) {
        LOG_WARN("construct array obj failed", K(ret));
      } else if (OB_NOT_NULL(res_arr)) {
        res_arr->clear();
      } else if (OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_allocator,ctx, subschema_id, res_arr, false))) {
        LOG_WARN("construct result array obj failed", K(ret));
      }
      if (OB_FAIL(ret) || is_null_res) {
      } else if (!is_prepend) {
        if (OB_FAIL(res_arr->insert_from(*src_arr))) {
          LOG_WARN("failed to insert elements from array", K(ret));
        } else if (OB_FAIL(append_elem(tmp_allocator, ctx, val_datum.at(j), val_subschema_id, val_arr, res_arr))) {
          LOG_WARN("failed to append element", K(ret));
        }
      } else {
        if (OB_FAIL(append_elem(tmp_allocator, ctx, val_datum.at(j), val_subschema_id, val_arr, res_arr))) {
          LOG_WARN("failed to append element", K(ret));
        } else if (OB_FAIL(res_arr->insert_from(*src_arr))) {
          LOG_WARN("failed to insert elements from array", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (is_null_res) {
        res_datum.at(j)->set_null();
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

int ObExprArrayAppendCommon::eval_append_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                                const ObBitVector &skip, const EvalBound &bound,
                                                bool is_prepend)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  const uint16_t val_subschema_id = expr.args_[1]->obj_meta_.get_subschema_id();
  ObIArrayType *src_arr = NULL;
  ObIArrayType *val_arr = NULL;
  ObIArrayType* res_arr = NULL;

  if (!ob_is_null(expr.obj_meta_.get_type()) && subschema_id != expr.obj_meta_.get_subschema_id()) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval source array failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval index failed", K(ret));
  } else {
    ObIVector *arr_vec = expr.args_[0]->get_vector(ctx);
    ObIVector *val_vec = expr.args_[1]->get_vector(ctx);
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
      } else if (OB_NOT_NULL(res_arr)) {
        res_arr->clear();
      } else if (OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_allocator,ctx, subschema_id, res_arr, false))) {
        LOG_WARN("construct child array obj failed", K(ret));
      }
      if (OB_FAIL(ret) || is_null_res) {
      } else if (!is_prepend) {
        if (OB_FAIL(res_arr->insert_from(*src_arr))) {
          LOG_WARN("failed to insert elements from array", K(ret));
        } else if (OB_FAIL(append_elem_vector(tmp_allocator, ctx, val_vec, idx,
                                              val_subschema_id, *expr.args_[1], val_arr, res_arr))){
          LOG_WARN("append array element failed", K(ret));
        }
      } else {
        if (OB_FAIL(append_elem_vector(tmp_allocator, ctx, val_vec, idx,
                                              val_subschema_id, *expr.args_[1], val_arr, res_arr))){
          LOG_WARN("failed to append element", K(ret));
        } else if (OB_FAIL(res_arr->insert_from(*src_arr))) {
          LOG_WARN("failed to insert elements from array", K(ret));
        }
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

int ObExprArrayAppendCommon::append_elem(ObIAllocator &tmp_allocator, ObEvalCtx &ctx,
                                         ObDatum *val_datum, uint16_t val_subschema_id,
                                         ObIArrayType *val_arr, ObIArrayType *res_arr)
{
  int ret = OB_SUCCESS;
  if (res_arr->is_nested_array()) {
    if (val_datum->is_null()) {
      if (OB_FAIL(res_arr->push_null())) {
        LOG_WARN("failed to push back null value", K(ret));
      }
    } else {
      ObArrayNested *nested_arr = dynamic_cast<ObArrayNested *>(res_arr);
      if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, val_subschema_id, val_datum->get_string(), val_arr))) {
        LOG_WARN("construct array obj failed", K(ret));
      } else if (OB_FAIL(nested_arr->push_back(*val_arr))) {
        LOG_WARN("failed to push back value", K(ret));
      }
    }
  } else {
    ObCollectionBasicType *elem_type = static_cast<ObCollectionBasicType *>(res_arr->get_array_type()->element_type_);
    if (OB_FAIL(ObArrayUtil::append(*res_arr, elem_type->basic_meta_.get_obj_type(), val_datum))) {
      LOG_WARN("failed to append array value", K(ret));
    }
  }
  return ret;
}

int ObExprArrayAppendCommon::append_elem_vector(ObIAllocator &tmp_allocator, ObEvalCtx &ctx,
                                                ObIVector *val_vec, int64_t idx,
                                                uint16_t val_subschema_id, ObExpr &param_expr,
                                                ObIArrayType *val_arr, ObIArrayType *res_arr)
{
  int ret = OB_SUCCESS;
  if (res_arr->is_nested_array()) {
    if (val_vec->is_null(idx)) {
      if (OB_FAIL(res_arr->push_null())) {
        LOG_WARN("failed to push back null value", K(ret));
      }
    } else {
      ObArrayNested *nested_arr = dynamic_cast<ObArrayNested *>(res_arr);
      if (val_vec->get_format() == VEC_UNIFORM || val_vec->get_format() == VEC_UNIFORM_CONST) {
        ObString val_arr_str = val_vec->get_string(idx);
        if (OB_FAIL(ObNestedVectorFunc::construct_param(tmp_allocator, ctx, val_subschema_id, val_arr_str, val_arr))) {
          LOG_WARN("construct array obj failed", K(ret));
        }
      } else if (OB_FAIL(ObNestedVectorFunc::construct_attr_param(tmp_allocator, ctx, param_expr, val_subschema_id, idx, val_arr))) {
        LOG_WARN("construct array obj failed", K(ret));
      }
      if (OB_SUCC(ret) && OB_FAIL(nested_arr->push_back(*val_arr))) {
        LOG_WARN("failed to push back value", K(ret));
      }
    }
  } else {
    ObCollectionBasicType *elem_type = static_cast<ObCollectionBasicType *>(res_arr->get_array_type()->element_type_);
    ObDatum val_datum;
    if (val_vec->is_null(idx)) {
      val_datum.set_null();
    } else {
      const char *payload = NULL;
      ObLength payload_len = 0;
      val_vec->get_payload(idx, payload, payload_len);
      val_datum.ptr_ = payload;
      val_datum.pack_ = payload_len;
    }
    if (OB_FAIL(ObArrayUtil::append(*res_arr, elem_type->basic_meta_.get_obj_type(), &val_datum))) {
      LOG_WARN("failed to append array value", K(ret));
    }
  }
  return ret;
}

ObExprArrayAppend::ObExprArrayAppend(common::ObIAllocator &alloc)
    : ObExprArrayAppendCommon(alloc, T_FUNC_SYS_ARRAY_APPEND, N_ARRAY_APPEND)
{
}

ObExprArrayAppend::ObExprArrayAppend(common::ObIAllocator &alloc, ObExprOperatorType type,
                                         const char *name, int32_t param_num, int32_t dimension)
    : ObExprArrayAppendCommon(alloc, type, name, param_num, dimension)
{
}

ObExprArrayAppend::~ObExprArrayAppend()
{
}

int ObExprArrayAppend::eval_array_append(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  return eval_append(expr, ctx, res, false);
}

int ObExprArrayAppend::eval_array_append_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                               const ObBitVector &skip, const int64_t batch_size)
{
  return eval_append_batch(expr, ctx, skip, batch_size, false);
}

int ObExprArrayAppend::eval_array_append_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                                const ObBitVector &skip, const EvalBound &bound)
{
  return eval_append_vector(expr, ctx, skip, bound, false);
}

int ObExprArrayAppend::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_array_append;
  rt_expr.eval_batch_func_ = eval_array_append_batch;
  rt_expr.eval_vector_func_ = eval_array_append_vector;
  return OB_SUCCESS;
}

ObExprArrayPrepend::ObExprArrayPrepend(common::ObIAllocator &alloc)
    : ObExprArrayAppendCommon(alloc, T_FUNC_SYS_ARRAY_PREPEND, N_ARRAY_PREPEND)
{
}

ObExprArrayPrepend::ObExprArrayPrepend(common::ObIAllocator &alloc, ObExprOperatorType type,
                                       const char *name, int32_t param_num, int32_t dimension)
    : ObExprArrayAppendCommon(alloc, type, name, param_num, dimension)
{
}

ObExprArrayPrepend::~ObExprArrayPrepend()
{
}

int ObExprArrayPrepend::eval_array_prepend(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  return eval_append(expr, ctx, res, true);
}

int ObExprArrayPrepend::eval_array_prepend_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                                 const ObBitVector &skip, const int64_t batch_size)
{
  return eval_append_batch(expr, ctx, skip, batch_size, true);
}

int ObExprArrayPrepend::eval_array_prepend_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                                  const ObBitVector &skip, const EvalBound &bound)
{
  return eval_append_vector(expr, ctx, skip, bound, true);
}

int ObExprArrayPrepend::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_array_prepend;
  rt_expr.eval_batch_func_ = eval_array_prepend_batch;
  rt_expr.eval_vector_func_ = eval_array_prepend_vector;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
