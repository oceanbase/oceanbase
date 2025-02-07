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
 * This file contains implementation for element_at expression.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_element_at.h"
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
ObExprElementAt::ObExprElementAt(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUNC_SYS_ELEMENT_AT, N_ELEMENT_AT, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprElementAt::~ObExprElementAt()
{
}

int ObExprElementAt::calc_result_type2(ObExprResType &type,
                                       ObExprResType &type1,
                                       ObExprResType &type2,
                                       common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObDataType res_data_type;
  ObSQLSessionInfo *session = NULL;
  ObExecContext *exec_ctx = NULL;
  ObSubSchemaValue arr_meta;

  if (OB_ISNULL(session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSQLSessionInfo is null", K(ret));
  } else if (OB_ISNULL(exec_ctx = session->get_cur_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObExecContext is null", K(ret));
  } else if (!ob_is_collection_sql_type(type1.get_type())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "ARRAY", ob_obj_type_str(type1.get_type()));
  } else if (!ob_is_numeric_type(type2.get_type())
         && !ob_is_varchar_or_char(type2.get_type(), type2.get_collation_type())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "INTEGER", ob_obj_type_str(type2.get_type()));
  } else if (OB_FAIL(exec_ctx->get_sqludt_meta_by_subschema_id(type1.get_subschema_id(), arr_meta))) {
    LOG_WARN("failed to get elem meta.", K(ret), K(type1.get_subschema_id()));
  } else {
    type2.set_calc_type(ObIntType);
    const ObSqlCollectionInfo *coll_info = reinterpret_cast<const ObSqlCollectionInfo *>(arr_meta.value_);
    ObCollectionArrayType *arr_type = static_cast<ObCollectionArrayType *>(coll_info->collection_meta_);
    if (arr_type->element_type_->type_id_ == ObNestedType::OB_BASIC_TYPE) {
      ObCollectionBasicType *elem_type = static_cast<ObCollectionBasicType *>(arr_type->element_type_);
      type.set_meta(elem_type->basic_meta_.get_meta_type());
      type.set_accuracy(elem_type->basic_meta_.get_accuracy());
    } else if (arr_type->element_type_->type_id_ == ObNestedType::OB_ARRAY_TYPE
               || arr_type->element_type_->type_id_ == ObNestedType::OB_VECTOR_TYPE) {
      ObString child_def;
      uint16_t child_subschema_id = 0;
      if (OB_FAIL(coll_info->get_child_def_string(child_def))) {
        LOG_WARN("failed to get child define", K(ret), K(*coll_info));
      } else if (OB_FAIL(session->get_cur_exec_ctx()->get_subschema_id_by_type_string(child_def, child_subschema_id))) {
        LOG_WARN("failed to get child subschema id", K(ret), K(*coll_info), K(child_def));
      } else {
        type.set_collection(child_subschema_id);
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected ObNestedType type", K(ret), K(arr_type->element_type_->type_id_));
    }
  }
  return ret;
}

int ObExprElementAt::eval_element_at(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  ObIArrayType *src_arr = NULL;
  ObIArrayType* child_arr = NULL;
  int64_t idx = 0;
  ObDatum *arr_datum = NULL;
  ObDatum *idx_datum = NULL;

  if (OB_FAIL(expr.args_[0]->eval(ctx, arr_datum))) {
    LOG_WARN("failed to eval source array arg", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, idx_datum))) {
    LOG_WARN("failed to eval index arg", K(ret));
  } else if (arr_datum->is_null() || idx_datum->is_null()) {
    res.set_null();
  } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, subschema_id, arr_datum->get_string(), src_arr))) {
    LOG_WARN("construct array obj failed", K(ret));
  } else if (OB_FALSE_IT(idx = idx_datum->get_int() - 1)) {
  } else if (idx < 0 || idx >= src_arr->size() || idx > UINT32_MAX) {
    res.set_null();
  } else if (src_arr->is_null(idx)) {
    res.set_null();
  } else if (src_arr->is_nested_array()) {
    uint16_t child_subschema_id = expr.obj_meta_.get_subschema_id();
    ObString child_arr_str;
    if (OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_allocator,ctx, child_subschema_id, child_arr, false))) {
      LOG_WARN("construct child array obj failed", K(ret));
    } else if (OB_FAIL(src_arr->at(static_cast<uint32_t>(idx), *child_arr))) {
      LOG_WARN("failed to get child array", K(ret), K(idx));
    } else if (OB_FAIL(ObArrayExprUtils::set_array_res(child_arr, child_arr->get_raw_binary_len(), expr, ctx, child_arr_str))) {
      LOG_WARN("get array binary string failed", K(ret));
    } else {
      res.set_string(child_arr_str);
    }
  } else {
    ObObj elem_obj;
    if (OB_FAIL(src_arr->elem_at(idx, elem_obj))) {
      LOG_WARN("failed to get element", K(ret), K(idx));
    } else {
      res.from_obj(elem_obj);
    }
  }
  return ret;
}

int ObExprElementAt::eval_element_at_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                           const ObBitVector &skip, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  ObIArrayType *src_arr = NULL;
  ObIArrayType* child_arr = NULL;

  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval source array failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval index failed", K(ret));
  } else {
    ObDatumVector arr_array = expr.args_[0]->locate_expr_datumvector(ctx);
    ObDatumVector idx_array = expr.args_[1]->locate_expr_datumvector(ctx);
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
      } else if (OB_FALSE_IT(idx = idx_array.at(j)->get_int() - 1)) {
      } else if (idx < 0 || idx >= src_arr->size() || idx > UINT32_MAX) {
        res_datum.at(j)->set_null();
      } else if (src_arr->is_null(idx)) {
        res_datum.at(j)->set_null();
      } else if (src_arr->is_nested_array()) {
        uint16_t child_subschema_id = expr.obj_meta_.get_subschema_id();
        ObString child_arr_str;
        if (OB_NOT_NULL(child_arr) && OB_FALSE_IT(child_arr->clear())) {
        } else if (OB_ISNULL(child_arr) && OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_allocator,ctx, child_subschema_id, child_arr, false))) {
          LOG_WARN("construct child array obj failed", K(ret));
        } else if (OB_FAIL(src_arr->at(static_cast<uint32_t>(idx), *child_arr))) {
          LOG_WARN("failed to get child array", K(ret), K(idx));
        } else {
          int32_t res_size = child_arr->get_raw_binary_len();
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
          } else if (OB_FAIL(child_arr->get_raw_binary(res_buf, res_buf_len))) {
            LOG_WARN("get array raw binary failed", K(ret), K(res_buf_len), K(res_size));
          } else if (OB_FAIL(output_result.lseek(res_size, 0))) {
            LOG_WARN("failed to lseek res.", K(ret), K(output_result), K(res_size));
          } else {
            output_result.set_result();
          }
        }
      } else {
        ObObj elem_obj;
        if (OB_FAIL(src_arr->elem_at(static_cast<uint32_t>(idx), elem_obj))) {
          LOG_WARN("failed to get element", K(ret), K(idx));
        } else {
          res_datum.at(j)->from_obj(elem_obj);
        }
      }
    } // end for
  }
  return ret;
}

int ObExprElementAt::eval_element_at_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                            const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  ObIArrayType *src_arr = NULL;
  ObIArrayType* child_arr = NULL;

  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval source array failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval index failed", K(ret));
  } else {
    ObIVector *arr_vec = expr.args_[0]->get_vector(ctx);
    VectorFormat arr_format = arr_vec->get_format();
    ObIVector *idx_vec = expr.args_[1]->get_vector(ctx);
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
      } else if (arr_format == VEC_UNIFORM || arr_format == VEC_UNIFORM_CONST) {
        ObString arr_str = arr_vec->get_string(idx);
        if (OB_FAIL(ObNestedVectorFunc::construct_param(tmp_allocator, ctx, subschema_id, arr_str, src_arr))) {
          LOG_WARN("construct array obj failed", K(ret));
        }
      } else if (OB_FAIL(ObNestedVectorFunc::construct_attr_param(
                     tmp_allocator, ctx, *expr.args_[0], subschema_id, idx, src_arr))) {
        LOG_WARN("construct array obj failed", K(ret));
      }
      if (OB_FAIL(ret) || is_null_res) {
      } else if (OB_FALSE_IT(arr_idx = idx_vec->get_int(idx) - 1)) {
      } else if (arr_idx < 0 || arr_idx >= src_arr->size() || arr_idx > UINT32_MAX) {
        is_null_res = true;
      } else if (src_arr->is_null(arr_idx)) {
        is_null_res = true;
      }
      if (OB_FAIL(ret)) {
      } else if (is_null_res) {
        res_vec->set_null(idx);
      } else if (src_arr->is_nested_array()) {
        uint16_t child_subschema_id = expr.obj_meta_.get_subschema_id();
        ObString child_arr_str;
        if (OB_NOT_NULL(child_arr) && OB_FALSE_IT(child_arr->clear())) {
        } else if (OB_ISNULL(child_arr) && OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_allocator,ctx, child_subschema_id, child_arr, false))) {
          LOG_WARN("construct child array obj failed", K(ret));
        } else if (OB_FALSE_IT(child_arr->clear())) {
        } else if (OB_FAIL(src_arr->at(static_cast<uint32_t>(arr_idx), *child_arr))) {
          LOG_WARN("failed to get child array", K(ret), K(arr_idx));
        } else if (res_format == VEC_DISCRETE) {
          if (OB_FAIL(ObArrayExprUtils::set_array_res<ObDiscreteFormat>(child_arr, expr, ctx, static_cast<ObDiscreteFormat *>(res_vec), idx))) {
            LOG_WARN("set array res failed", K(ret));
          }
        } else if (res_format == VEC_UNIFORM) {
          if (OB_FAIL(ObArrayExprUtils::set_array_res<ObUniformFormat<false>>(child_arr, expr, ctx, static_cast<ObUniformFormat<false> *>(res_vec), idx))) {
            LOG_WARN("set array res failed", K(ret));
          }
        } else if (OB_FAIL(ObArrayExprUtils::set_array_res<ObVectorBase>(child_arr, expr, ctx, static_cast<ObVectorBase *>(res_vec), idx))) {
          LOG_WARN("set array res failed", K(ret));
        }
      } else {
        ObObj elem_obj;
        if (OB_FAIL(src_arr->elem_at(static_cast<uint32_t>(arr_idx), elem_obj))) {
          LOG_WARN("failed to get element", K(ret), K(arr_idx));
        } else if (OB_FAIL(ObArrayExprUtils::set_obj_to_vector(res_vec, idx, elem_obj))) {
          LOG_WARN("failed to set object value to result vector", K(ret), K(idx), K(elem_obj));
        }
      }
      if (OB_SUCC(ret)) {
        eval_flags.set(idx);
      }
    } // end for
  }
  return ret;
}

int ObExprElementAt::cg_expr(ObExprCGCtx &expr_cg_ctx,
                         const ObRawExpr &raw_expr,
                         ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_element_at;
  rt_expr.eval_batch_func_ = eval_element_at_batch;
  rt_expr.eval_vector_func_ = eval_element_at_vector;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
