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
#include "sql/engine/expr/ob_expr_array_union.h"
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
ObExprArraySetOperation::ObExprArraySetOperation(common::ObIAllocator &alloc, 
                            ObExprOperatorType type, 
                            const char *name, 
                            int32_t param_num, 
                            int32_t dimension)
                            : ObFuncExprOperator(alloc, type, name, param_num, VALID_FOR_GENERATED_COL, dimension)
{
}

ObExprArraySetOperation::~ObExprArraySetOperation()
{
}

int ObExprArraySetOperation::calc_result_type2(ObExprResType &type, 
                                ObExprResType &type1, 
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
  ObExprResType deduce_type = type1;
  uint32_t depth = 0;
  ObDataType res_elem_type;
  bool is_vec = false;
  bool is_null_res = true;
  ObCollectionTypeBase *coll_type1 = NULL;
  ObCollectionTypeBase *coll_type2 = NULL;

  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec ctx is null", K(ret));
  } else if ((!ob_is_collection_sql_type(type1.get_type()) && !type1.is_null())
             || (!ob_is_collection_sql_type(type2.get_type()) && !type2.is_null())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid data type", K(ret), K(type2.get_type()), K(type2.get_type()));
  } else if (type1.is_null() || type2.is_null()) {
    type.set_null();
  } else if (OB_FAIL(ObArrayExprUtils::get_coll_type_by_subschema_id(exec_ctx, type1.get_subschema_id(), coll_type1))) {
    LOG_WARN("failed to get array type by subschema id", K(ret), K(type1.get_subschema_id()));
  } else if (coll_type1->type_id_ != ObNestedType::OB_ARRAY_TYPE && coll_type1->type_id_ != ObNestedType::OB_VECTOR_TYPE) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid collection type", K(ret), K(coll_type1->type_id_));
  } else if (OB_FAIL(ObArrayExprUtils::get_coll_type_by_subschema_id(exec_ctx, type2.get_subschema_id(), coll_type2))) {
    LOG_WARN("failed to get array type by subschema id", K(ret), K(type2.get_subschema_id()));
  } else if (coll_type2->type_id_ != ObNestedType::OB_ARRAY_TYPE && coll_type2->type_id_ != ObNestedType::OB_VECTOR_TYPE) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid collection type", K(ret), K(coll_type2->type_id_));
  } else if (OB_FAIL(ObExprResultTypeUtil::get_array_calc_type(exec_ctx, deduce_type, type2, deduce_type))) {
    LOG_WARN("deduce calc type failed", K(ret));
  } else if (OB_FAIL(ObArrayExprUtils::get_array_element_type(exec_ctx, 
                                          deduce_type.get_subschema_id(), 
                                          res_elem_type, 
                                          depth, 
                                          is_vec))) {
      LOG_WARN("failed to get result element type", K(ret));
  } else if (is_vec) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("vector type is not supported", K(ret));
  } else {
    type.set_collection(deduce_type.get_subschema_id());
    type1.set_calc_meta(deduce_type);
    type2.set_calc_meta(deduce_type);
  }
  return ret;
}

int ObExprArraySetOperation::calc_result_typeN(ObExprResType& type,
                                ObExprResType* types_stack,
                                int64_t param_num,
                                ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
  bool is_null_res = false;
  ObExprResType deduce_type = types_stack[0];

  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec ctx is null", K(ret));
  }
  for (int64_t i = 0; i < param_num && OB_SUCC(ret); i++) {
    ObCollectionTypeBase *coll_type = NULL;
    if (types_stack[i].is_null()) {
      is_null_res = true;
    } else if (!ob_is_collection_sql_type(types_stack[i].get_type())) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("invalid data type", K(ret), K(types_stack[i].get_type()));
    } else if (OB_FAIL(ObArrayExprUtils::get_coll_type_by_subschema_id(exec_ctx, types_stack[i].get_subschema_id(), coll_type))) {
      LOG_WARN("failed to get array type by subschema id", K(ret), K(types_stack[i].get_subschema_id()));
    } else if (coll_type->type_id_ != ObNestedType::OB_ARRAY_TYPE && coll_type->type_id_ != ObNestedType::OB_VECTOR_TYPE) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("invalid collection type", K(ret), K(coll_type->type_id_));
    } else if (i > 0 
              && !is_null_res 
              && OB_FAIL(ObExprResultTypeUtil::get_array_calc_type(exec_ctx, 
                                                  deduce_type, 
                                                  types_stack[i], 
                                                  deduce_type))) {
      LOG_WARN("deduce calc type failed", K(ret));
    }
  } // end for
  if (OB_FAIL(ret)) {
  } else if (is_null_res) {
    type.set_null();
  } else {
    uint32_t depth = 0;
    ObDataType res_elem_type;
    bool is_vec = false;
    if (OB_FAIL(ObArrayExprUtils::get_array_element_type(exec_ctx, 
                                      deduce_type.get_subschema_id(), 
                                      res_elem_type, 
                                      depth, 
                                      is_vec))) {
      LOG_WARN("failed to get result element type", K(ret));
    } else if (is_vec) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("vector type is not supported", K(ret));
    } else {
      type.set_collection(deduce_type.get_subschema_id());
      for (int64_t i = 0; i < param_num; i++) {
        types_stack[i].set_calc_meta(deduce_type);
      }
    }
  }
  return ret;
}

int ObExprArraySetOperation::eval_array_set_operation(const ObExpr &expr, 
                                ObEvalCtx &ctx, 
                                ObDatum &res, 
                                SetOperation operation)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  uint16_t subschema_id = expr.obj_meta_.get_subschema_id();
  ObIArrayType *res_arr = NULL;
  ObIArrayType *src_arr[expr.arg_cnt_];
  bool is_null_res = false;

  if (ob_is_null(expr.obj_meta_.get_type())) {
    is_null_res = true;
  } else {
    for (int64_t i = 0; i < expr.arg_cnt_ && OB_SUCC(ret) && !is_null_res; ++i) {
      ObDatum *datum = NULL;
      src_arr[i] = NULL;
      if (subschema_id != expr.args_[i]->obj_meta_.get_subschema_id()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subschema id is not match", 
            K(ret), 
            K(subschema_id), 
            K(expr.args_[i]->obj_meta_.get_subschema_id()));
      } else if (OB_FAIL(expr.args_[i]->eval(ctx, datum))) {
        LOG_WARN("failed to eval args", K(ret));
      } else if (datum->is_null()) {
        is_null_res = true;
      } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, 
                                              ctx, 
                                              subschema_id, 
                                              datum->get_string(), 
                                              src_arr[i]))) {
        LOG_WARN("construct array obj failed", K(ret));
      }
    }  
  }
  if (OB_FAIL(ret)) {
  } else if (is_null_res) {
    res.set_null();
  } else if (operation == EXCEPT) {
    if (OB_FAIL(src_arr[0]->except(tmp_allocator, src_arr[1], res_arr))) {
      LOG_WARN("failed to except array", K(ret));
    }
  } else if (OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_allocator, ctx, subschema_id, res_arr, false))) {
    LOG_WARN("construct array obj failed", K(ret));
  } else if (operation == UNIONINZE && OB_FAIL(res_arr->unionize(tmp_allocator, src_arr, expr.arg_cnt_))) {
    LOG_WARN("failed to union array", K(ret));
  } else if (operation == INTERSECT && OB_FAIL(res_arr->intersect(tmp_allocator, src_arr, expr.arg_cnt_))) {
    LOG_WARN("failed to intersect array", K(ret));
  }
  if (!is_null_res && OB_SUCC(ret)) {
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
  return ret;
}

int ObExprArraySetOperation::eval_array_set_operation_batch(const ObExpr &expr, 
                                ObEvalCtx &ctx, 
                                const ObBitVector &skip, 
                                const int64_t batch_size, 
                                SetOperation operation)
{
  int ret = OB_SUCCESS;
  ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  uint16_t subschema_id = expr.obj_meta_.get_subschema_id();
  ObIArrayType *res_arr = NULL;
  ObIArrayType *src_arr[expr.arg_cnt_];
  ObDatumVector arr_datums[expr.arg_cnt_];
  
  if (ob_is_null(expr.obj_meta_.get_type())) {
    // do nothing
  } else {
    for (int64_t i = 0; i < expr.arg_cnt_ && OB_SUCC(ret); ++i) {
      if (subschema_id != expr.args_[i]->obj_meta_.get_subschema_id()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subschema id not match", K(ret), K(subschema_id), K(expr.args_[i]->obj_meta_.get_subschema_id()));
      } else if (OB_FAIL(expr.args_[i]->eval_batch(ctx, skip, batch_size))) {
        LOG_WARN("eval source array failed", K(ret), K(i));
      } else {
        arr_datums[i] = expr.args_[i]->locate_expr_datumvector(ctx);
      }
    } // end for
  }
  for (int64_t j = 0; j < batch_size && OB_SUCC(ret); ++j) {
    bool is_null_res = false;
    if (skip.at(j) || eval_flags.at(j)) {
      continue;
    }
    eval_flags.set(j);
    if (ob_is_null(expr.obj_meta_.get_type())) {
      is_null_res = true;
    } else {
      for (int64_t i = 0; i < expr.arg_cnt_ && OB_SUCC(ret) && !is_null_res; ++i) {
        src_arr[i] = NULL;
        if (arr_datums[i].at(j)->is_null()) {
          is_null_res = true;
        } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator,
                                                ctx,
                                                subschema_id,
                                                arr_datums[i].at(j)->get_string(),
                                                src_arr[i]))) {
          LOG_WARN("construct array obj failed", K(ret));
        }
      } // end for  
    }
    if (OB_FAIL(ret)) {
    } else if (is_null_res) {
      res_datum.at(j)->set_null();
    } else if (operation == EXCEPT) {
      if (OB_FAIL(src_arr[0]->except(tmp_allocator, src_arr[1], res_arr))) {
        LOG_WARN("failed to except array", K(ret));
      }
    } else if (OB_NOT_NULL(res_arr) && OB_FALSE_IT(res_arr->clear())) {
    } else if (OB_ISNULL(res_arr) 
              && OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_allocator, ctx, subschema_id, res_arr, false))) {
      LOG_WARN("construct array obj failed", K(ret));
    } else if (operation == UNIONINZE && OB_FAIL(res_arr->unionize(tmp_allocator, src_arr, expr.arg_cnt_))) {
      LOG_WARN("failed to union array", K(ret));
    } else if (operation == INTERSECT && OB_FAIL(res_arr->intersect(tmp_allocator, src_arr, expr.arg_cnt_))) {
      LOG_WARN("failed to intersect array", K(ret));
    }
    if (!is_null_res && OB_SUCC(ret)) {
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
  return ret;
}

int ObExprArraySetOperation::eval_array_set_operation_vector(const ObExpr &expr, 
                                ObEvalCtx &ctx, 
                                const ObBitVector &skip, 
                                const EvalBound &bound,
                                SetOperation operation)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  uint16_t subschema_id = expr.obj_meta_.get_subschema_id();
  ObIArrayType *res_arr = NULL;
  ObIArrayType *src_arr[expr.arg_cnt_];
  ObIVector *arr_vec[expr.arg_cnt_];
  ObIVector *res_vec = expr.get_vector(ctx);
  VectorFormat res_format = expr.get_format(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

  if (ob_is_null(expr.obj_meta_.get_type())) {
    // do nothing
  } else {
    for (int64_t i = 0; i < expr.arg_cnt_ && OB_SUCC(ret); ++i) {
      if (subschema_id != expr.args_[i]->obj_meta_.get_subschema_id()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subschema id not match", K(ret), K(subschema_id), K(expr.args_[i]->obj_meta_.get_subschema_id()));
      } else if (OB_FAIL(expr.args_[i]->eval_vector(ctx, skip, bound))) {
        LOG_WARN("eval source array failed", K(ret));
      } else {
        arr_vec[i] = expr.args_[i]->get_vector(ctx);
      }
    } // end for
  }
  for (int64_t j = bound.start(); j < bound.end() && OB_SUCC(ret); ++j) {
    bool is_null_res = false;
    if (skip.at(j) || eval_flags.at(j)) {
      continue;
    }
    eval_flags.set(j);
    if (ob_is_null(expr.obj_meta_.get_type())) {
      is_null_res = true;
    } else {
      for (int64_t i = 0; i < expr.arg_cnt_ && OB_SUCC(ret) && !is_null_res; ++i) {
        src_arr[i] = NULL;
        if (arr_vec[i]->is_null(j)) {
          is_null_res = true;
        } else {
          ObString arr_str = arr_vec[i]->get_string(j);
          if (OB_FAIL(ObNestedVectorFunc::construct_param(tmp_allocator, 
                                              ctx, 
                                              subschema_id, 
                                              arr_str, 
                                              src_arr[i]))) {
            LOG_WARN("construct array obj failed", K(ret));
          }
        }
      } // end for
    }
    if (OB_FAIL(ret)) {
    } else if (is_null_res) {
      res_vec->set_null(j);
    } else if (operation == EXCEPT) {
      if (OB_FAIL(src_arr[0]->except(tmp_allocator, src_arr[1], res_arr))) {
        LOG_WARN("failed to except array", K(ret));
      }
    } else if (OB_NOT_NULL(res_arr) && OB_FALSE_IT(res_arr->clear())) {
    } else if (OB_ISNULL(res_arr) 
              && OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_allocator, ctx, subschema_id, res_arr, false))) {
      LOG_WARN("construct array obj failed", K(ret));
    } else if (operation == UNIONINZE && OB_FAIL(res_arr->unionize(tmp_allocator, src_arr, expr.arg_cnt_))) {
      LOG_WARN("failed to union array", K(ret));
    } else if (operation == INTERSECT && OB_FAIL(res_arr->intersect(tmp_allocator, src_arr, expr.arg_cnt_))) {
      LOG_WARN("failed to intersect array", K(ret));
    } 
    if (!is_null_res && OB_SUCC(ret)) {
      if (res_format == VEC_DISCRETE) {
        if (OB_FAIL(ObArrayExprUtils::set_array_res<ObDiscreteFormat>(res_arr, expr, ctx, static_cast<ObDiscreteFormat *>(res_vec), j))) {
          LOG_WARN("set array res failed", K(ret));
        }
      } else if (res_format == VEC_UNIFORM) {
        if (OB_FAIL(ObArrayExprUtils::set_array_res<ObUniformFormat<false>>(res_arr, expr, ctx, static_cast<ObUniformFormat<false> *>(res_vec), j))) {
          LOG_WARN("set array res failed", K(ret));
        }
      } else if (OB_FAIL(ObArrayExprUtils::set_array_res<ObVectorBase>(res_arr, expr, ctx, static_cast<ObVectorBase *>(res_vec), j))) {
        LOG_WARN("set array res failed", K(ret));
      }
    }
  } // end for
  return ret;
}

ObExprArrayUnion::ObExprArrayUnion(ObIAllocator &alloc)
    : ObExprArraySetOperation(alloc, T_FUNC_SYS_ARRAY_UNION, N_ARRAY_UNION, MORE_THAN_ONE,  NOT_ROW_DIMENSION)
{
}

ObExprArrayUnion::~ObExprArrayUnion()
{
}

int ObExprArrayUnion::eval_array_union(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  return eval_array_set_operation(expr, ctx, res, UNIONINZE);
}

int ObExprArrayUnion::eval_array_union_batch(const ObExpr &expr, 
                          ObEvalCtx &ctx,
                          const ObBitVector &skip, 
                          const int64_t batch_size)
{
  return eval_array_set_operation_batch(expr, ctx, skip, batch_size, UNIONINZE);
}

int ObExprArrayUnion::eval_array_union_vector(const ObExpr &expr, 
                          ObEvalCtx &ctx,
                          const ObBitVector &skip, 
                          const EvalBound &bound)
{
  return eval_array_set_operation_vector(expr, ctx, skip, bound, UNIONINZE);
}

int ObExprArrayUnion::cg_expr(ObExprCGCtx &expr_cg_ctx,
                          const ObRawExpr &raw_expr,
                          ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_array_union;
  rt_expr.eval_batch_func_ = eval_array_union_batch;
  rt_expr.eval_vector_func_ = eval_array_union_vector;   
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase