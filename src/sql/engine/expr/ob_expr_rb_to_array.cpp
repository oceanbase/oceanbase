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
 * This file contains implementation for rb_to_array.
 */
 
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_rb_to_array.h"
#include "sql/engine/expr/ob_expr_rb_func_helper.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "lib/roaringbitmap/ob_rb_utils.h"
#include "sql/engine/expr/ob_expr.h" // for ObExpr
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprRbToArray::ObExprRbToArray(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_RB_TO_ARRAY, N_RB_TO_ARRAY, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprRbToArray::~ObExprRbToArray()
{
}


int ObExprRbToArray::calc_result_type1(ObExprResType &type,
                                        ObExprResType &type1,
                                        common::ObExprTypeCtx &type_ctx) const
{
  INIT_SUCC(ret);
  ObSQLSessionInfo *session = NULL;
  ObExecContext *exec_ctx = NULL;
  ObString res_type_info = "ARRAY(BIGINT UNSIGNED)";
  uint16_t subschema_id;
  
  if (ob_is_null(type1.get_type())) {
    type.set_null();
  } else if (!(type1.is_roaringbitmap() || type1.is_hex_string())) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("invalid roaringbitmap data type provided.", K(ret), K(type1.get_type()), K(type1.get_collation_type()));
  } else if (OB_ISNULL(session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSQLSessionInfo is null", K(ret));
  } else if (OB_ISNULL(exec_ctx = session->get_cur_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObExecContext is null", K(ret));
  } else if (OB_FAIL(exec_ctx->get_subschema_id_by_type_string(res_type_info, subschema_id))) {
    LOG_WARN("failed get subschema id", K(ret), K(res_type_info));
  } else {
    type.set_collection(subschema_id);
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObCollectionSQLType]).get_length());
  }
  return ret;
}

int ObExprRbToArray::eval_rb_to_array(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        ObDatum &res)
{
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(ObRbExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session()), "ROARINGBITMAP"));
  ObExpr *rb_arg = expr.args_[0];
  bool is_rb_null = false;
  ObRoaringBitmap *rb = nullptr;

  if (OB_FAIL(ObRbExprHelper::get_input_roaringbitmap(ctx, tmp_allocator, rb_arg, rb, is_rb_null))) {
    LOG_WARN("fail to get input roaringbitmap", K(ret));
  } else if (is_rb_null) {
    res.set_null();
  } else {
    ObIArrayType *arr_res = NULL;
    if (OB_FAIL(rb_to_array(expr, ctx, tmp_allocator, rb, arr_res))) {
      LOG_WARN("fail to convert to array", K(ret));
    } else {
      ObString res_str;
      if (OB_FAIL(ObArrayExprUtils::set_array_res(arr_res, arr_res->get_raw_binary_len(), expr, ctx, res_str))) {
        LOG_WARN("get array binary string failed", K(ret));
      } else {
        res.set_string(res_str);
      }
    }
  }
  return ret;
}

int ObExprRbToArray::eval_rb_to_array_vector(const ObExpr &expr, 
                                              ObEvalCtx &ctx,
                                              const ObBitVector &skip, 
                                              const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(ObRbExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session()), "ROARINGBITMAP"));

  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval source array failed", K(ret));
  } else {
    ObIVector *rb_vec = expr.args_[0]->get_vector(ctx);
    ObIVector *res_vec = expr.get_vector(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    const uint16_t subschema_id = expr.obj_meta_.get_subschema_id();
    VectorFormat res_format = expr.get_format(ctx);


    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      bool is_rb_null = false;
      ObRoaringBitmap *rb = nullptr;
      ObIArrayType *arr_res = NULL;
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else if (rb_vec->is_null(idx)) {
        is_rb_null = true;
      } else {
        ObString arr_str;
        if (OB_FAIL(ObRbExprHelper::get_input_roaringbitmap(ctx, tmp_allocator, expr.args_[0], rb_vec, rb, is_rb_null, idx))) {
          LOG_WARN("fail to get input roaringbitmap", K(ret));
        } else if (OB_FAIL(rb_to_array(expr, ctx, tmp_allocator, rb, arr_res))) {
          LOG_WARN("fail to convert to array", K(ret));
        } 
      }

      if (OB_FAIL(ret)){
      } else if (is_rb_null) {
        res_vec->set_null(idx);
        eval_flags.set(idx);
      } else {
        if (res_format == VEC_DISCRETE) {
          if (OB_FAIL(ObArrayExprUtils::set_array_res<ObDiscreteFormat>(arr_res, expr, ctx, static_cast<ObDiscreteFormat *>(res_vec), idx))) {
            LOG_WARN("set array res failed", K(ret));
          }
        } else if (res_format == VEC_UNIFORM) {
          if (OB_FAIL(ObArrayExprUtils::set_array_res<ObUniformFormat<false>>(arr_res, expr, ctx, static_cast<ObUniformFormat<false> *>(res_vec), idx))) {
            LOG_WARN("set array res failed", K(ret));
          }
        } else if (OB_FAIL(ObArrayExprUtils::set_array_res<ObVectorBase>(arr_res, expr, ctx, static_cast<ObVectorBase *>(res_vec), idx))) {
          LOG_WARN("set array res failed", K(ret));
        } else {
          eval_flags.set(idx);
        }
      } 
    } //end for
  }
  return ret;
}

int ObExprRbToArray::rb_to_array(const ObExpr &expr, 
                ObEvalCtx &ctx,
                ObIAllocator &alloc, 
                ObRoaringBitmap *&rb,
                ObIArrayType *&arr_res)
{
  INIT_SUCC(ret);
  void *iter_buff = nullptr;
  const uint16_t meta_id = expr.obj_meta_.get_subschema_id();   
  if (OB_FAIL(ObArrayExprUtils::construct_array_obj(alloc, ctx, meta_id, arr_res, false))){
    LOG_WARN("fail to construct array obj.", K(ret));
  } else if(rb->get_cardinality() == 0) {
    // init empty array
  } else {
    ObRoaringBitmapIter *rb_iter = nullptr;
    if(OB_ISNULL(iter_buff = alloc.alloc(sizeof(ObRoaringBitmapIter)))){
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate result iter", K(ret));
    } else if (OB_FALSE_IT(rb_iter = new(iter_buff) ObRoaringBitmapIter(rb))){
    } else if (OB_FAIL(rb_iter->init())) {
      LOG_WARN("failed to init roaringbitmap iter", K(ret));
    } else {
      ObArrayFixedSize<uint64_t> *arr_data = dynamic_cast<ObArrayFixedSize<uint64_t> *>(arr_res);
      if (OB_UNLIKELY(OB_ISNULL(arr_data))) {
        LOG_WARN("failed to init array pointer", K(ret));
      } else {
        while(OB_SUCC(ret)) {
          uint64_t curr_val = rb_iter->get_curr_value();
          // push_back
          if (OB_FAIL(arr_data->push_back(curr_val))) {
            LOG_WARN("failed to push back value into array", K(ret), K(curr_val));
          } else if (OB_FAIL(rb_iter->get_next())) {
            // do nothing
          }
        }
      }
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
        rb_iter->destory();
      } else {
        LOG_WARN("failed to get next value on roaringbitmap iter", K(ret));
      }
    }
  }   
  ObRbUtils::rb_destroy(rb);
  return ret;
}

int ObExprRbToArray::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprRbToArray::eval_rb_to_array;
  rt_expr.eval_vector_func_ = ObExprRbToArray::eval_rb_to_array_vector;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase