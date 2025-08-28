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
 * This file contains implementation for array_overlaps.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_array_distinct.h"
#include "lib/udt/ob_collection_type.h"
#include "lib/udt/ob_array_type.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
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

ObExprArrayDistinct::ObExprArrayDistinct(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUNC_SYS_ARRAY_DISTINCT, N_ARRAY_DISTINCT, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprArrayDistinct::ObExprArrayDistinct(ObIAllocator &alloc,
                         ObExprOperatorType type,
                         const char *name,
                         int32_t param_num, 
                         int32_t dimension) : ObFuncExprOperator(alloc, type, name, param_num, VALID_FOR_GENERATED_COL, dimension) 
{
}

ObExprArrayDistinct::~ObExprArrayDistinct()
{
}

int ObExprArrayDistinct::calc_result_type1(ObExprResType &type,
                                           ObExprResType &type1,
                                           common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
  ObCollectionTypeBase *coll_type = NULL;
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec ctx is null", K(ret));
  } else if (type1.is_null()) {
    type.set_null();
  } else if (!ob_is_collection_sql_type(type1.get_type())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid param type", K(ret), K(type1.get_type()));
  } else if (OB_FAIL(ObArrayExprUtils::get_coll_type_by_subschema_id(exec_ctx, type1.get_subschema_id(), coll_type))) {
    LOG_WARN("failed to get array type by subschema id", K(ret), K(type1.get_subschema_id()));
  } else if (coll_type->type_id_ != ObNestedType::OB_ARRAY_TYPE && coll_type->type_id_ != ObNestedType::OB_VECTOR_TYPE) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid collection type", K(ret), K(coll_type->type_id_));
  } else {
    type.set_collection(type1.get_subschema_id());
  }
  
  return ret;
}

int ObExprArrayDistinct::eval_array_distinct(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t meta_id = expr.args_[0]->obj_meta_.get_subschema_id();
  ObIArrayType *arr_obj = NULL;
  ObIArrayType *arr_res = NULL;
  ObDatum *datum = NULL;
  bool bret = false;
  if (OB_FAIL(expr.args_[0]->eval(ctx, datum))) {
    LOG_WARN("failed to eval args", K(ret));
  } else if (datum->is_null()) {
    res.set_null();
  } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, meta_id, datum->get_string(), arr_obj))) {
    LOG_WARN("construct array obj failed", K(ret));
  } else if (OB_FAIL(arr_obj->distinct(tmp_allocator, arr_res))) {
    LOG_WARN("array distinct failed", K(ret));
  } else if (OB_FAIL(arr_res->init())) {
    LOG_WARN("array init failed", K(ret));
  } else {
    ObString res_str;
    if (OB_FAIL(ObArrayExprUtils::set_array_res(arr_res, arr_res->get_raw_binary_len(), expr, ctx, res_str))) {
      LOG_WARN("get array binary string failed", K(ret));
    } else {
      res.set_string(res_str);
    }
  }
  return ret;
}

int ObExprArrayDistinct::eval_array_distinct_batch(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                                                   const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t meta_id = expr.args_[0]->obj_meta_.get_subschema_id();
  ObIArrayType *arr_obj = NULL;
  ObIArrayType *arr_res = NULL;
  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval date_unit_datum failed", K(ret));
  } else {
    ObDatumVector in_array = expr.args_[0]->locate_expr_datumvector(ctx);
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      eval_flags.set(j);
      bool bret = false;
      if (in_array.at(j)->is_null()) {
        res_datum.at(j)->set_null();
      } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, meta_id, in_array.at(j)->get_string(), arr_obj))) {
        LOG_WARN("construct array obj failed", K(ret));
      } else if (OB_FAIL(arr_obj->distinct(tmp_allocator, arr_res))) {
        LOG_WARN("array distinct failed", K(ret));
      } else if (OB_FAIL(arr_res->init())) {
        LOG_WARN("array init failed", K(ret));
      } else {
        int32_t res_size = arr_res->get_raw_binary_len();
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
        } else if (OB_FAIL(arr_res->get_raw_binary(res_buf, res_buf_len))) {
          LOG_WARN("get array raw binary failed", K(ret), K(res_buf_len), K(res_size));
        } else if (OB_FAIL(output_result.lseek(res_size, 0))) {
          LOG_WARN("failed to lseek res.", K(ret), K(output_result), K(res_size));
        } else {
          output_result.set_result();
          arr_res->clear();
        }
      }
    }
  }
  return ret;
}

int ObExprArrayDistinct::eval_array_distinct_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                                    const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval params", K(ret));
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
    ObIVector *left_vec = expr.args_[0]->get_vector(ctx);
    VectorFormat left_format = left_vec->get_format();
    const uint16_t left_meta_id = expr.args_[0]->obj_meta_.get_subschema_id();
    ObIVector *res_vec = expr.get_vector(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    ObIArrayType *l_arr_obj = NULL;
    ObIArrayType *res_obj = NULL;
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      bool is_null_res = false;
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else if (left_vec->is_null(idx)) {
        is_null_res = true;
      } else {
        ObString left = left_vec->get_string(idx);
        if (OB_FAIL(ObNestedVectorFunc::construct_param(tmp_allocator, ctx, left_meta_id, left, l_arr_obj))) {
          LOG_WARN("construct array obj failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (is_null_res) {
        res_vec->set_null(idx);
        eval_flags.set(idx);
      } else if (OB_FAIL(l_arr_obj->distinct(tmp_allocator, res_obj))) {
        LOG_WARN("array distinct failed", K(ret));
      } else if (OB_FAIL(res_obj->init())) {
        LOG_WARN("array init failed", K(ret));
      } else if (res_format == VEC_DISCRETE) {
        if (OB_FAIL(ObArrayExprUtils::set_array_res<ObDiscreteFormat>(res_obj, expr, ctx, static_cast<ObDiscreteFormat *>(res_vec), idx))) {
          LOG_WARN("set array res failed", K(ret));
        }
      } else if (res_format == VEC_UNIFORM) {
        if (OB_FAIL(ObArrayExprUtils::set_array_res<ObUniformFormat<false>>(res_obj, expr, ctx, static_cast<ObUniformFormat<false> *>(res_vec), idx))) {
          LOG_WARN("set array res failed", K(ret));
        }
      } else if (OB_FAIL(ObArrayExprUtils::set_array_res<ObVectorBase>(res_obj, expr, ctx, static_cast<ObVectorBase *>(res_vec), idx))) {
        LOG_WARN("set array res failed", K(ret));
      } 
      if (OB_SUCC(ret) && !is_null_res) {
        eval_flags.set(idx);
        res_obj->clear();
      }
    }
  }
  return ret;
}

int ObExprArrayDistinct::cg_expr(ObExprCGCtx &expr_cg_ctx,
                         const ObRawExpr &raw_expr,
                         ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_array_distinct;
  rt_expr.eval_batch_func_ = eval_array_distinct_batch;
  rt_expr.eval_vector_func_ = eval_array_distinct_vector;

  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
