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
 * This file contains implementation for array_position.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_array_position.h"
#include "lib/udt/ob_collection_type.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

namespace oceanbase
{
namespace sql
{
ObExprArrayPosition::ObExprArrayPosition(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,
        T_FUNC_SYS_ARRAY_POSITION,
        N_ARRAY_POSITION,
        2,
        VALID_FOR_GENERATED_COL,
        NOT_ROW_DIMENSION)
{
}
ObExprArrayPosition::~ObExprArrayPosition() {}

int ObExprArrayPosition::calc_result_type2(ObExprResType &type,
                            ObExprResType &type1,
                            ObExprResType &type2,
                            common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
  uint16_t subschema_id;
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec_ctx is null", K(ret));
  } else if (OB_ISNULL(type_ctx.get_raw_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("raw expr is null", K(ret));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (type1.is_null()) {
    type.set_null();
  } else if (!ob_is_collection_sql_type(type1.get_type())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_WARN(OB_ERR_INVALID_TYPE_FOR_OP,
        ob_obj_type_str(type1.get_type()),
        ob_obj_type_str(type2.get_type()));
  } else if (type2.is_null()) {
    // do nothing
  } else if (OB_FAIL(ObArrayExprUtils::deduce_array_type(exec_ctx, type1, type2, subschema_id))) {
    LOG_WARN("failed to get result array type subschema id", K(ret));
  }

  if (OB_SUCC(ret) && !type1.is_null()) {
    type.set_int();
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
    type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
  }

  return ret;
}

int ObExprArrayPosition::eval_array_position(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  ObDatum *arr_datum = NULL;
  ObDatum *elem_datum = NULL;
  ObIArrayType *src_arr = NULL;
  int idx = 0;

  if (OB_FAIL(expr.args_[0]->eval(ctx, arr_datum))) {
    LOG_WARN("failed to eval args", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, elem_datum))) {
    LOG_WARN("failed to eval args", K(ret));
  } else if (arr_datum->is_null()) {
    res.set_null();
  } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator,
                                          ctx,
                                          subschema_id,
                                          arr_datum->get_string(),
                                          src_arr))) {
    LOG_WARN("construct array obj failed", K(ret));
  } else if (elem_datum->is_null()) {
    uint8_t *null_bitmaps = src_arr->get_nullbitmap();
    uint32_t length = src_arr->size();
    for (idx = 0; null_bitmaps != nullptr && idx < length; ++idx) {
      if (null_bitmaps[idx] > 0) {
        break;
      }
    }
    if (null_bitmaps != nullptr && idx < length) {
      res.set_int(idx + 1);
    } else {
      res.set_int(0);
    }
  } else {
    if (OB_FAIL(array_position(expr, tmp_allocator, ctx, src_arr, elem_datum, idx))) {
      LOG_WARN("array position failed", K(ret));
    } else {
      res.set_int(idx + 1);
    }
  }

  return ret;
}

int ObExprArrayPosition::eval_array_position_batch(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            const ObBitVector &skip,
                            const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  ObCollectionArrayType *arr_type = NULL;
  ObIArrayType *src_arr = NULL;

  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval source array failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval source val failed", K(ret));
  } else {
    ObDatumVector arr_array = expr.args_[0]->locate_expr_datumvector(ctx);
    ObDatumVector elem_array = expr.args_[1]->locate_expr_datumvector(ctx);
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
      int idx = 0;
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      eval_flags.set(j);
      if (arr_array.at(j)->is_null()) {
        res_datum.at(j)->set_null();
      } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator,
                                              ctx,
                                              subschema_id,
                                              arr_array.at(j)->get_string(),
                                              src_arr))) {
        LOG_WARN("construct array obj failed", K(ret));
      } else if (elem_array.at(j)->is_null()) {
        uint8_t *null_bitmaps = src_arr->get_nullbitmap();
        uint32_t length = src_arr->size();
        for (idx = 0; null_bitmaps != nullptr && idx < length; ++idx) {
          if (null_bitmaps[idx] > 0) {
            break;
          }
        }
        if (null_bitmaps != nullptr && idx < length) {
          res_datum.at(j)->set_int(idx + 1);
        } else {
          res_datum.at(j)->set_int(0);
        }
      } else {
        ObDatum *elem_datum = elem_array.at(j);
        if (OB_FAIL(array_position(expr, tmp_allocator, ctx, src_arr, elem_datum, idx))) {
          LOG_WARN("array position failed");
        } else {
          res_datum.at(j)->set_int(idx + 1);
        }
      } // end if
    }   // end for
  }

  return ret;
}

int ObExprArrayPosition::eval_array_position_vector(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            const ObBitVector &skip,
                            const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  ObIArrayType *src_arr = NULL;

  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval source array failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval source val failed", K(ret));
  } else {
    ObIVector *arr_vec = expr.args_[0]->get_vector(ctx);
    VectorFormat arr_format = arr_vec->get_format();
    ObIVector *val_vec = expr.args_[1]->get_vector(ctx);
    ObIVector *res_vec = expr.get_vector(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

    for (int64_t j = bound.start(); OB_SUCC(ret) && j < bound.end(); ++j) {
      int idx = 0;
      bool is_null_res = false;
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      eval_flags.set(j);
      if (arr_vec->is_null(j)) {
        is_null_res = true;
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
      } else if (val_vec->is_null(j)) {
        uint8_t *null_bitmaps = src_arr->get_nullbitmap();
        uint32_t length = src_arr->size();
        for (idx = 0; null_bitmaps != nullptr && idx < length; ++idx) {
          if (null_bitmaps[idx] > 0) {
            break;
          }
        }
        if (null_bitmaps != nullptr && idx < length) {
          res_vec->set_int(j, idx + 1);
        } else {
          res_vec->set_int(j, 0);
        }
      } else {
        if (OB_FAIL(array_position_vector(expr, tmp_allocator, ctx, src_arr, val_vec, j, idx))) {
          LOG_WARN("array position vector failed");
        } else {
          res_vec->set_int(j, idx + 1);
        }
      }
    } // end for
  }

  return ret;
}

int ObExprArrayPosition::array_position(const ObExpr &expr,
                            ObIAllocator &alloc,
                            ObEvalCtx &ctx,
                            ObIArrayType *src_arr,
                            ObDatum *val_datum,
                            int &idx)
{
  int ret = OB_SUCCESS;
  const ObObjType &right_type = expr.args_[1]->datum_meta_.type_;
  ObObjTypeClass right_tc = ob_obj_type_class(right_type);

  switch (right_tc) {
  case ObUIntTC:
  case ObIntTC: {
    int64_t val = val_datum->get_int();
    if (OB_FAIL(ObArrayUtil::position(*src_arr, val, idx))) {
      LOG_WARN("array position failed", K(ret));
    }
    break;
  }
  case ObFloatTC: {
    float val = val_datum->get_float();
    if (OB_FAIL(ObArrayUtil::position(*src_arr, val, idx))) {
      LOG_WARN("array position failed", K(ret));
    }
    break;
  }
  case ObDoubleTC: {
    double val = val_datum->get_double();
    if (OB_FAIL(ObArrayUtil::position(*src_arr, val, idx))) {
      LOG_WARN("array position failed", K(ret));
    }
    break;
  }
  case ObStringTC: {
    if (OB_FAIL(ObArrayUtil::position(*src_arr, val_datum->get_string(), idx))) {
      LOG_WARN("array position failed", K(ret));
    }
    break;
  }
  case ObCollectionSQLTC: {
    const uint16_t val_subshemaid = expr.args_[1]->obj_meta_.get_subschema_id();
    ObIArrayType *val = NULL;
    if (OB_FAIL(ObArrayExprUtils::get_array_obj(alloc,
                                      ctx,
                                      val_subshemaid,
                                      val_datum->get_string(),
                                      val))) {
      LOG_WARN("construct array obj failed", K(ret));
    } else if (OB_FAIL(ObArrayUtil::position(*src_arr, *val, idx))) {
      LOG_WARN("array position failed", K(ret));
    }
    break;
  }
  default: {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid type", K(ret), K(right_tc));
  }
  }

  return ret;
}

int ObExprArrayPosition::array_position_vector(const ObExpr &expr,
                            ObIAllocator &alloc,
                            ObEvalCtx &ctx,
                            ObIArrayType *src_arr,
                            ObIVector *val_vec,
                            int vec_idx,
                            int &idx)
{
  int ret = OB_SUCCESS;
  const ObObjType &right_type = expr.args_[1]->datum_meta_.type_;
  ObObjTypeClass right_tc = ob_obj_type_class(right_type);

  switch (right_tc) {
  case ObUIntTC:
  case ObIntTC: {
    int64_t val = val_vec->get_int(vec_idx);
    if (OB_FAIL(ObArrayUtil::position(*src_arr, val, idx))) {
      LOG_WARN("array position failed", K(ret));
    }
    break;
  }
  case ObFloatTC: {
    float val = val_vec->get_float(vec_idx);
    if (OB_FAIL(ObArrayUtil::position(*src_arr, val, idx))) {
      LOG_WARN("array position failed", K(ret));
    }
    break;
  }
  case ObDoubleTC: {
    double val = val_vec->get_double(vec_idx);
    if (OB_FAIL(ObArrayUtil::position(*src_arr, val, idx))) {
      LOG_WARN("array position failed", K(ret));
    }
    break;
  }
  case ObStringTC: {
    if (OB_FAIL(ObArrayUtil::position(*src_arr, val_vec->get_string(vec_idx), idx))) {
      LOG_WARN("array position failed", K(ret));
    }
    break;
  }
  case ObCollectionSQLTC: {
    const uint16_t val_subshemaid = expr.args_[1]->obj_meta_.get_subschema_id();
    ObIArrayType *val = NULL;
    if (OB_FAIL(ObArrayExprUtils::get_array_obj(alloc,
                                      ctx,
                                      val_subshemaid,
                                      val_vec->get_string(vec_idx),
                                      val))) {
      LOG_WARN("construct array obj failed", K(ret));
    } else if (OB_FAIL(ObArrayUtil::position(*src_arr, *val, idx))) {
      LOG_WARN("array position failed", K(ret));
    }
    break;
  }
  default: {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid type", K(ret), K(right_tc));
  }
  } // end switch

  return ret;
}

int ObExprArrayPosition::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                 ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_array_position;
  rt_expr.eval_batch_func_ = eval_array_position_batch;
  rt_expr.eval_vector_func_ = eval_array_position_vector;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase