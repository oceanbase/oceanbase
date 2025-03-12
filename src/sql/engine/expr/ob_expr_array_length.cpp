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
 * This file contains implementation for array_length expression.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_array_length.h"
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
ObExprArrayLength::ObExprArrayLength(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUNC_SYS_ARRAY_LENGTH, N_ARRAY_LENGTH, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprArrayLength::~ObExprArrayLength()
{
}

int ObExprArrayLength::calc_result_type1(ObExprResType &type,
                                        ObExprResType &type1,
                                        common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (type1.is_null()){
    // do nothing
  } else if (!ob_is_collection_sql_type(type1.get_type())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "ARRAY", ob_obj_type_str(type1.get_type()));
  }
  if (OB_SUCC(ret)) {
    type.set_uint32();
    type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObUInt32Type].scale_);
    type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObUInt32Type].precision_);
  }

  return ret;
}

int ObExprArrayLength::eval_array_length(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  ObCollectionArrayType *arr_type = NULL;
  ObDatum *datum_arr = nullptr;
  if (OB_FAIL(expr.args_[0]->eval(ctx, datum_arr))) {
    LOG_WARN("failed to eval source array arg", K(ret));
  } else if (datum_arr->is_null()) {
    res.set_null();
  } else if (OB_FAIL(
                 ObArrayExprUtils::get_array_type_by_subschema_id(ctx, subschema_id, arr_type))) {
    LOG_WARN("failed to get array type by subschema id", K(ret), K(subschema_id));
  } else {
    ObString arr_str = datum_arr->get_string();
    uint32_t len = 0;
    char *raw_str = nullptr;
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_allocator,
                                      ObLongTextType,
                                      CS_TYPE_BINARY,
                                      true,
                                      arr_str))) {
      LOG_WARN("fail to get real data.", K(ret), K(arr_str));
    } else if (arr_type->type_id_ == ObNestedType::OB_ARRAY_TYPE){
      raw_str = arr_str.ptr();
      len = *reinterpret_cast<uint32_t *>(raw_str);
      res.set_uint32(len);
    } else if (arr_type->type_id_ == ObNestedType::OB_VECTOR_TYPE) {
      len = arr_str.length() / sizeof(float);
      res.set_uint32(len);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected array type", K(ret));
    }
  }

  return ret;
}

int ObExprArrayLength::eval_array_length_batch(const ObExpr &expr,
                                              ObEvalCtx &ctx,
                                              const ObBitVector &skip,
                                              const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  ObCollectionArrayType *arr_type = NULL;
  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("failed to eval args", K(ret));
  } else {
    ObDatumVector arr_array = expr.args_[0]->locate_expr_datumvector(ctx);
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      eval_flags.set(j);
      if (arr_array.at(j)->is_null()) {
        res_datum.at(j)->set_null();
      } else if (OB_FAIL( ObArrayExprUtils::get_array_type_by_subschema_id(ctx, subschema_id, arr_type))) {
        LOG_WARN("failed to get array type by subschema id", K(ret), K(subschema_id));
      } else {
        ObString arr_str = arr_array.at(j)->get_string();
        uint32_t len = 0;
        char *raw_str = nullptr;
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_allocator,
                                          ObLongTextType,
                                          CS_TYPE_BINARY,
                                          true,
                                          arr_str))) {
          LOG_WARN("fail to get real data.", K(ret), K(arr_str));
        } else if (arr_type->type_id_ == ObNestedType::OB_ARRAY_TYPE){
          raw_str = arr_str.ptr();
          len = *reinterpret_cast<uint32_t *>(raw_str);
          res_datum.at(j)->set_uint32(len);
        } else if (arr_type->type_id_ == ObNestedType::OB_VECTOR_TYPE) {
          len = arr_str.length() / sizeof(float);
          res_datum.at(j)->set_uint32(len);
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected array type", K(ret));
        }
      }
    } // end for
  }

  return ret;
}

int ObExprArrayLength::eval_array_length_vector(const ObExpr &expr,
                                                ObEvalCtx &ctx,
                                                const ObBitVector &skip,
                                                const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  ObCollectionArrayType *arr_type = NULL;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval source array failed", K(ret));
  } else {
    uint32 attr_count = expr.args_[0]->attrs_cnt_;
    ObIVector *arr_vec = expr.args_[0]->get_vector(ctx);
    ObIVector *len_vec = nullptr;
    if (attr_count >= 1) {
      len_vec = expr.args_[0]->attrs_[0]->get_vector(ctx);
    }
    char *raw_str = nullptr;
    uint32_t len = 0;
    ObIVector *res_vec = expr.get_vector(ctx);
    VectorFormat arr_format = arr_vec->get_format();
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      }
      eval_flags.set(idx);
      if (arr_vec->is_null(idx)) {
        res_vec->set_null(idx);
      } else if (OB_FAIL(ObArrayExprUtils::get_array_type_by_subschema_id(ctx, subschema_id, arr_type))) {
        LOG_WARN("failed to get array type by subschema id", K(ret), K(subschema_id));
      } else if (arr_format == VEC_UNIFORM || arr_format == VEC_UNIFORM_CONST) {
        ObString arr_str = arr_vec->get_string(idx);
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_allocator,
                                            ObLongTextType,
                                            CS_TYPE_BINARY,
                                            true,
                                            arr_str))) {
          LOG_WARN("fail to get real data.", K(ret), K(arr_str));
        } else if (arr_type->type_id_ == ObNestedType::OB_ARRAY_TYPE) {
          raw_str = arr_str.ptr();
          len = *reinterpret_cast<uint32_t *>(raw_str);
          res_vec->set_uint(idx, static_cast<uint64_t>(len));
        } else if (arr_type->type_id_ == ObNestedType::OB_VECTOR_TYPE) {
          len = arr_str.length() / sizeof(float);
          res_vec->set_uint(idx, static_cast<uint64_t>(len));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected array type", K(ret));
        }
      } else {
        const char *payload = nullptr;
        ObLength payload_len = 0;
        len_vec->get_payload(idx, payload, payload_len);
        len = *(reinterpret_cast<const uint32_t *>(payload));
        res_vec->set_uint(idx, static_cast<uint64_t>(len));
      }
    }
  }

  return ret;
}

int ObExprArrayLength::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_array_length;
  rt_expr.eval_batch_func_ = eval_array_length_batch;
  rt_expr.eval_vector_func_ = eval_array_length_vector;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase