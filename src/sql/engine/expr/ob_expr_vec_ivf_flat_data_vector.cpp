/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX COMMON

#include "sql/engine/expr/ob_expr_vec_ivf_flat_data_vector.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
ObExprVecIVFFlatDataVector::ObExprVecIVFFlatDataVector(ObIAllocator &allocator)
  : ObFuncExprOperator(allocator, T_FUN_SYS_VEC_IVF_FLAT_DATA_VECTOR, N_VEC_IVF_FLAT_DATA_VECTOR, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
  need_charset_convert_ = false;
}

int ObExprVecIVFFlatDataVector::calc_result_typeN(ObExprResType &type,
                                       ObExprResType *types,
                                       int64_t param_num,
                                       ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
  ObDataType elem_type;
  elem_type.meta_.set_float();
  uint16_t subschema_id;
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec ctx is null", K(ret));
  } else if (OB_FAIL(exec_ctx->get_subschema_id_by_collection_elem_type(ObNestedType::OB_VECTOR_TYPE,
                                                                        elem_type, subschema_id))) {
    LOG_WARN("failed to get collection subschema id", K(ret));
  } else {
    type.set_collection(subschema_id);
  }
  return ret;
}

int ObExprVecIVFFlatDataVector::calc_resultN(ObObj &result,
                                  const ObObj *objs_array,
                                  int64_t param_num,
                                  ObExprCtx &expr_ctx) const
{
  // TODO by query ivf index
  return OB_NOT_SUPPORTED;
}

int ObExprVecIVFFlatDataVector::cg_expr(
    ObExprCGCtx &expr_cg_ctx,
    const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  UNUSED(expr_cg_ctx);
  if (OB_UNLIKELY(rt_expr.arg_cnt_ != 2) || OB_ISNULL(rt_expr.args_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(rt_expr.arg_cnt_), KP(rt_expr.args_), K(rt_expr.type_));
  } else {
    rt_expr.eval_func_ = generate_data_vector;
  }
  return ret;
}

int ObExprVecIVFFlatDataVector::generate_data_vector(
    const ObExpr &expr,
    ObEvalCtx &eval_ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;

  common::ObArenaAllocator tmp_allocator("IVFFlatExprVec", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObExpr *calc_vector_expr = expr.args_[0];
  ObExpr *calc_distance_algo_expr = expr.args_[1];
  ObDatum *res = nullptr;
  if (OB_ISNULL(calc_vector_expr) || OB_ISNULL(calc_distance_algo_expr)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null exprs", K(ret), KP(calc_vector_expr), KP(calc_distance_algo_expr));
  } else {
    ObVectorIndexDistAlgorithm dis_algo = VIDA_MAX;
    if (OB_FAIL(ret)) {
    } else if (calc_distance_algo_expr->datum_meta_.type_ != ObUInt64Type) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("calc distance algo expr is invalid", K(ret), KPC(calc_distance_algo_expr));
    } else if (OB_FAIL(calc_distance_algo_expr->eval(eval_ctx, res))) {
      LOG_WARN("calc table id expr failed", K(ret));
    } else if (FALSE_IT(dis_algo = static_cast<ObVectorIndexDistAlgorithm>(res->get_uint64()))) {
    } else if (VIDA_MAX <= dis_algo) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected distance algo", K(ret), K(dis_algo));
    } else {
      ObIArrayType *arr = NULL;
      float *norm_vector = nullptr;
      bool is_null = false;
      if (calc_vector_expr->datum_meta_.type_ != ObCollectionSQLType) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("calc vector expr is invalid", K(ret), KPC(calc_vector_expr));
      } else if (OB_FAIL(ObArrayExprUtils::get_type_vector(*(calc_vector_expr), eval_ctx, tmp_allocator, arr,
                                                           is_null))) {
        LOG_WARN("failed to get vector", K(ret), KPC(calc_vector_expr));
      } else if (is_null) {
        expr_datum.set_null();
      } else if (dis_algo == ObVectorIndexDistAlgorithm::VIDA_COS) {
        if (OB_ISNULL(norm_vector = reinterpret_cast<float *>(tmp_allocator.alloc(sizeof(float) * arr->size())))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc vector", K(ret));
        } else if (OB_FAIL(ObVectorNormalize::L2_normalize_vector(arr->size(),
                                                                  reinterpret_cast<float *>(arr->get_data()),
                                                                  norm_vector))) {
          LOG_WARN("failed to normalize vector", K(ret));
        }
      }
      if (OB_FAIL(ret) || is_null) {
      } else {
        float *data = norm_vector == nullptr ? reinterpret_cast<float*>(arr->get_data()) : norm_vector;
        ObString data_str(arr->size() * sizeof(float), reinterpret_cast<char*>(data));
        ObString res_str;
        if (OB_FAIL(ObArrayExprUtils::set_array_res(nullptr,
                                          data_str.length(),
                                          expr,
                                          eval_ctx,
                                          res_str,
                                          data_str.ptr()))) {
          LOG_WARN("fail to set array res", K(ret), K(data_str));
        } else {
          expr_datum.set_string(res_str);
        }
      }
    }
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
