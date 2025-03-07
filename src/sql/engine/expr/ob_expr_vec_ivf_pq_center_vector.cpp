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

#include "sql/engine/expr/ob_expr_vec_ivf_pq_center_vector.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_type/ob_vector_common_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
ObExprVecIVFPQCenterVector::ObExprVecIVFPQCenterVector(ObIAllocator &allocator)
  : ObFuncExprOperator(allocator, T_FUN_SYS_VEC_IVF_PQ_CENTER_VECTOR, N_VEC_IVF_PQ_CENTER_VECTOR, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
  need_charset_convert_ = false;
}

int ObExprVecIVFPQCenterVector::calc_result_typeN(ObExprResType &type,
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

int ObExprVecIVFPQCenterVector::calc_resultN(ObObj &result,
                                  const ObObj *objs_array,
                                  int64_t param_num,
                                  ObExprCtx &expr_ctx) const
{
  // TODO by query ivf index
  return OB_NOT_SUPPORTED;
}

int ObExprVecIVFPQCenterVector::cg_expr(
    ObExprCGCtx &expr_cg_ctx,
    const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  UNUSED(expr_cg_ctx);
  if (OB_UNLIKELY(rt_expr.arg_cnt_ != 1) || OB_UNLIKELY(rt_expr.arg_cnt_ != 4) || OB_ISNULL(rt_expr.args_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(rt_expr.arg_cnt_), KP(rt_expr.args_), K(rt_expr.type_));
  } else {
    rt_expr.eval_func_ = generate_pq_center_vector;
  }
  return ret;
}

int ObExprVecIVFPQCenterVector::generate_pq_center_vector(
    const ObExpr &expr,
    ObEvalCtx &eval_ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *datum = nullptr;
  if (1 == expr.arg_cnt_) {
    expr_datum.set_null();
    LOG_DEBUG("[vec debug] generate empty pq center vector since only one arg", K(ret), K(1 == expr.arg_cnt_));
  } else if (4 == expr.arg_cnt_) {
    // for pq centroid table, return residual vector
    ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx);
    common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
    ObTableID table_id;
    ObTabletID tablet_id;
    ObVectorIndexDistAlgorithm dis_algo = VIDA_MAX;
    ObSEArray<float*, 64> centers;
    bool contain_null = false;
    ObIArrayType *arr = NULL;
    if (OB_FAIL(ObVectorIndexUtil::eval_ivf_centers_common(
        tmp_allocator, expr, eval_ctx, centers, table_id, tablet_id, dis_algo, contain_null, arr))) {
      LOG_WARN("failed to eval ivf centers", K(ret), K(expr), K(eval_ctx));
    } else if (contain_null) {
      // do nothing
    } else {
      ObVectorNormalizeInfo norm_info;
      float *residual_vec = nullptr;
      int64_t center_idx = 0;
      if (OB_FAIL(ObVectorIndexUtil::calc_residual_vector(
          tmp_allocator,
          arr->size(),
          centers,
          reinterpret_cast<float*>(arr->get_data()),
          VIDA_L2 == dis_algo ? nullptr: &norm_info,
          residual_vec))) {
        LOG_WARN("failed to get nearest center", K(ret));
      } else {
        ObString data_str(arr->size() * sizeof(float), reinterpret_cast<char*>(residual_vec));
        ObString res_str;
        if (OB_FAIL(ObArrayExprUtils::set_array_res(nullptr,
                                          data_str.length(),
                                          eval_ctx.get_expr_res_alloc(),
                                          res_str,
                                          data_str.ptr()))) {
          LOG_WARN("fail to set array res", K(ret), K(data_str));
        } else {
          expr_datum.set_string(res_str);
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg cnt", K(ret), K(expr.arg_cnt_));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
