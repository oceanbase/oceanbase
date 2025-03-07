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

#include "sql/engine/expr/ob_expr_vec_ivf_sq8_data_vector.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_type/ob_vector_common_util.h"
#include "sql/engine/ob_exec_context.h"


namespace oceanbase
{
using namespace common;
namespace sql
{
ObExprVecIVFSQ8DataVector::ObExprVecIVFSQ8DataVector(ObIAllocator &allocator)
  : ObFuncExprOperator(allocator, T_FUN_SYS_VEC_IVF_SQ8_DATA_VECTOR, N_VEC_IVF_SQ8_DATA_VECTOR, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
  need_charset_convert_ = false;
}

int ObExprVecIVFSQ8DataVector::calc_result_typeN(ObExprResType &type,
                                       ObExprResType *types,
                                       int64_t param_num,
                                       ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
  ObDataType elem_type;
  elem_type.meta_.set_utinyint();
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

int ObExprVecIVFSQ8DataVector::calc_resultN(ObObj &result,
                                  const ObObj *objs_array,
                                  int64_t param_num,
                                  ObExprCtx &expr_ctx) const
{
  // TODO by query ivf index
  return OB_NOT_SUPPORTED;
}

int ObExprVecIVFSQ8DataVector::cg_expr(
    ObExprCGCtx &expr_cg_ctx,
    const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(rt_expr.arg_cnt_ != 3 && rt_expr.arg_cnt_ != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param count", K(rt_expr.arg_cnt_), K(rt_expr.args_), K(rt_expr.type_));
  } else if (OB_ISNULL(rt_expr.args_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, rt_expr.args_ is nullptr", K(rt_expr.arg_cnt_), K(rt_expr.args_), K(rt_expr.type_));
  } else {
    rt_expr.eval_func_ = generate_data_vector;
  }
  return ret;
}

int ObExprVecIVFSQ8DataVector::cal_u8_data_vector(ObIAllocator &alloc, uint32_t size, float *min_vec, float *step_vec, float *data_vec, uint8_t *&res_vec)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(min_vec) || OB_ISNULL(step_vec) || OB_ISNULL(data_vec)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null pointer", K(ret), KP(min_vec), KP(step_vec), KP(res_vec), KP(data_vec));
  } else if (OB_ISNULL(res_vec = reinterpret_cast<uint8_t *>(alloc.alloc(sizeof(uint8_t) * size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(sizeof(uint8_t) * size));
  }
  for (int i = 0; OB_SUCC(ret) && i < size; ++i) {
    if (fabs(step_vec[i]) < 1e-10) {
      // special case2: max[i] = min[i] -> step[i] = 0, set res_vec to {0}
      res_vec[i] = 0;
    } else {
      int64_t tmp_val = static_cast<int64_t>(floor((data_vec[i] - min_vec[i]) / step_vec[i]));
      if (tmp_val < 0) {
        res_vec[i] = 0;
      } else if (tmp_val > ObIvfConstant::SQ8_META_STEP_SIZE) {
        res_vec[i] = ObIvfConstant::SQ8_META_STEP_SIZE;
      } else {
        res_vec[i] = tmp_val;
      }
    }
  }
  return ret;
}

int ObExprVecIVFSQ8DataVector::generate_data_vector(
    const ObExpr &expr,
    ObEvalCtx &eval_ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (expr.arg_cnt_ == 1) {
    expr_datum.set_null();
    LOG_DEBUG("[vec index debug] sq8 data vector with single argument", KP(&expr), K(expr), K(expr_datum), K(eval_ctx), K(lbt()));
  } else if (OB_UNLIKELY(3 != expr.arg_cnt_) || OB_ISNULL(expr.args_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(expr), KP(expr.args_));
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx);
    common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
    ObExpr *calc_vector_expr = expr.args_[0];
    ObExpr *calc_table_id_expr = expr.args_[1];
    ObExpr *calc_part_id_expr = expr.args_[2];
    ObIArrayType *arr = nullptr;
    bool contain_null = false;
    ObTableID table_id;
    ObTabletID tablet_id;
    if (OB_ISNULL(calc_vector_expr) || calc_vector_expr->datum_meta_.type_ != ObCollectionSQLType) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("calc vector expr is invalid", K(ret), KPC(calc_vector_expr));
    } else if (OB_FAIL(ObArrayExprUtils::get_type_vector(*(calc_vector_expr), eval_ctx, tmp_allocator, arr, contain_null))) {
      LOG_WARN("failed to get vector", K(ret), KPC(calc_vector_expr));
    } else if (contain_null) {
      expr_datum.set_null();
    } else if (OB_FAIL(ObVectorIndexUtil::calc_location_ids(eval_ctx, calc_table_id_expr, calc_part_id_expr, table_id, tablet_id))) {
      LOG_WARN("fail to calc location ids", K(ret), K(table_id), K(tablet_id), KP(calc_table_id_expr), KP(calc_part_id_expr));
    } else {
      ObFixedArray<float*, ObIAllocator> meta_vectors(tmp_allocator);
      share::ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService*);
      float *min_vec = nullptr;
      float *step_vec = nullptr;
      uint8_t *res_vec = nullptr;
      float *data_vec = reinterpret_cast<float*>(arr->get_data());
      ObExprVecIvfCenterIdCache *cache = ObVectorIndexUtil::get_ivf_center_id_cache_ctx(expr.expr_ctx_id_, &eval_ctx.exec_ctx_);
      if (OB_ISNULL(service)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("service is nullptr", K(ret));
      } else if (OB_FAIL(meta_vectors.init(SQ_META_SIZE))) {
        LOG_WARN("fail to init meta vectors", K(ret));
      } else if (OB_FAIL(ObVectorIndexUtil::get_ivf_aux_info(service, cache, table_id, tablet_id, tmp_allocator, meta_vectors))) {
        LOG_WARN("failed to get centers", K(ret), K(table_id), K(tablet_id));
      } else if (meta_vectors.empty()) {
        // special case 1: empty meta table, set res_vec to {0}
        if (OB_ISNULL(res_vec = reinterpret_cast<uint8_t *>(tmp_allocator.alloc(sizeof(uint8_t) * arr->size())))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc uint8_t * failed", K(ret), K(sizeof(uint8_t) * arr->size()));
        } else {
          for (int i = 0; OB_SUCC(ret) && i < arr->size(); ++i) {
            res_vec[i] = 0;
          }
        }
      } else if (OB_FAIL(meta_vectors.at(ObIvfConstant::SQ8_META_MIN_IDX, min_vec))) {
        LOG_WARN("fail to get min vector from meta", K(ret));
      } else if (OB_FAIL(meta_vectors.at(ObIvfConstant::SQ8_META_STEP_IDX, step_vec))) {
        LOG_WARN("fail to get min vector from meta", K(ret));
      } else if (OB_FAIL(cal_u8_data_vector(tmp_allocator, arr->size(), min_vec, step_vec, data_vec, res_vec))) {
        LOG_WARN("fail to cal u8 data vector", K(ret), K(arr->size()));
      }


      if (OB_SUCC(ret)) {
        ObString data_str(arr->size() * sizeof(uint8_t), reinterpret_cast<char*>(res_vec));
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
  }

  return ret;
}


}  // namespace sql
}  // namespace oceanbase
