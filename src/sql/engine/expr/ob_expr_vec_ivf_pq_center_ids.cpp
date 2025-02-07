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

#include "sql/engine/expr/ob_expr_vec_ivf_pq_center_ids.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_type/ob_vector_common_util.h"
#include "share/ob_vec_index_builder_util.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{
ObExprVecIVFPQCenterIds::ObExprVecIVFPQCenterIds(ObIAllocator &allocator)
  : ObFuncExprOperator(allocator, T_FUN_SYS_VEC_IVF_PQ_CENTER_IDS, N_VEC_IVF_PQ_CENTER_IDS, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
  need_charset_convert_ = false;
}

int ObExprVecIVFPQCenterIds::calc_result_typeN(ObExprResType &type,
                                       ObExprResType *types,
                                       int64_t param_num,
                                       ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSEDx(param_num, types);
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
  uint16_t subschema_id = 0;
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null exec_ctx", K(ret), KP(session));
  } else if (OB_FAIL(exec_ctx->get_subschema_id_by_type_string(ObVecIndexBuilderUtil::IVF_PQ_CENTER_IDS_COL_TYPE_NAME, subschema_id))) {
    LOG_WARN("failed to get type1 child subschema id", K(ret));
  } else {
    type.set_collection(subschema_id);
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObCollectionSQLType]).get_length());
  }
  return ret;
}

int ObExprVecIVFPQCenterIds::calc_resultN(ObObj &result,
                                  const ObObj *objs_array,
                                  int64_t param_num,
                                  ObExprCtx &expr_ctx) const
{
  // TODO by query ivf index
  return OB_NOT_SUPPORTED;
}

int ObExprVecIVFPQCenterIds::cg_expr(
    ObExprCGCtx &expr_cg_ctx,
    const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(rt_expr.arg_cnt_ != 7 && rt_expr.arg_cnt_ != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param count", K(rt_expr.arg_cnt_), K(rt_expr.args_), K(rt_expr.type_));
  } else if (OB_UNLIKELY(rt_expr.arg_cnt_ == 7 && OB_ISNULL(rt_expr.args_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, rt_expr.args_ is nullptr", K(rt_expr.arg_cnt_), K(rt_expr.args_), K(rt_expr.type_));
  } else {
    rt_expr.eval_func_ = calc_pq_center_ids;
  }
  return ret;
}

int ObExprVecIVFPQCenterIds::generate_empty_pq_ids(
    ObIAllocator &allocator,
    int pq_m,
    const ObTabletID &pq_cent_tablet_id,
    ObArrayBinary &arr_binary)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < pq_m; ++i) {
    ObPqCenterId pq_center_id(pq_cent_tablet_id.id(), i + 1, 0/*center_id*/);
    ObString pq_center_id_str;
    if (OB_FAIL(ObVectorClusterHelper::set_pq_center_id_to_string(pq_center_id, pq_center_id_str, &allocator))) {
      LOG_WARN("fail to set pq center id to string", K(ret), K(pq_center_id));
    } else if (OB_FAIL(arr_binary.push_back(pq_center_id_str))) {
      LOG_WARN("failed to push back null value", K(ret));
    }
  }
  return ret;
}

int ObExprVecIVFPQCenterIds::calc_pq_center_ids(
    const ObExpr &expr,
    ObEvalCtx &eval_ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (expr.arg_cnt_ == 1) {
    expr_datum.set_null();
    LOG_DEBUG("[vec index debug]succeed to genearte empty pq scenter id", KP(&expr), K(expr), K(expr_datum), K(eval_ctx));
  } else if (OB_UNLIKELY(7 != expr.arg_cnt_) || OB_ISNULL(expr.args_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(expr), KP(expr.args_));
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx);
    common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
    ObExpr *calc_vector_expr = expr.args_[0];
    ObExpr *calc_centroid_table_id_expr = expr.args_[1];
    ObExpr *calc_centroid_part_id_expr = expr.args_[2];
    ObExpr *calc_pq_centroid_table_id_expr = expr.args_[3];
    ObExpr *calc_pq_centroid_part_id_expr = expr.args_[4];
    ObExpr *calc_distance_algo_expr = expr.args_[5];
    ObExpr *calc_pq_m_expr = expr.args_[6];
    // 0. init
    ObIArrayType *res_arr = nullptr;
    ObDatum *res = nullptr;
    const uint16_t res_subschema_id = expr.obj_meta_.get_subschema_id();
    ObArrayBinary *res_binary_array = nullptr;
    if (OB_ISNULL(calc_vector_expr) || OB_ISNULL(calc_distance_algo_expr)
      || OB_ISNULL(calc_centroid_table_id_expr) || OB_ISNULL(calc_centroid_part_id_expr)
      || OB_ISNULL(calc_pq_centroid_table_id_expr) || OB_ISNULL(calc_pq_centroid_part_id_expr)
      || OB_ISNULL(calc_pq_m_expr)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null exprs", K(ret),
                                     KP(calc_vector_expr),
                                     KP(calc_distance_algo_expr),
                                     KP(calc_centroid_table_id_expr),
                                     KP(calc_centroid_part_id_expr),
                                     KP(calc_pq_centroid_table_id_expr),
                                     KP(calc_pq_centroid_part_id_expr),
                                     KP(calc_pq_m_expr));
    } else if (OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_allocator, eval_ctx, res_subschema_id, res_arr, false/*read_only*/))) {
      LOG_WARN("construct child array obj failed", K(ret));
    } else if (OB_ISNULL(res_binary_array = static_cast<ObArrayBinary *>(res_arr))) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null ObIArrayType", K(ret));
    }

    // 1. eval cur vector, pq m and pq tablet location
    ObIArrayType *arr = NULL;
    // is_empty_pq_ids = true means that the cluster center has not been generated or the current input vector is null.
    // In this case, all pq ids is set to 1.
    bool is_empty_pq_ids = false;
    uint64_t pq_m = 0;
    ObTableID pq_cent_table_id;
    ObTabletID pq_cent_tablet_id;
    if (OB_FAIL(ret)) {
    } else if (calc_vector_expr->datum_meta_.type_ != ObCollectionSQLType) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("calc vector expr is invalid", K(ret), KPC(calc_vector_expr));
    } else if (OB_FAIL(ObArrayExprUtils::get_type_vector(*(calc_vector_expr), eval_ctx, tmp_allocator, arr, is_empty_pq_ids))) {
      LOG_WARN("failed to get vector", K(ret), KPC(calc_vector_expr));
    } else if (OB_FAIL(calc_pq_m_expr->eval(eval_ctx, res))) {
      LOG_WARN("calc table id expr failed", K(ret));
    } else if (FALSE_IT(pq_m = res->get_uint64())) {
    } else if (OB_FAIL(ObVectorIndexUtil::calc_location_ids(
          eval_ctx,
          calc_pq_centroid_table_id_expr,
          calc_pq_centroid_part_id_expr,
          pq_cent_table_id,
          pq_cent_tablet_id))) {
      LOG_WARN("fail to calc location ids",
                K(ret), K(pq_cent_table_id), K(pq_cent_tablet_id), KP(calc_pq_centroid_table_id_expr), KP(calc_pq_centroid_part_id_expr));
    } else if (is_empty_pq_ids) {
      if (OB_FAIL(generate_empty_pq_ids(tmp_allocator, pq_m, pq_cent_tablet_id, *res_binary_array))) {
        LOG_WARN("fail to gen empty pq ids", K(ret), K(pq_m), K(pq_cent_tablet_id));
      }
    } else if (OB_ISNULL(arr) || pq_m > arr->size()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected pq_m or arr type", K(ret), K(pq_m), KP(arr));
    }

    // 2. get dist algorithm
    ObVectorIndexDistAlgorithm dis_algo = VIDA_MAX;
    if (OB_FAIL(ret) || is_empty_pq_ids) {
    } else if (calc_distance_algo_expr->datum_meta_.type_ != ObUInt64Type) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("calc distance algo expr is invalid", K(ret), KPC(calc_distance_algo_expr));
    } else if (OB_FAIL(calc_distance_algo_expr->eval(eval_ctx, res))) {
      LOG_WARN("calc table id expr failed", K(ret));
    } else if (FALSE_IT(dis_algo = static_cast<ObVectorIndexDistAlgorithm>(res->get_uint64()))) {
    } else if (VIDA_MAX <= dis_algo) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected distance algo", K(ret), K(dis_algo));
    }

    // 3. calc residul vec
    int64_t center_idx = 0;
    float *residual_vec = nullptr;
    share::ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService*);
    ObVectorNormalizeInfo norm_info;
    ObTableID cent_table_id;
    ObTabletID cent_tablet_id;
    ObArray<float *> splited_residual;
    ObExprVecIvfCenterIdCache *cache = nullptr;
    ObExprVecIvfCenterIdCache *pq_cache = nullptr;
    ObVectorIndexUtil::get_ivf_pq_center_id_cache_ctx(expr.expr_ctx_id_, &eval_ctx.exec_ctx_, cache, pq_cache);
    if (OB_FAIL(ret) || is_empty_pq_ids) {
    } else if (OB_FAIL(ObVectorIndexUtil::calc_location_ids(
          eval_ctx,
          calc_centroid_table_id_expr,
          calc_centroid_part_id_expr,
          cent_table_id,
          cent_tablet_id))) {
      LOG_WARN("fail to calc location ids", K(ret), K(cent_table_id), K(cent_tablet_id), KP(calc_centroid_table_id_expr), KP(calc_centroid_part_id_expr));
    } else {
      ObSEArray<float*, 64> centers;
      if (OB_FAIL(ObVectorIndexUtil::get_ivf_aux_info(service, cache, cent_table_id, cent_tablet_id, tmp_allocator, centers))) {
        LOG_WARN("failed to get centers", K(ret));
      } else if (centers.empty()) {
        is_empty_pq_ids = true;
        if (OB_FAIL(generate_empty_pq_ids(tmp_allocator, pq_m, pq_cent_tablet_id, *res_binary_array))) {
          LOG_WARN("fail to gen empty pq ids", K(ret), K(pq_m), K(pq_cent_tablet_id));
        }
      } else if (OB_FAIL(ObVectorIndexUtil::calc_residual_vector(
          tmp_allocator, arr->size(), centers, reinterpret_cast<float*>(arr->get_data()),
          VIDA_L2 == dis_algo ? nullptr: &norm_info, residual_vec))) {
        LOG_WARN("fail to calc residual vector", K(ret), K(dis_algo));
      } else if (OB_FAIL(splited_residual.reserve(pq_m))) {
        LOG_WARN("fail to init splited residual array", K(ret), K(pq_m));
      } else if (OB_FAIL(ObVectorIndexUtil::split_vector(tmp_allocator, pq_m, arr->size(), residual_vec, splited_residual))) {
        LOG_WARN("fail to split vector", K(ret), K(pq_m), K(arr->size()), KP(residual_vec));
      }
    }

    // 4. calc pq cent id
    if (OB_SUCC(ret) && !is_empty_pq_ids) {
      ObSEArray<float*, 64> pq_centers;
      int64_t center_size_per_m = 0;
      int64_t pq_dim = arr->size() / pq_m;
      if (OB_FAIL(ObVectorIndexUtil::get_ivf_aux_info(service, pq_cache, pq_cent_table_id, pq_cent_tablet_id, tmp_allocator, pq_centers))) {
        LOG_WARN("failed to get centers", K(ret));
      } else if (pq_centers.count() % pq_m != 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid pq centers", K(ret), K(pq_m), K(pq_centers.count()));
      } else {
        center_size_per_m = pq_centers.count() / pq_m;
      }
      ObVectorClusterHelper helper;
      int64_t pq_center_idx = 0;
      // row_i = pq_centers[m_id - 1][center_id - 1] since m_id and center_id start from 1
      for (int i = 0; OB_SUCC(ret) && i < pq_m; ++i) {
        if (OB_FAIL(helper.get_nearest_probe_centers(
            splited_residual.at(i),
            pq_dim,
            pq_centers,
            1/*nprobe*/,
            tmp_allocator,
            VIDA_L2 == dis_algo ? nullptr: &norm_info,
            i * center_size_per_m,
            (i + 1) * center_size_per_m))) {
          LOG_WARN("failed to get nearest center", K(ret));
        } else if (OB_FAIL(helper.get_center_idx(0, pq_center_idx))) {
          LOG_WARN("failed to get center idx", K(ret));
        } else {
          ObPqCenterId pq_center_id(pq_cent_tablet_id.id(), i + 1, pq_center_idx % center_size_per_m + 1);
          ObString pq_center_id_str;
          if (OB_FAIL(ObVectorClusterHelper::set_pq_center_id_to_string(pq_center_id, pq_center_id_str, &tmp_allocator))) {
            LOG_WARN("fail to set pq center id to string", K(ret), K(pq_center_id));
          } else if (OB_FAIL(res_binary_array->push_back(pq_center_id_str))) {
            LOG_WARN("failed to push back null value", K(ret));
          } else {
            helper.reset();
          }
        }
      }
    }

    // 5. set result
    if (OB_SUCC(ret)) {
      ObString res_str;
      if (OB_FAIL(ObArrayExprUtils::set_array_res(
              res_arr, res_arr->get_raw_binary_len(), expr, eval_ctx, res_str))) {
        LOG_WARN("get array binary string failed", K(ret));
      } else {
        expr_datum.set_string(res_str);
      }
    }
  }
  return ret;
}


}  // namespace sql
}  // namespace oceanbase
