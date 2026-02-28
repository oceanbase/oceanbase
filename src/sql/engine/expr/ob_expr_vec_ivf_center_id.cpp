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

#include "sql/engine/expr/ob_expr_vec_ivf_center_id.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_type/ob_vector_common_util.h"
#include "share/vector_index/ob_vector_index_ivf_cache_mgr.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
ObExprVecIVFCenterID::ObExprVecIVFCenterID(ObIAllocator &allocator)
  : ObFuncExprOperator(allocator, T_FUN_SYS_VEC_IVF_CENTER_ID, N_VEC_IVF_CENTER_ID, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
  need_charset_convert_ = false;
}

int ObExprVecIVFCenterID::calc_result_typeN(ObExprResType &type,
                                       ObExprResType *types,
                                       int64_t param_num,
                                       ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSEDx(param_num, types);
  type.set_varchar();
  type.set_collation_type(CS_TYPE_BINARY);
  return ret;
}

int ObExprVecIVFCenterID::calc_resultN(ObObj &result,
                                  const ObObj *objs_array,
                                  int64_t param_num,
                                  ObExprCtx &expr_ctx) const
{
  // TODO by query ivf index
  return OB_NOT_SUPPORTED;
}

int ObExprVecIVFCenterID::cg_expr(
    ObExprCGCtx &expr_cg_ctx,
    const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(rt_expr.arg_cnt_ != 4 && rt_expr.arg_cnt_ != 1 && rt_expr.arg_cnt_ != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param count", K(rt_expr.arg_cnt_), K(rt_expr.args_), K(rt_expr.type_));
  } else if (OB_UNLIKELY(rt_expr.arg_cnt_ == 4 && OB_ISNULL(rt_expr.args_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, rt_expr.args_ is nullptr", K(rt_expr.arg_cnt_), K(rt_expr.args_), K(rt_expr.type_));
  } else {
    rt_expr.eval_func_ = calc_center_id;
  }
  return ret;
}

int ObExprVecIVFCenterID::calc_center_id(
    const ObExpr &expr,
    ObEvalCtx &eval_ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (expr.arg_cnt_ == 1) {
    expr_datum.set_null();
    LOG_DEBUG("[vec index debug]succeed to genearte empty center id", KP(&expr), K(expr), K(expr_datum), K(eval_ctx));
  } else if (expr.arg_cnt_ == 2) {
    int64_t buf_len = OB_DOC_ID_COLUMN_BYTE_LENGTH;
    char *buf = expr.get_str_res_mem(eval_ctx, buf_len);
    ObString str(buf_len, 0, buf);
    ObCenterId center_id(1, 0);
    if (OB_FAIL(ObVectorClusterHelper::set_center_id_to_string(center_id, str))) {
      LOG_WARN("failed to set center_id to string", K(ret), K(center_id), K(str));
    } else {
      expr_datum.set_string(str);
    }
  } else if (OB_UNLIKELY(4 != expr.arg_cnt_) || OB_ISNULL(expr.args_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(expr), KP(expr.args_));
  } else {
    common::ObArenaAllocator tmp_allocator("IVFExprCID", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObTableID table_id;
    ObTabletID tablet_id;
    ObVectorIndexDistAlgorithm dis_algo = VIDA_MAX;
    bool contain_null = false;
    ObIArrayType *arr = NULL;
    int64_t center_idx = 0; // use 0 as center idx if vector is null

    // First, parse expression arguments to get vector, table_id, tablet_id, etc.
    if (OB_FAIL(ObVectorIndexUtil::eval_ivf_centers_common(
        tmp_allocator, expr, eval_ctx, table_id, tablet_id, dis_algo, contain_null, arr))) {
      LOG_WARN("failed to eval ivf centers", K(ret), K(expr), K(eval_ctx));
    } else if (contain_null) {
      // do nothing, center_idx remains 0
    } else {
      // Try to get cache
      ObIvfCacheMgrGuard cache_guard;
      ObIvfCentCache *cent_cache = nullptr;
      bool is_cache_usable = false;
      float *centers_data = nullptr;
      int64_t centers_count = 0;
      int64_t centers_dim = 0;

      if (OB_FAIL(ObVectorIndexUtil::get_ivf_centers_cache(false /*is_vectorized*/, false /*is_pq_centers*/, cache_guard, cent_cache, table_id, tablet_id, is_cache_usable))) {
      LOG_WARN("failed to get centers cache", K(ret));
      } else if (is_cache_usable && cent_cache->has_hgraph_index()) {
        // Cache available and has HGraph index, use HGraph search directly
        float *vector_data = reinterpret_cast<float*>(arr->get_data());
        if (OB_FAIL(ObVectorIndexUtil::get_nearest_center_with_hgraph(vector_data, cent_cache, center_idx))) {
          LOG_WARN("failed to get nearest center with hgraph", K(ret));
        } else {
          LOG_DEBUG("HGraph search succeeded", K(center_idx), K(tablet_id));
        }
      } else {
        if (is_cache_usable) {
          // Cache available but no HGraph index
          if (OB_ISNULL(cent_cache)) {
            ret = OB_ERR_NULL_VALUE;
            LOG_WARN("cent cache is null", K(ret));
          } else {
            centers_data = cent_cache->get_centroids();
            if (OB_ISNULL(centers_data)) {
              ret = OB_ERR_NULL_VALUE;
              LOG_WARN("centroids is null", K(ret));
            } else {
              centers_count = cent_cache->get_count();
              centers_dim = cent_cache->get_cent_vec_dim();
            }
          }
        } else {
          ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService*);
          if (OB_ISNULL(service)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("service is nullptr", K(ret));
          } else {
            centers_dim = arr->size();
            if (OB_FAIL(service->get_ivf_aux_info(table_id, tablet_id, centers_dim, tmp_allocator, centers_data, centers_count))) {
              LOG_WARN("failed to get centers by scanning table", K(ret), K(table_id), K(tablet_id), K(centers_dim));
            }
          }
        }

        if (OB_SUCC(ret) && centers_data != nullptr) {
          ObVectorClusterHelper helper;
          ObVectorNormalizeInfo norm_info;
          if (OB_FAIL(helper.get_nearest_probe_centers(
              reinterpret_cast<float*>(arr->get_data()),
              arr->size(),
              centers_data,
              centers_count,
              centers_dim,
              1/*nprobe*/,
              tmp_allocator,
              VIDA_COS != dis_algo ? nullptr: &norm_info))) {
            LOG_WARN("failed to get nearest center", K(ret));
          } else if (OB_FAIL(helper.get_center_idx(0, center_idx))) {
            LOG_WARN("failed to get center idx", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      int64_t buf_len = OB_DOC_ID_COLUMN_BYTE_LENGTH;
      char *buf = expr.get_str_res_mem(eval_ctx, buf_len);
      ObString str(buf_len, 0, buf);
      ObCenterId center_id(tablet_id.id(), center_idx);
      if (OB_FAIL(ObVectorClusterHelper::set_center_id_to_string(center_id, str))) {
        LOG_WARN("failed to set center_id to string", K(ret), K(center_id), K(str));
      } else {
        expr_datum.set_string(str);
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
