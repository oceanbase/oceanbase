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
  UNUSEDx(param_num, types, type_ctx);
  type.set_varchar();
  type.set_collation_type(CS_TYPE_BINARY);
  type.set_length(OB_MAX_VARCHAR_LENGTH);
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
  if (OB_UNLIKELY(rt_expr.arg_cnt_ != 9 && rt_expr.arg_cnt_ != 8 && rt_expr.arg_cnt_ != 1 && rt_expr.arg_cnt_ != 4 && rt_expr.arg_cnt_ != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param count", K(rt_expr.arg_cnt_), K(rt_expr.args_), K(rt_expr.type_));
  } else if (OB_UNLIKELY((rt_expr.arg_cnt_ == 9 || rt_expr.arg_cnt_ == 8) && OB_ISNULL(rt_expr.args_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, rt_expr.args_ is nullptr", K(rt_expr.arg_cnt_), K(rt_expr.args_), K(rt_expr.type_));
  } else {
    rt_expr.eval_func_ = calc_pq_center_ids;
  }
  return ret;
}

// [version(uint32_t)][tablet_id(uint64_t)][pq_id(uint32_t)...]
int ObExprVecIVFPQCenterIds::generate_empty_pq_ids(char *buf, int pq_m, int nbits, uint64_t tablet_id)
{
  int ret = OB_SUCCESS;
  int64_t buf_pos = 0;
  // version
  *(int32_t*)buf = ObVecIVFPQCenterIDS::CUR_VERSION;
  buf_pos += ObVecIVFPQCenterIDS::VERSION_SIZE;
  // tablet_id
  *(uint64_t*)(buf + buf_pos) = tablet_id;
  buf_pos += ObVecIVFPQCenterIDS::TABLET_ID_SIZE;
  // 8 or 16 or 32
  int64_t pq_id_size = ObVecIVFPQCenterIDS::get_pq_id_size(nbits);
  PQEncoderGeneric encoder((uint8_t*)(buf + buf_pos), nbits);
  for (int i = 0; i < pq_m; ++i) {
    encoder.encode(0);
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
  } else if (expr.arg_cnt_ == 2 || expr.arg_cnt_ == 4) {
    char *vb_buf = nullptr;
    int64_t res_len = 0;
    uint64_t pq_m = 1;
    uint64_t nbits = 1;
    uint64_t pq_cent_tablet_id = 1;
    if (FALSE_IT(res_len = ObVecIVFPQCenterIDS::get_total_size(pq_m, nbits))) {
    } else if (OB_ISNULL(vb_buf = expr.get_str_res_mem(eval_ctx, res_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc res buf", K(ret), K(res_len), K(expr));
    } else if (OB_FAIL(generate_empty_pq_ids(vb_buf, pq_m, nbits,
                                             pq_cent_tablet_id))) {
      LOG_WARN("fail to gen empty pq ids", K(ret), K(pq_m),
               K(pq_cent_tablet_id));
    }
    if (OB_SUCC(ret)) {
      ObString res_str;
      res_str.assign_ptr(vb_buf, res_len);
      expr_datum.set_string(res_str);
    }
  } else if (OB_UNLIKELY(9 != expr.arg_cnt_ && 8 != expr.arg_cnt_) || OB_ISNULL(expr.args_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(expr), KP(expr.args_));
  } else {
    common::ObArenaAllocator tmp_allocator("IVFPQExprPqCID", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObExpr *calc_vector_expr = expr.args_[0];
    ObExpr *calc_centroid_table_id_expr = expr.args_[1];
    ObExpr *calc_centroid_part_id_expr = expr.args_[2];
    ObExpr *calc_cid_expr = nullptr;
    ObExpr *calc_pq_centroid_table_id_expr = nullptr;
    ObExpr *calc_pq_centroid_part_id_expr = nullptr;
    ObExpr *calc_distance_algo_expr = nullptr;
    ObExpr *calc_pq_m_expr = nullptr;
    ObExpr *calc_nbits_expr = nullptr;
    if (expr.arg_cnt_ == 9) {
      calc_cid_expr = expr.args_[3];
      calc_pq_centroid_table_id_expr = expr.args_[4];
      calc_pq_centroid_part_id_expr = expr.args_[5];
      calc_distance_algo_expr = expr.args_[6];
      calc_pq_m_expr = expr.args_[7];
      calc_nbits_expr = expr.args_[8];
    } else {
      calc_pq_centroid_table_id_expr = expr.args_[3];
      calc_pq_centroid_part_id_expr = expr.args_[4];
      calc_distance_algo_expr = expr.args_[5];
      calc_pq_m_expr = expr.args_[6];
      calc_nbits_expr = expr.args_[7];
    }
    // 0. init
    ObDatum *res = nullptr;
    char *vb_buf = nullptr;
    int64_t res_len = 0;
    int64_t buf_pos = 0;
    if (OB_ISNULL(calc_vector_expr) || OB_ISNULL(calc_distance_algo_expr)
      || OB_ISNULL(calc_centroid_table_id_expr) || OB_ISNULL(calc_centroid_part_id_expr)
      || OB_ISNULL(calc_pq_centroid_table_id_expr) || OB_ISNULL(calc_pq_centroid_part_id_expr)
      || OB_ISNULL(calc_pq_m_expr) || OB_ISNULL(calc_nbits_expr)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null exprs", K(ret),
                                     KP(calc_vector_expr),
                                     KP(calc_distance_algo_expr),
                                     KP(calc_centroid_table_id_expr),
                                     KP(calc_centroid_part_id_expr),
                                     KP(calc_pq_centroid_table_id_expr),
                                     KP(calc_pq_centroid_part_id_expr),
                                     KP(calc_pq_m_expr),
                                     KP(calc_nbits_expr));
    }

    // 1. eval cur vector, pq m and pq tablet location
    ObIArrayType *arr = NULL;
    // is_empty_pq_ids = true means that the cluster center has not been generated or the current input vector is null.
    // In this case, all pq ids is set to 1.
    bool is_empty_pq_ids = false;
    uint64_t pq_m = 0;
    ObTableID pq_cent_table_id;
    ObTabletID pq_cent_tablet_id;
    uint64_t nbits = 0;
    int64_t pq_id_size = 0;
    if (OB_FAIL(ret)) {
    } else if (calc_vector_expr->datum_meta_.type_ != ObCollectionSQLType) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("calc vector expr is invalid", K(ret), KPC(calc_vector_expr));
    } else if (OB_FAIL(ObArrayExprUtils::get_type_vector(*(calc_vector_expr), eval_ctx, tmp_allocator, arr, is_empty_pq_ids))) {
      LOG_WARN("failed to get vector", K(ret), KPC(calc_vector_expr));
    } else if (OB_FAIL(calc_pq_m_expr->eval(eval_ctx, res))) {
      LOG_WARN("calc table id expr failed", K(ret));
    } else if (FALSE_IT(pq_m = res->get_uint64())) {
    } else if (OB_FAIL(calc_nbits_expr->eval(eval_ctx, res))) {
      LOG_WARN("calc table id expr failed", K(ret));
    } else if (FALSE_IT(nbits = res->get_uint64())) {
    } else if (FALSE_IT(pq_id_size = ObVecIVFPQCenterIDS::get_pq_id_size(nbits))) {
    } else if (FALSE_IT(res_len = ObVecIVFPQCenterIDS::get_total_size(pq_m, nbits))) {
    } else if (OB_ISNULL(vb_buf = expr.get_str_res_mem(eval_ctx, res_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc res buf", K(ret), K(res_len), K(expr));
    } else if (OB_FAIL(ObVectorIndexUtil::calc_location_ids(
          eval_ctx,
          calc_pq_centroid_table_id_expr,
          calc_pq_centroid_part_id_expr,
          pq_cent_table_id,
          pq_cent_tablet_id))) {
      LOG_WARN("fail to calc location ids",
                K(ret), K(pq_cent_table_id), K(pq_cent_tablet_id), KP(calc_pq_centroid_table_id_expr), KP(calc_pq_centroid_part_id_expr));
    } else if (is_empty_pq_ids) {
      if (OB_FAIL(generate_empty_pq_ids(vb_buf, pq_m, nbits, pq_cent_tablet_id.id()))) {
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
    ObCenterId center_id;
    float *residual_vec = nullptr;
    share::ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService*);
    ObVectorNormalizeInfo norm_info;
    ObTableID cent_table_id;
    ObTabletID cent_tablet_id;
    ObArray<float *> splited_residual;
    ObIvfCacheMgrGuard cache_guard;
    ObIvfCentCache *cent_cache = nullptr;
    if (OB_FAIL(ret) || is_empty_pq_ids) {
    } else if (OB_FAIL(ObVectorIndexUtil::calc_location_ids(
          eval_ctx,
          calc_centroid_table_id_expr,
          calc_centroid_part_id_expr,
          cent_table_id,
          cent_tablet_id))) {
      LOG_WARN("fail to calc location ids", K(ret), K(cent_table_id), K(cent_tablet_id), KP(calc_centroid_table_id_expr), KP(calc_centroid_part_id_expr));
    } else {
      float *centers_data = nullptr;
      int64_t centers_count = 0;
      int64_t centers_dim = 0;
      bool is_cache_usable = false;
      bool has_hgraph = false;
      if (OB_FAIL(ObVectorIndexUtil::get_ivf_centers_cache(false /*is_vectorized*/, false /*is_pq_centers*/, cache_guard, cent_cache, cent_table_id, cent_tablet_id, is_cache_usable))) {
        LOG_WARN("failed to get centers cache", K(ret));
      } else if (!is_cache_usable) {
        centers_dim = arr->size();
        if (OB_FAIL(service->get_ivf_aux_info(cent_table_id, cent_tablet_id, centers_dim, tmp_allocator, centers_data, centers_count))) {
          LOG_WARN("failed to get centers by scanning table", K(ret), K(cent_table_id), K(cent_tablet_id), K(centers_dim));
        }
      } else if(OB_ISNULL(cent_cache)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("cent cache is null", K(ret));
      } else if (is_cache_usable && !cent_cache->has_hgraph_index()) {
        centers_data = cent_cache->get_centroids();
        if (OB_ISNULL(centers_data)) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("centroids is null", K(ret));
        } else {
          centers_count = cent_cache->get_count();
          centers_dim = cent_cache->get_cent_vec_dim();
        }
      } else if (is_cache_usable && cent_cache->has_hgraph_index()) {
        has_hgraph = true;
      }

      if (OB_SUCC(ret)) {
        bool has_centers = (centers_data != nullptr && centers_count > 0) || has_hgraph;
        if (!has_centers) {
          is_empty_pq_ids = true;
          if (OB_FAIL(generate_empty_pq_ids(vb_buf, pq_m, nbits, pq_cent_tablet_id.id()))) {
            LOG_WARN("fail to gen empty pq ids", K(ret), K(pq_m), K(pq_cent_tablet_id));
          }
        } else if (OB_NOT_NULL(calc_cid_expr)) {
          if (OB_FAIL(calc_cid_expr->eval(eval_ctx, res))) {
            LOG_WARN("failed to eval center id", K(ret));
          } else if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(center_id, res->get_string(),
                            share::ObVectorClusterHelper::IvfParseCentIdFlag::IVF_PARSE_CENTER_ID))) {
            LOG_WARN("failed to get center id", K(ret));
          } else {
            float *center_vec = nullptr;

            if (is_cache_usable && has_hgraph) {
              if (OB_FAIL(cent_cache->read_centroid(center_id.center_id_, center_vec, true /*deep_copy*/, &tmp_allocator))) {
                LOG_WARN("failed to read centroid from cache", K(ret), K(center_id.center_id_));
              }
            } else {
              center_vec = centers_data + (center_id.center_id_ - 1) * centers_dim;
            }
            if (OB_SUCC(ret)) {
              float *norm_vector = nullptr;
              if (dis_algo == VIDA_COS) {
                if (OB_ISNULL(norm_vector = static_cast<float*>(tmp_allocator.alloc(arr->size() * sizeof(float))))) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("failed to alloc norm vector", K(ret));
                } else if (FALSE_IT(MEMSET(norm_vector, 0, arr->size() * sizeof(float)))) {
                } else if (OB_FAIL(norm_info.normalize_func_(arr->size(), reinterpret_cast<float*>(arr->get_data()), norm_vector, nullptr))) {
                  LOG_WARN("failed to normalize vector", K(ret));
                }
              }
              float *data = norm_vector == nullptr ? reinterpret_cast<float*>(arr->get_data()) : norm_vector;
              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(ObVectorIndexUtil::calc_residual_vector(tmp_allocator, arr->size(), data, center_vec, residual_vec))) {
                LOG_WARN("fail to calc residual vector", K(ret), K(arr->size()));
              } else if (OB_FAIL(splited_residual.reserve(pq_m))) {
                LOG_WARN("fail to init splited residual array", K(ret), K(pq_m));
              } else if (OB_FAIL(ObVectorIndexUtil::split_vector(pq_m, arr->size(), residual_vec, splited_residual))) {
                LOG_WARN("fail to split vector", K(ret), K(pq_m), K(arr->size()), KP(residual_vec));
              }
            }
          }
        } else if (has_hgraph) {
          if (OB_FAIL(ObVectorIndexUtil::calc_residual_vector_by_use_hgraph(tmp_allocator,
                                                                            arr->size(),
                                                                            reinterpret_cast<float*>(arr->get_data()),
                                                                            VIDA_COS == dis_algo ? &norm_info : nullptr,
                                                                            cent_cache,
                                                                            residual_vec))) {
            LOG_WARN("failed to calc residual vector by hgraph", K(ret));
          } else if (OB_FAIL(splited_residual.reserve(pq_m))) {
            LOG_WARN("fail to init splited residual array", K(ret), K(pq_m));
          } else if (OB_FAIL(ObVectorIndexUtil::split_vector(pq_m, arr->size(), residual_vec, splited_residual))) {
            LOG_WARN("fail to split vector", K(ret), K(pq_m), K(arr->size()), KP(residual_vec));
          }
        } else {
          // calc_cid_expr is null, need to find nearest center
          // Use calc_residual_vector which will use HGraph if available, or fallback to centers array
          if (OB_FAIL(ObVectorIndexUtil::calc_residual_vector(
              tmp_allocator, arr->size(), centers_data, centers_count, centers_dim, reinterpret_cast<float*>(arr->get_data()),
              VIDA_COS != dis_algo ? nullptr: &norm_info, residual_vec))) { // cos need norm
            LOG_WARN("fail to calc residual vector", K(ret), K(dis_algo));
          } else if (OB_FAIL(splited_residual.reserve(pq_m))) {
            LOG_WARN("fail to init splited residual array", K(ret), K(pq_m));
          } else if (OB_FAIL(ObVectorIndexUtil::split_vector(pq_m, arr->size(), residual_vec, splited_residual))) {
            LOG_WARN("fail to split vector", K(ret), K(pq_m), K(arr->size()), KP(residual_vec));
          }
        }
      }
    }

    // 4. calc pq cent id
    if (OB_SUCC(ret) && !is_empty_pq_ids) {
      int64_t center_size_per_m = 0;
      int64_t pq_dim = arr->size() / pq_m;
      float *pq_centroids_data = nullptr;
      int64_t pq_centroids_count = 0;
      int64_t pq_centroids_dim = 0;
      ObIvfCacheMgrGuard pq_cache_guard;
      ObIvfCentCache *pq_cent_cache = nullptr;
      bool is_pq_cache_usable = false;
      if (OB_FAIL(ObVectorIndexUtil::get_ivf_centers_cache(false /*is_vectorized*/, true /*is_pq_centers*/, pq_cache_guard, pq_cent_cache, pq_cent_table_id, cent_tablet_id, is_pq_cache_usable))) {
        LOG_WARN("failed to get pq centers cache", K(ret));
      } else if (!is_pq_cache_usable) {
        pq_centroids_dim = pq_dim;
        if (OB_FAIL(service->get_ivf_aux_info(pq_cent_table_id, pq_cent_tablet_id, pq_dim, tmp_allocator, pq_centroids_data, pq_centroids_count))) {
          LOG_WARN("failed to get pq centers by scanning table", K(ret), K(pq_cent_table_id), K(pq_cent_tablet_id), K(pq_dim));
        }
      } else {
        pq_centroids_data = pq_cent_cache->get_centroids();
        if (OB_ISNULL(pq_centroids_data)) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("centroids is null", K(ret));
        } else {
          pq_centroids_count = pq_cent_cache->get_count();
          pq_centroids_dim = pq_cent_cache->get_cent_vec_dim();
        }
      }
      if (OB_SUCC(ret)) {
        if (pq_centroids_count == 0 || pq_centroids_count % pq_m != 0) {
          ret = OB_INVALID_ARGUMENT;
          SQL_RESV_LOG(ERROR, "invalid size of pq centers", K(ret), K(pq_centroids_count), K(pq_m));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT,
            "size of pq centers, should be greater than zero and able to devide m exactly");
        } else {
          center_size_per_m = pq_centroids_count / pq_m;
        }
      }
      ObVectorClusterHelper helper;
      int64_t pq_center_idx = 0;
      // version
      *(int32_t*)vb_buf = ObVecIVFPQCenterIDS::CUR_VERSION;
      buf_pos += ObVecIVFPQCenterIDS::VERSION_SIZE;
      // tablet_id
      *(uint64_t*)(vb_buf + buf_pos) = pq_cent_tablet_id.id();
      buf_pos += ObVecIVFPQCenterIDS::TABLET_ID_SIZE;
      // row_i = pq_centers[m_id - 1][center_id - 1] since m_id and center_id start from 1
      PQEncoderGeneric encoder((uint8_t*)(vb_buf + buf_pos), nbits);
      for (int i = 0; OB_SUCC(ret) && i < pq_m; ++i) {
        int l_idx = i * center_size_per_m;
        int r_idx = (i + 1) * center_size_per_m;
        if (OB_FAIL(helper.get_nearest_probe_centers(
            splited_residual.at(i),
            pq_dim,
            pq_centroids_data,
            pq_centroids_count,
            pq_centroids_dim,
            1/*nprobe*/,
            tmp_allocator,
            nullptr, // no need normlize, Reference faiss
            l_idx,
            r_idx))) {
          LOG_WARN("failed to get nearest center", K(ret));
        } else if (OB_FAIL(helper.get_pq_center_idx(0/*idx*/, center_size_per_m, pq_center_idx))) {
          LOG_WARN("failed to get center idx", K(ret));
        } else {
          encoder.encode(pq_center_idx - 1);
          helper.reset();
        }
      }
    }

    // 5. set result
    if (OB_SUCC(ret)) {
      ObString res_str;
      res_str.assign_ptr(vb_buf, res_len);
      expr_datum.set_string(res_str);
    }
  }
  return ret;
}


}  // namespace sql
}  // namespace oceanbase
