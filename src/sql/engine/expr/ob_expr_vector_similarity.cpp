/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_vector_similarity.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "share/vector_type/ob_vector_norm.h"
#include "share/vector_type/ob_vector_common_util.h"

namespace oceanbase
{
namespace sql
{
ObExprVectorSimilarity::ObExprVectorSimilarity(ObIAllocator &alloc)
    : ObExprVector(alloc, T_FUN_SYS_VECTOR_SIMILARITY, N_VECTOR_SIMILARITY, TWO_OR_THREE, NOT_ROW_DIMENSION)
{}

ObExprVectorSimilarity::ObExprVectorSimilarity(
    ObIAllocator &alloc,
    ObExprOperatorType type,
    const char *name,
    int32_t param_num,
    int32_t dimension)
      : ObExprVector(alloc, type, name, param_num, dimension)
{}

int ObExprVectorSimilarity::calc_result_typeN(
    ObExprResType &type,
    ObExprResType *types_stack,
    int64_t param_num,
    common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num > 3)) {
    ObString func_name_(get_name());
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else if (OB_FAIL(calc_result_type2(type, types_stack[0], types_stack[1], type_ctx))) {
    LOG_WARN("failed to calc result type", K(ret));
  }
  return ret;
}

int ObExprVectorSimilarity::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                  ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = ObExprVectorSimilarity::calc_similarity;
  return ret;
}

int ObExprVectorSimilarity::calc_similarity(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObVecSimilarityType similarity_type = ObVecSimilarityType::COSINE; // default metric
  if (3 == expr.arg_cnt_) {
    ObDatum *datum = NULL;
    if (OB_FAIL(expr.args_[2]->eval(ctx, datum))) {
      LOG_WARN("eval failed", K(ret));
    } else if (datum->is_null()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arg", K(ret), K(*datum));
    } else {
      similarity_type = static_cast<ObVecSimilarityType>(datum->get_int());
    }
  }
  if (FAILEDx(calc_similarity(expr, ctx, res_datum, similarity_type))) {
    LOG_WARN("failed to calc similarity", K(ret), K(similarity_type));
  }
  return ret;
}

int ObExprVectorSimilarity::calc_similarity(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum, ObVecSimilarityType similarity_type)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObIArrayType *arr_l = NULL;
  ObIArrayType *arr_r = NULL;
  bool contain_null = false;
  double similarity = 0.0;
  if (similarity_type < ObVecSimilarityType::COSINE || similarity_type >= ObVecSimilarityType::MAX_TYPE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect similarity type", K(ret), K(similarity_type));
  } else if (OB_FAIL(ObArrayExprUtils::get_type_vector(*(expr.args_[0]), ctx, tmp_allocator, arr_l, contain_null))) {
    LOG_WARN("failed to get vector", K(ret), K(*expr.args_[0]));
  } else if (OB_FAIL(ObArrayExprUtils::get_type_vector(*(expr.args_[1]), ctx, tmp_allocator, arr_r, contain_null))) {
    LOG_WARN("failed to get vector", K(ret), K(*expr.args_[1]));
  } else if (contain_null) {
    res_datum.set_null();
  } else if (OB_ISNULL(arr_l) || OB_ISNULL(arr_r)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(arr_l), K(arr_r));
  } else if ((arr_l->get_array_type()->is_sparse_vector_type() || arr_r->get_array_type()->is_sparse_vector_type())) {
    LOG_WARN("calc similarity for sparse vector is not supported", K(ret));
  } else {
    if (OB_UNLIKELY(arr_l->size() != arr_r->size())) {
      ret = OB_ERR_INVALID_VECTOR_DIM;
      LOG_WARN("check array validty failed", K(ret), K(arr_l->size()), K(arr_r->size()));
    } else if (arr_l->contain_null() || arr_r->contain_null()) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("array with null can't calculate vector similarity", K(ret));
    } else if (SimilarityFunc<float>::similarity_funcs[similarity_type] == nullptr) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support", K(ret), K(similarity_type));
    } else {
      float *data_l = reinterpret_cast<float*>(arr_l->get_data());
      float *data_r = reinterpret_cast<float*>(arr_r->get_data());
      const int64_t size = static_cast<int64_t>(arr_l->size());

      if (similarity_type == ObVecSimilarityType::COSINE || similarity_type == ObVecSimilarityType::DOT) {
        float *data_norm_l = nullptr;
        float *data_norm_r = nullptr;
        if (OB_ISNULL(data_norm_l = static_cast<float *>(tmp_allocator.alloc(size * sizeof(float))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(ret));
        } else if (OB_ISNULL(data_norm_r = static_cast<float *>(tmp_allocator.alloc(size * sizeof(float))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(ret));
        } else if (OB_FAIL(share::ObVectorNormalize::L2_normalize_vector(size, data_l, data_norm_l)) ||
            OB_FAIL(share::ObVectorNormalize::L2_normalize_vector(size, data_r, data_norm_r))) {
          LOG_WARN("fail to normalize vectors", K(ret));
        } else {
          data_l = data_norm_l;
          data_r = data_norm_r;
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(SimilarityFunc<float>::similarity_funcs[similarity_type](data_l, data_r, size, similarity))) {
        if (OB_ERR_NULL_VALUE == ret) {
          res_datum.set_null();
          ret = OB_SUCCESS; // ignore
        } else {
          LOG_WARN("failed to calc similarity", K(ret), K(similarity_type));
        }
      } else {
        res_datum.set_double(similarity);
      }
    }
  }

  return ret;
}

ObExprVectorL2Similarity::ObExprVectorL2Similarity(ObIAllocator &alloc)
    : ObExprVectorSimilarity(alloc, T_FUN_SYS_VECTOR_L2_SIMILARITY, N_VECTOR_L2_SIMILARITY, 2, NOT_ROW_DIMENSION) {}

int ObExprVectorL2Similarity::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                    ObExpr &rt_expr) const
{
    int ret = OB_SUCCESS;
    rt_expr.eval_func_ = ObExprVectorL2Similarity::calc_l2_similarity;
    return ret;
}

int ObExprVectorL2Similarity::calc_l2_similarity(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  return ObExprVectorSimilarity::calc_similarity(expr, ctx, res_datum, ObVecSimilarityType::EUCLIDEAN);
}

ObExprVectorCosineSimilarity::ObExprVectorCosineSimilarity(ObIAllocator &alloc)
    : ObExprVectorSimilarity(alloc, T_FUN_SYS_VECTOR_COS_SIMILARITY, N_VECTOR_COS_SIMILARITY, 2, NOT_ROW_DIMENSION) {}

int ObExprVectorCosineSimilarity::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                    ObExpr &rt_expr) const
{
    int ret = OB_SUCCESS;
    rt_expr.eval_func_ = ObExprVectorCosineSimilarity::calc_cosine_similarity;
    return ret;
}

int ObExprVectorCosineSimilarity::calc_cosine_similarity(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  return ObExprVectorSimilarity::calc_similarity(expr, ctx, res_datum, ObVecSimilarityType::COSINE);
}

ObExprVectorIPSimilarity::ObExprVectorIPSimilarity(ObIAllocator &alloc)
    : ObExprVectorSimilarity(alloc, T_FUN_SYS_VECTOR_IP_SIMILARITY, N_VECTOR_INNER_PRODUCT_SIMILARITY, 2, NOT_ROW_DIMENSION) {}

int ObExprVectorIPSimilarity::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                    ObExpr &rt_expr) const
{
    int ret = OB_SUCCESS;
    rt_expr.eval_func_ = ObExprVectorIPSimilarity::calc_ip_similarity;
    return ret;
}

int ObExprVectorIPSimilarity::calc_ip_similarity(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  return ObExprVectorSimilarity::calc_similarity(expr, ctx, res_datum, ObVecSimilarityType::DOT);
}

} // sql
} // oceanbase
