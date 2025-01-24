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
#include "sql/engine/expr/ob_expr_vector.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "share/vector_type/ob_vector_l2_distance.h"
#include "share/vector_type/ob_vector_cosine_distance.h"
#include "share/vector_type/ob_vector_ip_distance.h"
#include "share/vector_type/ob_vector_norm.h"
#include "share/vector_type/ob_vector_l1_distance.h"

namespace oceanbase
{
namespace sql
{
ObExprVector::ObExprVector(ObIAllocator &alloc,
                         ObExprOperatorType type,
                         const char *name,
                         int32_t param_num,
                         int32_t dimension) : ObFuncExprOperator(alloc, type, name, param_num, VALID_FOR_GENERATED_COL, dimension)
{
}

// [a,b,c,...] is array type, there is no dim_cnt_ in ObCollectionArrayType
int ObExprVector::calc_result_type2(
    ObExprResType &type,
    ObExprResType &type1,
    ObExprResType &type2,
    common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  uint16_t unused_id = UINT16_MAX;
  if (OB_FAIL(ObArrayExprUtils::calc_cast_type2(type1, type2, type_ctx, unused_id))) {
    LOG_WARN("failed to calc cast type", K(ret), K(type1));
  } else {
    type.set_type(ObDoubleType);
    type.set_calc_type(ObDoubleType);
  }
  return ret;
}

int ObExprVector::calc_result_type1(
    ObExprResType &type,
    ObExprResType &type1,
    common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObArrayExprUtils::calc_cast_type(type1, type_ctx))) {
    LOG_WARN("failed to calc cast type", K(ret), K(type1));
  } else {
    type.set_type(ObDoubleType);
    type.set_calc_type(ObDoubleType);
  }
  return ret;
}

ObExprVectorDistance::ObExprVectorDistance(ObIAllocator &alloc)
    : ObExprVector(alloc, T_FUN_SYS_VECTOR_DISTANCE, N_VECTOR_DISTANCE, TWO_OR_THREE, NOT_ROW_DIMENSION)
{}

ObExprVectorDistance::ObExprVectorDistance(
    ObIAllocator &alloc,
    ObExprOperatorType type,
    const char *name,
    int32_t param_num,
    int32_t dimension)
      : ObExprVector(alloc, type, name, param_num, dimension)
{}

ObExprVectorDistance::FuncPtrType ObExprVectorDistance::distance_funcs[] =
{
  ObVectorCosineDistance::cosine_distance_func,
  ObVectorIpDistance::ip_distance_func,
  ObVectorL2Distance::l2_distance_func,
  ObVectorL1Distance::l1_distance_func,
  ObVectorL2Distance::l2_square_func,
  nullptr,
};

int ObExprVectorDistance::calc_result_typeN(
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

int ObExprVectorDistance::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                  ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = ObExprVectorDistance::calc_distance;
  return ret;
}

int ObExprVectorDistance::calc_distance(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObVecDisType dis_type = ObVecDisType::EUCLIDEAN; // default metric
  if (3 == expr.arg_cnt_) {
    ObDatum *datum = NULL;
    if (OB_FAIL(expr.args_[2]->eval(ctx, datum))) {
      LOG_WARN("eval failed", K(ret));
    } else if (datum->is_null()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arg", K(ret), K(*datum));
    } else {
      dis_type = static_cast<ObVecDisType>(datum->get_int());
    }
  }
  if (FAILEDx(calc_distance(expr, ctx, res_datum, dis_type))) {
    LOG_WARN("failed to calc distance", K(ret), K(dis_type));
  }
  return ret;
}

int ObExprVectorDistance::calc_distance(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum, ObVecDisType dis_type)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObIArrayType *arr_l = NULL;
  ObIArrayType *arr_r = NULL;
  bool contain_null = false;
  if (dis_type < ObVecDisType::COSINE || dis_type >= ObVecDisType::MAX_TYPE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect distance type", K(ret), K(dis_type));
  } else if (OB_FAIL(ObArrayExprUtils::get_type_vector(*(expr.args_[0]), ctx, tmp_allocator, arr_l, contain_null))) {
    LOG_WARN("failed to get vector", K(ret), K(*expr.args_[0]));
  } else if (OB_FAIL(ObArrayExprUtils::get_type_vector(*(expr.args_[1]), ctx, tmp_allocator, arr_r, contain_null))) {
    LOG_WARN("failed to get vector", K(ret), K(*expr.args_[1]));
  } else if (contain_null) {
    res_datum.set_null();
  } else if (OB_ISNULL(arr_l) || OB_ISNULL(arr_r)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(arr_l), K(arr_r));
  } else if (OB_UNLIKELY(arr_l->size() != arr_r->size())) {
    ret = OB_ERR_INVALID_VECTOR_DIM;
    LOG_WARN("check array validty failed", K(ret), K(arr_l->size()), K(arr_r->size()));
  } else if (arr_l->contain_null() || arr_r->contain_null()) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("array with null can't calculate vector distance", K(ret));
  } else {
    double distance = 0.0;
    const float *data_l = reinterpret_cast<const float*>(arr_l->get_data());
    const float *data_r = reinterpret_cast<const float*>(arr_r->get_data());
    const uint32_t size = arr_l->size();
    if (distance_funcs[dis_type] == nullptr) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support", K(ret), K(dis_type));
    } else if (OB_FAIL(distance_funcs[dis_type](data_l, data_r, size, distance))) {
      if (OB_ERR_NULL_VALUE == ret) {
        res_datum.set_null();
        ret = OB_SUCCESS; // ignore
      } else {
        LOG_WARN("failed to calc distance", K(ret), K(dis_type));
      }
    } else {
      res_datum.set_double(distance);
    }
  }
  return ret;
}

ObExprVectorL1Distance::ObExprVectorL1Distance(ObIAllocator &alloc)
    : ObExprVectorDistance(alloc, T_FUN_SYS_L1_DISTANCE, N_VECTOR_L1_DISTANCE, 2, NOT_ROW_DIMENSION) {}

int ObExprVectorL1Distance::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                    ObExpr &rt_expr) const
{
    int ret = OB_SUCCESS;
    rt_expr.eval_func_ = ObExprVectorL1Distance::calc_l1_distance;
    return ret;
}

int ObExprVectorL1Distance::calc_l1_distance(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  return ObExprVectorDistance::calc_distance(expr, ctx, res_datum, ObVecDisType::MANHATTAN);
}

ObExprVectorL2Distance::ObExprVectorL2Distance(ObIAllocator &alloc)
    : ObExprVectorDistance(alloc, T_FUN_SYS_L2_DISTANCE, N_VECTOR_L2_DISTANCE, 2, NOT_ROW_DIMENSION) {}

int ObExprVectorL2Distance::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                    ObExpr &rt_expr) const
{
    int ret = OB_SUCCESS;
    rt_expr.eval_func_ = ObExprVectorL2Distance::calc_l2_distance;
    return ret;
}

int ObExprVectorL2Distance::calc_l2_distance(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  return ObExprVectorDistance::calc_distance(expr, ctx, res_datum, ObVecDisType::EUCLIDEAN);
}

ObExprVectorCosineDistance::ObExprVectorCosineDistance(ObIAllocator &alloc)
    : ObExprVectorDistance(alloc, T_FUN_SYS_COSINE_DISTANCE, N_VECTOR_COS_DISTANCE, 2, NOT_ROW_DIMENSION) {}

int ObExprVectorCosineDistance::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                    ObExpr &rt_expr) const
{
    int ret = OB_SUCCESS;
    rt_expr.eval_func_ = ObExprVectorCosineDistance::calc_cosine_distance;
    return ret;
}

int ObExprVectorCosineDistance::calc_cosine_distance(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  return ObExprVectorDistance::calc_distance(expr, ctx, res_datum, ObVecDisType::COSINE);
}

ObExprVectorIPDistance::ObExprVectorIPDistance(ObIAllocator &alloc)
    : ObExprVectorDistance(alloc, T_FUN_SYS_INNER_PRODUCT, N_VECTOR_INNER_PRODUCT, 2, NOT_ROW_DIMENSION) {}

int ObExprVectorIPDistance::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                    ObExpr &rt_expr) const
{
    int ret = OB_SUCCESS;
    rt_expr.eval_func_ = ObExprVectorIPDistance::calc_inner_product;
    return ret;
}

int ObExprVectorIPDistance::calc_inner_product(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  return ObExprVectorDistance::calc_distance(expr, ctx, res_datum, ObVecDisType::DOT);
}

ObExprVectorNegativeIPDistance::ObExprVectorNegativeIPDistance(ObIAllocator &alloc)
    : ObExprVectorDistance(alloc, T_FUN_SYS_NEGATIVE_INNER_PRODUCT, N_VECTOR_NEGATIVE_INNER_PRODUCT, 2, NOT_ROW_DIMENSION) {}

int ObExprVectorNegativeIPDistance::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                    ObExpr &rt_expr) const
{
    int ret = OB_SUCCESS;
    rt_expr.eval_func_ = ObExprVectorNegativeIPDistance::calc_negative_inner_product;
    return ret;
}

int ObExprVectorNegativeIPDistance::calc_negative_inner_product(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObExprVectorDistance::calc_distance(expr, ctx, res_datum, ObVecDisType::DOT))) {
    LOG_WARN("fail to calc distance", K(ret), K(ObVecDisType::DOT));
  } else if (!res_datum.is_null() && res_datum.get_double() != 0) {
    double value = -1 * res_datum.get_double();
    res_datum.set_double(value);
  }
  return ret;
}

ObExprVectorDims::ObExprVectorDims(ObIAllocator &alloc)
    : ObExprVector(alloc, T_FUN_SYS_VECTOR_DIMS, N_VECTOR_DIMS, 1, NOT_ROW_DIMENSION) {}

int ObExprVectorDims::calc_result_type1(
    ObExprResType &type,
    ObExprResType &type1,
    common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObArrayExprUtils::calc_cast_type(type1, type_ctx))) {
    LOG_WARN("failed to calc cast type", K(ret), K(type1));
  } else {
    type.set_type(ObIntType);
    type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
    type.set_calc_type(ObIntType);
  }
  return ret;
}
int ObExprVectorDims::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                    ObExpr &rt_expr) const
{
    int ret = OB_SUCCESS;
    rt_expr.eval_func_ = ObExprVectorDims::calc_dims;
    if (rt_expr.arg_cnt_ != 1 || OB_ISNULL(rt_expr.args_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("count of children is not 1 or children is null", K(ret), K(rt_expr.arg_cnt_), K(rt_expr.args_));
    } else if (rt_expr.args_[0]->type_ == T_FUN_SYS_CAST) {
      // return error if cast failed
      rt_expr.args_[0]->extra_  &= ~CM_WARN_ON_FAIL;
    }
    return ret;
}

int ObExprVectorDims::calc_dims(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObIArrayType *arr = NULL;
  bool contain_null = false;
  if (OB_FAIL(ObArrayExprUtils::get_type_vector(*(expr.args_[0]), ctx, tmp_allocator, arr, contain_null))) {
    LOG_WARN("failed to get vector", K(ret), K(*expr.args_[0]));
  } else if (contain_null) {
    res_datum.set_null();
  } else if (OB_ISNULL(arr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(arr));
  } else if (arr->contain_null()) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("array with null can't calculate vector norm", K(ret));
  } else {
    res_datum.set_int(arr->size());
  }
  return ret;
}

ObExprVectorNorm::ObExprVectorNorm(ObIAllocator &alloc)
    : ObExprVector(alloc, T_FUN_SYS_VECTOR_NORM, N_VECTOR_NORM, 1, NOT_ROW_DIMENSION) {}

int ObExprVectorNorm::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                    ObExpr &rt_expr) const
{
    int ret = OB_SUCCESS;
    rt_expr.eval_func_ = ObExprVectorNorm::calc_norm;
    if (rt_expr.arg_cnt_ != 1 || OB_ISNULL(rt_expr.args_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("count of children is not 1 or children is null", K(ret), K(rt_expr.arg_cnt_), K(rt_expr.args_));
    } else if (rt_expr.args_[0]->type_ == T_FUN_SYS_CAST) {
      // return error if cast failed
      rt_expr.args_[0]->extra_  &= ~CM_WARN_ON_FAIL;
    }
    return ret;
}

int ObExprVectorNorm::calc_norm(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObIArrayType *arr = NULL;
  bool contain_null = false;
  if (OB_FAIL(ObArrayExprUtils::get_type_vector(*(expr.args_[0]), ctx, tmp_allocator, arr, contain_null))) {
    LOG_WARN("failed to get vector", K(ret), K(*expr.args_[0]));
  } else if (contain_null) {
    res_datum.set_null();
  } else if (OB_ISNULL(arr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(arr));
  } else if (arr->contain_null()) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("array with null can't calculate vector norm", K(ret));
  } else {
    double norm = 0.0;
    const float *data = reinterpret_cast<const float*>(arr->get_data());
    if (OB_FAIL(ObVectorNorm::vector_norm_func(data, arr->size(), norm))) {
      LOG_WARN("failed to calc vector norm", K(ret));
    } else {
      res_datum.set_double(norm);
    }
  }
  return ret;
}

} // sql
} // oceanbase