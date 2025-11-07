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
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "share/vector_type/ob_vector_norm.h"
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"  // ObTimeUtility
#include "sql/engine/ob_exec_context.h"
namespace oceanbase
{
namespace sql
{

ObExprVectorDistance::SparseVectorDisFunc::FuncPtrType ObExprVectorDistance::SparseVectorDisFunc::spiv_distance_funcs[] =
{
  nullptr, // cosine_distance
  ObSparseVectorIpDistance::spiv_ip_distance_func,
  nullptr, // l2_distance
  nullptr, // l1_distance
  nullptr, // l2_square
  nullptr,
};

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
  if (OB_FAIL(ObArrayExprUtils::calc_cast_type2(type_, type1, type2, type_ctx, unused_id))) {
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
  if (OB_FAIL(ObArrayExprUtils::calc_cast_type(type_, type1, type_ctx))) {
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
  double distance = 0.0;

  if (dis_type < ObVecDisType::COSINE || dis_type >= ObVecDisType::MAX_TYPE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect distance type", K(ret), K(dis_type));
  } else if (OB_ISNULL(expr.args_[0]) || OB_ISNULL(expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null args", K(ret), K(OB_P(expr.args_[0])), K(OB_P(expr.args_[1])));
  } else {
    bool has_sparse_vector = false;
    ObCollectionArrayType *arr_type_l = nullptr;
    ObCollectionArrayType *arr_type_r = nullptr;

    for (int i = 0; i < 2 && !has_sparse_vector; ++i) {
      ObSubSchemaValue value;
      uint16_t subschema_id = expr.args_[i]->obj_meta_.get_subschema_id();
      if (OB_SUCC(ctx.exec_ctx_.get_sqludt_meta_by_subschema_id(subschema_id, value))) {
        const ObSqlCollectionInfo *coll_info = reinterpret_cast<const ObSqlCollectionInfo *>(value.value_);
        if (coll_info != NULL) {
          ObCollectionArrayType *arr_type = static_cast<ObCollectionArrayType *>(coll_info->collection_meta_);
          if (i == 0) {
            arr_type_l = arr_type;
          } else {
            arr_type_r = arr_type;
          }
          if (arr_type != NULL && arr_type->type_id_ == ObNestedType::OB_SPARSE_VECTOR_TYPE) {
            has_sparse_vector = true;
          }
        }
      }
    }

    if (has_sparse_vector) {
      ObIArrayType *arr_l = NULL;
      ObIArrayType *arr_r = NULL;
      bool vector_is_null = false;

      if (OB_FAIL(ObArrayExprUtils::get_type_vector(*(expr.args_[0]), ctx, tmp_allocator, arr_l, vector_is_null))) {
        LOG_WARN("failed to get vector", K(ret), K(*expr.args_[0]));
      } else if (OB_FAIL(ObArrayExprUtils::get_type_vector(*(expr.args_[1]), ctx, tmp_allocator, arr_r, vector_is_null))) {
        LOG_WARN("failed to get vector", K(ret), K(*expr.args_[1]));
      } else if (vector_is_null) {
        res_datum.set_null();
      } else if (OB_ISNULL(arr_l) || OB_ISNULL(arr_r)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret), K(arr_l), K(arr_r));
      } else if (arr_l->get_array_type()->is_sparse_vector_type() && arr_r->get_array_type()->is_sparse_vector_type()) {
        const ObMapType *spv_l = dynamic_cast<const ObMapType *>(arr_l);
        const ObMapType *spv_r = dynamic_cast<const ObMapType *>(arr_r);
        if (OB_ISNULL(spv_l) || OB_ISNULL(spv_r)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sparse vector type cast failed", K(ret));
        } else if (dis_type != ObVecDisType::DOT) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("sparse vector not support", K(ret), K(dis_type));
        } else if (OB_FAIL(SparseVectorDisFunc::spiv_distance_funcs[dis_type](spv_l, spv_r, distance))) {
          LOG_WARN("sparse vector failed to calc distance", K(ret), K(dis_type));
        } else {
          res_datum.set_double(distance);
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("calc distance for sparse vector and other type is not supported", K(ret));
      }
    } else {
      const float *data_l = NULL;
      const float *data_r = NULL;
      int64_t size_l = 0;
      int64_t size_r = 0;
      bool vector_is_null = false;
      bool is_arg0_static_const = expr.args_[0]->is_static_const_;
      bool is_arg1_static_const = expr.args_[1]->is_static_const_;
      bool has_single_const = (is_arg0_static_const && !is_arg1_static_const) ||
                              (!is_arg0_static_const && is_arg1_static_const);
      VectorConstCache* const_cache = nullptr;
      if (has_single_const) {
        const_cache = get_vector_const_cache(expr.expr_ctx_id_, &ctx.exec_ctx_);
      }

      if (has_single_const && const_cache && is_arg0_static_const && const_cache->is_cached(0)) {
        data_l = const_cache->get_cached_data(0);
        size_l = const_cache->get_cached_size(0);
      } else if (OB_FAIL(get_normal_vector_data(expr.args_[0], ctx, tmp_allocator, data_l, size_l, arr_type_l, vector_is_null))) {
        LOG_WARN("failed to get first vector data", K(ret));
      } else if (vector_is_null) {
        res_datum.set_null();
      } else if (is_arg0_static_const && has_single_const && const_cache && !const_cache->is_cached(0)) {
        if (OB_FAIL(const_cache->add_const_cache(0, data_l, size_l))) {
          LOG_WARN("failed to cache arg0 data", K(ret));
          ret = OB_SUCCESS;
        }
      }
      if (OB_SUCC(ret)) {
        if (has_single_const && const_cache && is_arg1_static_const && const_cache->is_cached(1)) {
          data_r = const_cache->get_cached_data(1);
          size_r = const_cache->get_cached_size(1);
        } else if (OB_FAIL(get_normal_vector_data(expr.args_[1], ctx, tmp_allocator, data_r, size_r, arr_type_r, vector_is_null))) {
          LOG_WARN("failed to get second vector data", K(ret));
        } else if (vector_is_null) {
          res_datum.set_null();
        } else if (is_arg1_static_const && has_single_const && const_cache && !const_cache->is_cached(1)) {
          if (OB_FAIL(const_cache->add_const_cache(1, data_r, size_r))) {
            LOG_WARN("failed to cache arg1 data", K(ret));
            ret = OB_SUCCESS;
          }
        }
        if (OB_SUCC(ret) && !vector_is_null){
          if (size_l != size_r) {
            ret = OB_ERR_INVALID_VECTOR_DIM;
            LOG_WARN("vector dimension mismatch", K(ret), K(size_l), K(size_r));
          } else {
            if (DisFunc<float>::distance_funcs[dis_type] == nullptr) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("unsupported distance type", K(ret), K(dis_type));
            } else if (OB_FAIL(DisFunc<float>::distance_funcs[dis_type](data_l, data_r, size_l, distance))) {
              if (OB_ERR_NULL_VALUE == ret) {
                res_datum.set_null();
                ret = OB_SUCCESS; //ignore
              } else {
                LOG_WARN("failed to calc distance", K(ret), K(dis_type));
              }
            } else if (::isinf(distance)) {
              ret = OB_NUMERIC_OVERFLOW;
              LOG_WARN("distance value is overflow", K(distance));
              FORWARD_USER_ERROR(OB_NUMERIC_OVERFLOW, "distance value is overflow");
            } else {
              res_datum.set_double(distance);
            }
          }
        }
      }
    }
  }
  return ret;
}

ObExprVectorDistance::VectorConstCache::VectorConstCache(common::ObIAllocator *allocator)
  : ObExprOperatorCtx(),
    allocator_(allocator),
    param1_data_(nullptr),
    param1_size_(0),
    param2_data_(nullptr),
    param2_size_(0),
    param1_cached_(false),
    param2_cached_(false)
{
}

const float *ObExprVectorDistance::VectorConstCache::get_cached_data(int arg_idx) const
{
  return (arg_idx == 0) ? param1_data_ : param2_data_;
}
int64_t ObExprVectorDistance::VectorConstCache::get_cached_size(int arg_idx) const
{
  return (arg_idx == 0) ? param1_size_ : param2_size_;
}
bool ObExprVectorDistance::VectorConstCache::is_cached(int arg_idx) const
{
  return (arg_idx == 0) ? param1_cached_ : param2_cached_;
}
int ObExprVectorDistance::VectorConstCache::add_const_cache(int arg_idx, const float *data, int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_) || OB_ISNULL(data) || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments for cache", K(ret), K(data), K(size));
  } else {
    size_t data_len = size * sizeof(float);
    void *cache_buf = allocator_->alloc(data_len);
    if (OB_ISNULL(cache_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate cache buffer", K(ret), K(data_len));
    } else {
      MEMCPY(cache_buf, data, data_len);
      if (arg_idx == 0) {
        param1_data_ = static_cast<const float*>(cache_buf);
        param1_size_ = size;
        param1_cached_ = true;
      } else {
        param2_data_ = static_cast<const float*>(cache_buf);
        param2_size_ = size;
        param2_cached_ = true;
      }
    }
  }
  return ret;
}

ObExprVectorDistance::VectorConstCache* ObExprVectorDistance::get_vector_const_cache(const uint64_t& id, ObExecContext *exec_ctx)
{
  VectorConstCache* cache_ctx = nullptr;
  if (ObExpr::INVALID_EXP_CTX_ID != id) {
    cache_ctx = static_cast<VectorConstCache*>(exec_ctx->get_expr_op_ctx(id));
    if (OB_ISNULL(cache_ctx)) {
      void *cache_ctx_buf = nullptr;
      int ret = exec_ctx->create_expr_op_ctx(id, sizeof(VectorConstCache), cache_ctx_buf);
      if (OB_SUCC(ret) && OB_NOT_NULL(cache_ctx_buf)) {
        cache_ctx = new (cache_ctx_buf) VectorConstCache(&exec_ctx->get_allocator());
      }
    }
  }
  return cache_ctx;
}

int ObExprVectorDistance::get_normal_vector_data(const ObExpr *arg, ObEvalCtx &ctx,
                                         common::ObArenaAllocator &tmp_allocator,
                                         const float *&data, int64_t &size,
                                         ObCollectionArrayType *arr_type,
                                         bool &is_null)
{
  int ret = OB_SUCCESS;
  data = NULL;
  size = 0;

  if (OB_ISNULL(arg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_normal_vector_data null arg", K(ret));
  } else {
    ObDatum *datum = NULL;
    if (OB_FAIL(arg->eval(ctx, datum))) {
      LOG_WARN("get_normal_vector_data eval failed", K(ret));
    } else if (OB_ISNULL(datum)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get_normal_vector_data null datum", K(ret));
    } else if (datum->is_null()) {
      is_null = true;
    } else {
      ObString blob_data = datum->get_string();
      if (blob_data.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("empty blob data", K(ret));
      } else if (blob_data.length() < sizeof(float)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("blob data too small", K(ret), K(blob_data.length()));
      } else {
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_allocator,
                                                              ObLongTextType,
                                                              CS_TYPE_BINARY,
                                                              true,
                                                              blob_data))) {
          LOG_WARN("read_real_string_data failed", K(ret));
        } else {
          ObCollectionArrayType *local_arr_type = arr_type;
          if (local_arr_type == nullptr) {
            ObSubSchemaValue value;
            uint16_t subschema_id = arg->obj_meta_.get_subschema_id();
            if (OB_FAIL(ctx.exec_ctx_.get_sqludt_meta_by_subschema_id(subschema_id, value))) {
              LOG_WARN("failed to get subschema ctx", K(ret));
            } else {
              const ObSqlCollectionInfo *coll_info = reinterpret_cast<const ObSqlCollectionInfo *>(value.value_);
              local_arr_type = static_cast<ObCollectionArrayType *>(coll_info->collection_meta_);
            }
          }
          if (OB_SUCC(ret) && local_arr_type != nullptr) {
            if (local_arr_type->type_id_ == ObNestedType::OB_VECTOR_TYPE) {
              if (blob_data.length() % sizeof(float) != 0) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("VECTOR raw data length invalid", K(ret), K(blob_data.length()));
              } else {
                size = blob_data.length() / sizeof(float);
                data = reinterpret_cast<const float*>(blob_data.ptr());
                if (size != 0 && OB_ISNULL(data)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("VECTOR data pointer is null", K(ret), K(size));
                }
              }
            } else if (local_arr_type->type_id_ == ObNestedType::OB_ARRAY_TYPE) {
              uint32_t len = 0;
              uint8_t *null_bitmaps = NULL;
              const char *array_data = NULL;
              uint32_t data_len = 0;
              if (OB_FAIL(ObArrayExprUtils::get_array_data(blob_data, local_arr_type, len, null_bitmaps, array_data, data_len))) {
                LOG_WARN("failed to get array data", K(ret));
              } else {
                //check if array has null value
                bool contain_null = false;
                if (null_bitmaps != NULL) {
                  for (uint32_t i = 0; i < len && !contain_null; ++i) {
                    if (null_bitmaps[i] != 0) {
                      contain_null = true;
                    }
                  }
                }
                if (contain_null) {
                  ret = OB_ERR_NULL_VALUE;
                  LOG_WARN("array with null can't calculate vector distance", K(ret));
                  FORWARD_USER_ERROR(OB_ERR_NULL_VALUE, "Null value");
                } else {
                  size = len;
                  data = reinterpret_cast<const float*>(array_data);
                }
              }
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected array type", K(ret), K(local_arr_type->type_id_));
            }

            if (OB_SUCC(ret)) {
              if (0 >= size || ObExprVector::MAX_VECTOR_DIM < size) {
                ret = OB_ERR_INVALID_VECTOR_DIM;
                LOG_WARN("invalid vector dimension", K(ret), K(size));
              }
            }
          }
        }
      }
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

ObExprVectorL2Squared::ObExprVectorL2Squared(ObIAllocator &alloc)
    : ObExprVectorDistance(alloc, T_FUN_SYS_L2_SQUARED, N_VECTOR_L2_SQUARED, 2, NOT_ROW_DIMENSION) {}

int ObExprVectorL2Squared::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                    ObExpr &rt_expr) const
{
    int ret = OB_SUCCESS;
    rt_expr.eval_func_ = ObExprVectorL2Squared::calc_l2_squared;
    return ret;
}

int ObExprVectorL2Squared::calc_l2_squared(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  return ObExprVectorDistance::calc_distance(expr, ctx, res_datum, ObVecDisType::EUCLIDEAN_SQUARED);
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
  if (OB_FAIL(ObArrayExprUtils::calc_cast_type(type_, type1, type_ctx))) {
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
