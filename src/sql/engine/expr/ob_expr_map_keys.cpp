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
 * This file contains implementation for map_keys and map_values.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_map_keys.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/udt/ob_map_type.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

namespace oceanbase
{
namespace sql
{
ObExprMapComponents::ObExprMapComponents(ObIAllocator &alloc,
                         ObExprOperatorType type,
                         const char *name,
                         int32_t param_num, 
                         int32_t dimension) : ObFuncExprOperator(alloc, type, name,1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION) 
{
}

ObExprMapComponents::~ObExprMapComponents()
{
}

int ObExprMapComponents::calc_map_components_result_type(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx,
                                bool is_key)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  ObExecContext *exec_ctx = NULL;
  uint16_t subschema_id;
  uint16_t component_subid;
  ObSubSchemaValue map_meta;
  const ObSqlCollectionInfo *coll_info = NULL;
  common::ObArenaAllocator tmp_allocator;

  if (OB_ISNULL(session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSQLSessionInfo is null", K(ret));
  } else if (OB_ISNULL(exec_ctx = session->get_cur_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObExecContext is null", K(ret));
  } else if (ob_is_null(type1.get_type())) {
    type.set_null();
  } else if (!ob_is_collection_sql_type(type1.get_type())) {
    // not collection type
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input type", K(ret));
  } else if (OB_FAIL(exec_ctx->get_sqludt_meta_by_subschema_id(type1.get_subschema_id(), map_meta))) {
    LOG_WARN("failed to get elem meta.", K(ret), K(type1.get_subschema_id()));
  } else if (map_meta.type_ != ObSubSchemaType::OB_SUBSCHEMA_COLLECTION_TYPE) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid subschema type", K(ret), K(map_meta.type_));
  } else if (OB_ISNULL(coll_info = static_cast<const ObSqlCollectionInfo *>(map_meta.value_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("coll info is null", K(ret));
  } else if (coll_info->collection_meta_->type_id_ != ObNestedType::OB_MAP_TYPE) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid collection type", K(ret), K(coll_info->collection_meta_->type_id_));
  } else {
    ObString attr_def;
    if (is_key && OB_FAIL(coll_info->get_map_attr_def_string(tmp_allocator, attr_def))) {
      LOG_WARN("failed to get map key define", K(ret), K(*coll_info));
    } else if (!is_key && OB_FAIL(coll_info->get_map_attr_def_string(tmp_allocator, attr_def, true))) {
      LOG_WARN("failed to get map value define", K(ret), K(*coll_info));
    } else if (OB_FAIL(exec_ctx->get_subschema_id_by_type_string(attr_def, component_subid))) {
      LOG_WARN("failed to get type1 key subschema id", K(ret), K(*coll_info), K(attr_def));
    } else {
      type.set_collection(component_subid);
      type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObCollectionSQLType]).get_length());
    }
  }
  return ret;
}

int ObExprMapComponents::eval_map_components(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res, bool is_key) {
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  uint16_t res_subschema_id = expr.obj_meta_.get_subschema_id();
  ObIArrayType *arr_res = NULL;
  uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  ObDatum *map_datum = NULL;

  if (ob_is_null(expr.obj_meta_.get_type())) {
    res.set_null();
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, map_datum))) {
    LOG_WARN("failed to eval source map", K(ret));
  } else if (map_datum->is_null()) {
    res.set_null();
  } else {
    ObString map_blob = map_datum->get_string();
    if (OB_FAIL(get_map_components_arr(tmp_allocator, ctx, map_blob, arr_res, res_subschema_id, subschema_id, is_key))) {
      LOG_WARN("failed to get map key array", K(ret));
    } else {
      ObString res_str;
      if (OB_FAIL(ObArrayExprUtils::set_array_res(arr_res, arr_res->get_raw_binary_len(), expr, ctx, res_str))) {
        LOG_WARN("get key array binary string failed", K(ret));
      } else {
        res.set_string(res_str);
      }
    }
  }
  return ret;
}

int ObExprMapComponents::eval_map_components_vector(const ObExpr &expr, 
                         ObEvalCtx &ctx,
                         const ObBitVector &skip, 
                         const EvalBound &bound,
                         bool is_key)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  uint16_t res_subschema_id = expr.obj_meta_.get_subschema_id();
  uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();

  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval source map failed", K(ret));
  } else {
    ObIVector *map_vec = expr.args_[0]->get_vector(ctx);
    ObIVector *res_vec = expr.get_vector(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      ObIArrayType *res_arr = NULL;
      bool is_null_res = false;
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      }
      if (map_vec->is_null(idx)) {
        is_null_res = true;
      } else {
        ObString map_blob = map_vec->get_string(idx);
        if (OB_FAIL(get_map_components_arr(tmp_allocator, ctx, map_blob, res_arr, res_subschema_id, subschema_id, is_key))) {
          LOG_WARN("failed to get map key array", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (is_null_res) {
        res_vec->set_null(idx);
      } else if (res_format == VEC_DISCRETE) {
        if (OB_FAIL(ObArrayExprUtils::set_array_res<ObDiscreteFormat>(res_arr, expr, ctx, static_cast<ObDiscreteFormat *>(res_vec), idx))) {
          LOG_WARN("set array res failed", K(ret));
        }
      } else if (res_format == VEC_UNIFORM) {
        if (OB_FAIL(ObArrayExprUtils::set_array_res<ObUniformFormat<false>>(res_arr, expr, ctx, static_cast<ObUniformFormat<false> *>(res_vec), idx))) {
          LOG_WARN("set array res failed", K(ret));
        }
      } else if (OB_FAIL(ObArrayExprUtils::set_array_res<ObVectorBase>(res_arr, expr, ctx, static_cast<ObVectorBase *>(res_vec), idx))) {
        LOG_WARN("set array res failed", K(ret));
      } 
    } // end for
  }
  return ret;
}

int ObExprMapComponents::get_map_components_arr_vector(ObIAllocator &tmp_allocator, 
                                  ObEvalCtx &ctx, 
                                  ObExpr &param_expr,
                                  const uint16_t subschema_id, 
                                  int64_t row_idx, 
                                  ObIArrayType *&arr_res,
                                  bool is_key)
{
  int ret = OB_SUCCESS;
  ObSubSchemaValue value;
  const ObSqlCollectionInfo *coll_info = NULL;
  ObCollectionMapType *map_type = NULL;
  ObIArrayType *map_obj = NULL;

  if (OB_FAIL(ctx.exec_ctx_.get_sqludt_meta_by_subschema_id(subschema_id, value))) {
    LOG_WARN("failed to get subschema ctx", K(ret));
  } else if (value.type_ >= OB_SUBSCHEMA_MAX_TYPE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid subschema type", K(ret), K(value));
  } else if (OB_ISNULL(coll_info = reinterpret_cast<const ObSqlCollectionInfo *>(value.value_))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("collect info is null", K(ret), K(subschema_id));
  } else if (coll_info->collection_meta_->type_id_ != ObNestedType::OB_MAP_TYPE) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid collection type", K(ret), K(coll_info->collection_meta_->type_id_));
  }else if (OB_ISNULL(map_type = dynamic_cast<ObCollectionMapType *>(coll_info->collection_meta_))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("map type is null", K(ret), K(subschema_id));
  } else if (OB_FAIL(ObArrayTypeObjFactory::construct(tmp_allocator, *map_type, map_obj, true))) {
    LOG_WARN("construct map obj failed", K(ret), K(subschema_id), K(coll_info));
  } else {
    ObDatum attr_val[param_expr.attrs_cnt_];
    for (uint32_t i = 0; i < param_expr.attrs_cnt_; ++i) {
      ObIVector *vec = param_expr.attrs_[i]->get_vector(ctx);
      const char *payload = NULL;
      ObLength payload_len = 0;
      vec->get_payload(row_idx, payload, payload_len);
      attr_val[i].ptr_ = payload;
      attr_val[i].pack_ = payload_len;
    }
    if (OB_FAIL(dynamic_cast<ObMapType *>(map_obj)->init(attr_val, param_expr.attrs_cnt_))) {
      LOG_WARN("failed to get map obj", K(ret));
    }else {
      if (is_key){
        arr_res = dynamic_cast<ObMapType *>(map_obj)->get_key_array();
      } else {
        arr_res = dynamic_cast<ObMapType *>(map_obj)->get_value_array();
      }
    }
  }

  return ret;
}

int ObExprMapComponents::get_map_components_arr(ObIAllocator &tmp_allocator,
                                    ObEvalCtx &ctx,
                                    ObString &map_blob, 
                                    ObIArrayType *&arr_res, 
                                    uint16_t &res_subschema_id,
                                    uint16_t &subschema_id,
                                    bool is_key)
{
  int ret = OB_SUCCESS;
  ObSubSchemaValue value;
  const ObSqlCollectionInfo *coll_info = NULL;
  ObCollectionMapType *map_type = NULL;
  ObIArrayType *map_obj = NULL;

  if (OB_FAIL(ctx.exec_ctx_.get_sqludt_meta_by_subschema_id(subschema_id, value))) {
    LOG_WARN("failed to get subschema ctx", K(ret));
  } else if (value.type_ >= OB_SUBSCHEMA_MAX_TYPE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid subschema type", K(ret), K(value));
  } else if (OB_ISNULL(coll_info = reinterpret_cast<const ObSqlCollectionInfo *>(value.value_))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("collect info is null", K(ret), K(subschema_id));
  } else if (coll_info->collection_meta_->type_id_ != ObNestedType::OB_MAP_TYPE) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid collection type", K(ret), K(coll_info->collection_meta_->type_id_));
  }else if (OB_ISNULL(map_type = dynamic_cast<ObCollectionMapType *>(coll_info->collection_meta_))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("map type is null", K(ret), K(subschema_id));
  } else if (OB_FAIL(ObArrayTypeObjFactory::construct(tmp_allocator, *map_type, map_obj, true))) {
    LOG_WARN("construct map obj failed", K(ret), K(subschema_id), K(coll_info));
  } else {
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_allocator,
                                                          ObLongTextType,
                                                          CS_TYPE_BINARY,
                                                          true,
                                                          map_blob))) {
      LOG_WARN("fail to get real data.", K(ret), K(map_blob));
    } else if (OB_FAIL(dynamic_cast<ObMapType *>(map_obj)->init(map_blob))) {
      LOG_WARN("failed to get map obj", K(ret));
    } else {
      if (is_key){
        arr_res = dynamic_cast<ObMapType *>(map_obj)->get_key_array();
      } else {
        arr_res = dynamic_cast<ObMapType *>(map_obj)->get_value_array();
      }
    }
  }
  return ret;
}

ObExprMapKeys::ObExprMapKeys(ObIAllocator &alloc)
    : ObExprMapComponents(alloc, T_FUN_SYS_MAP_KEYS, N_MAP_KEYS, 1, NOT_ROW_DIMENSION)
{
}

ObExprMapKeys::~ObExprMapKeys()
{
}

int ObExprMapKeys::calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const
{
  return ObExprMapComponents::calc_map_components_result_type(type, type1, type_ctx, true);
}

int ObExprMapKeys::eval_map_keys(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  return ObExprMapComponents::eval_map_components(expr, ctx, res, true);
}

int ObExprMapKeys::eval_map_keys_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                          const ObBitVector &skip, const EvalBound &bound)
{
  return ObExprMapComponents::eval_map_components_vector(expr, ctx, skip, bound, true);
}

int ObExprMapKeys::cg_expr(ObExprCGCtx &expr_cg_ctx,
                         const ObRawExpr &raw_expr,
                         ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_map_keys;
  rt_expr.eval_vector_func_ = eval_map_keys_vector;
  return OB_SUCCESS;
}

ObExprMapValues::ObExprMapValues(ObIAllocator &alloc)
    : ObExprMapComponents(alloc, T_FUN_SYS_MAP_VALUES, N_MAP_VALUES, 1, NOT_ROW_DIMENSION)
{
}

ObExprMapValues::~ObExprMapValues()
{
}

int ObExprMapValues::calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const
{
  return ObExprMapComponents::calc_map_components_result_type(type, type1, type_ctx, false);
}

int ObExprMapValues::eval_map_values(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  return ObExprMapComponents::eval_map_components(expr, ctx, res, false);
}

int ObExprMapValues::eval_map_values_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                          const ObBitVector &skip, const EvalBound &bound)
{
  return ObExprMapComponents::eval_map_components_vector(expr, ctx, skip, bound, false);
}

int ObExprMapValues::cg_expr(ObExprCGCtx &expr_cg_ctx,
                         const ObRawExpr &raw_expr,
                         ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_map_values;
  rt_expr.eval_vector_func_ = eval_map_values_vector;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase