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
 * This file contains implementation for map.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_map.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

namespace oceanbase
{
namespace sql
{
ObExprMap::ObExprMap(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_MAP, N_MAP, PARAM_NUM_UNKNOWN, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprMap::ObExprMap(ObIAllocator &alloc,
                         ObExprOperatorType type,
                         const char *name,
                         int32_t param_num, 
                         int32_t dimension) : ObFuncExprOperator(alloc, type, name, param_num, VALID_FOR_GENERATED_COL, dimension) 
{
}

ObExprMap::~ObExprMap()
{
}

int ObExprMap::calc_result_typeN(ObExprResType& type,
                                   ObExprResType* types_stack,
                                   int64_t param_num,
                                   ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
  uint16_t subschema_id;

  uint16_t key_subid;
  uint16_t value_subid;
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec ctx is null", K(ret));
  } else if (param_num % 2 != 0) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("map must have even number of arguments", K(ret), K(param_num));
  } else if (param_num > MAX_ARRAY_ELEMENT_SIZE * 2) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "array element size exceed max", K(ret), K(param_num), K(MAX_ARRAY_ELEMENT_SIZE));
  } else if (OB_FAIL(deduce_element_type(exec_ctx, types_stack, param_num, key_subid, value_subid))) {
    LOG_WARN("failed to deduce map element type", K(ret));
  } else if (OB_FAIL(ObArrayExprUtils::deduce_map_subschema_id(exec_ctx, key_subid, value_subid, subschema_id))) {
    LOG_WARN("failed to get collection subschema id", K(ret));
  } else {
    type.set_collection(subschema_id);
  }
  return ret;
}

int ObExprMap::eval_map(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObSubSchemaValue value;
  uint16_t subschema_id = expr.obj_meta_.get_subschema_id();
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
  } else if (OB_ISNULL(map_type = dynamic_cast<ObCollectionMapType *>(coll_info->collection_meta_))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("map type is null", K(ret), K(subschema_id));
  } else if (OB_FAIL(ObArrayTypeObjFactory::construct(tmp_allocator, *map_type, map_obj))) {
    LOG_WARN("construct array obj failed", K(ret), K(subschema_id), K(coll_info));
  } else {
    ObCollectionArrayType *key_type = dynamic_cast<ObCollectionArrayType *>(map_type->key_type_);
    ObCollectionArrayType *value_type = dynamic_cast<ObCollectionArrayType *>(map_type->value_type_);
    ObIArrayType *key_arr = dynamic_cast<ObMapType *>(map_obj)->get_key_array();
    ObIArrayType *value_arr = dynamic_cast<ObMapType *>(map_obj)->get_value_array();
    ObIArrayType *full_key_arr = NULL;
    ObCollectionBasicType *key_elem = NULL;
    uint32_t *idx_arr;
    uint32_t idx_count = 0;
    if (key_type->element_type_->type_id_ != ObNestedType::OB_BASIC_TYPE) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("map key in collection type is not supported", K(ret));
    } else if (OB_ISNULL(key_elem = dynamic_cast<ObCollectionBasicType *>(key_type->element_type_))) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("key_elem_type is null", K(ret), K(key_type));
    } else if (OB_FAIL(key_arr->clone_empty(tmp_allocator, full_key_arr, false))) {
      OB_LOG(WARN, "clone empty failed", K(ret));
    } else if (OB_ISNULL(idx_arr = static_cast<uint32_t *>(tmp_allocator.alloc(expr.arg_cnt_ / 2 * sizeof(uint32_t))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for tmpbuf", K(ret), K(expr.arg_cnt_ / 2 * sizeof(uint32_t)));
    } else if (OB_FAIL(construct_key_array(ctx, expr, key_elem->basic_meta_.get_obj_type(), full_key_arr, idx_arr, idx_count))) {
      LOG_WARN("construct key array failed", K(ret));
    }
    for (int i = 0; i < idx_count && OB_SUCC(ret); i++) {
      if (OB_FAIL(key_arr->insert_from(*full_key_arr, idx_arr[i] / 2))) {
        OB_LOG(WARN, "append key element failed", K(ret), K(i));
      } else if (OB_FAIL(ObArrayExprUtils::add_elem_to_array(expr, ctx, tmp_allocator, value_type, value_arr, idx_arr[i] + 1))) {
        OB_LOG(WARN, "failed to add elem to value array", K(ret), K(i));
      }
    } // end for
    if (OB_FAIL(ret)) {
    } else if (key_arr->size() != value_arr->size()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("key and value array size not equal", K(ret), K(key_arr->size()), K(value_arr->size()));
    } else {
      dynamic_cast<ObMapType *>(map_obj)->set_size(key_arr->size());
      ObString res_str;
      if (OB_FAIL(ObArrayExprUtils::set_array_res(map_obj, map_obj->get_raw_binary_len(), expr, ctx, res_str))) {
        LOG_WARN("get array binary string failed", K(ret), K(*coll_info));
      } else {
        res.set_string(res_str);
      }
    }
  }
  return ret;
}

int ObExprMap::deduce_element_type(ObExecContext *exec_ctx, ObExprResType* types_stack,
                                   int64_t param_num, uint16_t &key_subid, uint16_t &value_subid)
{
  int ret = OB_SUCCESS;
  uint16_t value_elem_subid = ObInvalidSqlType;
  ObExprResType coll_calc_type;
  ObDataType key_type;
  ObDataType value_type;
  key_type.meta_.set_utinyint(); // default type
  value_type.meta_.set_utinyint(); // default type
  bool is_first_value_elem = true;
  // calculate element type
  for (int64_t i = 0; i < param_num && OB_SUCC(ret); i++) {
    if (types_stack[i].is_null()) {
    } else if (i % 2 == 0) {
      // key param
      if (ob_is_collection_sql_type(types_stack[i].get_type())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("map key type is array", K(ret), K(types_stack[i].get_type()));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "collection type as map key");
      } else if (!ob_is_array_supported_type(types_stack[i].get_type())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported element type", K(ret), K(types_stack[i].get_type()));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "array element type");
      } else if (ob_is_varbinary_or_binary(types_stack[i].get_type(), types_stack[i].get_collation_type())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("array element in binary type isn't supported", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "array element in binary type");
      } else if (OB_FAIL(ObExprResultTypeUtil::get_deduce_element_type(types_stack[i], key_type))) {
        LOG_WARN("get deduce type failed", K(ret), K(types_stack[i].get_type()), K(key_type.get_obj_type()), K(i));
      }
    } else {
      // value param
      if (ob_is_collection_sql_type(types_stack[i].get_type())) {
        // element is collection type
        if (is_first_value_elem) {
          is_first_value_elem = false;
          coll_calc_type = types_stack[i];
          value_elem_subid = types_stack[i].get_subschema_id();
          value_type.meta_.set_collection(value_elem_subid);
        } else if (value_elem_subid == ObInvalidSqlType) {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_WARN("map value element type dismatch", K(ret));
        } else if (value_elem_subid != types_stack[i].get_subschema_id()) {
          ObExprResType tmp_calc_type;
          if (OB_FAIL(ObExprResultTypeUtil::get_array_calc_type(exec_ctx, coll_calc_type, types_stack[i], tmp_calc_type))) {
            LOG_WARN("failed to check array compatibilty", K(ret));
          } else {
            value_elem_subid = tmp_calc_type.get_subschema_id();
            coll_calc_type = tmp_calc_type;
            value_type.meta_.set_collection(value_elem_subid);
          }
        }
      } else if (value_elem_subid != ObInvalidSqlType) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("map value element type dismatch", K(ret));
      } else if (!ob_is_array_supported_type(types_stack[i].get_type())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported element type", K(ret), K(types_stack[i].get_type()));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "array element type");
      } else if (ob_is_varbinary_or_binary(types_stack[i].get_type(), types_stack[i].get_collation_type())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("array element in binary type isn't supported", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "array element in binary type");
      } else if (OB_FAIL(ObExprResultTypeUtil::get_deduce_element_type(types_stack[i], value_type))) {
        LOG_WARN("get deduce type failed", K(ret), K(types_stack[i].get_type()), K(value_type.get_obj_type()), K(i));
      } else {
        is_first_value_elem = false;
      }
    }
  } // end for
  
  // set params calculate type
  for (int64_t i = 0; i < param_num && OB_SUCC(ret); i++) {
    if (types_stack[i].is_null()) {
    } else if (i % 2 == 0) {
      // key param
      if (types_stack[i].get_type() != key_type.get_obj_type()) {
        types_stack[i].set_calc_meta(key_type.get_meta_type());
        types_stack[i].set_calc_accuracy(key_type.get_accuracy());
      }
    } else {
      // value param
      if (value_elem_subid == ObInvalidSqlType) {
        // basic type
        if (types_stack[i].get_type() != value_type.get_obj_type()) {
          types_stack[i].set_calc_meta(value_type.get_meta_type());
          types_stack[i].set_calc_accuracy(value_type.get_accuracy());
        }
      } else {
        // collection type
        if (types_stack[i].get_subschema_id() != value_elem_subid) {
          types_stack[i].set_calc_meta(value_type.get_meta_type());
        }
      }
    }
  } // end for

  // get subschema id
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(exec_ctx->get_subschema_id_by_collection_elem_type(ObNestedType::OB_ARRAY_TYPE,
                                                                        key_type, key_subid))) {
    LOG_WARN("failed to get key subschema id", K(ret));
  } else if (value_elem_subid == ObInvalidSqlType) {
    if (OB_FAIL(exec_ctx->get_subschema_id_by_collection_elem_type(ObNestedType::OB_ARRAY_TYPE,
                                                                   value_type, value_subid))) {
    LOG_WARN("failed to get value subschema id", K(ret));
    }
  } else {
    if (OB_FAIL(ObArrayExprUtils::deduce_nested_array_subschema_id(exec_ctx, value_type, value_subid))) {
      LOG_WARN("failed to deduce nested array subschema id", K(ret));
    }
  }
  return ret;
}

#define CALC_KEY_IDX(Element_Type, Get_Func)                                           \
  std::map<Element_Type, uint32_t> key_idx;                                            \
  for (uint32_t i = 0; i < expr.arg_cnt_ && OB_SUCC(ret); i+=2) {                      \
    ObDatum *datum = NULL;                                                             \
    if (OB_FAIL(expr.args_[i]->eval(ctx, datum))) {                                    \
      LOG_WARN("failed to eval args", K(ret), K(i));                                   \
    } else if (OB_FAIL(ObArrayUtil::append(*full_key_arr, elem_type, datum))) {        \
      LOG_WARN("failed to append array value", K(ret), K(i));                          \
    } else if (datum->is_null()) {                                                     \
      idx_arr[0] = i;                                                                  \
      idx_count = 1;                                                                   \
    } else {                                                                           \
      key_idx[datum->Get_Func()] = i;                                                  \
    }                                                                                  \
  }                                                                                    \
  std::map<Element_Type, uint32_t>::iterator it = key_idx.begin();                     \
  for (; it != key_idx.end() && OB_SUCC(ret); ++it) {                                  \
    idx_arr[idx_count++] = it->second;                                                 \
  }

int ObExprMap::construct_key_array(ObEvalCtx &ctx, const ObExpr &expr,
                                   const ObObjType elem_type, ObIArrayType *&full_key_arr,
                                   uint32_t *&idx_arr, uint32_t &idx_count)
{
  int ret = OB_SUCCESS;
  switch (elem_type) {
  case ObNullType: {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect null value", K(ret));
    break;
  }
  case ObTinyIntType: {
    CALC_KEY_IDX(int8_t, get_tinyint);
    break;
  }
  case ObSmallIntType: {
    CALC_KEY_IDX(int16_t, get_smallint);
    break;
  }
  case ObInt32Type: {
    CALC_KEY_IDX(int32_t, get_int32);
    break;
  }
  case ObIntType: {
    CALC_KEY_IDX(int64_t, get_int);
    break;
  }
  case ObUTinyIntType: {
    CALC_KEY_IDX(uint8_t, get_utinyint);
    break;
  }
  case ObUSmallIntType: {
    CALC_KEY_IDX(uint16_t, get_usmallint);
    break;
  }
  case ObUInt32Type: {
    CALC_KEY_IDX(uint32_t, get_uint32);
    break;
  }
  case ObUInt64Type: {
    CALC_KEY_IDX(uint64_t, get_uint64);
    break;
  }
  case ObUFloatType:
  case ObFloatType: {
    CALC_KEY_IDX(float, get_float);
    break;
  }
  case ObUDoubleType:
  case ObDoubleType: {
    CALC_KEY_IDX(double, get_double);
    break;
  }
  case ObVarcharType: {
    CALC_KEY_IDX(ObString, get_string);
    break;
  }
  default:
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported element type", K(ret), K(elem_type));
  } // end switch
  if (OB_SUCC(ret) && OB_FAIL(full_key_arr->init())) {
    LOG_WARN("failed to init array", K(ret));
  }
  return ret;
}

int ObExprMap::cg_expr(ObExprCGCtx &expr_cg_ctx,
                         const ObRawExpr &raw_expr,
                         ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_map;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
