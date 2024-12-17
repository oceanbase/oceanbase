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
 * This file contains implementation for array.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_array_map.h"
#include "lib/udt/ob_collection_type.h"
#include "lib/udt/ob_array_type.h"
#include "lib/udt/ob_array_utils.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/ob_exec_context.h"


using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

namespace oceanbase
{
namespace sql
{

OB_DEF_SERIALIZE(ObExprArrayMapInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, serialization::make_ser_carray(param_exprs_, param_num_));
  if (OB_SUCC(ret)) {
    if (param_num_ > 0) {
      uint32_t len = sizeof(uint32_t) * param_num_;
      MEMCPY(buf + pos, param_idx_, len);
      pos += len;
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObExprArrayMapInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, serialization::make_ser_carray(param_exprs_, param_num_));
  if (OB_SUCC(ret)) {
    if (param_num_ > 0) {
      uint32_t len = sizeof(uint32_t) * param_num_;
      param_idx_ = static_cast<uint32_t*>(allocator_.alloc(len));
      if (OB_ISNULL(param_idx_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret), K(len));
      } else {
        MEMCPY(param_idx_, buf + pos, len);
        pos += len;
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObExprArrayMapInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, serialization::make_ser_carray(param_exprs_, param_num_));
  if (param_num_ > 0) {
    len += (sizeof(uint32_t) * param_num_);
  }
  return len;
}

int ObExprArrayMapInfo::deep_copy(common::ObIAllocator &allocator,
                                     const ObExprOperatorType type,
                                     ObIExprExtraInfo *&copied_info) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObExprExtraInfoFactory::alloc(allocator, type, copied_info))) {
    LOG_WARN("Failed to allocate memory for ObExprArrayMapInfo", K(ret));
  } else if (OB_ISNULL(copied_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("extra_info should not be nullptr", K(ret));
  } else if (param_num_ == 0) {
    // do nothing
  } else {
    ObExprArrayMapInfo *other = static_cast<ObExprArrayMapInfo *>(copied_info);
    int64_t alloc_size = param_num_ * (sizeof(ObExpr *) + sizeof(uint32_t));
    char *buf = static_cast<char *>(allocator.alloc(alloc_size));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(alloc_size));
    } else {
      other->param_exprs_ = reinterpret_cast<ObExpr **>(buf);
      other->param_idx_ = reinterpret_cast<uint32_t *>(buf + (param_num_ * sizeof(ObExpr*)));
      for (int64_t i = 0; i < param_num_; i++) {
        other->param_exprs_[i] = param_exprs_[i];
        other->param_idx_[i] = param_idx_[i];
      }
    }
  }
  return ret;
}


ObExprArrayMap::ObExprArrayMap(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUNC_SYS_ARRAY_MAP, N_ARRAY_MAP, MORE_THAN_ONE, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprArrayMap::ObExprArrayMap(ObIAllocator &alloc,
                         ObExprOperatorType type,
                         const char *name,
                         int32_t param_num,
                         int32_t dimension) : ObFuncExprOperator(alloc, type, name, param_num, NOT_VALID_FOR_GENERATED_COL, dimension)
{
}

ObExprArrayMap::~ObExprArrayMap()
{
}

int ObExprArrayMap::calc_result_typeN(ObExprResType& type,
                                   ObExprResType* types_stack,
                                   int64_t param_num,
                                   ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
  ObDataType elem_type;
  uint16_t subschema_id;
  bool is_null_res = false;
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec ctx is null", K(ret));
  }
  for (int64_t i = 1; i < param_num && OB_SUCC(ret) && !is_null_res; i++) {
    if (types_stack[i].is_null()) {
      is_null_res = true;
    } else if (!ob_is_collection_sql_type(types_stack[i].get_type())) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("invalid data type", K(ret), K(types_stack[i].get_type()));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (is_null_res) {
    if (OB_FAIL(ObArrayExprUtils::set_null_collection_type(exec_ctx, type))) {
      LOG_WARN("failed to set null collection", K(ret));
    }
  } else {
    if (types_stack[0].get_type() == ObDecimalIntType || types_stack[0].get_type() == ObNumberType ||
        types_stack[0].get_type() == ObUNumberType) {
      // decimalint isn't supported in array, so cast to supported type
      ObObjType calc_type = ObIntType;
      if (types_stack[0].get_scale() != 0) {
        calc_type = ObDoubleType;
      }
      types_stack[0].set_calc_type(calc_type);
      elem_type.set_meta_type(types_stack[0].get_calc_meta());
      elem_type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[calc_type]);
    } else {
      elem_type.set_meta_type(types_stack[0].get_obj_meta());
      elem_type.set_accuracy(types_stack[0].get_accuracy());
    }
    if (ob_is_collection_sql_type(elem_type.get_obj_type())) {
      if (OB_FAIL(ObArrayExprUtils::deduce_nested_array_subschema_id(exec_ctx, elem_type, subschema_id))) {
        LOG_WARN("failed to deduce nested array subschema id", K(ret));
      } else {
        type.set_collection(subschema_id);
      }
    } else if (!ob_is_array_supported_type(elem_type.get_obj_type())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported element type", K(ret), K(elem_type.get_obj_type()));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "array element type");
    } else if (ob_is_varbinary_or_binary(elem_type.get_obj_type(), elem_type.get_collation_type())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported binary", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "array element in binary type");
    } else if (OB_FAIL(exec_ctx->get_subschema_id_by_collection_elem_type(ObNestedType::OB_ARRAY_TYPE,
                                                                          elem_type, subschema_id))) {
      LOG_WARN("failed to get collection subschema id", K(ret));
    } else {
      type.set_collection(subschema_id);
    }
  }
  return ret;
}

int ObExprArrayMap::eval_array_map(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t res_meta_id = expr.obj_meta_.get_subschema_id();
  ObIArrayType *arr_res = NULL;
  ObDatum *datum[expr.arg_cnt_];
  ObIArrayType *arr_obj[expr.arg_cnt_];
  ObDatum *datum_val = NULL;
  bool is_null_res = false;
  uint32_t arr_dim = 0;
  bool bret = false;
  ObSubSchemaValue value;

  for (int64_t i = 1; i < expr.arg_cnt_ && OB_SUCC(ret) && !is_null_res; i++) {
    const uint16_t meta_id = expr.args_[i]->obj_meta_.get_subschema_id();
    arr_obj[i - 1] = NULL;
    if (OB_FAIL(expr.args_[i]->eval(ctx, datum[i]))) {
      LOG_WARN("failed to eval args", K(ret));
    } else if (datum[i]->is_null()) {
      is_null_res = true;
    } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, meta_id, datum[i]->get_string(), arr_obj[i - 1]))) {
      LOG_WARN("construct array obj failed", K(ret));
    } else if (arr_dim == 0) {
      arr_dim = arr_obj[i - 1]->size();
    } else if (arr_dim != arr_obj[i - 1]->size()) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("array dimension mismatch", K(ret), K(arr_dim), K(arr_obj[i - 1]->size()), K(i));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (is_null_res) {
    res.set_null();
  } else if (OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_allocator, ctx, res_meta_id, arr_res, false))) {
    LOG_WARN("construct array obj failed", K(ret));
  } else if (OB_FAIL(ctx.exec_ctx_.get_sqludt_meta_by_subschema_id(res_meta_id, value))) {
    LOG_WARN("failed to get subschema ctx", K(ret));
  } else if (value.type_ >= OB_SUBSCHEMA_MAX_TYPE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid subschema type", K(ret), K(value));
  } else {
    const ObSqlCollectionInfo *coll_info = reinterpret_cast<const ObSqlCollectionInfo *>(value.value_);
    ObCollectionArrayType *arr_type = static_cast<ObCollectionArrayType *>(coll_info->collection_meta_);
    ObCollectionBasicType *elem_type = static_cast<ObCollectionBasicType *>(arr_type->element_type_);
    ObExprArrayMapInfo *info = static_cast<ObExprArrayMapInfo *>(expr.extra_info_);
    ObIArrayType *child_obj = NULL;
    for (uint32_t i = 0; i < arr_dim && OB_SUCC(ret); i++) {
      ObSQLUtils::clear_expr_eval_flags(*expr.args_[0], ctx);
      for (uint32_t j = 0; j < info->param_num_ && OB_SUCC(ret); j++) {
        ObExpr *lambda_para = info->param_exprs_[j];
        uint32_t para_idx = info->param_idx_[j];
        if (para_idx >= (expr.arg_cnt_ - 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid param idx", K(ret), K(para_idx), K(expr.arg_cnt_));
        } else if (lambda_para != NULL) {
          if (arr_obj[para_idx]->is_null(i)) {
            lambda_para->locate_datum_for_write(ctx).set_null();
          } else {
            switch (lambda_para->obj_meta_.get_type()) {
              case ObTinyIntType: {
                ObArrayFixedSize<int8_t> *arr_ptr = static_cast<ObArrayFixedSize<int8_t> *>(arr_obj[para_idx]);
                int8_t val = (*arr_ptr)[i];
                lambda_para->locate_datum_for_write(ctx).set_int(val);
                break;
              }
              case ObInt32Type: {
                ObArrayFixedSize<int32_t> *arr_ptr = static_cast<ObArrayFixedSize<int32_t> *>(arr_obj[para_idx]);
                int32_t val = (*arr_ptr)[i];
                lambda_para->locate_datum_for_write(ctx).set_int32(val);
                break;
              }
              case ObIntType: {
                ObArrayFixedSize<int64_t> *arr_ptr = static_cast<ObArrayFixedSize<int64_t> *>(arr_obj[para_idx]);
                int64_t val = (*arr_ptr)[i];
                lambda_para->locate_datum_for_write(ctx).set_int(val);
                break;
              }
              case ObUInt64Type: {
                ObArrayFixedSize<uint64_t> *arr_ptr = static_cast<ObArrayFixedSize<uint64_t> *>(arr_obj[para_idx]);
                uint64_t val = (*arr_ptr)[i];
                lambda_para->locate_datum_for_write(ctx).set_uint(val);
                break;
              }
              case ObFloatType: {
                ObArrayFixedSize<float> *arr_ptr = static_cast<ObArrayFixedSize<float> *>(arr_obj[para_idx]);
                float val = (*arr_ptr)[i];
                lambda_para->locate_datum_for_write(ctx).set_float(val);
                break;
              }
              case ObDoubleType: {
                ObArrayFixedSize<double> *arr_ptr = static_cast<ObArrayFixedSize<double> *>(arr_obj[para_idx]);
                double val = (*arr_ptr)[i];
                lambda_para->locate_datum_for_write(ctx).set_double(val);
                break;
              }
              case ObVarcharType: {
                ObArrayBinary *binary_array = static_cast<ObArrayBinary *>(arr_obj[para_idx]);
                ObString val = (*binary_array)[i];
                lambda_para->locate_datum_for_write(ctx).set_string(val);
                break;
              }
              case ObCollectionSQLType: {
                ObArrayNested *nest_array = static_cast<ObArrayNested *>(arr_obj[para_idx]);
                ObIArrayType *child_type = nest_array->get_child_array();
                ObString elem_str;
                if (child_obj == NULL && OB_FAIL(child_type->clone_empty(tmp_allocator, child_obj, false))) {
                  LOG_WARN("clone empty failed", K(ret));
                } else if (FALSE_IT(child_obj->clear())) {
                } else if (OB_FAIL(nest_array->at(i, *child_obj))) {
                  LOG_WARN("get array element failed", K(ret), K(i));
                } else if (OB_FAIL(ObArrayExprUtils::set_array_res(child_obj, child_obj->get_raw_binary_len(), *lambda_para, ctx, elem_str))) {
                  LOG_WARN("get array binary string failed", K(ret));
                } else {
                  lambda_para->locate_datum_for_write(ctx).set_string(elem_str);
                }
                break;
              }
              default: {
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("unsupported element type", K(ret), K(lambda_para->obj_meta_.get_type()));
              }
            }
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(expr.args_[0]->eval(ctx, datum_val))) {
        LOG_WARN("failed to eval args", K(ret));
      } else if (elem_type->type_id_ == ObNestedType::OB_BASIC_TYPE) {
        if (OB_FAIL(ObArrayUtil::append(*arr_res, elem_type->basic_meta_.get_obj_type(), datum_val))) {
          LOG_WARN("failed to append array value", K(ret), K(i));
        }
      } else if (elem_type->type_id_ == ObNestedType::OB_ARRAY_TYPE || elem_type->type_id_ == ObNestedType::OB_VECTOR_TYPE) {
        uint16_t elem_subid = expr.args_[0]->obj_meta_.get_subschema_id();
        ObArrayNested *nest_array = static_cast<ObArrayNested *>(arr_res);
        if (OB_FAIL(ObArrayExprUtils::add_elem_to_nested_array(tmp_allocator, ctx, elem_subid, *datum_val, nest_array))) {
          LOG_WARN("failed to push back value", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObString res_str;
      if (OB_FAIL(arr_res->init())) {
        LOG_WARN("array init failed", K(ret));
      } else if (OB_FAIL(ObArrayExprUtils::set_array_res(arr_res, arr_res->get_raw_binary_len(), expr, ctx, res_str))) {
        LOG_WARN("get array binary string failed", K(ret));
      } else {
        res.set_string(res_str);
      }
    }
  }
  return ret;
}

int ObExprArrayMap::get_array_map_lambda_params(const ObRawExpr *raw_expr, ObArray<uint32_t> &param_idx,
                                                ObArray<ObExpr *> &param_exprs) const
{
  int ret = OB_SUCCESS;
  if (raw_expr->get_expr_type() == T_EXEC_VAR) {
    const ObVarRawExpr *var_expr = static_cast<const ObVarRawExpr *>(raw_expr);
    int64_t idx = var_expr->get_ref_index();
    bool found = false;
    if (OB_ISNULL(get_rt_expr(*raw_expr))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("expr is null", K(ret));
    }
    for (uint32_t i = 0; i < param_idx.count() && found && OB_SUCC(ret); i++) {
      if (idx == param_idx[i] && get_rt_expr(*raw_expr) == param_exprs[i]) {
        found = true;
      } else if (get_rt_expr(*raw_expr) == param_exprs[i]) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param idx mismatch", K(ret), K(idx), K(param_idx[i]));
      }
    }
    if (OB_SUCC(ret) && !found) {
      if (OB_FAIL(param_idx.push_back(idx))) {
        LOG_WARN("param idx append failed", K(ret), K(idx));
      } else if (OB_FAIL(param_exprs.push_back(get_rt_expr(*raw_expr)))) {
        LOG_WARN("param expr append failed", K(ret), K(idx));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < raw_expr->get_param_count(); i++) {
      const ObRawExpr *child_expr = NULL;
      if (OB_ISNULL(child_expr = raw_expr->get_param_expr(i))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret));
      } else if (child_expr->get_expr_type() == T_FUNC_SYS_ARRAY_MAP) {
        // do nothing
      } else if (OB_FAIL(get_array_map_lambda_params(child_expr, param_idx, param_exprs))) {
        LOG_WARN("construct array map info failed", K(ret));
      }
    }
  }

  return ret;
}


int ObExprArrayMap::cg_expr(ObExprCGCtx &expr_cg_ctx,
                            const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  ObIExprExtraInfo *extra_info = nullptr;
  if (OB_FAIL(ObExprExtraInfoFactory::alloc(*expr_cg_ctx.allocator_, rt_expr.type_, extra_info))) {
    LOG_WARN("Failed to allocate memory for ObExprArrayMapInfo", K(ret));
  } else if (OB_ISNULL(extra_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("extra_info should not be nullptr", K(ret));
  } else {
    ObExprArrayMapInfo *var_params_info = static_cast<ObExprArrayMapInfo *>(extra_info);
    ObArray<ObExpr *> param_exprs;
    ObArray<uint32_t> param_idx;
    if (OB_FAIL(get_array_map_lambda_params(&raw_expr, param_idx, param_exprs))) {
      LOG_WARN("get array map lambda params failed", K(ret));
    } else if (param_exprs.count() == 0) {
      // do nothing
    } else {
      int64_t alloc_size = param_exprs.count() * (sizeof(ObExpr *) + sizeof(uint32_t));
      var_params_info->param_num_ = param_exprs.count();
      char *buf = static_cast<char *>(expr_cg_ctx.allocator_->alloc(alloc_size));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret), K(alloc_size));
      } else {
        var_params_info->param_exprs_ = reinterpret_cast<ObExpr **>(buf);
        var_params_info->param_idx_ = reinterpret_cast<uint32_t *>(buf + (param_exprs.count() * sizeof(ObExpr*)));
        for (int64_t i = 0; OB_SUCC(ret) && i < param_exprs.count(); i++) {
          var_params_info->param_exprs_[i] = param_exprs[i];
          var_params_info->param_idx_[i] = param_idx[i];
        }
      }
    }
    if (OB_SUCC(ret)) {
      rt_expr.extra_info_ = extra_info;
      rt_expr.eval_func_ = eval_array_map;
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
