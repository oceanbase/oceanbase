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
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_func_part_hash.h"
#include "lib/oblog/ob_log.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/engine/expr/ob_expr_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprFuncPartHashBase::ObExprFuncPartHashBase(common::ObIAllocator &alloc, ObExprOperatorType type,
          const char *name, int32_t param_num, int32_t dimension,
          bool is_internal_for_mysql,
          bool is_internal_for_oracle)
    : ObFuncExprOperator(alloc, type, name, param_num, NOT_VALID_FOR_GENERATED_COL, dimension,
                         is_internal_for_mysql, is_internal_for_oracle)
{
}

template<typename T>
int ObExprFuncPartHashBase::calc_value_for_mysql(const T &input, T &output,
                                                 const ObObjType input_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(input.is_null())) {
    output.set_int(0);
  } else {
    int64_t num = 0;
    switch (ob_obj_type_class(input_type)) {
      case ObIntTC: {
        num = input.get_int();
        break;
      }
      case ObUIntTC: {
        num = static_cast<int64_t>(input.get_uint64());
        break;
      }
      case ObBitTC: {
        num = static_cast<int64_t>(input.get_bit());
        break;
      }
      case ObYearTC: {
        num = static_cast<int64_t>(input.get_year());
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("type is wrong", K(ret), K(input_type));
        break;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(INT64_MIN == num)) {
        num = INT64_MAX;
      } else {
        num = num < 0 ? -num : num;
      }
      output.set_int(num);
    } else {
      LOG_WARN("Failed to get value", K(ret));
    }
  }
  LOG_TRACE("calc hash value with mysql mode", K(ret));
  return ret;
}

ObExprFuncPartHash::ObExprFuncPartHash(ObIAllocator &alloc)
    : ObExprFuncPartHashBase(alloc, T_FUN_SYS_PART_HASH, N_PART_HASH, MORE_THAN_ZERO, NOT_ROW_DIMENSION)
{
}

ObExprFuncPartHash::~ObExprFuncPartHash()
{
}

int ObExprFuncPartHash::calc_result_typeN(ObExprResType &type,
                                          ObExprResType *types_stack,
                                          int64_t param_num,
                                          ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types_stack)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("objs_stack is null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
      ObObjTypeClass tc = types_stack[i].get_type_class();
      if ((lib::is_oracle_mode())
          && ObResolverUtils::is_valid_oracle_partition_data_type(types_stack[i].get_type(),
                                                                  false)) {
        //do nothing since oracle mode support all these data types
      } else if (OB_UNLIKELY(ObIntTC != tc && ObUIntTC != tc && ObBitTC != tc && ObYearTC != tc)) {
        ret = OB_ERR_PARTITION_FUNC_NOT_ALLOWED_ERROR;
        LOG_WARN("expr type class is not correct", "type", types_stack[i].get_type_class());
        LOG_USER_ERROR(OB_ERR_PARTITION_FUNC_NOT_ALLOWED_ERROR);
      }
    }
  }
  if (OB_SUCC(ret)) {
    type.set_int();
    type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
    type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  }
  return ret;
}

int ObExprFuncPartHash::calc_hash_value_with_seed(const ObObj &obj, int64_t seed, uint64_t &res)
{
  int ret = OB_SUCCESS;
  ObObjType type = obj.get_type();
  //定长类型需要去除末尾空格, 见
  if (ObCharType == type || ObNCharType == type) {
    ObObj obj_trimmed;
    int32_t val_len = obj.get_val_len();
    const char* obj1_str = obj.get_string_ptr();
    bool is_utf16 = ObCharset::charset_type_by_coll(obj.get_collation_type()) == CHARSET_UTF16;
    while (val_len >= (is_utf16 ? 2 : 1)) {
      if (is_utf16
          && OB_PADDING_CHAR == *(obj1_str + val_len - 1)
          && OB_PADDING_BINARY == *(obj1_str + val_len - 2)) {
          val_len -= 2;
      } else if (OB_PADDING_CHAR == *(obj1_str + val_len - 1)) {
        --val_len;
      } else {
        break;
      }
    }
    obj_trimmed.set_collation_type(obj.get_collation_type());
    obj_trimmed.set_string(ObCharType, obj.get_string_ptr(), val_len);
    if (OB_FAIL(obj_trimmed.hash_murmur(res, seed))) {
      LOG_WARN("fail to do hash", K(ret));
    }
  } else {
    if (OB_FAIL(obj.hash_murmur(res, seed))) {
      LOG_WARN("fail to do hash", K(ret));
    }
  }
  return ret;
}
int ObExprFuncPartHash::calc_value_for_oracle(const ObObj *objs_stack,
                                              int64_t param_num,
                                              ObObj &result)
{
  int ret = OB_SUCCESS;
  uint64_t hash_code = 0;
  int64_t result_num = 0;
  if (OB_ISNULL(objs_stack) || 0 == param_num) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("objs_stack is null or number incorrect", K(objs_stack), K(param_num), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
    const ObObj &obj1 = objs_stack[i];
    const ObObjType type1 = obj1.get_type();
    if (ObNullType == type1) {
      //do nothing, hash_code not changed
    } else if (!is_oracle_supported_type(type1)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("type is wrong", K(ret), K(obj1), K(type1));
    } else if (OB_FAIL(calc_hash_value_with_seed(obj1, hash_code, hash_code))) {
      LOG_WARN("fail to do hash", K(ret));
    }
  }
  result_num = static_cast<int64_t>(hash_code);
  result_num = result_num < 0 ? -result_num : result_num;
  result.set_int(result_num);
  LOG_TRACE("calc hash value with oracle mode", KP(objs_stack), K(objs_stack[0]), K(param_num), K(result), K(ret));
  return ret;
}
bool ObExprFuncPartHash::is_oracle_supported_type(const common::ObObjType type)
{
  bool supported = false;
  switch (type) {
    case ObIntType:
    case ObFloatType:
    case ObDoubleType:
    case ObNumberType:
    case ObDateTimeType:
    case ObCharType:
    case ObVarcharType:
    case ObTimestampTZType:
    case ObTimestampLTZType:
    case ObTimestampNanoType:
    case ObRawType:
    case ObIntervalYMType:
    case ObIntervalDSType:
    case ObNumberFloatType:
    case ObNCharType:
    case ObNVarchar2Type:
    case ObURowIDType: {
      supported = true;
      break;
    }
    default: {
      supported = false;
    }
  }
  return supported;
}

bool ObExprFuncPartHash::is_virtual_part_for_oracle(const ObTaskExecutorCtx *task_ec)
{
  return (NULL != task_ec && task_ec->get_calc_virtual_part_id_params().is_inited()
          && OB_INVALID_ID != task_ec->get_calc_virtual_part_id_params().get_ref_table_id()
          && is_ora_virtual_table(task_ec->get_calc_virtual_part_id_params().get_ref_table_id()));
}

int ObExprFuncPartHash::calc_value(
    ObExprCtx &expr_ctx,
    const ObObj *objs_stack,
    int64_t param_num,
    ObObj &result)
{
  int ret = OB_SUCCESS;
  //DO not change this function's result.
  //This will influence data.
  //If you need to do, remember ObTableLocation has the same code!!!
  CHECK_COMPATIBILITY_MODE(expr_ctx.my_session_);
  if (lib::is_oracle_mode()) {
    // Oracle 的 hash 分区允许多列，例如：
    //   CREATE TABLE HASH_PART_TAB (ID NUMBER,DEAL_DATE DATE,AREA_CODE NUMBER)
    //   PARTITION BY HASH (DEAL_DATE, ID) PARTITIONS 12;
    if (OB_ISNULL(expr_ctx.exec_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("execute context is NULL", K(ret));
    } else {
      ObTaskExecutorCtx *task_ec = expr_ctx.exec_ctx_->get_task_executor_ctx();
      // if (NULL != task_ec && task_ec->get_calc_virtual_part_id_params().is_inited()
      //     && OB_INVALID_ID != task_ec->get_calc_virtual_part_id_params().get_ref_table_id()
      //     && is_ora_virtual_table(task_ec->get_calc_virtual_part_id_params().get_ref_table_id())) {
      if (is_virtual_part_for_oracle(task_ec)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("virtual table can't partition by hash", K(ret));
      } else {
        ret = calc_value_for_oracle(objs_stack, param_num, result);
      }
    }
  } else {
    //mysql模式仅允许一个参数, 语法上就已限制
    if (OB_ISNULL(objs_stack) || 1 != param_num) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("objs_stack is null or number incorrect", K(objs_stack), K(param_num), K(ret));
    } else {
      ret = calc_value_for_mysql(objs_stack[0], result, objs_stack[0].get_type());
    }
  }
  return ret;
}

int ObExprFuncPartHash::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    if (1 != rt_expr.arg_cnt_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("expect one parameter in mysql", K(ret));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "part hash");
    }
  }

  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = eval_part_hash;
  }
  return ret;
}

int ObExprFuncPartHash::eval_part_hash(
    const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    ObTaskExecutorCtx *task_ec = ctx.exec_ctx_.get_task_executor_ctx();
    if (is_virtual_part_for_oracle(task_ec)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("virtual table can't partition by hash", K(ret));
    } else {
      if (OB_FAIL(eval_oracle_part_hash(expr, ctx, expr_datum, 0))) {
        LOG_WARN("evaluate oracle partition hash failed", K(ret));
      } else {
        expr_datum.set_int(std::abs(expr_datum.get_int()));
      }
    }
  } else {
    // for mysql, see calc_value_for_mysql
    ObDatum *arg0 = NULL;
    if (OB_FAIL(expr.eval_param_value(ctx, arg0))) {
      LOG_WARN("evaluate parameter failed", K(ret));
    } else if (arg0->is_null()) {
      expr_datum.set_int(0);
    } else if (OB_FAIL(calc_value_for_mysql(*arg0, expr_datum, expr.args_[0]->datum_meta_.type_))) {
      LOG_WARN("calc value for mysql failed", K(ret));
    }
  }
  return ret;
}

int ObExprFuncPartHash::eval_oracle_part_hash(
    const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum, uint64_t seed)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = seed;
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i++) {
    ObDatum *d = NULL;
    const ObExpr &arg = *expr.args_[i];
    if (OB_FAIL(arg.eval(ctx, d))) {
      LOG_WARN("evaluate parameter failed", K(ret));
    } else if (d->is_null()) {
      // do nothing
    } else if (!is_oracle_supported_type(arg.datum_meta_.type_)) {
      if (ob_is_user_defined_sql_type(arg.datum_meta_.type_)) {
        ret = OB_ERR_INVALID_XML_DATATYPE;
        LOG_USER_ERROR(OB_ERR_INVALID_XML_DATATYPE, "-", "ANYDATA");
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("wrong type", K(ret), K(arg.datum_meta_));
      }
    } else {
      if (ObCharType == arg.datum_meta_.type_
          || ObNCharType == arg.datum_meta_.type_) {
        ObDatum str = *d;
        const bool is_utf16 = CHARSET_UTF16 == ObCharset::charset_type_by_coll(
            arg.datum_meta_.cs_type_);
        const char *end = str.ptr_ + str.len_;
        while (end - str.ptr_ >= (is_utf16 ? 2 : 1)) {
          if (is_utf16 && OB_PADDING_CHAR == *(end - 1) && OB_PADDING_BINARY == *(end - 2)) {
            end -= 2;
          } else if (OB_PADDING_CHAR == *(end - 1)) {
            end -= 1;
          } else {
            break;
          }
        }
        str.len_ = end - str.ptr_;
        if (OB_FAIL(arg.basic_funcs_->murmur_hash_(str, hash_val, hash_val))) {
          LOG_WARN("hash failed", K(ret));
        }
      } else {
        if (OB_FAIL(arg.basic_funcs_->murmur_hash_(*d, hash_val, hash_val))) {
          LOG_WARN("hash failed", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    expr_datum.set_int(static_cast<int64_t>(hash_val));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
