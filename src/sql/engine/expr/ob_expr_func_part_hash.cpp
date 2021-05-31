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

namespace oceanbase {
using namespace common;
namespace sql {

ObExprFuncPartOldHash::ObExprFuncPartOldHash(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PART_HASH_V1, N_PART_HASH_V1, MORE_THAN_ZERO, NOT_ROW_DIMENSION)
{}

ObExprFuncPartOldHash::~ObExprFuncPartOldHash()
{}

int ObExprFuncPartOldHash::calc_result_typeN(
    ObExprResType& type, ObExprResType* types_stack, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types_stack)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("objs_stack is null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
      ObObjTypeClass tc = types_stack[i].get_type_class();
      if ((share::is_oracle_mode()) &&
          ObResolverUtils::is_valid_oracle_partition_data_type(types_stack[i].get_type(), false)) {
        // do nothing since oracle mode support all these data types
      } else if (OB_UNLIKELY(ObIntTC != tc && ObUIntTC != tc)) {
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

int ObExprFuncPartOldHash::calc_resultN(
    ObObj& result, const ObObj* objs_stack, int64_t param_num, ObExprCtx& expr_ctx) const
{
  return calc_value(expr_ctx, objs_stack, param_num, result);
}

int ObExprFuncPartOldHash::calc_value_for_mysql(const ObObj& obj1, ObObj& result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(obj1.is_null())) {
    result.set_int(0);
  } else if (OB_UNLIKELY(ObIntTC != obj1.get_type_class() && ObUIntTC != obj1.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("type is wrong", K(ret), K(obj1));
  } else {
    int64_t num = (ObIntTC == obj1.get_type_class()) ? obj1.get_int() : static_cast<int64_t>(obj1.get_uint64());
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(INT64_MIN == num)) {
        num = INT64_MAX;
      } else {
        num = num < 0 ? -num : num;
      }
      result.set_int(num);
    } else {
      LOG_WARN("Failed to get value", K(ret), K(obj1));
    }
  }
  LOG_TRACE("calc hash value with mysql mode", K(obj1), K(result), K(ret));
  return ret;
}

uint64_t ObExprFuncPartOldHash::calc_hash_value_with_seed(const ObObj& obj, int64_t seed)
{
  uint64 hval = 0;
  ObObjType type = obj.get_type();
  if (ObCharType == type || ObNCharType == type) {
    ObObj obj_trimmed;
    int32_t val_len = obj.get_val_len();
    const char* obj1_str = obj.get_string_ptr();
    while (val_len >= 1) {
      if (OB_PADDING_CHAR == *(obj1_str + val_len - 1)) {
        --val_len;
      } else {
        break;
      }
    }
    obj_trimmed.set_collation_type(obj.get_collation_type());
    obj_trimmed.set_string(ObCharType, obj.get_string_ptr(), val_len);
    hval = obj_trimmed.hash(seed);
  } else {
    hval = obj.hash(seed);
  }
  return hval;
}

int ObExprFuncPartOldHash::calc_value_for_oracle(const ObObj* objs_stack, int64_t param_num, ObObj& result)
{
  int ret = OB_SUCCESS;
  uint64_t hash_code = 0;
  int64_t result_num = 0;
  if (OB_ISNULL(objs_stack) || 0 == param_num) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("objs_stack is null or number incorrect", K(objs_stack), K(param_num), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
    const ObObj& obj1 = objs_stack[i];
    const ObObjType type1 = obj1.get_type();
    if (ObNullType == type1) {
      // do nothing, hash_code not changed
    } else if (!is_oracle_supported_type(type1)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("type is wrong", K(ret), K(obj1), K(type1));
    } else {
      hash_code = calc_hash_value_with_seed(obj1, hash_code);
    }
  }
  result_num = static_cast<int64_t>(hash_code);
  result_num = result_num < 0 ? -result_num : result_num;
  result.set_int(result_num);
  LOG_TRACE("calc hash value with oracle mode", KP(objs_stack), K(objs_stack[0]), K(param_num), K(result), K(ret));
  return ret;
}

bool ObExprFuncPartOldHash::is_virtual_part_for_oracle(const ObTaskExecutorCtx* task_ec)
{
  return (NULL != task_ec && task_ec->get_calc_virtual_part_id_params().is_inited() &&
          OB_INVALID_ID != task_ec->get_calc_virtual_part_id_params().get_ref_table_id() &&
          is_ora_virtual_table(task_ec->get_calc_virtual_part_id_params().get_ref_table_id()));
}

int ObExprFuncPartOldHash::calc_value(ObExprCtx& expr_ctx, const ObObj* objs_stack, int64_t param_num, ObObj& result)
{
  int ret = OB_SUCCESS;
  // DO not change this function's result.
  // This will influence data.
  // If you need to do, remember ObTableLocation has the same code!!!
  CHECK_COMPATIBILITY_MODE(expr_ctx.my_session_);
  if (share::is_oracle_mode()) {
    if (OB_ISNULL(expr_ctx.exec_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("execute context is NULL", K(ret));
    } else {
      ObTaskExecutorCtx* task_ec = expr_ctx.exec_ctx_->get_task_executor_ctx();
      // if (NULL != task_ec && task_ec->get_calc_virtual_part_id_params().is_inited()
      //     && OB_INVALID_ID != task_ec->get_calc_virtual_part_id_params().get_ref_table_id()
      //     && is_ora_virtual_table(task_ec->get_calc_virtual_part_id_params().get_ref_table_id())) {
      if (is_virtual_part_for_oracle(task_ec)) {
        ret = calc_oracle_vt_part_id(
            *task_ec, task_ec->get_calc_virtual_part_id_params().get_ref_table_id(), objs_stack, param_num, result);
      } else {
        ret = calc_value_for_oracle(objs_stack, param_num, result);
      }
    }
  } else {
    // only one param for mysql
    if (OB_ISNULL(objs_stack) || 1 != param_num) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("objs_stack is null or number incorrect", K(objs_stack), K(param_num), K(ret));
    } else {
      ret = calc_value_for_mysql(objs_stack[0], result);
    }
  }
  return ret;
}

int ObExprFuncPartOldHash::calc_oracle_vt_part_id(ObTaskExecutorCtx& task_exec_ctx, const uint64_t table_id,
    const ObObj* objs_stack, int64_t param_num, ObObj& result)
{
  int ret = OB_SUCCESS;
  // only hash(svr_ip, svr_port) partition method supported in oracle virtual.
  if (OB_ISNULL(objs_stack) || param_num != 2 || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(objs_stack), K(param_num), K(table_id));
  } else if (!objs_stack[0].is_string_type() || !objs_stack[1].is_number()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(objs_stack[0]), K(objs_stack[1]));
  } else {
    ObString ip = objs_stack[0].get_string();
    number::ObNumber port_num;
    objs_stack[1].get_number(port_num);
    int32_t port = atoi(port_num.format());
    ObAddr addr;
    addr.set_ip_addr(ip, port);
    int64_t part_id = OB_INVALID_ID;
    if (OB_FAIL(task_exec_ctx.calc_virtual_partition_id(table_id, addr, part_id))) {
      LOG_WARN("calculate virtual table partition id failed", K(table_id), K(addr));
    } else {
      result.set_int(part_id);
    }
  }
  return ret;
}

int ObExprFuncPartOldHash::cg_expr(ObExprCGCtx&, const ObRawExpr&, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    if (1 != rt_expr.arg_cnt_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect one parameter in mysql", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = eval_old_part_hash;
  }
  return ret;
}

int ObExprFuncPartOldHash::eval_old_part_hash(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    ObTaskExecutorCtx* task_ec = ctx.exec_ctx_.get_task_executor_ctx();
    if (is_virtual_part_for_oracle(task_ec)) {
      if (OB_FAIL(eval_vt_old_part_id(
              expr, ctx, expr_datum, *task_ec, task_ec->get_calc_virtual_part_id_params().get_ref_table_id()))) {
        LOG_WARN("evaluate virtual partition hash failed", K(ret));
      }
    } else {
      if (OB_FAIL(eval_oracle_old_part_hash(expr, ctx, expr_datum, 0))) {
        LOG_WARN("evaluate oracle partition hash failed", K(ret));
      }
    }
  } else {
    // for mysql, see calc_value_for_mysql
    ObDatum* arg0 = NULL;
    if (OB_FAIL(expr.eval_param_value(ctx, arg0))) {
      LOG_WARN("evaluate parameter failed", K(ret));
    } else if (arg0->is_null()) {
      expr_datum.set_int(0);
    } else {
      // arg0 is ObIntTC or ObUIntTC, checked in calc_result_typeN
      expr_datum.set_int(std::abs(arg0->get_int()));
    }
  }
  return ret;
}

int ObExprFuncPartOldHash::eval_oracle_old_part_hash(
    const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, uint64_t seed)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = seed;
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i++) {
    ObDatum* d = NULL;
    const ObExpr& arg = *expr.args_[i];
    if (OB_FAIL(arg.eval(ctx, d))) {
      LOG_WARN("evaluate parameter failed", K(ret));
    } else if (d->is_null()) {
      // do nothing
    } else if (!is_oracle_supported_type(arg.datum_meta_.type_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wrong type", K(ret), K(arg.datum_meta_));
    } else {
      if (ObCharType == arg.datum_meta_.type_ || ObNCharType == arg.datum_meta_.type_) {
        ObDatum str = *d;
        const char* end = str.ptr_ + str.len_;
        while (end - str.ptr_ >= 1) {
          if (OB_PADDING_CHAR == *(end - 1)) {
            end -= 1;
          } else {
            break;
          }
        }
        str.len_ = end - str.ptr_;
        hash_val = arg.basic_funcs_->default_hash_(str, hash_val);
      } else {
        hash_val = arg.basic_funcs_->default_hash_(*d, hash_val);
      }
    }
  }
  if (OB_SUCC(ret)) {
    expr_datum.set_int(std::abs(static_cast<int64_t>(hash_val)));
  }
  return ret;
}

bool ObExprFuncPartOldHash::is_oracle_supported_type(const common::ObObjType type)
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
    case ObNVarchar2Type: {
      supported = true;
      break;
    }
    default: {
      supported = false;
    }
  }
  return supported;
}

int ObExprFuncPartOldHash::eval_vt_old_part_id(
    const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, ObTaskExecutorCtx& task_exec_ctx, const uint64_t table_id)
{
  // only hash(svr_ip, svr_port) partition method supported in oracle virtual.
  int ret = OB_SUCCESS;
  ObDatum* svr_ip = NULL;
  ObDatum* svr_port = NULL;
  if (2 != expr.arg_cnt_ || 0 == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(expr.arg_cnt_), K(table_id));
  } else if (!expr.args_[0]->obj_meta_.is_string_type() || !expr.args_[1]->obj_meta_.is_number()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(expr.args_[0]->obj_meta_), K(expr.args_[1]->obj_meta_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, svr_ip, svr_port))) {
    LOG_WARN("evaluate parameter failed", K(ret));
  } else {
    int64_t port = 0;
    if (!svr_port->is_null()) {
      if (OB_FAIL(ObExprUtil::trunc_num2int64(*svr_port, port))) {
        LOG_WARN("truncate number to int64 failed", K(ret));
      }
    }
    ObAddr addr;
    if (OB_SUCC(ret) && !svr_ip->is_null()) {
      addr.set_ip_addr(svr_ip->get_string(), static_cast<uint32_t>(port));
    }
    if (OB_SUCC(ret)) {
      int64_t part_id = OB_INVALID_ID;
      if (OB_FAIL(task_exec_ctx.calc_virtual_partition_id(table_id, addr, part_id))) {
        LOG_WARN("calculate virtual table partition id failed", K(table_id), K(addr));
      } else {
        expr_datum.set_int(part_id);
      }
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////
ObExprFuncPartHash::ObExprFuncPartHash(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PART_HASH_V2, N_PART_HASH_V2, MORE_THAN_ZERO, NOT_ROW_DIMENSION)
{}

ObExprFuncPartHash::~ObExprFuncPartHash()
{}

int ObExprFuncPartHash::calc_result_typeN(
    ObExprResType& type, ObExprResType* types_stack, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types_stack)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("objs_stack is null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
      ObObjTypeClass tc = types_stack[i].get_type_class();
      if ((share::is_oracle_mode()) &&
          ObResolverUtils::is_valid_oracle_partition_data_type(types_stack[i].get_type(), false)) {
        // do nothing since oracle mode support all these data types
      } else if (OB_UNLIKELY(ObIntTC != tc && ObUIntTC != tc)) {
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

int ObExprFuncPartHash::calc_resultN(
    ObObj& result, const ObObj* objs_stack, int64_t param_num, ObExprCtx& expr_ctx) const
{
  return calc_value(expr_ctx, objs_stack, param_num, result);
}

int ObExprFuncPartHash::calc_value_for_mysql(const ObObj& obj1, ObObj& result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(obj1.is_null())) {
    result.set_int(0);
  } else if (OB_UNLIKELY(ObIntTC != obj1.get_type_class() && ObUIntTC != obj1.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("type is wrong", K(ret), K(obj1));
  } else {
    int64_t num = (ObIntTC == obj1.get_type_class()) ? obj1.get_int() : static_cast<int64_t>(obj1.get_uint64());
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(INT64_MIN == num)) {
        num = INT64_MAX;
      } else {
        num = num < 0 ? -num : num;
      }
      result.set_int(num);
    } else {
      LOG_WARN("Failed to get value", K(ret), K(obj1));
    }
  }
  LOG_TRACE("calc hash value with mysql mode", K(obj1), K(result), K(ret));
  return ret;
}

uint64_t ObExprFuncPartHash::calc_hash_value_with_seed(const ObObj& obj, int64_t seed)
{
  uint64 hval = 0;
  ObObjType type = obj.get_type();
  if (ObCharType == type || ObNCharType == type) {
    ObObj obj_trimmed;
    int32_t val_len = obj.get_val_len();
    const char* obj1_str = obj.get_string_ptr();
    while (val_len >= 1) {
      if (OB_PADDING_CHAR == *(obj1_str + val_len - 1)) {
        --val_len;
      } else {
        break;
      }
    }
    obj_trimmed.set_collation_type(obj.get_collation_type());
    obj_trimmed.set_string(ObCharType, obj.get_string_ptr(), val_len);
    hval = obj_trimmed.hash_murmur(seed);
  } else {
    hval = obj.hash_murmur(seed);
  }
  return hval;
}

int ObExprFuncPartHash::calc_value_for_oracle(const ObObj* objs_stack, int64_t param_num, ObObj& result)
{
  int ret = OB_SUCCESS;
  uint64_t hash_code = 0;
  int64_t result_num = 0;
  if (OB_ISNULL(objs_stack) || 0 == param_num) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("objs_stack is null or number incorrect", K(objs_stack), K(param_num), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
    const ObObj& obj1 = objs_stack[i];
    const ObObjType type1 = obj1.get_type();
    if (ObNullType == type1) {
      // do nothing, hash_code not changed
    } else if (!is_oracle_supported_type(type1)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("type is wrong", K(ret), K(obj1), K(type1));
    } else {
      hash_code = calc_hash_value_with_seed(obj1, hash_code);
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
    case ObNVarchar2Type: {
      supported = true;
      break;
    }
    default: {
      supported = false;
    }
  }
  return supported;
}

bool ObExprFuncPartHash::is_virtual_part_for_oracle(const ObTaskExecutorCtx* task_ec)
{
  return (NULL != task_ec && task_ec->get_calc_virtual_part_id_params().is_inited() &&
          OB_INVALID_ID != task_ec->get_calc_virtual_part_id_params().get_ref_table_id() &&
          is_ora_virtual_table(task_ec->get_calc_virtual_part_id_params().get_ref_table_id()));
}

int ObExprFuncPartHash::calc_value(ObExprCtx& expr_ctx, const ObObj* objs_stack, int64_t param_num, ObObj& result)
{
  int ret = OB_SUCCESS;
  // DO not change this function's result.
  // This will influence data.
  // If you need to do, remember ObTableLocation has the same code!!!
  CHECK_COMPATIBILITY_MODE(expr_ctx.my_session_);
  if (share::is_oracle_mode()) {
    // Oracle support multi column hash partition:
    //   CREATE TABLE HASH_PART_TAB (ID NUMBER,DEAL_DATE DATE,AREA_CODE NUMBER)
    //   PARTITION BY HASH (DEAL_DATE, ID) PARTITIONS 12;
    if (OB_ISNULL(expr_ctx.exec_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("execute context is NULL", K(ret));
    } else {
      ObTaskExecutorCtx* task_ec = expr_ctx.exec_ctx_->get_task_executor_ctx();
      // if (NULL != task_ec && task_ec->get_calc_virtual_part_id_params().is_inited()
      //     && OB_INVALID_ID != task_ec->get_calc_virtual_part_id_params().get_ref_table_id()
      //     && is_ora_virtual_table(task_ec->get_calc_virtual_part_id_params().get_ref_table_id())) {
      if (is_virtual_part_for_oracle(task_ec)) {
        ret = calc_oracle_vt_part_id(
            *task_ec, task_ec->get_calc_virtual_part_id_params().get_ref_table_id(), objs_stack, param_num, result);
      } else {
        ret = calc_value_for_oracle(objs_stack, param_num, result);
      }
    }
  } else {
    // only one param in mysql
    if (OB_ISNULL(objs_stack) || 1 != param_num) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("objs_stack is null or number incorrect", K(objs_stack), K(param_num), K(ret));
    } else {
      ret = calc_value_for_mysql(objs_stack[0], result);
    }
  }
  return ret;
}

int ObExprFuncPartHash::calc_oracle_vt_part_id(ObTaskExecutorCtx& task_exec_ctx, const uint64_t table_id,
    const ObObj* objs_stack, int64_t param_num, ObObj& result)
{
  int ret = OB_SUCCESS;
  // only hash(svr_ip, svr_port) partition method supported in oracle virtual.
  if (OB_ISNULL(objs_stack) || param_num != 2 || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(objs_stack), K(param_num), K(table_id));
  } else if (!objs_stack[0].is_string_type() || !objs_stack[1].is_number()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(objs_stack[0]), K(objs_stack[1]));
  } else {
    ObString ip = objs_stack[0].get_string();
    number::ObNumber port_num;
    objs_stack[1].get_number(port_num);
    int32_t port = atoi(port_num.format());
    ObAddr addr;
    addr.set_ip_addr(ip, port);
    int64_t part_id = OB_INVALID_ID;
    if (OB_FAIL(task_exec_ctx.calc_virtual_partition_id(table_id, addr, part_id))) {
      LOG_WARN("calculate virtual table partition id failed", K(table_id), K(addr));
    } else {
      result.set_int(part_id);
    }
  }
  return ret;
}

int ObExprFuncPartHash::eval_vt_part_id(
    const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, ObTaskExecutorCtx& task_exec_ctx, const uint64_t table_id)
{
  // only hash(svr_ip, svr_port) partition method supported in oracle virtual.
  int ret = OB_SUCCESS;
  ObDatum* svr_ip = NULL;
  ObDatum* svr_port = NULL;
  if (2 != expr.arg_cnt_ || 0 == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(expr.arg_cnt_), K(table_id));
  } else if (!expr.args_[0]->obj_meta_.is_string_type() || !expr.args_[1]->obj_meta_.is_number()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(expr.args_[0]->obj_meta_), K(expr.args_[1]->obj_meta_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, svr_ip, svr_port))) {
    LOG_WARN("evaluate parameter failed", K(ret));
  } else {
    int64_t port = 0;
    if (!svr_port->is_null()) {
      if (OB_FAIL(ObExprUtil::trunc_num2int64(*svr_port, port))) {
        LOG_WARN("truncate number to int64 failed", K(ret));
      }
    }
    ObAddr addr;
    if (OB_SUCC(ret) && !svr_ip->is_null()) {
      addr.set_ip_addr(svr_ip->get_string(), static_cast<uint32_t>(port));
    }
    if (OB_SUCC(ret)) {
      int64_t part_id = OB_INVALID_ID;
      if (OB_FAIL(task_exec_ctx.calc_virtual_partition_id(table_id, addr, part_id))) {
        LOG_WARN("calculate virtual table partition id failed", K(table_id), K(addr));
      } else {
        expr_datum.set_int(part_id);
      }
    }
  }
  return ret;
}

int ObExprFuncPartHash::cg_expr(ObExprCGCtx&, const ObRawExpr&, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    if (1 != rt_expr.arg_cnt_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect one parameter in mysql", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = eval_part_hash;
  }
  return ret;
}

int ObExprFuncPartHash::eval_part_hash(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    ObTaskExecutorCtx* task_ec = ctx.exec_ctx_.get_task_executor_ctx();
    if (is_virtual_part_for_oracle(task_ec)) {
      if (OB_FAIL(eval_vt_part_id(
              expr, ctx, expr_datum, *task_ec, task_ec->get_calc_virtual_part_id_params().get_ref_table_id()))) {
        LOG_WARN("evaluate virtual partition hash failed", K(ret));
      }
    } else {
      if (OB_FAIL(eval_oracle_part_hash(expr, ctx, expr_datum, 0))) {
        LOG_WARN("evaluate oracle partition hash failed", K(ret));
      } else {
        expr_datum.set_int(std::abs(expr_datum.get_int()));
      }
    }
  } else {
    // for mysql, see calc_value_for_mysql
    ObDatum* arg0 = NULL;
    if (OB_FAIL(expr.eval_param_value(ctx, arg0))) {
      LOG_WARN("evaluate parameter failed", K(ret));
    } else if (arg0->is_null()) {
      expr_datum.set_int(0);
    } else {
      // arg0 is ObIntTC or ObUIntTC, checked in calc_result_typeN
      expr_datum.set_int(std::abs(arg0->get_int()));
    }
  }
  return ret;
}

int ObExprFuncPartHash::eval_oracle_part_hash(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, uint64_t seed)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = seed;
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i++) {
    ObDatum* d = NULL;
    const ObExpr& arg = *expr.args_[i];
    if (OB_FAIL(arg.eval(ctx, d))) {
      LOG_WARN("evaluate parameter failed", K(ret));
    } else if (d->is_null()) {
      // do nothing
    } else if (!is_oracle_supported_type(arg.datum_meta_.type_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wrong type", K(ret), K(arg.datum_meta_));
    } else {
      if (ObCharType == arg.datum_meta_.type_ || ObNCharType == arg.datum_meta_.type_) {
        ObDatum str = *d;
        const char* end = str.ptr_ + str.len_;
        while (end - str.ptr_ >= 1) {
          if (OB_PADDING_CHAR == *(end - 1)) {
            end -= 1;
          } else {
            break;
          }
        }
        str.len_ = end - str.ptr_;
        hash_val = arg.basic_funcs_->murmur_hash_(str, hash_val);
      } else {
        hash_val = arg.basic_funcs_->murmur_hash_(*d, hash_val);
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
