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

#include "sql/engine/expr/ob_expr_autoinc_nextval.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{
OB_SERIALIZE_MEMBER_INHERIT(ObExprAutoincNextval, ObExprOperator);
ObExprAutoincNextval::ObExprAutoincNextval(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_AUTOINC_NEXTVAL,
                         N_AUTOINC_NEXTVAL,
                         ZERO_OR_ONE,
                         NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
  /* NextVal是一个人肉生成的FuncOp */
  disable_operand_auto_cast();
}


ObExprAutoincNextval::ObExprAutoincNextval(
    common::ObIAllocator &alloc,
    ObExprOperatorType type,
    const char *name,
    int32_t param_num,
    ObValidForGeneratedColFlag valid_for_generated_col,
    int32_t dimension,
    bool is_internal_for_mysql/* = false */,
    bool is_internal_for_oracle/* = false */)
  : ObFuncExprOperator(alloc,
                       type,
                       name,
                       param_num,
                       valid_for_generated_col,
                       dimension,
                       is_internal_for_mysql,
                       is_internal_for_oracle)
{
  disable_operand_auto_cast();
}

ObExprAutoincNextval::~ObExprAutoincNextval()
{
}


int ObExprAutoincNextval::calc_result_typeN(ObExprResType &type,
                                     ObExprResType *types_array,
                                     int64_t param_num,
                                     ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (param_num == 1) {
    // 显式插入一个值，如 nextval(16, __values.c1)
    // 此场景下 nextval 的返回值类型和插入值保持一致
    type = types_array[0];
  } else {
    type.set_uint64();
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].scale_);
    type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].precision_);
  }
  type.set_result_flag(NOT_NULL_FLAG);


  CK(NULL != type_ctx.get_session());
  if (OB_SUCC(ret)) {
    if (OB_SUCC(ret) && 1 == param_num) {
      // column_conv() is add before nextval() in static tying engine. Parameter 2 is converted
      // to defined type, only int/uint/float/double allowed.
      ObObjTypeClass tc = ob_obj_type_class(type.get_type());
      if (!(ObNullTC == tc || ObIntTC == tc || ObUIntTC == tc
           || ObFloatTC == tc || ObDoubleTC == tc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("only int/uint/float/double type class supported for auto_increment column",
                 K(ret));
      } else {
        static_cast<ObObjMeta &>(type) = types_array[0];
      }
    }
  }

  return ret;
}

//check generate auto-inc value or not and cast.
int ObExprAutoincNextval::check_and_cast(ObObj &result,
                                         ObObjType result_type,
                                         const ObObj *objs_array,
                                         int64_t param_num,
                                         ObExprCtx &expr_ctx,
                                         AutoincParam *autoinc_param,
                                         bool &is_to_generate,
                                         uint64_t &casted_value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(autoinc_param) ||
      OB_ISNULL(expr_ctx.my_session_) ||
      OB_ISNULL(objs_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument(s)", K(ret));
  } else if (1 == param_num) {
    is_to_generate = true;
  } else if (2 == param_num) {
    // 如果是严格模式，则不合规的转换会报错，如 "abc" 不会被转换成 0
    if (CM_IS_STRICT_MODE(expr_ctx.my_session_->get_sql_mode())) {
      expr_ctx.cast_mode_ &= ~(CM_WARN_ON_FAIL);
    }
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    const ObObj &param = objs_array[1];
    bool try_sync = true;
    if (param.is_null()) {
      is_to_generate = true;
    } else {
      bool auto_val_on_zero = (!(SMO_NO_AUTO_VALUE_ON_ZERO & expr_ctx.my_session_->get_sql_mode()));
      // generate value on zero
      if (auto_val_on_zero && param.is_zero()) {
        is_to_generate = true;
      } else {
        ret = get_casted_value_by_result_type(cast_ctx,
                                              result_type,
                                              param,
                                              casted_value,
                                              try_sync);
        if (OB_SUCC(ret)) {
          if (auto_val_on_zero && 0 == casted_value) {
            // casted to zero; generate auto-increment value too
            is_to_generate = true;
          }
        } else {
          // see:
          if (autoinc_param->is_ignore_) {
            is_to_generate = true;
          }
        }
      }
    } //end else if

    // do not generate; sync value specified by user
    if (OB_SUCC(ret)) {
      if (!is_to_generate) {
        result = param;
        if (try_sync && casted_value > autoinc_param->value_to_sync_) {
          autoinc_param->value_to_sync_ = casted_value;
          autoinc_param->sync_flag_ = true;
        }
      }
    }
    // failed to cast; keep original value
    if (OB_FAIL(ret)) {
      result = param;
      ret = OB_SUCCESS;
      // sync value 0 (no use here)
      autoinc_param->value_to_sync_ = 0;
      autoinc_param->sync_flag_ = true;
    }
    // 如果是严格模式，则不合规的转换会报错，如 "abc" 不会被转换成 0
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Error unexpected", K(ret), K(param_num));
  }
  return ret;
}


// 这个函数要解决的问题是：当用户插入一个负数到 signed int 自增列时，
// 要允许插入。需要一个机制来判断：用户插入的是负数。本函数就是解决这个问题
//
// 详细 MySQL 行为参考 ：
//
// 出参说明：
// casted_value 用于设置到 ObPacket 的 lii_ 域，是一个 unsigned 值
//    当 param 是一个负数的时候，casted_value = UINT64_MAX
// try_sync 是为了处理兼容性问题：当 casted_value = UINT64_MAX 时，
//    通过设置 try_sync = false，使得不去和 last_sync_value 比较，
//    否则总是会把 UINT64_MAX sync 到其它节点作为插入的最大值。这不对。
int ObExprAutoincNextval::get_casted_value_by_result_type(ObCastCtx &cast_ctx,
                                                          ObObjType result_type,
                                                          const ObObj &param,
                                                          uint64_t &casted_value,
                                                          bool &try_sync)
{
  int ret = OB_SUCCESS;
  ObObj tmp_object;
  const ObObj *res_object = nullptr;
  if (OB_FAIL(ObObjCaster::to_type(result_type,
                                   cast_ctx,
                                   param,
                                   tmp_object,
                                   res_object))) {
    LOG_TRACE("fail cast param", K(param), K(ret));
  } else if (res_object->is_unsigned()) {
    // unsigned, cast to uint64_t
    EXPR_GET_UINT64_V2(*res_object, casted_value);
  } else {
    // signed, cast to int64_t
    int64_t value = 0;
    EXPR_GET_INT64_V2(*res_object, value);
    if (value < 0) {
      try_sync = false;
      casted_value = UINT64_MAX;
    } else {
      casted_value = static_cast<uint64_t>(value);
    }
  }
  return ret;
}

int ObExprAutoincNextval::get_uint_value(const ObExpr &input_expr,
                                         ObDatum *input_value,
                                         bool &is_zero, uint64_t &casted_value)
{
  int ret = OB_SUCCESS;
  if (NULL == input_value || input_value->is_null()) {
    is_zero = true;
    casted_value = 0;
  } else {
    ObObjTypeClass tc = ob_obj_type_class(input_expr.datum_meta_.type_);
    switch (tc) {
      case ObIntTC: {
        is_zero = 0 == input_value->get_int();
        casted_value = input_value->get_int() < 0 ? 0 : input_value->get_int();
        break;
      }
      case ObUIntTC: {
        is_zero = 0 == input_value->get_uint();
        casted_value = input_value->get_uint();
        break;
      }
      case ObFloatTC: {
        is_zero = 0 == input_value->get_float();
        if (input_value->get_float() > 0) {
          casted_value = static_cast<uint64_t>(input_value->get_float() + 0.5);
        } else {
          casted_value = 0;
        }
        break;
      }
      case ObDoubleTC: {
        is_zero = 0 == input_value->get_double();
        if (input_value->get_double() > 0) {
          casted_value = static_cast<uint64_t>(input_value->get_double() + 0.5);
        } else {
          casted_value = 0;
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("only int/float/double types support auto increment",
                 K(ret), K(input_expr.datum_meta_));
    }
  }
  return ret;
}

int ObExprAutoincNextval::get_input_value(const ObExpr &expr,
                                          ObEvalCtx &ctx,
                                          ObDatum *input_value,
                                          share::AutoincParam &autoinc_param,
                                          bool &is_to_generate,
                                          uint64_t &casted_value)
{
  int ret = OB_SUCCESS;
  if (NULL == input_value || input_value->is_null()) {
    is_to_generate = true;
  } else {
    bool is_zero = false;
    if (expr.arg_cnt_ == 1) {
      if (OB_ISNULL(expr.args_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr.args_ is null", K(ret));
      } else if (OB_FAIL(get_uint_value(*expr.args_[0], input_value, is_zero, casted_value))) {
        LOG_WARN("get casted unsigned int value failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (!(SMO_NO_AUTO_VALUE_ON_ZERO & ctx.exec_ctx_.get_my_session()->get_sql_mode())) {
        if (is_zero) {
          is_to_generate = true;
        }
      }
    }
  }

  // do not generate; sync value specified by user
  if (OB_SUCC(ret)) {
    if (!is_to_generate) {
      if (casted_value > autoinc_param.value_to_sync_) {
        autoinc_param.value_to_sync_ = casted_value;
        autoinc_param.sync_flag_ = true;
      }
    }
  }
  return ret;
}

int ObExprAutoincNextval::generate_autoinc_value(const ObSQLSessionInfo &my_session,
                                                 uint64_t &new_val,
                                                 ObAutoincrementService &auto_service,
                                                 AutoincParam *autoinc_param,
                                                 ObPhysicalPlanCtx *plan_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(autoinc_param) ||
      OB_ISNULL(plan_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument(s)", K(ret), K(autoinc_param), K(plan_ctx));
  } else {
    // sync insert value globally before sync value globally
    if (OB_FAIL(auto_service.sync_insert_value_global(*autoinc_param))) {
      LOG_WARN("failed to sync insert value globally", K(ret));
    }
    if (OB_SUCC(ret)) {
      uint64_t value = 0;
      CacheHandle *&cache_handle = autoinc_param->cache_handle_;
      // get cache handle when allocate first auto-increment value
      if (OB_ISNULL(cache_handle)) {
        if (OB_FAIL(auto_service.get_handle(*autoinc_param, cache_handle))) {
          LOG_WARN("failed to get auto_increment handle", K(ret));
        } else if (OB_ISNULL(cache_handle)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Error unexpceted", K(ret), K(cache_handle));
        }
      }

      if (OB_SUCC(ret)) {
        // get auto-increment value
        if (OB_FAIL(cache_handle->next_value(value))) {
          LOG_DEBUG("failed to get auto_increment value", K(ret), K(value));
          // release handle No.1
          auto_service.release_handle(cache_handle);
          // invalid cache handle; record count
          ++autoinc_param->autoinc_intervals_count_;
          if (OB_FAIL(auto_service.get_handle(*autoinc_param, cache_handle))) {
            LOG_WARN("failed to get auto_increment handle", K(ret));
          } else if (OB_FAIL(cache_handle->next_value(value))) {
            LOG_WARN("failed to get auto_increment value", K(ret));
          }
        }
      }
      if (OB_UNLIKELY(OB_DATA_OUT_OF_RANGE == ret) && !is_strict_mode(my_session.get_sql_mode())) {
        ret = OB_SUCCESS;
        value = ObAutoincrementService::get_max_value(autoinc_param->autoinc_col_type_);
      }
      if (OB_SUCC(ret)) {
        new_val = value;
        plan_ctx->set_autoinc_id_tmp(value);
      }
    }
  }
  return ret;
}

int ObExprAutoincNextval::cg_expr(
        ObExprCGCtx &op_cg_ctx,
        const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(0 == rt_expr.arg_cnt_ || 1 == rt_expr.arg_cnt_);
  if (OB_FAIL(ObAutoincNextvalInfo::init_autoinc_nextval_info(
          op_cg_ctx.allocator_, raw_expr, rt_expr, type_))) {
    LOG_WARN("fail to init_autoinc_nextval_info", K(ret), K(type_));
  } else {
    rt_expr.eval_func_ = eval_nextval;
  }
  return ret;
}

int ObExprAutoincNextval::eval_nextval(
    const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *input_value = NULL;
  ObPhysicalPlanCtx *plan_ctx = ctx.exec_ctx_.get_physical_plan_ctx();
  ObSQLSessionInfo *my_session = ctx.exec_ctx_.get_my_session();
  if (OB_ISNULL(plan_ctx) || OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no phy plan context", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, input_value))) {
    LOG_WARN("evaluate parameter failed", K(ret));
  } else {
    uint64_t autoinc_table_id =
            static_cast<ObAutoincNextvalInfo *>(expr.extra_info_)->autoinc_table_id_;
    uint64_t autoinc_col_id =
            static_cast<ObAutoincNextvalInfo *>(expr.extra_info_)->autoinc_col_id_;
    ObAutoincrementService &auto_service = ObAutoincrementService::get_instance();
    ObIArray<AutoincParam> &autoinc_params = plan_ctx->get_autoinc_params();
    bool is_to_generate = false;
    AutoincParam *autoinc_param = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < autoinc_params.count(); ++i) {
      if (autoinc_table_id == autoinc_params.at(i).autoinc_table_id_ &&
          autoinc_col_id == autoinc_params.at(i).autoinc_col_id_) {
        autoinc_param = &(autoinc_params.at(i));
        break;
      }
    }
    // this column with column_index is auto-increment column
    if (OB_ISNULL(autoinc_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should find auto-increment param", K(ret), K(autoinc_table_id), K(autoinc_col_id), K(autoinc_params));
    }

    // sync last user specified value first(compatible with MySQL)
    if (OB_SUCC(ret)) {
      if (OB_FAIL(auto_service.sync_insert_value_local(*autoinc_param))) {
        LOG_WARN("failed to sync last insert value", K(ret));
      }
    }

    uint64_t new_val = 0;
    if (OB_SUCC(ret)) {
      // check : to generate auto-increment value or not
      if (OB_FAIL(get_input_value(
              expr, ctx, input_value, *autoinc_param, is_to_generate, new_val))) {
        LOG_WARN("check generation failed", K(ret));
      } else if (is_to_generate && OB_FAIL(generate_autoinc_value(*my_session, new_val, auto_service,
                                                                  autoinc_param, plan_ctx))) {
        LOG_WARN("generate autoinc value failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (!is_to_generate) {
        expr_datum.set_datum(*input_value); // keep the input datum
      } else {
        ObObjTypeClass tc = ob_obj_type_class(expr.datum_meta_.type_);
        switch (tc) {
          case ObIntTC:
          case ObUIntTC: {
            expr_datum.set_uint(new_val);
            break;
          }
          case ObFloatTC: {
            expr_datum.set_float(static_cast<float>(new_val));
            break;
          }
          case ObDoubleTC: {
            expr_datum.set_double(static_cast<double>(new_val));
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("only int/float/double types support auto increment",
                     K(ret), K(expr.datum_meta_));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (autoinc_param->autoinc_desired_count_ > 0) {
        --autoinc_param->autoinc_desired_count_;
      }
      plan_ctx->set_autoinc_col_value(new_val);
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObAutoincNextvalExtra,
                    autoinc_table_id_,
                    autoinc_col_id_,
                    autoinc_table_name_,
                    autoinc_column_name_);

int ObAutoincNextvalExtra::init_autoinc_nextval_extra(common::ObIAllocator *allocator,
                                                      ObRawExpr *&expr,
                                                      const uint64_t autoinc_table_id,
                                                      const uint64_t autoinc_col_id,
                                                      const ObString autoinc_table_name,
                                                      const ObString autoinc_column_name)
{
  int ret = OB_SUCCESS;
  ObAutoincNextvalExtra *autoinc_nextval_extra = NULL;
  void *buf = NULL;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (OB_ISNULL(buf = allocator->alloc(sizeof(ObAutoincNextvalExtra)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    autoinc_nextval_extra = new(buf) ObAutoincNextvalExtra();
    autoinc_nextval_extra->autoinc_table_id_ = autoinc_table_id;
    autoinc_nextval_extra->autoinc_col_id_ = autoinc_col_id;
    autoinc_nextval_extra->autoinc_table_name_ = autoinc_table_name;
    autoinc_nextval_extra->autoinc_column_name_ = autoinc_column_name;
  }
  if (OB_SUCC(ret)) {
    expr->set_extra(reinterpret_cast<uint64_t>(autoinc_nextval_extra));
    LOG_DEBUG("succ init_autoinc_nextval_extra", KPC(autoinc_nextval_extra));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObAutoincNextvalInfo, autoinc_table_id_, autoinc_col_id_);

int ObAutoincNextvalInfo::init_autoinc_nextval_info(common::ObIAllocator *allocator,
                                                    const ObRawExpr &raw_expr,
                                                    ObExpr &expr,
                                                    const ObExprOperatorType type)
{
  int ret = OB_SUCCESS;
  ObAutoincNextvalExtra *autoinc_nextval_extra = NULL;
  ObAutoincNextvalInfo *autoinc_nextval_info = NULL;
  void *buf = NULL;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (OB_ISNULL(buf = allocator->alloc(sizeof(ObAutoincNextvalInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_ISNULL(autoinc_nextval_extra =
          reinterpret_cast<ObAutoincNextvalExtra *>(raw_expr.get_extra()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("raw_expr.extra_ is null", K(ret));
  } else {
    autoinc_nextval_info = new(buf) ObAutoincNextvalInfo(*allocator, type);
    autoinc_nextval_info->autoinc_table_id_ = autoinc_nextval_extra->autoinc_table_id_;
    autoinc_nextval_info->autoinc_col_id_ = autoinc_nextval_extra->autoinc_col_id_;
  }
  if (OB_SUCC(ret)) {
    expr.extra_info_ = autoinc_nextval_info;
    LOG_DEBUG("succ init_autoinc_nextval_info", KPC(autoinc_nextval_info));
  }
  return ret;
}

int ObAutoincNextvalInfo::deep_copy(common::ObIAllocator &allocator,
                                    const ObExprOperatorType type,
                                    ObIExprExtraInfo *&copied_info) const
{
  int ret = OB_SUCCESS;
  ObAutoincNextvalInfo *copied_autoinc_nextval_info = NULL;
  if (OB_FAIL(ObExprExtraInfoFactory::alloc(allocator, type, copied_info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc expr extra info", K(ret));
  } else if (OB_ISNULL(copied_autoinc_nextval_info =
          dynamic_cast<ObAutoincNextvalInfo *>(copied_autoinc_nextval_info))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret));
  } else {
    copied_autoinc_nextval_info->autoinc_table_id_ = autoinc_table_id_;
    copied_autoinc_nextval_info->autoinc_col_id_ = autoinc_col_id_;
  }
  return ret;
}

}//end namespace sql
}//end namespace oceanbase
