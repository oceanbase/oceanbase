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

namespace oceanbase {
using namespace common;
using namespace share;
namespace sql {
ObExprAutoincNextval::ObExprAutoincNextval(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_AUTOINC_NEXTVAL, N_AUTOINC_NEXTVAL, ONE_OR_TWO, NOT_ROW_DIMENSION)
{
  disable_operand_auto_cast();
}

ObExprAutoincNextval::~ObExprAutoincNextval()
{}

int ObExprAutoincNextval::calc_result_typeN(
    ObExprResType& type, ObExprResType* types_array, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  type.set_uint64();
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].scale_);
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].precision_);
  type.set_result_flag(OB_MYSQL_NOT_NULL_FLAG);

  CK(NULL != type_ctx.get_session());
  if (OB_SUCC(ret) && type_ctx.get_session()->use_static_typing_engine()) {
    CK(types_array[0].is_uint64());
    if (OB_SUCC(ret) && 2 == param_num) {
      // column_conv() is add before nextval() in static tying engine. Parameter 2 is converted
      // to defined type, only int/uint/float/double allowed.
      ObObjTypeClass tc = ob_obj_type_class(type.get_type());
      if (!(ObNullTC == tc || ObIntTC == tc || ObUIntTC == tc || ObFloatTC == tc || ObDoubleTC == tc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("only int/uint/float/double type class supported for auto_increment column", K(ret));
      } else {
        static_cast<ObObjMeta&>(type) = types_array[1];
      }
    }
  }

  return ret;
}

int ObExprAutoincNextval::calc_resultN(
    ObObj& result, const ObObj* objs_array, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(1 != param_num && 2 != param_num) || OB_ISNULL(objs_array) || OB_ISNULL(expr_ctx.phy_plan_ctx_) ||
      OB_UNLIKELY(!objs_array[0].is_uint64())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument(s)", K(ret), K(objs_array), K(param_num), K(objs_array), K(expr_ctx.phy_plan_ctx_));
  } else {
    ObAutoincrementService& auto_service = ObAutoincrementService::get_instance();
    ObPhysicalPlanCtx* plan_ctx = expr_ctx.phy_plan_ctx_;
    ObIArray<AutoincParam>& autoinc_params = plan_ctx->get_autoinc_params();
    bool is_to_generate = false;
    AutoincParam* autoinc_param = NULL;
    uint64_t autoinc_col_id = objs_array[0].get_uint64();
    for (int64_t i = 0; OB_SUCC(ret) && i < autoinc_params.count(); ++i) {
      if (autoinc_col_id == autoinc_params.at(i).autoinc_col_id_) {
        autoinc_param = &(autoinc_params.at(i));
        break;
      }
    }
    // this column with column_index is auto-increment column
    if (OB_ISNULL(autoinc_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should find auto-increment param", K(ret));
    }

    // sync last user specified value first(compatible with MySQL)
    if (OB_SUCC(ret)) {
      if (OB_FAIL(auto_service.sync_insert_value_local(*autoinc_param))) {
        LOG_WARN("failed to sync last insert value", K(ret));
      }
    }

    uint64_t casted_value = 0;
    if (OB_SUCC(ret)) {
      uint64_t new_val = 0;
      // check : to generate auto-increment value or not
      if (OB_FAIL(
              check_and_cast(result, objs_array, param_num, expr_ctx, autoinc_param, is_to_generate, casted_value))) {
        LOG_WARN("check generation failed", K(ret));

      } else if (is_to_generate && OB_FAIL(generate_autoinc_value(new_val, auto_service, autoinc_param, plan_ctx))) {
        LOG_WARN("generate autoinc value failed", K(ret));
      } else if (is_to_generate) {
        result.set_uint64(new_val);
      }
    }

    if (OB_SUCC(ret)) {
      if (autoinc_param->autoinc_desired_count_ > 0) {
        --autoinc_param->autoinc_desired_count_;
      }

      if (OB_HIDDEN_PK_INCREMENT_COLUMN_ID != autoinc_param->autoinc_col_id_) {
        if (is_to_generate) {
          plan_ctx->set_autoinc_col_value(result.get_uint64());
        } else {
          plan_ctx->set_autoinc_col_value(casted_value);
        }
      }
    }
  }
  return ret;
}

// check generate auto-inc value or not and cast.
int ObExprAutoincNextval::check_and_cast(ObObj& result, const ObObj* objs_array, int64_t param_num, ObExprCtx& expr_ctx,
    AutoincParam* autoinc_param, bool& is_to_generate, uint64_t& casted_value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(autoinc_param) || OB_ISNULL(expr_ctx.my_session_) || OB_ISNULL(objs_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument(s)", K(ret));
  } else if (1 == param_num) {
    is_to_generate = true;
  } else if (2 == param_num) {
    expr_ctx.cast_mode_ &= ~(CM_WARN_ON_FAIL);
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    const ObObj& param = objs_array[1];
    if (param.is_null()) {
      is_to_generate = true;
    } else if (!(SMO_NO_AUTO_VALUE_ON_ZERO & expr_ctx.my_session_->get_sql_mode())) {
      // generate value on zero
      if (param.is_zero()) {
        is_to_generate = true;
      } else {
        EXPR_GET_UINT64_V2(param, casted_value);
        if (OB_SUCC(ret)) {
          if (0 == casted_value) {
            // casted to zero; generate auto-increment value too
            is_to_generate = true;
          }
        } else {
          // the code "EXPR_GET_UINT64_V2(param, casted_value)" above is wrong,
          // auto increment column can be int or double in mysql, the correct operation
          // should be:
          // 1. cast param to column type, not the hard-coding 'uint'.
          // 2. if cast failed and sql mode is non-strict, set is_to_generate to true.
          //
          // cases below are all in non-strict mode.
          //
          // case 1:
          // column c1 is tinyint auto increment without unsigned, then insert -12 into c1.
          // -12 should be casted to tinyint or int, and we will not generate next value.
          // but we try to cast -12 to uint and get an error, we MUST NOT set is_to_generate
          // to true below.
          //
          // case 2:
          // column c1 is tinyint auto increment without unsigned, then insert 'abc' into c1.
          // try to cast 'abc' to tinyint or uint will always throw an error, but this time
          // we MUST set is_to_generate to true to generate nextval.
          //
          // so I removed the condition 'is_strict_mode(expr_ctx.my_session_->get_sql_mode())'
          // below, until we cast param to column type.

          if (autoinc_param->is_ignore_ || false /*!is_strict_mode(expr_ctx.my_session_->get_sql_mode())*/) {
            is_to_generate = true;
          }
        }
      }
    }  // end else if

    // do not generate; sync value specified by user
    if (OB_SUCC(ret)) {
      if (!is_to_generate) {
        result = param;
        // sync value
        if (0 == casted_value) {
          EXPR_GET_UINT64_V2(param, casted_value);
        }
        if (OB_SUCC(ret)) {
          if (casted_value > autoinc_param->value_to_sync_) {
            autoinc_param->value_to_sync_ = casted_value;
            autoinc_param->sync_flag_ = true;
          }
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
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Error unexpected", K(ret), K(param_num));
  }
  return ret;
}

int ObExprAutoincNextval::get_uint_value(
    const ObExpr& input_expr, ObDatum* input_value, bool& is_zero, uint64_t& casted_value)
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
        LOG_WARN("only int/float/double types support auto increment", K(ret), K(input_expr.datum_meta_));
    }
  }
  return ret;
}

int ObExprAutoincNextval::get_input_value(const ObExpr& input_expr, ObEvalCtx& ctx, ObDatum* input_value,
    share::AutoincParam& autoinc_param, bool& is_to_generate, uint64_t& casted_value)
{
  int ret = OB_SUCCESS;
  if (NULL == input_value || input_value->is_null()) {
    is_to_generate = true;
  } else {
    bool is_zero = false;
    if (OB_FAIL(get_uint_value(input_expr, input_value, is_zero, casted_value))) {
      LOG_WARN("get casted unsigned int value failed", K(ret));
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

int ObExprAutoincNextval::generate_autoinc_value(
    uint64_t& new_val, ObAutoincrementService& auto_service, AutoincParam* autoinc_param, ObPhysicalPlanCtx* plan_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(autoinc_param) || OB_ISNULL(plan_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument(s)", K(ret), K(autoinc_param), K(plan_ctx));
  } else {
    // sync insert value globally before sync value globally
    if (OB_FAIL(auto_service.sync_insert_value_global(*autoinc_param))) {
      LOG_WARN("failed to sync insert value globally", K(ret));
    }
    if (OB_SUCC(ret)) {
      uint64_t value = 0;
      CacheHandle*& cache_handle = autoinc_param->cache_handle_;
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

      if (OB_SUCC(ret)) {
        new_val = value;
        if (OB_HIDDEN_PK_INCREMENT_COLUMN_ID != autoinc_param->autoinc_col_id_) {
          plan_ctx->set_autoinc_id_tmp(value);
        }
      }
    }
  }
  return ret;
}

int ObExprAutoincNextval::cg_expr(ObExprCGCtx&, const ObRawExpr&, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(1 == rt_expr.arg_cnt_ || 2 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = eval_nextval;
  return ret;
}

int ObExprAutoincNextval::eval_nextval(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* col_id = NULL;
  ObDatum* input_value = NULL;
  ObPhysicalPlanCtx* plan_ctx = ctx.exec_ctx_.get_physical_plan_ctx();
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no phy plan context", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, col_id, input_value))) {
    LOG_WARN("evaluate parameter failed", K(ret));
  } else if (col_id->is_null()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("id is null", K(ret));
  } else {
    ObAutoincrementService& auto_service = ObAutoincrementService::get_instance();
    ObIArray<AutoincParam>& autoinc_params = plan_ctx->get_autoinc_params();
    bool is_to_generate = false;
    AutoincParam* autoinc_param = NULL;
    uint64_t autoinc_col_id = col_id->get_uint();
    for (int64_t i = 0; OB_SUCC(ret) && i < autoinc_params.count(); ++i) {
      if (autoinc_col_id == autoinc_params.at(i).autoinc_col_id_) {
        autoinc_param = &(autoinc_params.at(i));
        break;
      }
    }
    // this column with column_index is auto-increment column
    if (OB_ISNULL(autoinc_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should find auto-increment param", K(ret));
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
      if (OB_FAIL(get_input_value(*expr.args_[1], ctx, input_value, *autoinc_param, is_to_generate, new_val))) {
        LOG_WARN("check generation failed", K(ret));
      } else if (is_to_generate && OB_FAIL(generate_autoinc_value(new_val, auto_service, autoinc_param, plan_ctx))) {
        LOG_WARN("generate autoinc value failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (!is_to_generate) {
        expr_datum.set_datum(*input_value);  // keep the input datum
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
            LOG_WARN("only int/float/double types support auto increment", K(ret), K(expr.datum_meta_));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (autoinc_param->autoinc_desired_count_ > 0) {
        --autoinc_param->autoinc_desired_count_;
      }

      if (OB_HIDDEN_PK_INCREMENT_COLUMN_ID != autoinc_param->autoinc_col_id_) {
        if (is_to_generate) {
          plan_ctx->set_autoinc_col_value(new_val);
        }
      }
    }
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
