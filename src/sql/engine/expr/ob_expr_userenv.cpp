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

#include "sql/engine/expr/ob_expr_userenv.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "lib/ob_errno.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::share;
using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace oceanbase {
namespace sql {

ObExprUserEnv::ObExprUserEnv(common::ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_USERENV, N_USERENV, 1, NOT_ROW_DIMENSION)
{}

ObExprUserEnv::~ObExprUserEnv()
{}

ObExprUserEnv::UserEnvParameter ObExprUserEnv::parameters_[] = {
    {"SCHEMAID", false, calc_schemaid_result1, eval_schemaid_result1},
    {"SESSIONID", false, calc_sessionid_result1, eval_sessionid_result1}};

int ObExprUserEnv::calc_result_type1(ObExprResType& type, ObExprResType& arg_type1, common::ObExprTypeCtx& type_ctx,
    common::ObIArray<common::ObObj*>& arg_arrs) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(ObStringTC != arg_type1.get_type_class()) || OB_ISNULL(type_ctx.get_session())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument or session is NULL", K(ret), K(arg_type1.get_type_class()));
  } else {
    if (1 != arg_arrs.count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument.", K(ret));
    } else {
      const common::ObObj* value = arg_arrs.at(0);
      UserEnvParameter para;
      if (type_ctx.get_session()->use_static_typing_engine()) {
        // in parameter for userenv must be constant
        // if it is not string, throw error
        arg_type1.set_calc_type(arg_type1.get_type());
      } else {
        arg_type1.set_calc_type(common::ObVarcharType);
      }
      arg_type1.set_calc_collation_type(arg_type1.get_collation_type());
      if (OB_FAIL(check_arg_valid(*value, para))) {
        LOG_WARN("fail to check argument", K(ret));
      } else if (para.is_str) {
        // return string type
        type.set_varchar();
        // FIXME:default length is 64, for different parameters, the return length is also different
        type.set_length(DEFAULT_LENGTH);
        type.set_collation_type(type_ctx.get_session()->get_nls_collation());
      } else {
        // return number type
        type.set_type(ObNumberType);
        type.set_precision(PRECISION_UNKNOWN_YET);
        type.set_scale(NUMBER_SCALE_UNKNOWN_YET);
      }
    }
  }
  return ret;
}

int ObExprUserEnv::check_arg_valid(const common::ObObj& value, UserEnvParameter& para) const
{
  int ret = OB_SUCCESS;
  bool found = false;
  if (ObStringTC == value.get_type_class()) {
    ObString data = value.get_varchar();

    for (int64_t i = 0; !found && i < ARRAYSIZEOF(parameters_); ++i) {
      if (0 == data.case_compare(parameters_[i].name)) {
        found = true;
        para = parameters_[i];
      }
    }
  }
  if (!found) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(value), K(ret));
  }
  return ret;
}

int ObExprUserEnv::calc_result1(common::ObObj& result, const common::ObObj& arg1, common::ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  UserEnvParameter para;
  if (OB_FAIL(check_arg_valid(arg1, para))) {
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(para.calc(result, arg1, expr_ctx))) {
    LOG_WARN("fail to calculate result", K(ret));
  }
  return ret;
}

int ObExprUserEnv::calc_schemaid_result1(common::ObObj& result, const common::ObObj& arg1, common::ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);

  if (OB_ISNULL(expr_ctx.my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr_ctx.my_session_ is null", K(ret));
  } else {
    // get schemaid
    uint64_t dbid = OB_INVALID_ID;
    share::schema::ObSchemaGetterGuard* schema_guard = expr_ctx.exec_ctx_->get_virtual_table_ctx().schema_guard_;
    if (OB_ISNULL(schema_guard)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("wrong schema", K(ret));
    } else if (OB_FAIL(schema_guard->get_database_id(
                   expr_ctx.my_session_->get_effective_tenant_id(), expr_ctx.my_session_->get_user_name(), dbid))) {
      LOG_WARN("fail to get database id", K(ret));
    } else {
      number::ObNumber res_nmb;
      if (OB_FAIL(res_nmb.from(dbid, *expr_ctx.calc_buf_))) {
        LOG_WARN("fail to assign number", K(ret));
      } else {
        result.set_number(res_nmb);
      }
    }
  }
  return ret;
}

int ObExprUserEnv::calc_sessionid_result1(common::ObObj& result, const common::ObObj& arg1, common::ObExprCtx& expr_ctx)
{
  UNUSED(arg1);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr_ctx.my_session_ is null", K(ret));
  } else {
    number::ObNumber sid;
    int64_t sid_ = expr_ctx.my_session_->get_sessid();
    LOG_DEBUG("^^^^session id", K(*(expr_ctx.my_session_)), K(sid_));
    if (OB_FAIL(sid.from(sid_, *expr_ctx.calc_buf_))) {
      LOG_WARN("fail to assign number", K(ret), K(sid));
    } else {
      result.set_number(sid);
    }
  }
  return ret;
}

int ObExprUserEnv::cg_expr(ObExprCGCtx& ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(raw_expr);
  // for now expr. res type must be ObNumberType and arg type must be string.
  // change calc_user_env_expr function if it is not true.
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_) || OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]) ||
      ObNumberType != rt_expr.datum_meta_.type_ || !ob_is_string_type(rt_expr.args_[0]->datum_meta_.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid expr or arg res type", K(ret), K(rt_expr));
  } else {
    rt_expr.eval_func_ = calc_user_env_expr;
  }
  return ret;
}

int ObExprUserEnv::calc_user_env_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res)
{
  int ret = OB_SUCCESS;
  ObDatum* arg = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (arg->is_null()) {
    res.set_null();
  } else {
    const ObString& arg_str = arg->get_string();
    ObString arg_str_utf8;
    ObIAllocator& calc_alloc = ctx.get_reset_tmp_alloc();
    const ObCollationType& arg_cs_type = expr.args_[0]->datum_meta_.cs_type_;
    if (OB_FAIL(ObExprUtil::convert_string_collation(
            arg_str, arg_cs_type, arg_str_utf8, ObCharset::get_system_collation(), calc_alloc))) {
      LOG_WARN("convert string collation failed", K(ret), K(arg_str), K(arg_cs_type));
    } else {
      UserEnvParameter para;
      bool found = false;
      for (int64_t i = 0; !found && i < ARRAYSIZEOF(parameters_); ++i) {
        if (0 == arg_str_utf8.case_compare(parameters_[i].name)) {
          found = true;
          para = parameters_[i];
        }
      }
      OZ(para.eval(expr, ctx, res));
    }
  }
  return ret;
}

int ObExprUserEnv::eval_schemaid_result1(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res)
{
  int ret = OB_SUCCESS;
  ObDatum* arg = NULL;
  CK(OB_NOT_NULL(ctx.exec_ctx_.get_my_session()));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
      LOG_WARN("eval arg failed", K(ret));
    } else if (arg->is_null()) {
      res.set_null();
    } else {
      uint64_t dbid = OB_INVALID_ID;
      share::schema::ObSchemaGetterGuard* schema_guard = ctx.exec_ctx_.get_virtual_table_ctx().schema_guard_;
      if (OB_ISNULL(schema_guard)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("wrong schema", K(ret));
      } else if (OB_FAIL(schema_guard->get_database_id(ctx.exec_ctx_.get_my_session()->get_effective_tenant_id(),
                     ctx.exec_ctx_.get_my_session()->get_user_name(),
                     dbid))) {
        LOG_WARN("fail to get database id", K(ret));
      } else {
        number::ObNumber res_nmb;
        ObNumStackOnceAlloc res_alloc;
        if (OB_FAIL(res_nmb.from(dbid, res_alloc))) {
          LOG_WARN("fail to assign number", K(ret));
        } else {
          res.set_number(res_nmb);
        }
      }
    }
  }
  return ret;
}

int ObExprUserEnv::eval_sessionid_result1(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res)
{
  int ret = OB_SUCCESS;
  ObDatum* arg = NULL;
  CK(OB_NOT_NULL(ctx.exec_ctx_.get_my_session()));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
      LOG_WARN("eval arg failed", K(ret));
    } else if (arg->is_null()) {
      res.set_null();
    } else {
      const uint64_t sid = ctx.exec_ctx_.get_my_session()->get_sessid();
      ObNumStackOnceAlloc tmp_alloc;
      number::ObNumber res_nmb;
      if (OB_FAIL(res_nmb.from(sid, tmp_alloc))) {
        LOG_WARN("failed to convert int to number", K(ret));
      } else {
        res.set_number(res_nmb);
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
