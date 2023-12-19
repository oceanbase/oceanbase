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
#include "sql/engine/expr/ob_expr_sys_context.h"

using namespace oceanbase::share;
using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprUserEnv::ObExprUserEnv(common::ObIAllocator &alloc)
: ObFuncExprOperator(alloc,
                    T_FUN_SYS_USERENV,
                    N_USERENV,
                    1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprUserEnv::~ObExprUserEnv() {}

ObExprUserEnv::UserEnvParameter ObExprUserEnv::parameters_[] =
{
  {"SCHEMAID", false, eval_schemaid_result1 },
  {"SESSIONID", false, eval_sessionid_result1 },
  {"LANG", true, eval_lang },
  {"LANGUAGE", true, eval_language },
  {"INSTANCE", false, eval_instance },
  {"SID", false, eval_sessionid_result1},
  {"CLIENT_INFO", true, eval_client_info},
};

ObExprUserEnv::NLS_Lang ObExprUserEnv::lang_map_[] =
{
  {"AMERICAN", "US"},
};

int ObExprUserEnv::calc_result_type1(ObExprResType& type,
                                ObExprResType& arg_type1,
                                common::ObExprTypeCtx& type_ctx,
                                common::ObIArray<common::ObObj*> &arg_arrs) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObStringTC != arg_type1.get_type_class()) ||
      OB_ISNULL(type_ctx.get_session())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument or session is NULL", K(ret), K(arg_type1.get_type_class()));
  } else {
    if (1 != arg_arrs.count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument.", K(ret));
    } else {
      const common::ObObj *value = arg_arrs.at(0);
      UserEnvParameter para;
      // userenv要求参数一定是常量
      // CG时如果发现参数类型不是string，会报错
      arg_type1.set_calc_type(arg_type1.get_type());
      arg_type1.set_calc_collation_type(arg_type1.get_collation_type());
      if (OB_FAIL(check_arg_valid(*value, para, nullptr))) {
        LOG_WARN("fail to check argument", K(ret));
      } else if (para.is_str) {
        type.set_varchar();
        type.set_collation_type(type_ctx.get_session()->get_nls_collation());
        type.set_collation_level(CS_LEVEL_SYSCONST);
        const ObLengthSemantics def_ls = type_ctx.get_session()->get_actual_nls_length_semantics();
        type.set_length_semantics(def_ls);
        type.set_length(DEFAULT_LENGTH);
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

int ObExprUserEnv::check_arg_valid(const common::ObObj &value, UserEnvParameter &para,
                                  ObIAllocator *alloc) const
{
  int ret = OB_SUCCESS;
  bool found = false;
  if (ObStringTC == value.get_type_class()) {
    ObString data = value.get_varchar();
    ObString convert_data;
    if (nullptr == alloc) {
      common::ObArenaAllocator tmp_alloc;
      OZ(ObExprUtil::convert_string_collation(data, value.get_collation_type(), convert_data,
                                              ObCharset::get_system_collation(), tmp_alloc));
      for (int64_t i = 0; OB_SUCC(ret) && !found && i < ARRAYSIZEOF(parameters_); ++i) {
        if (0 == convert_data.case_compare(parameters_[i].name)) {
          found = true;
          para = parameters_[i];
        }
      }
    } else {
      OZ(ObExprUtil::convert_string_collation(data, value.get_collation_type(), convert_data,
                                              ObCharset::get_system_collation(), *alloc));
      for (int64_t i = 0; OB_SUCC(ret) && !found && i < ARRAYSIZEOF(parameters_); ++i) {
        if (0 == convert_data.case_compare(parameters_[i].name)) {
          found = true;
          para = parameters_[i];
        }
      }
    }
  }
  if (OB_SUCC(ret) && !found) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(value), K(ret));
  }
  return ret;
}

int ObExprUserEnv::cg_expr(ObExprCGCtx &ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(raw_expr);
  // for now expr. res type must be ObNumberType and arg type must be string.
  // change calc_user_env_expr function if it is not true.
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_) || OB_ISNULL(rt_expr.args_) ||
      OB_ISNULL(rt_expr.args_[0]) || !ob_is_string_type(rt_expr.args_[0]->datum_meta_.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid expr or arg res type", K(ret), K(rt_expr));
  } else {
    rt_expr.eval_func_ = calc_user_env_expr;
  }
  return ret;
}

int ObExprUserEnv::calc_user_env_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (arg->is_null()) {
    res.set_null();
  } else {
    const ObString &arg_str = arg->get_string();
    ObString arg_str_utf8;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    const ObCollationType &arg_cs_type = expr.args_[0]->datum_meta_.cs_type_;
    // userenv表达式要求参数必须是const，所以不能加隐式cast,只能在这里手动转
    if (OB_FAIL(ObExprUtil::convert_string_collation(arg_str, arg_cs_type, arg_str_utf8,
                                                     ObCharset::get_system_collation(),
                                                     calc_alloc))) {
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
      if (found) {
        OZ(para.eval(expr, ctx, res));
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(arg_str_utf8), K(ret));
      }
    }
  }
  return ret;
}

int ObExprUserEnv::eval_schemaid_result1(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  CK(OB_NOT_NULL(ctx.exec_ctx_.get_my_session()));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
      LOG_WARN("eval arg failed", K(ret));
    } else if (arg->is_null()) {
      res.set_null();
    } else {
      // 获取schemaid
      // 对于oracle来说，由于schemaid和userid是一致的，不存在user在其他schema下
      // 所以在这里，ob返回databaseid，即user名字对应的相同名字的database的id
      uint64_t dbid = OB_INVALID_ID;
      share::schema::ObSchemaGetterGuard schema_guard;
      const ObUserInfo *user_info = nullptr;
      if (OB_FAIL(ObExprSysContext::get_schema_guard(schema_guard,
                  ctx.exec_ctx_.get_my_session()->get_effective_tenant_id()))) {
        LOG_WARN("failed to get schema guard", K(ret));
      } else if (FALSE_IT(user_info = schema_guard.get_user_info(
                  ctx.exec_ctx_.get_my_session()->get_effective_tenant_id(),
                  ctx.exec_ctx_.get_my_session()->get_priv_user_id()))) {
        // do nothing
      } else if (OB_FAIL(schema_guard.get_database_id(
        ctx.exec_ctx_.get_my_session()->get_effective_tenant_id(),
        user_info ? user_info->get_user_name_str() : ctx.exec_ctx_.get_my_session()->get_user_name(),
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

int ObExprUserEnv::eval_sessionid_result1(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  CK(OB_NOT_NULL(ctx.exec_ctx_.get_my_session()));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
      LOG_WARN("eval arg failed", K(ret));
    } else if (arg->is_null()) {
      res.set_null();
    } else {
      const uint64_t sid = ctx.exec_ctx_.get_my_session()->get_compatibility_sessid();
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

int ObExprUserEnv::eval_instance(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  uint64_t instance_id = GCTX.server_id_;
  number::ObNumber res_nmb;
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  ObIAllocator &calc_alloc = alloc_guard.get_allocator();
  if (OB_FAIL(res_nmb.from(instance_id, calc_alloc))) {
    LOG_WARN("failed to get number", K(ret));
  } else {
    res.set_number(res_nmb);
  }
  return ret;
}

int ObExprUserEnv::eval_language(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  ObObj language;
  ObObj territory;
  ObObj characterset;
  //return language_territory.characterset
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(session->get_sys_variable(SYS_VAR_NLS_LANGUAGE, language))) {
      LOG_WARN("failed to get sys var", K(ret));
    } else if (OB_FAIL(session->get_sys_variable(SYS_VAR_NLS_TERRITORY, territory))) {
      LOG_WARN("failed to get sys var", K(ret));
    } else if (OB_FAIL(session->get_sys_variable(SYS_VAR_NLS_CHARACTERSET, characterset))) {
      LOG_WARN("failed to get sys var", K(ret));
    } else {
      const int64_t MAX_LANGUAGE_LEN = 256;
      char language_str[MAX_LANGUAGE_LEN];
      int64_t pos = 0;
      if (OB_UNLIKELY(MAX_LANGUAGE_LEN < language.get_string().length()
                                         + territory.get_string().length()
                                         + characterset.get_string().length() + 2)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("language str is not enough", K(ret));
      } else {
        MEMCPY(language_str, language.get_string().ptr(), language.get_string().length());
        pos += language.get_string().length();
        language_str[pos++] = '_';
        MEMCPY(language_str + pos, territory.get_string().ptr(), territory.get_string().length());
        pos += territory.get_string().length();
        language_str[pos++] = '.';
        MEMCPY(language_str + pos, characterset.get_string().ptr(),
                                   characterset.get_string().length());
        pos += characterset.get_string().length();
        ObString out_language(pos, language_str);
        ObExprStrResAlloc res_alloc(expr, ctx);
        ObEvalCtx::TempAllocGuard alloc_guard(ctx);
        ObIAllocator &calc_alloc = alloc_guard.get_allocator();
        OZ(ObExprUtil::convert_string_collation(out_language, ObCharset::get_system_collation(),
                                                out_language, expr.datum_meta_.cs_type_,
                                                calc_alloc));
        OZ(deep_copy_ob_string(res_alloc, out_language, out_language));
        OX(res.set_string(out_language));
      }
    }
  }
  return ret;
}

int ObExprUserEnv::eval_lang(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  ObObj language;
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(session->get_sys_variable(SYS_VAR_NLS_LANGUAGE, language))) {
      LOG_WARN("failed to get sys var", K(ret));
    } else {
      bool found = false;
      ObString abbreviated;
      for (int64_t i = 0; !found && i < ARRAYSIZEOF(lang_map_); ++i) {
        if (0 == language.get_string().case_compare(lang_map_[i].language)) {
          found = true;
          abbreviated.assign(lang_map_[i].abbreviated,
                             static_cast<int32_t>(strlen(lang_map_[i].abbreviated)));
        }
      }
      if (!found) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(language));
      } else {
        ObExprStrResAlloc res_alloc(expr, ctx);
        ObEvalCtx::TempAllocGuard alloc_guard(ctx);
        ObIAllocator &calc_alloc = alloc_guard.get_allocator();
        OZ(ObExprUtil::convert_string_collation(abbreviated, ObCharset::get_system_collation(),
                                                abbreviated, expr.datum_meta_.cs_type_,
                                                calc_alloc));
        OZ(deep_copy_ob_string(res_alloc, abbreviated, abbreviated));
        OX(res.set_string(abbreviated));
      }
    }
  }
  return ret;
}

int ObExprUserEnv::eval_client_info(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    ObString client_info;
    ObExprStrResAlloc res_alloc(expr, ctx);
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    OZ(ObExprUtil::convert_string_collation(session->get_client_info(),
                                            ObCharset::get_system_collation(),
                                            client_info, expr.datum_meta_.cs_type_,
                                            calc_alloc));
    OZ(deep_copy_ob_string(res_alloc, client_info, client_info));
    OX(res.set_string(client_info));
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
