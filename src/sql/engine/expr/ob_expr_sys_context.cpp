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

#include "sql/engine/expr/ob_expr_sys_context.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "lib/ob_errno.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace oceanbase {
namespace sql {

ObExprSysContext::ObExprSysContext(common::ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_SYS_CONTEXT, N_SYS_CONTEXT, 2 /*TWO_OR_THREE*/, NOT_ROW_DIMENSION)
{}

ObExprSysContext::~ObExprSysContext()
{}

int ObExprSysContext::calc_result_type2(
    ObExprResType& type, ObExprResType& arg1, ObExprResType& arg2, common::ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo* session = type_ctx.get_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    arg1.set_calc_type(ObVarcharType);
    arg1.set_calc_collation_type(session->get_nls_collation());
    arg2.set_calc_type(ObVarcharType);
    arg2.set_calc_collation_type(session->get_nls_collation());
    type.set_varchar();
    type.set_collation_type(session->get_nls_collation());
    type.set_collation_level(CS_LEVEL_SYSCONST);
    const ObLengthSemantics def_ls = session->get_actual_nls_length_semantics();
    type.set_length_semantics(def_ls);
    // The data type of the return value is VARCHAR2.
    // The default maximum size of the return value is 256 bytes.
    // We can override this default by specifying the optional length parameter
    // FIXME:But Now we don't support the third parameter which is the user specified length
    type.set_length(DEFAULT_LENGTH);
  }
  return ret;
}

int ObExprSysContext::calc_result2(
    common::ObObj& result, const common::ObObj& arg1, const common::ObObj& arg2, common::ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  calc_fun fun = nullptr;
  if (arg1.is_null() || arg2.is_null()) {
    result.set_null();
  } else {
    CK(OB_NOT_NULL(expr_ctx.calc_buf_));
    ObString ns_str;
    ObString para_str;
    ObCollationType ns_coll = arg1.get_collation_type();
    ObCollationType para_coll = arg2.get_collation_type();
    ObCollationType sys_coll = ObCharset::get_system_collation();
    OZ(ObExprUtil::convert_string_collation(arg1.get_string(), ns_coll, ns_str, sys_coll, *expr_ctx.calc_buf_));
    OZ(ObExprUtil::convert_string_collation(arg2.get_string(), para_coll, para_str, sys_coll, *expr_ctx.calc_buf_));
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_calc_fun(ns_str, para_str, fun))) {
        LOG_WARN("fail to get calc function", K(ret));
      } else if (nullptr == fun) {
        // not found return null
        result.set_null();
      } else if (OB_FAIL(fun(result, arg1, arg2, expr_ctx))) {
        LOG_WARN("fail to calc result", K(ret));
      }
      if (OB_SUCC(ret)) {
        if (!result.is_null()) {
          result.set_collation(result_type_);
        }
      }
    }
  }
  return ret;
}

// define valid namespace parameter
// 3 types:
//    userenv
//    sys_session_roles  //not supported
//    user_define   //not supported
const char namespace_paras[][10] = {"USERENV"};

ObExprSysContext::UserEnvParameter ObExprSysContext::userenv_parameters_[] = {
    {"CURRENT_USERID", calc_schemaid, eval_schemaid},
    {"CURRENT_USER", calc_user, eval_user},
    {"SESSION_USERID", calc_schemaid, eval_schemaid},
    {"SESSION_USER", calc_user, eval_user},
    {"CON_ID", get_tenant_id, eval_tenant_id},
    {"CON_NAME", get_tenant_name, eval_tenant_name},
    {"CURRENT_SCHEMA", calc_current_schema, eval_current_schema},
    {"CURRENT_SCHEMAID", calc_current_schemaid, eval_current_schemaid},
    {"SESSIONID", calc_sessionid_result, eval_sessionid},
    {"IP_ADDRESS", calc_ip_address, eval_ip_address},
};

// if namespace is userenv, but the parameter is not found, then report error
int ObExprSysContext::get_userenv_fun(const common::ObString& para_str, calc_fun& fun)
{
  int ret = OB_SUCCESS;

  fun = nullptr;
  // userenv
  bool found = false;
  for (int64_t i = 0; !found && i < ARRAYSIZEOF(userenv_parameters_); ++i) {
    if (0 == para_str.case_compare(userenv_parameters_[i].name)) {
      found = true;
      fun = userenv_parameters_[i].calc;
    }
  }
  if (!found) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(para_str));
  }
  return ret;
}

int ObExprSysContext::get_calc_fun(const common::ObString& ns_str, const common::ObString& para_str, calc_fun& fun)
{
  int ret = OB_SUCCESS;
  // userenv
  if (0 == ns_str.case_compare(namespace_paras[0])) {
    if (OB_FAIL(get_userenv_fun(para_str, fun))) {
      LOG_WARN("fail to get userenv calc function", K(ret), K(ns_str));
    }
  } else {
    // not supported, return null
    fun = nullptr;
  }
  return ret;
}

int ObExprSysContext::calc_schemaid(
    common::ObObj& result, const common::ObObj& arg1, const common::ObObj& arg2, common::ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);

  if (OB_ISNULL(expr_ctx.my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr_ctx.my_session_ is null", K(ret));
  } else {
    uint64_t dbid = OB_INVALID_ID;
    share::schema::ObSchemaGetterGuard* schema_guard = expr_ctx.exec_ctx_->get_virtual_table_ctx().schema_guard_;
    if (OB_ISNULL(schema_guard)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("wrong schema", K(ret));
    } else if (OB_FAIL(schema_guard->get_database_id(expr_ctx.my_session_->get_effective_tenant_id(),
                   expr_ctx.my_session_->get_user_name(),  // user name is same as database name
                   dbid))) {
      LOG_WARN("fail to get database id", K(ret));
    } else {
      EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
      ObObj res;
      res.set_int(dbid);
      ObString val_str;
      EXPR_GET_VARCHAR_V2(res, val_str);
      result.set_varchar(val_str);
    }
  }
  return ret;
}

int ObExprSysContext::calc_current_schemaid(
    common::ObObj& result, const common::ObObj& arg1, const common::ObObj& arg2, common::ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);

  if (OB_ISNULL(expr_ctx.my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr_ctx.my_session_ is null", K(ret));
  } else {
    uint64_t dbid = expr_ctx.my_session_->get_database_id();
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    ObObj res;
    res.set_int(dbid);
    ObString val_str;
    EXPR_GET_VARCHAR_V2(res, val_str);
    result.set_varchar(val_str);
  }
  return ret;
}

int ObExprSysContext::calc_current_schema(
    common::ObObj& result, const common::ObObj& arg1, const common::ObObj& arg2, common::ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);

  if (OB_ISNULL(expr_ctx.my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr_ctx.my_session_ is null", K(ret));
  } else {
    const ObString database_name = expr_ctx.my_session_->get_database_name();
    ObObj tmp;
    tmp.set_varchar(database_name);
    if (OB_FAIL(ob_write_obj(*expr_ctx.calc_buf_, tmp, result))) {
      LOG_WARN("fail to write obj", K(ret));
    }
  }
  return ret;
}

int ObExprSysContext::calc_user(
    common::ObObj& result, const common::ObObj& arg1, const common::ObObj& arg2, common::ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);

  if (OB_ISNULL(expr_ctx.my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr_ctx.my_session_ is null", K(ret));
  } else {
    const ObString user_name = expr_ctx.my_session_->get_user_name();
    ObObj tmp;
    tmp.set_varchar(user_name);
    if (OB_FAIL(ob_write_obj(*expr_ctx.calc_buf_, tmp, result))) {
      LOG_WARN("fail to write obj", K(ret));
    }
  }
  return ret;
}

int ObExprSysContext::get_tenant_name(
    common::ObObj& result, const common::ObObj& arg1, const common::ObObj& arg2, common::ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);

  if (OB_ISNULL(expr_ctx.my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr_ctx.my_session_ is null", K(ret));
  } else {
    const ObTenantSchema* tenant_schema = NULL;
    share::schema::ObSchemaGetterGuard* schema_guard = expr_ctx.exec_ctx_->get_virtual_table_ctx().schema_guard_;
    if (OB_ISNULL(schema_guard)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("wrong schema", K(ret));
    } else if (OB_FAIL(schema_guard->get_tenant_info(expr_ctx.my_session_->get_effective_tenant_id(), tenant_schema))) {
      LOG_WARN("failed to get tenant info", K(ret));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant schema is null", K(ret));
    } else {
      const ObString& user_name = tenant_schema->get_tenant_name_str();
      ObObj tmp;
      tmp.set_varchar(user_name);
      if (OB_FAIL(ob_write_obj(*expr_ctx.calc_buf_, tmp, result))) {
        LOG_WARN("fail to write obj", K(ret));
      }
    }
  }
  return ret;
}

int ObExprSysContext::get_tenant_id(
    common::ObObj& result, const common::ObObj& arg1, const common::ObObj& arg2, common::ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);

  if (OB_ISNULL(expr_ctx.my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr_ctx.my_session_ is null", K(ret));
  } else {
    uint64_t tenant_id = expr_ctx.my_session_->get_effective_tenant_id();
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    ObObj res;
    res.set_int(tenant_id);
    ObString val_str;
    EXPR_GET_VARCHAR_V2(res, val_str);
    result.set_varchar(val_str);
  }
  return ret;
}

int ObExprSysContext::calc_sessionid_result(
    common::ObObj& result, const common::ObObj& arg1, const common::ObObj& arg2, common::ObExprCtx& expr_ctx)
{
  UNUSED(arg1);
  UNUSED(arg2);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr_ctx.my_session_ is null", K(ret));
  } else {
    int64_t sid_ = expr_ctx.my_session_->get_sessid();
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    ObObj sid;
    sid.set_int(sid_);
    ObString sid_str;
    EXPR_GET_VARCHAR_V2(sid, sid_str);
    result.set_varchar(sid_str);
  }
  return ret;
}

int ObExprSysContext::calc_ip_address(
    common::ObObj& result, const common::ObObj& arg1, const common::ObObj& arg2, common::ObExprCtx& expr_ctx)
{
  UNUSED(arg1);
  UNUSED(arg2);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr_ctx.my_session_ is null", K(ret));
  } else {
    const ObString& user_at_client_ip = expr_ctx.my_session_->get_user_at_client_ip();
    int64_t start = 0;
    if (OB_FAIL(extract_ip(user_at_client_ip, start))) {
      LOG_WARN("get real ip failed", K(ret));
    } else {
      ObString ip_address(user_at_client_ip.length() - start,
          user_at_client_ip.length() - start,
          const_cast<char*>(user_at_client_ip.ptr() + start));
      ObObj tmp;
      tmp.set_varchar(ip_address);
      if (OB_FAIL(ob_write_obj(*expr_ctx.calc_buf_, tmp, result))) {
        LOG_WARN("fail to write obj", K(ret));
      }
    }
  }
  return ret;
}

int ObExprSysContext::get_userenv_fun(const ObString& pram_str, eval_fun& fun)
{
  int ret = OB_SUCCESS;

  fun = nullptr;
  // userenv
  bool found = false;
  for (int64_t i = 0; !found && i < ARRAYSIZEOF(userenv_parameters_); ++i) {
    if (0 == pram_str.case_compare(userenv_parameters_[i].name)) {
      found = true;
      fun = userenv_parameters_[i].eval;
    }
  }
  if (!found) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pram_str));
  }
  return ret;
}

int ObExprSysContext::get_eval_fun(const ObString& ns_str, const ObString& para_str, eval_fun& fun)
{
  int ret = OB_SUCCESS;
  // userenv
  if (0 == ns_str.case_compare(namespace_paras[0])) {
    if (OB_FAIL(get_userenv_fun(para_str, fun))) {
      LOG_WARN("fail to get userenv calc function", K(ret), K(ns_str));
    }
  } else {
    // not supported, return null
    fun = nullptr;
  }
  return ret;
}

int ObExprSysContext::eval_schemaid(
    const ObExpr& expr, ObDatum& res, const ObDatum& arg1, const ObDatum& arg2, ObEvalCtx& ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);

  const ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    uint64_t dbid = OB_INVALID_ID;
    share::schema::ObSchemaGetterGuard* schema_guard = ctx.exec_ctx_.get_virtual_table_ctx().schema_guard_;
    OV(OB_NOT_NULL(schema_guard), OB_SCHEMA_ERROR);
    OZ(schema_guard->get_database_id(session->get_effective_tenant_id(), session->get_user_name(), dbid));
    OZ(uint_string(expr, ctx, dbid, res));
  }
  return ret;
}

int ObExprSysContext::eval_current_schemaid(
    const ObExpr& expr, ObDatum& res, const ObDatum& arg1, const ObDatum& arg2, ObEvalCtx& ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);

  const ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    uint64_t dbid = session->get_database_id();
    OZ(uint_string(expr, ctx, dbid, res));
  }
  return ret;
}

int ObExprSysContext::eval_current_schema(
    const ObExpr& expr, ObDatum& res, const ObDatum& arg1, const ObDatum& arg2, ObEvalCtx& ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);

  const ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    const ObString& db_name = session->get_database_name();
    ObString out_db_name;
    ObExprStrResAlloc res_alloc(expr, ctx);
    ObIAllocator& calc_alloc = ctx.get_reset_tmp_alloc();
    OZ(ObExprUtil::convert_string_collation(
        db_name, ObCharset::get_system_collation(), out_db_name, expr.datum_meta_.cs_type_, calc_alloc));
    OZ(deep_copy_ob_string(res_alloc, out_db_name, out_db_name));
    OX(res.set_string(out_db_name));
  }
  return ret;
}

int ObExprSysContext::eval_user(
    const ObExpr& expr, ObDatum& res, const ObDatum& arg1, const ObDatum& arg2, ObEvalCtx& ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);

  const ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    const ObString& user_name = session->get_user_name();
    ObString out_user_name;
    ObExprStrResAlloc res_alloc(expr, ctx);
    ObIAllocator& calc_alloc = ctx.get_reset_tmp_alloc();
    OZ(ObExprUtil::convert_string_collation(
        user_name, ObCharset::get_system_collation(), out_user_name, expr.datum_meta_.cs_type_, calc_alloc));
    OZ(deep_copy_ob_string(res_alloc, out_user_name, out_user_name));
    OX(res.set_string(out_user_name));
  }
  return ret;
}

int ObExprSysContext::eval_tenant_name(
    const ObExpr& expr, ObDatum& res, const ObDatum& arg1, const ObDatum& arg2, ObEvalCtx& ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);

  const ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    const ObTenantSchema* tenant_schema = NULL;
    share::schema::ObSchemaGetterGuard* schema_guard = ctx.exec_ctx_.get_virtual_table_ctx().schema_guard_;
    CK(OB_NOT_NULL(schema_guard));
    OZ(schema_guard->get_tenant_info(session->get_effective_tenant_id(), tenant_schema));
    CK(OB_NOT_NULL(tenant_schema));
    if (OB_SUCC(ret)) {
      const ObString& user_name = tenant_schema->get_tenant_name_str();
      ObString out_user_name;
      ObExprStrResAlloc res_alloc(expr, ctx);
      ObIAllocator& calc_alloc = ctx.get_reset_tmp_alloc();
      OZ(ObExprUtil::convert_string_collation(
          user_name, ObCharset::get_system_collation(), out_user_name, expr.datum_meta_.cs_type_, calc_alloc));
      OZ(deep_copy_ob_string(res_alloc, out_user_name, out_user_name));
      OX(res.set_string(out_user_name));
    }
  }
  return ret;
}

int ObExprSysContext::eval_tenant_id(
    const ObExpr& expr, ObDatum& res, const ObDatum& arg1, const ObDatum& arg2, ObEvalCtx& ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);

  const ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    uint64_t tenant_id = session->get_effective_tenant_id();
    OZ(uint_string(expr, ctx, tenant_id, res));
  }
  return ret;
}

int ObExprSysContext::eval_sessionid(
    const ObExpr& expr, ObDatum& res, const ObDatum& arg1, const ObDatum& arg2, ObEvalCtx& ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);

  const ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    const uint64_t sid = session->get_sessid();
    OZ(uint_string(expr, ctx, sid, res));
  }
  return ret;
}

int ObExprSysContext::eval_ip_address(
    const ObExpr& expr, common::ObDatum& res, const common::ObDatum& arg1, const common::ObDatum& arg2, ObEvalCtx& ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);
  const ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    const ObString& user_at_client_ip = session->get_user_at_client_ip();
    int64_t start = 0;
    if (OB_FAIL(extract_ip(user_at_client_ip, start))) {
      LOG_WARN("get real ip failed", K(ret));
    } else {
      ObString ip_address(user_at_client_ip.length() - start,
          user_at_client_ip.length() - start,
          const_cast<char*>(user_at_client_ip.ptr() + start));
      ObString out_ip_address;
      ObExprStrResAlloc res_alloc(expr, ctx);
      ObIAllocator& calc_alloc = ctx.get_reset_tmp_alloc();
      OZ(ObExprUtil::convert_string_collation(
          ip_address, ObCharset::get_system_collation(), out_ip_address, expr.datum_meta_.cs_type_, calc_alloc));
      OZ(deep_copy_ob_string(res_alloc, out_ip_address, out_ip_address));
      OX(res.set_string(out_ip_address));
    }
  }
  return ret;
}

int ObExprSysContext::eval_sys_context(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res)
{
  int ret = OB_SUCCESS;
  ObDatum* ns = NULL;
  ObDatum* para = NULL;
  eval_fun fun = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, ns, para))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (ns->is_null() || para->is_null()) {
    res.set_null();
  } else {
    ObIAllocator& calc_alloc = ctx.get_reset_tmp_alloc();
    ObString ns_str;
    ObString para_str;
    ObCollationType ns_coll = expr.args_[0]->datum_meta_.cs_type_;
    ObCollationType para_coll = expr.args_[1]->datum_meta_.cs_type_;
    ObCollationType sys_coll = ObCharset::get_system_collation();
    OZ(ObExprUtil::convert_string_collation(ns->get_string(), ns_coll, ns_str, sys_coll, calc_alloc));
    OZ(ObExprUtil::convert_string_collation(para->get_string(), para_coll, para_str, sys_coll, calc_alloc));

    OZ(get_eval_fun(ns_str, para_str, fun));
    if (OB_SUCC(ret)) {
      if (NULL == fun) {
        res.set_null();
      } else if (OB_FAIL(fun(expr, res, *ns, *para, ctx))) {
        LOG_WARN("fail to calc result", K(ret));
      }
    }
  }
  return ret;
}

int ObExprSysContext::cg_expr(ObExprCGCtx& ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(raw_expr);
  CK(2 == rt_expr.arg_cnt_);
  OX(rt_expr.eval_func_ = eval_sys_context);
  return ret;
}

int ObExprSysContext::uint_string(const ObExpr& expr, ObEvalCtx& ctx, uint64_t id, ObDatum& res)
{
  int ret = OB_SUCCESS;
  ObFastFormatInt ffi(id);
  ObExprStrResAlloc res_alloc(expr, ctx);
  ObIAllocator& calc_alloc = ctx.get_reset_tmp_alloc();
  ObString id_str_utf8(ffi.length(), ffi.ptr());
  ObString id_str;
  OZ(ObExprUtil::convert_string_collation(
      id_str_utf8, ObCharset::get_system_collation(), id_str, expr.datum_meta_.cs_type_, calc_alloc));
  OZ(deep_copy_ob_string(res_alloc, id_str, id_str));
  OX(res.set_string(id_str));
  return ret;
}

int ObExprSysContext::extract_ip(const ObString& user_at_client_ip, int64_t& start)
{
  int ret = OB_SUCCESS;
  start = 0;
  if (user_at_client_ip.length() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wrong length of ip address param", K(ret));
  } else {
    bool end_loop = false;
    for (; !end_loop && start < user_at_client_ip.length(); ++start) {
      if ('@' == user_at_client_ip[start]) {
        end_loop = true;
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
