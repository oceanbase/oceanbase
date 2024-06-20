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
#include "lib/utility/ob_fast_convert.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/engine/ob_exec_context.h"
#include "share/ob_global_context_operator.h"

using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprSysContext::ObExprSysContext(common::ObIAllocator &alloc)
: ObFuncExprOperator(alloc,
                    T_FUN_SYS_SYS_CONTEXT,
                    N_SYS_CONTEXT,
                    2 /*TWO_OR_THREE*/, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprSysContext::~ObExprSysContext() {}

int ObExprSysContext::calc_result_type2(ObExprResType& type,
                              ObExprResType& arg1,
                              ObExprResType& arg2,
                              common::ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = type_ctx.get_session();
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

// define valid namespace parameter
// 3 types:
//    userenv
//    sys_session_roles  //not supported
//    user_define   //not supported
const char namespace_paras[][10] =
{
  "USERENV"
};

ObExprSysContext::UserEnvParameter ObExprSysContext::userenv_parameters_[] =
{
  {"CURRENT_USERID", eval_curr_user_id},
  {"CURRENT_USER", eval_curr_user},
  {"SESSION_USERID", eval_schemaid},
  {"SESSION_USER", eval_user},
  {"CON_ID", eval_tenant_id},
  {"CON_NAME", eval_tenant_name},
  {"CURRENT_SCHEMA", eval_current_schema},
  {"CURRENT_SCHEMAID", eval_current_schemaid},
  {"SESSIONID", eval_sessionid},
  {"IP_ADDRESS", eval_ip_address},
  {"DB_NAME", eval_tenant_name},
  {"SID", eval_sessionid},
  {"INSTANCE", eval_instance},
  {"INSTANCE_NAME", eval_instance_name},
  {"LANG", eval_lang},
  {"LANGUAGE", eval_language},
  {"ACTION", eval_action},
  {"CLIENT_INFO", eval_client_info},
  {"MODULE", eval_module},
  {"CLIENT_IDENTIFIER", eval_client_identifier},
  {"PROXY_USER", eval_proxy_user},
  {"PROXY_USERID", eval_proxy_user_id},
  {"AUTHENTICATED_IDENTITY", eval_auth_identity},
};
ObExprSysContext::NLS_Lang ObExprSysContext::lang_map_[] =
{
  {"AMERICAN", "US"},
};

int ObExprSysContext::get_userenv_fun(const ObString &pram_str, eval_fun &fun)
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


int ObExprSysContext::get_eval_fun(const ObString& ns_str, const ObString& para_str,
                                   eval_fun &fun)
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

int ObExprSysContext::eval_schemaid(const ObExpr &expr, ObDatum &res,
                                    const ObDatum &arg1, const ObDatum &arg2,
                                    ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);

  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    uint64_t dbid = OB_INVALID_ID;
    share::schema::ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(get_schema_guard(schema_guard, session->get_effective_tenant_id()))) {
      LOG_WARN("failed to get schema guard", K(ret));
    } else {
      OZ(schema_guard.get_database_id(session->get_effective_tenant_id(),
                                      session->get_user_name(), dbid));
      char out_id[256];
      sprintf(out_id, "%lu", dbid);
      ObString out_id_str(strlen(out_id), out_id);
      ObExprStrResAlloc res_alloc(expr, ctx);
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &calc_alloc = alloc_guard.get_allocator();
      OZ(ObExprUtil::convert_string_collation(out_id_str, ObCharset::get_system_collation(),
                                              out_id_str, expr.datum_meta_.cs_type_,
                                              calc_alloc));
      OZ(deep_copy_ob_string(res_alloc, out_id_str, out_id_str));
      OX(res.set_string(out_id_str));
    }
  }
  return ret;
}

int ObExprSysContext::eval_curr_user_id(const ObExpr &expr, ObDatum &res,
                                        const ObDatum &arg1, const ObDatum &arg2,
                                        ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    uint64_t curr_user_id = session->get_priv_user_id();
    if (OB_INVALID_ID == curr_user_id) {
      share::schema::ObSchemaGetterGuard schema_guard;
      OZ (get_schema_guard(schema_guard, session->get_effective_tenant_id()));
      OZ(schema_guard.get_database_id(session->get_effective_tenant_id(),
                                      session->get_user_name(), curr_user_id));
    }
    if (OB_SUCC(ret)) {
      char out_id[256];
      sprintf(out_id, "%lu", curr_user_id);
      ObString out_id_str(strlen(out_id), out_id);
      ObExprStrResAlloc res_alloc(expr, ctx);
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &calc_alloc = alloc_guard.get_allocator();
      OZ(ObExprUtil::convert_string_collation(out_id_str, ObCharset::get_system_collation(),
                                              out_id_str, expr.datum_meta_.cs_type_,
                                              calc_alloc));
      OZ(deep_copy_ob_string(res_alloc, out_id_str, out_id_str));
      OX(res.set_string(out_id_str));
    }
  }
  return ret;
}

int ObExprSysContext::eval_curr_user(const ObExpr &expr, ObDatum &res,
                                     const ObDatum &arg1, const ObDatum &arg2,
                                     ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);

  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    share::schema::ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(get_schema_guard(schema_guard, session->get_effective_tenant_id()))) {
      LOG_WARN("failed to get schema guard", K(ret));
    } else {
      // Only PL will call set_priv_user_id(), which won't change effective_tenant_id_;
      // For the other scenarios, user_id_ is always correspond with effective_tenant_id_ even switch_tenant() is called.
      const ObUserInfo *user_info = schema_guard.get_user_info(session->get_effective_tenant_id(),
                                                                session->get_priv_user_id());
      if (OB_NOT_NULL(user_info)) {
        const ObString &user_name = user_info->get_user_name_str();
        ObString out_user_name;
        ObExprStrResAlloc res_alloc(expr, ctx);
        ObEvalCtx::TempAllocGuard alloc_guard(ctx);
        ObIAllocator &calc_alloc = alloc_guard.get_allocator();
        OZ(ObExprUtil::convert_string_collation(user_name, ObCharset::get_system_collation(),
                                                out_user_name, expr.datum_meta_.cs_type_,
                                                calc_alloc));
        OZ(deep_copy_ob_string(res_alloc, out_user_name, out_user_name));
        OX(res.set_string(out_user_name));
      } else {
        const ObString &user_name = session->get_user_name();
        ObString out_user_name;
        ObExprStrResAlloc res_alloc(expr, ctx);
        ObEvalCtx::TempAllocGuard alloc_guard(ctx);
        ObIAllocator &calc_alloc = alloc_guard.get_allocator();
        OZ(ObExprUtil::convert_string_collation(user_name, ObCharset::get_system_collation(),
                                                out_user_name, expr.datum_meta_.cs_type_,
                                                calc_alloc));
        OZ(deep_copy_ob_string(res_alloc, out_user_name, out_user_name));
        OX(res.set_string(out_user_name));
      }
    }
  }
  return ret;
}

int ObExprSysContext::eval_current_schemaid(const ObExpr &expr, ObDatum &res,
                                            const ObDatum &arg1, const ObDatum &arg2,
                                            ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);

  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    uint64_t dbid = session->get_database_id();
    char out_id[256];
    sprintf(out_id, "%lu", dbid);
    ObString out_id_str(strlen(out_id), out_id);
    ObExprStrResAlloc res_alloc(expr, ctx);
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    OZ(ObExprUtil::convert_string_collation(out_id_str, ObCharset::get_system_collation(),
                                            out_id_str, expr.datum_meta_.cs_type_,
                                            calc_alloc));
    OZ(deep_copy_ob_string(res_alloc, out_id_str, out_id_str));
    OX(res.set_string(out_id_str));
  }
  return ret;
}

int ObExprSysContext::eval_current_schema(const ObExpr &expr, ObDatum &res, const ObDatum &arg1,
                                            const ObDatum &arg2, ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);

  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    const ObString &db_name = session->get_database_name();
    ObString out_db_name;
    ObExprStrResAlloc res_alloc(expr, ctx);
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    OZ(ObExprUtil::convert_string_collation(db_name, ObCharset::get_system_collation(),
                                            out_db_name, expr.datum_meta_.cs_type_,
                                            calc_alloc));
    OZ(deep_copy_ob_string(res_alloc, out_db_name, out_db_name));
    OX(res.set_string(out_db_name));
  }
  return ret;
}

int ObExprSysContext::eval_user(const ObExpr &expr, ObDatum &res, const ObDatum &arg1,
                                const ObDatum &arg2, ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);

  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    const ObString &user_name = session->get_user_name();
    ObString out_user_name;
    ObExprStrResAlloc res_alloc(expr, ctx);
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    OZ(ObExprUtil::convert_string_collation(user_name, ObCharset::get_system_collation(),
                                            out_user_name, expr.datum_meta_.cs_type_,
                                            calc_alloc));
    OZ(deep_copy_ob_string(res_alloc, out_user_name, out_user_name));
    OX(res.set_string(out_user_name));
  }
  return ret;
}

int ObExprSysContext::eval_tenant_name(const ObExpr &expr, ObDatum &res,
                                       const ObDatum &arg1, const ObDatum &arg2,
                                       ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);

  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    const ObTenantSchema *tenant_schema = NULL;
    share::schema::ObSchemaGetterGuard schema_guard;
    OZ (get_schema_guard(schema_guard, session->get_effective_tenant_id()));
    OZ(schema_guard.get_tenant_info(session->get_effective_tenant_id(), tenant_schema));
    CK(OB_NOT_NULL(tenant_schema));
    if (OB_SUCC(ret)) {
      const ObString &user_name = tenant_schema->get_tenant_name_str();
      ObString out_user_name;
      ObExprStrResAlloc res_alloc(expr, ctx);
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &calc_alloc = alloc_guard.get_allocator();
      OZ(ObExprUtil::convert_string_collation(user_name, ObCharset::get_system_collation(),
                                              out_user_name, expr.datum_meta_.cs_type_,
                                              calc_alloc));
      OZ(deep_copy_ob_string(res_alloc, out_user_name, out_user_name));
      OX(res.set_string(out_user_name));
    }
  }
  return ret;
}

int ObExprSysContext::eval_tenant_id(const ObExpr &expr, ObDatum &res, const ObDatum &arg1,
                                     const ObDatum &arg2, ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);

  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    uint64_t tenant_id = session->get_effective_tenant_id();
    char out_id[256];
    sprintf(out_id, "%lu", tenant_id);
    ObString out_id_str(strlen(out_id), out_id);
    ObExprStrResAlloc res_alloc(expr, ctx);
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    OZ(ObExprUtil::convert_string_collation(out_id_str, ObCharset::get_system_collation(),
                                            out_id_str, expr.datum_meta_.cs_type_,
                                            calc_alloc));
    OZ(deep_copy_ob_string(res_alloc, out_id_str, out_id_str));
    OX(res.set_string(out_id_str));
  }
  return ret;
}

int ObExprSysContext::eval_sessionid(const ObExpr &expr, ObDatum &res, const ObDatum &arg1,
                                     const ObDatum &arg2, ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);

  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    const uint64_t sid = session->get_compatibility_sessid();
    char out_id[256];
    sprintf(out_id, "%lu", sid);
    ObString out_id_str(strlen(out_id), out_id);
    ObExprStrResAlloc res_alloc(expr, ctx);
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    OZ(ObExprUtil::convert_string_collation(out_id_str, ObCharset::get_system_collation(),
                                            out_id_str, expr.datum_meta_.cs_type_,
                                            calc_alloc));
    OZ(deep_copy_ob_string(res_alloc, out_id_str, out_id_str));
    OX(res.set_string(out_id_str));
  }
  return ret;
}

int ObExprSysContext::eval_ip_address(const ObExpr &expr, common::ObDatum &res,
                            const common::ObDatum &arg1,
                            const common::ObDatum &arg2, ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    const ObString &user_at_client_ip = session->get_user_at_client_ip();
    int64_t start =0;
    if (OB_FAIL(extract_ip(user_at_client_ip, start))) {
      LOG_WARN("get real ip failed", K(ret));
    } else {
      ObString ip_address(user_at_client_ip.length() - start,
                          user_at_client_ip.length() - start,
                          const_cast<char *>(user_at_client_ip.ptr() + start));
      ObString out_ip_address;
      ObExprStrResAlloc res_alloc(expr, ctx);
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &calc_alloc = alloc_guard.get_allocator();
      OZ(ObExprUtil::convert_string_collation(ip_address, ObCharset::get_system_collation(),
                                              out_ip_address, expr.datum_meta_.cs_type_,
                                              calc_alloc));
      OZ(deep_copy_ob_string(res_alloc, out_ip_address, out_ip_address));
      OX(res.set_string(out_ip_address));
    }
  }
  return ret;
}

int ObExprSysContext::eval_instance(const ObExpr &expr, common::ObDatum &res,
                                    const common::ObDatum &arg1, const common::ObDatum &arg2,
                                    ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);
  uint64_t instance_id = GCTX.server_id_;
  //OZ(uint_string(expr, ctx, instance_id, res));
  char out_id[256];
  sprintf(out_id, "%lu", instance_id);
  ObString out_id_str(strlen(out_id), out_id);
  ObExprStrResAlloc res_alloc(expr, ctx);
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  ObIAllocator &calc_alloc = alloc_guard.get_allocator();
  OZ(ObExprUtil::convert_string_collation(out_id_str, ObCharset::get_system_collation(),
                                          out_id_str, expr.datum_meta_.cs_type_,
                                          calc_alloc));
  OZ(deep_copy_ob_string(res_alloc, out_id_str, out_id_str));
  OX(res.set_string(out_id_str));
  return ret;
}

int ObExprSysContext::eval_instance_name(const ObExpr &expr, common::ObDatum &res,
                                    const common::ObDatum &arg1, const common::ObDatum &arg2,
                                    ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);
  const ObAddr &server_addr = GCTX.self_addr();
  char ip_port_str[MAX_IP_PORT_LENGTH];
  if (OB_FAIL(server_addr.ip_port_to_string(ip_port_str, sizeof(ip_port_str)))) {
    LOG_WARN("fail to generate ip_port_str", K(ret), K(server_addr));
  } else {
    ObString out_instance(strlen(ip_port_str), ip_port_str);
    ObExprStrResAlloc res_alloc(expr, ctx);
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    OZ(ObExprUtil::convert_string_collation(out_instance, ObCharset::get_system_collation(),
                                            out_instance, expr.datum_meta_.cs_type_,
                                            calc_alloc));
    OZ(deep_copy_ob_string(res_alloc, out_instance, out_instance));
    OX(res.set_string(out_instance));
  }
  return ret;
}

int ObExprSysContext::eval_language(const ObExpr &expr, common::ObDatum &res,
                                    const common::ObDatum &arg1, const common::ObDatum &arg2,
                                    ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);
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
int ObExprSysContext::eval_lang(const ObExpr &expr, common::ObDatum &res,
                                const common::ObDatum &arg1, const common::ObDatum &arg2,
                                ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);
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
          abbreviated.assign(lang_map_[i].abbreviated, static_cast<int32_t>(strlen(lang_map_[i].abbreviated)));
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

int ObExprSysContext::eval_action(const ObExpr &expr, common::ObDatum &res,
                                  const common::ObDatum &arg1, const common::ObDatum &arg2,
                                  ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    ObString action;
    ObExprStrResAlloc res_alloc(expr, ctx);
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    OZ(ObExprUtil::convert_string_collation(session->get_action_name(),
                                            ObCharset::get_system_collation(),
                                            action, expr.datum_meta_.cs_type_,
                                            calc_alloc));
    OZ(deep_copy_ob_string(res_alloc, action, action));
    OX(res.set_string(action));
  }
  return ret;
}

int ObExprSysContext::eval_client_info(const ObExpr &expr, common::ObDatum &res,
                                       const common::ObDatum &arg1, const common::ObDatum &arg2,
                                       ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);
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

int ObExprSysContext::eval_module(const ObExpr &expr, common::ObDatum &res,
                                       const common::ObDatum &arg1, const common::ObDatum &arg2,
                                       ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    ObString module;
    ObExprStrResAlloc res_alloc(expr, ctx);
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    OZ(ObExprUtil::convert_string_collation(session->get_module_name(),
                                            ObCharset::get_system_collation(),
                                            module, expr.datum_meta_.cs_type_,
                                            calc_alloc));
    OZ(deep_copy_ob_string(res_alloc, module, module));
    OX(res.set_string(module));
  }
  return ret;
}

int ObExprSysContext::eval_client_identifier(const ObExpr &expr,
                                             common::ObDatum &res,
                                             const common::ObDatum &arg1,
                                             const common::ObDatum &arg2,
                                             ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    if (session->get_client_identifier().length() > 0) {
      ObString client_id;
      ObExprStrResAlloc res_alloc(expr, ctx);
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &calc_alloc = alloc_guard.get_allocator();
      OZ(ObExprUtil::convert_string_collation(session->get_client_identifier(),
                                              ObCharset::get_system_collation(),
                                              client_id, expr.datum_meta_.cs_type_,
                                              calc_alloc));
      OZ(deep_copy_ob_string(res_alloc, client_id, client_id));
      OX(res.set_string(client_id));
    } else {
      res.set_null();
    }
  }
  return ret;
}

int ObExprSysContext::eval_application_context(const ObExpr &expr, ObDatum &res,
                                               const ObString &arg1, const ObString &arg2,
                                               ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObString value;
  ObString out_value;
  ObSQLSessionInfo *session = nullptr;
  bool exist = false;
  ObISQLClient *sql_client = nullptr;
  if (arg1.length() > OB_MAX_CONTEXT_STRING_LENGTH) {
    ret = OB_ERR_INVALID_INPUT_ARGUMENT;
    LOG_USER_ERROR(OB_ERR_INVALID_INPUT_ARGUMENT, 1/*param_idx*/);
  } else if (arg2.length() > OB_MAX_CONTEXT_STRING_LENGTH) {
    ret = OB_ERR_INVALID_INPUT_ARGUMENT;
    LOG_USER_ERROR(OB_ERR_INVALID_INPUT_ARGUMENT, 2/*param_idx*/);
  } else if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get session", K(ret));
  } else if (OB_FAIL(session->get_context_values(arg1, arg2, value, exist))) {
    LOG_WARN("failed to get context from session", K(ret));
  } else if (!exist) {
    ObGlobalContextOperator ctx_operator;
    share::schema::ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(get_schema_guard(schema_guard, session->get_effective_tenant_id()))) {
      LOG_WARN("failed to get schema guard", K(ret));
    } else {
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &calc_alloc = alloc_guard.get_allocator();
      const ObString &username = session->get_user_name();
      const ObString &client_id = session->get_client_identifier();
      const ObContextSchema *ctx_schema = nullptr;
      if (OB_FAIL(schema_guard.get_context_schema_with_name(session->get_effective_tenant_id(),
                                                      arg1, ctx_schema))) {
        LOG_WARN("failed to get context schema", K(ret));
      } else if (OB_ISNULL(ctx_schema)) {
        // not exist, do nothing and return null
      } else if (OB_ISNULL(sql_client = ctx.exec_ctx_.get_sql_proxy())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql proxy", K(ret));
      } else if (OB_FAIL(ctx_operator.read_global_context(ctx_schema->get_tenant_id(),
                                                  ctx_schema->get_context_id(),
                                                  arg2,
                                                  client_id,
                                                  username,
                                                  value,
                                                  exist,
                                                  *sql_client,
                                                  calc_alloc))) {
        LOG_WARN("failed to read global context value", K(ret));
      } else if (exist) {
        ObExprStrResAlloc res_alloc(expr, ctx);
        OZ(ObExprUtil::convert_string_collation(value,
                                                ObCharset::get_system_collation(),
                                                out_value, expr.datum_meta_.cs_type_,
                                                calc_alloc));
        OZ(deep_copy_ob_string(res_alloc, out_value, out_value));
      }
    }
  } else {
    ObExprStrResAlloc res_alloc(expr, ctx);
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    OZ(ObExprUtil::convert_string_collation(value,
                                            ObCharset::get_system_collation(),
                                            out_value, expr.datum_meta_.cs_type_,
                                            calc_alloc));
    OZ(deep_copy_ob_string(res_alloc, out_value, out_value));
  }
  if (OB_SUCC(ret)) {
    if (exist) {
      OX(res.set_string(out_value));
    } else {
      res.set_null();
    }
  }
  return ret;
}

int ObExprSysContext::eval_sys_context(const ObExpr &expr, ObEvalCtx &ctx,
                                       ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *ns = NULL;
  ObDatum *para = NULL;
  eval_fun fun = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, ns, para))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (ns->is_null() || para->is_null()) {
    res.set_null();
  } else {
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    ObString ns_str;
    ObString para_str;
    ObCollationType ns_coll = expr.args_[0]->datum_meta_.cs_type_;
    ObCollationType para_coll = expr.args_[1]->datum_meta_.cs_type_;
    ObCollationType sys_coll = ObCharset::get_system_collation();
    OZ(ob_write_string(calc_alloc, ns->get_string(), ns_str));
    OZ(ob_write_string(calc_alloc, para->get_string(), para_str));
    OZ(ObExprUtil::convert_string_collation(ns_str, ns_coll, ns_str,
                                            sys_coll, calc_alloc));
    OZ(ObExprUtil::convert_string_collation(para_str, para_coll, para_str,
                                            sys_coll, calc_alloc));

    OZ(get_eval_fun(ns_str, para_str, fun));
    if (OB_SUCC(ret)) {
      if (NULL == fun) {
        if (ns->is_null() || para->is_null()) {
          res.set_null();
        } else {
          ObCharset::caseup(sys_coll, ns_str);
          ObCharset::caseup(sys_coll, para_str);
          if (OB_FAIL(eval_application_context(expr, res, ns_str, para_str, ctx))) {
            LOG_WARN("failed to eval application context", K(ret));
          }
        }
      } else if (OB_FAIL(fun(expr, res, *ns, *para, ctx))) {
        LOG_WARN("fail to calc result", K(ret));
      }
    }
  }
  return ret;
}

int ObExprSysContext::cg_expr(ObExprCGCtx &ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(raw_expr);
  CK(2 == rt_expr.arg_cnt_);
  OX(rt_expr.eval_func_ = eval_sys_context);
  return ret;
}

int ObExprSysContext::uint_string(const ObExpr &expr, ObEvalCtx &ctx,
                                  uint64_t id, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObFastFormatInt ffi(id);
  ObExprStrResAlloc res_alloc(expr, ctx);
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  ObIAllocator &calc_alloc = alloc_guard.get_allocator();
  ObString id_str_utf8(ffi.length(), ffi.ptr());
  ObString id_str;
  OZ(ObExprUtil::convert_string_collation(id_str_utf8, ObCharset::get_system_collation(),
        id_str, expr.datum_meta_.cs_type_, calc_alloc));
  OZ(deep_copy_ob_string(res_alloc, id_str, id_str));
  OX(res.set_string(id_str));
  return ret;
}

int ObExprSysContext::extract_ip(const ObString &user_at_client_ip, int64_t &start)
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

int ObExprSysContext::get_schema_guard(share::schema::ObSchemaGetterGuard &schema_guard, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get schema_service", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  }
  return ret;
}

int ObExprSysContext::eval_proxy_user(const ObExpr &expr, ObDatum &res, const ObDatum &arg1,
                                const ObDatum &arg2, ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);

  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    const ObString &user_name = session->get_proxy_user_name();
    ObString out_user_name;
    ObExprStrResAlloc res_alloc(expr, ctx);
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    OZ(ObExprUtil::convert_string_collation(user_name, ObCharset::get_system_collation(),
                                            out_user_name, expr.datum_meta_.cs_type_,
                                            calc_alloc));
    OZ(deep_copy_ob_string(res_alloc, out_user_name, out_user_name));
    OX(res.set_string(out_user_name));
  }
  return ret;
}

int ObExprSysContext::eval_proxy_user_id(const ObExpr &expr, ObDatum &res,
                                    const ObDatum &arg1, const ObDatum &arg2,
                                    ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);

  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    uint64_t id = session->get_proxy_user_id();
    char out_id[256];
    sprintf(out_id, "%lu", id);
    ObString out_id_str(strlen(out_id), out_id);
    ObExprStrResAlloc res_alloc(expr, ctx);
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    OZ(ObExprUtil::convert_string_collation(out_id_str, ObCharset::get_system_collation(),
                                            out_id_str, expr.datum_meta_.cs_type_,
                                            calc_alloc));
    OZ(deep_copy_ob_string(res_alloc, out_id_str, out_id_str));
    OX(res.set_string(out_id_str));
  }
  return ret;
}

int ObExprSysContext::eval_auth_identity(const ObExpr &expr, ObDatum &res, const ObDatum &arg1,
                                const ObDatum &arg2, ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(arg1);
  UNUSED(arg2);

  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    const ObString user_name = session->get_user_name();
    ObString out_user_name;
    ObExprStrResAlloc res_alloc(expr, ctx);
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    OZ(ObExprUtil::convert_string_collation(user_name, ObCharset::get_system_collation(),
                                            out_user_name, expr.datum_meta_.cs_type_,
                                            calc_alloc));
    OZ(deep_copy_ob_string(res_alloc, out_user_name, out_user_name));
    OX(res.set_string(out_user_name));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
