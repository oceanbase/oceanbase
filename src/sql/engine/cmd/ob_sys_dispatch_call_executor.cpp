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

#include "sql/engine/cmd/ob_sys_dispatch_call_executor.h"

#include "lib/ob_errno.h"
#include "observer/ob_inner_sql_connection.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "share/ob_define.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/resolver/cmd/ob_sys_dispatch_call_stmt.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{

using namespace common;
using namespace lib;
using namespace share::schema;
using namespace observer;

namespace sql
{

int ObSysDispatchCallExecutor::execute(ObExecContext &ctx, ObSysDispatchCallStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObFreeSessionCtx free_session_ctx;
  ObSQLSessionInfo *session = nullptr;
  ObInnerSQLConnectionPool *pool = nullptr;
  ObInnerSQLConnection *conn = nullptr;
  int64_t affected_rows = 0;

  CompatModeGuard worker_guard(ObCompatibilityMode::MYSQL_MODE == stmt.get_tenant_compat_mode()
                                   ? Worker::CompatMode::MYSQL
                                   : Worker::CompatMode::ORACLE);

  OZ (create_session(stmt.get_designated_tenant_id(), free_session_ctx, session));
  CK (OB_NOT_NULL(session));
  OZ (init_session(*session,
                   stmt.get_designated_tenant_id(),
                   stmt.get_designated_tenant_name(),
                   stmt.get_tenant_compat_mode()));
  CK (OB_NOT_NULL(pool = static_cast<ObInnerSQLConnectionPool *>(ctx.get_sql_proxy()->get_pool())));
  OZ (pool->acquire_spi_conn(session, conn));
  OZ (conn->execute_write(stmt.get_designated_tenant_id(), stmt.get_call_stmt(), affected_rows),
      stmt);
  if (OB_NOT_NULL(conn)) {
    ctx.get_sql_proxy()->close(conn, ret);
  }
  if (OB_NOT_NULL(session)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(destroy_session(free_session_ctx, session))) {
      LOG_WARN("failed to destroy session", KR(tmp_ret));
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
    } else {
      session = nullptr;
    }
  }

  return ret;
}

int ObSysDispatchCallExecutor::create_session(const uint64_t tenant_id,
                                              ObFreeSessionCtx &free_session_ctx,
                                              ObSQLSessionInfo *&session_info)
{
  int ret = OB_SUCCESS;
  uint32_t sid = sql::ObSQLSessionInfo::INVALID_SESSID;
  uint64_t proxy_sid = 0;
  if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_mgr_ is null");
  } else if (OB_FAIL(GCTX.session_mgr_->create_sessid(sid))) {
    LOG_WARN("alloc session id failed");
  } else if (OB_FAIL(GCTX.session_mgr_->create_session(
                 tenant_id, sid, proxy_sid, ObTimeUtility::current_time(), session_info))) {
    LOG_WARN("create session failed", K(ret), K(sid));
    GCTX.session_mgr_->mark_sessid_unused(sid);
    session_info = nullptr;
  } else if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected session info is null", K(ret));
  } else {
    free_session_ctx.sessid_ = sid;
    free_session_ctx.proxy_sessid_ = proxy_sid;
  }
  return ret;
}

int ObSysDispatchCallExecutor::init_session(sql::ObSQLSessionInfo &session,
                                            const uint64_t tenant_id,
                                            const ObString &tenant_name,
                                            const ObCompatibilityMode compat_mode)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObPrivSet db_priv_set = OB_PRIV_SET_EMPTY;
  const bool print_info_log = true;
  const bool is_sys_tenant = true;
  ObPCMemPctConf pc_mem_conf;
  ObObj compatibility_mode;
  ObObj sql_mode;
  ObSEArray<const ObUserInfo *, 1> user_infos;
  const ObUserInfo *user_info = nullptr;

  CK (OB_NOT_NULL(GCTX.schema_service_));
  OZ (GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard));
  if (ObCompatibilityMode::ORACLE_MODE == compat_mode) {
    compatibility_mode.set_int(1);
    sql_mode.set_uint(ObUInt64Type, DEFAULT_ORACLE_MODE);
  } else {
    compatibility_mode.set_int(0);
    sql_mode.set_uint(ObUInt64Type, DEFAULT_MYSQL_MODE);
  }
  OX (session.set_inner_session());
  OZ (session.load_default_sys_variable(print_info_log, is_sys_tenant));
  OZ (session.update_max_packet_size());
  OZ (session.init_tenant(tenant_name.ptr(), tenant_id));
  OZ (session.load_all_sys_vars(schema_guard));
  OZ (session.update_sys_variable(share::SYS_VAR_SQL_MODE, sql_mode));
  OZ (session.update_sys_variable(share::SYS_VAR_OB_COMPATIBILITY_MODE, compatibility_mode));
  OZ (session.update_sys_variable(share::SYS_VAR_NLS_DATE_FORMAT,
                                  ObTimeConverter::COMPAT_OLD_NLS_DATE_FORMAT));
  OZ (session.update_sys_variable(share::SYS_VAR_NLS_TIMESTAMP_FORMAT,
                                  ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT));
  OZ (session.update_sys_variable(share::SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT,
                                  ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT));
  OZ (session.get_pc_mem_conf(pc_mem_conf));
  CK (OB_NOT_NULL(GCTX.sql_engine_));

  if (ObCompatibilityMode::ORACLE_MODE == compat_mode) {
    OX (session.set_database_id(OB_ORA_SYS_DATABASE_ID));
    OZ (session.set_default_database(OB_ORA_SYS_SCHEMA_NAME));
    OZ (schema_guard.get_user_info(tenant_id, OB_ORA_SYS_USER_NAME, user_infos));
  } else {
    OX (session.set_database_id(OB_SYS_DATABASE_ID));
    OZ (session.set_default_database(OB_SYS_DATABASE_NAME));
    OZ (schema_guard.get_user_info(tenant_id, OB_SYS_USER_NAME, user_infos));
  }
  OV (1 == user_infos.count(),
      0 == user_infos.count() ? OB_USER_NOT_EXIST : OB_ERR_UNEXPECTED,
      K(user_infos));
  CK (OB_NOT_NULL(user_info = user_infos.at(0)));
  OZ (session.set_user(
          user_info->get_user_name(), user_info->get_host_name_str(), user_info->get_user_id()));
  OX (session.set_priv_user_id(user_info->get_user_id()));
  OX (session.set_user_priv_set(user_info->get_priv_set()));
  OZ (schema_guard.get_db_priv_set(
          tenant_id, user_info->get_user_id(), OB_SYS_DATABASE_NAME, db_priv_set));
  OX (session.set_db_priv_set(db_priv_set));

  OX (session.set_shadow(true));
  OX (session.gen_gtt_session_scope_unique_id());
  OX (session.gen_gtt_trans_scope_unique_id());
  return ret;
}

int ObSysDispatchCallExecutor::destroy_session(ObFreeSessionCtx &free_session_ctx,
                                               ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_mgr_ is null");
  } else if (OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session_info is null");
  } else {
    session_info->set_session_sleep();
    GCTX.session_mgr_->revert_session(session_info);
    GCTX.session_mgr_->free_session(free_session_ctx);
    GCTX.session_mgr_->mark_sessid_unused(free_session_ctx.sessid_);
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
