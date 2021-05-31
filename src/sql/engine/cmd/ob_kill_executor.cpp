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
#include "sql/resolver/cmd/ob_kill_stmt.h"
#include "sql/engine/cmd/ob_kill_executor.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/cmd/ob_kill_session_arg.h"
#include "lib/net/ob_addr.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_srv_rpc_proxy.h"
#include "observer/ob_server_struct.h"
#include "share/ob_rpc_struct.h"
namespace oceanbase {
using namespace common;
using namespace obrpc;
namespace sql {

int ObKillExecutor::execute(ObExecContext& ctx, ObKillStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObKillSessionArg arg;
  ObAddr addr;
  ObSQLSessionMgr* session_mgr = ctx.get_session_mgr();

  if (OB_ISNULL(session_mgr)) {
    ret = OB_SUCCESS;
    LOG_WARN("data member from ObExeccontext is NULL", K(ret), K(session_mgr));
  } else if (OB_FAIL(arg.init(ctx, stmt))) {
    LOG_WARN("fail to init kill_session arg", K(ret), K(arg), K(ctx), K(stmt));
  } else if (OB_FAIL(kill_session(arg, *session_mgr))) {
    if (OB_ENTRY_NOT_EXIST == ret) {  // doesn't find sessid in current server
      if (OB_FAIL(get_remote_session_location(arg, ctx, addr))) {
        LOG_WARN("fail to get remote session location", K(ret), K(arg), K(ctx), K(addr));
      } else if (OB_FAIL(kill_remote_session(ctx, addr, arg))) {
        LOG_WARN("fail to kill remote session", K(ret), K(ctx), K(addr), K(arg));
      } else { /*do nothing*/
      }
    } else {
      LOG_WARN("fail to kill session", K(ret), K(arg));
    }
  } else { /*do nothing*/
  }

  if (OB_UNKNOWN_CONNECTION == ret) {
    LOG_USER_ERROR(OB_UNKNOWN_CONNECTION, static_cast<uint64_t>(arg.sess_id_));
  } else if (OB_ERR_KILL_DENIED == ret) {
    LOG_USER_ERROR(OB_ERR_KILL_DENIED, static_cast<uint64_t>(arg.sess_id_));
  }
  return ret;
}

// If you are in system tenant, you can kill all threads and statements in any tenant.
// If you have the SUPER privilege, you can kill all threads and statements at your Tenant.
// Otherwise, you can kill only your own threads and statements.
int ObKillSession::kill_session(const ObKillSessionArg& arg, ObSQLSessionMgr& sess_mgr)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSQLSessionInfo* sess_info = NULL;
  ObAddr addr;
  uint32_t sess_id = arg.sess_id_;
  if (OB_FAIL(get_session(sess_mgr, sess_id, sess_info))) {
    LOG_WARN("fail to get session", K(ret), K(sess_id));
  } else if (OB_ISNULL(sess_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL", K(ret), K(arg));
  } else if ((OB_SYS_TENANT_ID == arg.tenant_id_) ||
             ((arg.tenant_id_ == sess_info->get_priv_tenant_id()) &&
                 (arg.has_user_super_privilege_ || arg.user_id_ == sess_info->get_user_id()))) {
    if (arg.is_query_) {
      if (OB_FAIL(sess_mgr.kill_query(*sess_info))) {
        LOG_WARN("fail to kill query", K(ret), K(arg));
      }
    } else {
      if (OB_FAIL(sess_mgr.kill_session(*sess_info))) {
        LOG_WARN("fail to kill session", K(ret), K(arg));
      }
    }
  } else {
    ret = OB_ERR_KILL_DENIED;
  }

  if (OB_UNLIKELY(sess_info != NULL && OB_SUCCESS != (tmp_ret = sess_mgr.revert_session(sess_info)))) {
    // ignore tmp_ret, our monitor will find the error
    LOG_ERROR("revert session fail", K(tmp_ret), K(arg));
  }
  return ret;
}

int ObKillSession::get_session(ObSQLSessionMgr& sess_mgr, uint32_t sess_id, ObSQLSessionInfo*& sess_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  sess_info = NULL;
  ObSQLSessionInfo* sess = NULL;
  uint32_t version = 0;
  do {
    sess = NULL;
    if (OB_FAIL(sess_mgr.get_session(version, sess_id, sess))) {
      LOG_WARN("fail to get_session", K(ret), K(version), K(sess_id));
    } else if (OB_ISNULL(sess)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get session fail", K(ret), K(version), K(sess_id));
    } else if (OB_UNLIKELY(sess->is_shadow())) {
      ret = OB_ENTRY_NOT_EXIST;
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = sess_mgr.revert_session(sess)))) {
        LOG_ERROR(
            "fail to revert session", K(sess_id), K(version), "proxy_sessid", sess->get_proxy_sessid(), K(tmp_ret));
      }
    } else { /*do nothing*/
    }
    ++version;
  } while (OB_UNLIKELY(OB_ENTRY_NOT_EXIST == ret && version <= ObSQLSessionMgr::MAX_VERSION));

  if (OB_SUCC(ret)) {
    sess_info = sess;
  }

  return ret;
}

int ObKillExecutor::get_remote_session_location(const ObKillSessionArg& arg, ObExecContext& ctx, ObAddr& addr)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLProxy* sql_proxy = ctx.get_sql_proxy();
    sqlclient::ObMySQLResult* result_set = NULL;
    ObSQLSessionInfo* cur_sess = ctx.get_my_session();
    ObSqlString read_sql;
    char svr_ip[OB_IP_STR_BUFF] = "";
    int64_t svr_port = 0;
    int64_t tmp_real_str_len = 0;

    // execute sql
    if (OB_ISNULL(sql_proxy) || OB_ISNULL(cur_sess)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy or sesion from exec context is NULL", K(ret), K(sql_proxy), K(cur_sess));
    } else if (OB_FAIL(generate_read_sql(arg.sess_id_, read_sql))) {
      LOG_WARN("fail to generate sql", K(ret), K(read_sql), K(*cur_sess), K(arg));
    } else if (OB_FAIL(sql_proxy->read(res, read_sql.ptr()))) {
      LOG_WARN("fail to read by sql proxy", K(ret), K(read_sql));
    } else if (OB_ISNULL(result_set = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result set is NULL", K(ret), K(read_sql));
    } else { /*do nothing*/
    }

    // read result_set
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(result_set->next())) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        ret = OB_UNKNOWN_CONNECTION;
      }
      LOG_WARN("fail to get next row", K(ret), K(result_set));
    } else {
      UNUSED(tmp_real_str_len);
      EXTRACT_STRBUF_FIELD_MYSQL(*result_set, "svr_ip", svr_ip, OB_IP_STR_BUFF, tmp_real_str_len);
      EXTRACT_INT_FIELD_MYSQL(*result_set, "svr_port", svr_port, int64_t);
    }

    // set addr
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(OB_ITER_END != result_set->next())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("more than one sessid record", K(ret), K(arg), K(read_sql));
    } else if (OB_UNLIKELY(!addr.set_ip_addr(svr_ip, static_cast<int32_t>(svr_port)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to set ip_addr", K(ret), K(svr_ip), K(svr_port));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObKillExecutor::generate_read_sql(uint32_t sess_id, ObSqlString& sql)
{
  int ret = OB_SUCCESS;
  const char* sql_str = "select svr_ip, svr_port from oceanbase.__all_virtual_processlist \
              where id = %u";
  if (OB_FAIL(sql.append_fmt(sql_str, sess_id))) {
    LOG_WARN("fail to append sql", K(ret), K(sess_id));
  }
  return ret;
}

int ObKillExecutor::kill_remote_session(ObExecContext& ctx, const ObAddr& addr, const ObKillSessionArg& arg)
{
  int ret = OB_SUCCESS;
  ObSrvRpcProxy* rpc_proxy = GCTX.srv_rpc_proxy_;
  ObSQLSessionInfo* session = ctx.get_my_session();
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx);
  if (OB_ISNULL(rpc_proxy) || OB_ISNULL(session) || OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("some params are NULL", K(ret), K(rpc_proxy), K(session), K(plan_ctx));
  } else {
    int64_t timeout = plan_ctx->get_timeout_timestamp() - ObTimeUtility::current_time();
    uint64_t tenant_id = THIS_WORKER.get_rpc_tenant() > 0 ? THIS_WORKER.get_rpc_tenant() : session->get_rpc_tenant_id();
    if (OB_UNLIKELY(timeout <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("task_execute timeout before rpc",
          K(ret),
          K(addr),
          K(timeout),
          K(arg),
          K(tenant_id),
          "timeout_ts",
          plan_ctx->get_timeout_timestamp());
    } else if (OB_FAIL(rpc_proxy->to(addr).by(tenant_id).as(OB_SYS_TENANT_ID).timeout(timeout).kill_session(arg))) {
      LOG_WARN("fail to kill remote session", K(ret), K(addr), K(tenant_id), K(timeout), K(arg));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObRpcKillSessionP::process()
{
  int ret = OB_SUCCESS;
  ObKillSessionArg& arg = arg_;
  ObSQLSessionMgr* session_mgr = gctx_.session_mgr_;

  if (OB_ISNULL(session_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session mgr from gctx is NULL", K(ret));
  } else if (OB_FAIL(kill_session(arg, *session_mgr))) {
    LOG_WARN("fail to kill sessoin", K(ret), K(arg));
    ret = OB_ENTRY_NOT_EXIST == ret ? OB_UNKNOWN_CONNECTION : ret;
  } else { /*do nothing*/
  }

  return ret;
}
}  // namespace sql
}  // namespace oceanbase
