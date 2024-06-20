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
#include "observer/ob_server.h"
#include "share/ob_rpc_struct.h"
namespace oceanbase
{
using namespace common;
using namespace obrpc;
namespace sql
{

int ObKillExecutor::execute(ObExecContext &ctx, ObKillStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObKillSessionArg arg;
  ObAddr addr;
  ObSQLSessionMgr &session_mgr = OBSERVER.get_sql_session_mgr();
  if (OB_FAIL(arg.init(ctx, stmt))) {
    LOG_WARN("fail to init kill_session arg", K(ret), K(arg), K(ctx), K(stmt));
  } else {
    uint32_t SERVER_SESSID_TAG = 1ULL << 31;
    bool is_client_id_support = false;
    if (OB_NOT_NULL(ctx.get_my_session())) {
      is_client_id_support = ctx.get_my_session()->is_client_sessid_support();
    }
    bool direct_mode = !is_client_id_support ||
        ((arg.sess_id_ & SERVER_SESSID_TAG) >> 31) || arg.is_query_ == true;
    // Direct connection scenario kill session or kill query
    if (direct_mode) {
      if (OB_FAIL(kill_session(arg, session_mgr))) {
        if (OB_ENTRY_NOT_EXIST == ret) {//doesn't find sessid in current server
          if (OB_FAIL(get_remote_session_location(arg, ctx, addr))) {
            LOG_WARN("fail to get remote session location", K(ret), K(arg), K(ctx), K(addr));
          } else if (OB_FAIL(kill_remote_session(ctx, addr, arg))) {
            LOG_WARN("fail to kill remote session", K(ret), K(ctx), K(addr), K(arg));
          } else { /*do nothing*/}
        } else {
          LOG_WARN("fail to kill session", K(ret), K(arg));
        }
      }
    } else {
      // Proxy connection scenario kill session.
      if (OB_FAIL(kill_client_session(arg, session_mgr, ctx))) {
        if (ret == OB_ERR_KILL_CLIENT_SESSION) {
          LOG_DEBUG("Succ to Kill Client Session", K(ret), K(arg));
        } else {
          LOG_WARN("Fail to kill client session", K(ret), K(arg));
        }
      } else {
      }
    }
  }

  if (OB_UNKNOWN_CONNECTION == ret) {
    LOG_USER_ERROR(OB_UNKNOWN_CONNECTION, static_cast<uint64_t>(arg.sess_id_));
  } else if (OB_ERR_KILL_DENIED == ret) {
    LOG_USER_ERROR(OB_ERR_KILL_DENIED, static_cast<uint64_t>(arg.sess_id_));
  }
  return ret;
}

int ObKillExecutor::kill_client_session(const ObKillSessionArg &arg, ObSQLSessionMgr &sess_mgr,
                                       ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSQLSessionInfo *sess_info = NULL;
  ObSQLSessionInfo *curr_sess_info = NULL;
  ObAddr addr;
  uint32_t client_sess_id = arg.sess_id_;
  uint32_t server_sess_id = INVALID_SESSID;
  // Proxy connection scenario kill session
  if (OB_ISNULL(curr_sess_info = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret), K(ctx));
  } else if (OB_FAIL(sess_mgr.get_client_sess_map().get_refactored(client_sess_id, server_sess_id))) {
    //The current machine does not have this id and needs to be broadcast to other machines.
    // 1. In order to obtain the create time of the killed client ID for storing the map,
    //    if it cannot be found on the current machine, you need to search globally.
    // 2. If no one can be found, the kill will fail directly. It should be an illegal ID.
    // 3. If found, the first address that can be obtained is recorded. If the address is
    //    specified, the time will be sent back when sending remotely.
    // If all machines are unsuccessful, it should be that this ID does not exist or
    // there is a network problem. If some are successful and some fail, it is a network problem.
    LOG_WARN("fail to get client session in this server", K(ret), K(client_sess_id));
    ret = OB_SUCCESS;
  } else if (OB_FAIL(sess_mgr.get_session(server_sess_id, sess_info))) {
    ret = OB_SUCCESS;
  } else if (OB_ISNULL(sess_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL", K(ret), K(client_sess_id));
  } else if (client_sess_id == curr_sess_info->get_client_sessid()) {
    // If it is kill the session currently executing the kill command
    // it can directly return the error code to the proxy.
    sess_info->set_mark_killed(true);
    ret = OB_ERR_KILL_CLIENT_SESSION;
    LOG_INFO("current server conclude kill client session", K(arg.sess_id_));
  } else {
  }
  if (OB_SUCC(ret)) {
    ObAddr cs_addr;
    int64_t create_time = 0;
    // current server not have cs_id, find it in remote.
    // If there is no link between proxy and server,
    // unknown client session id will be reported.
    if (OB_FAIL(get_remote_session_location(arg, ctx, cs_addr, true))) {
      LOG_WARN("fail to get client session location, unknown client sessid",
              K(ret), K(arg), K(ctx), K(cs_addr));
      // Obtain the client establishment time for map maintenance.
    } else if (OB_FAIL(get_client_session_create_time_and_auth(arg, ctx, cs_addr, create_time))) {
      LOG_WARN("fail to get client session create time or no auth",
              K(ret), K(arg), K(ctx), K(cs_addr), K(ret));
      // If the time cannot be obtained, return kill failure.
      if (ret == OB_ENTRY_NOT_EXIST) {
        ret = OB_ERR_KILL_CLIENT_SESSION_FAILED;
      }
    } else if (cs_addr.is_valid()) {
      obrpc::ObKillClientSessionArg cs_arg;
      obrpc::ObKillClientSessionRes cs_result;
      common::ObZone zone;
      ObArray<share::ObServerInfoInTable> servers_info;
      bool is_kill_succ = true;
      // Determine the broadcast range based on whether it is a system tenant
      // bool is_sys_kill = curr_sess_info->get_effective_tenant_id() == OB_SYS_TENANT_ID;
      // Currently, there is no interface for querying node addresses at tenant granularity,
      // which can be optimized later.
      LOG_DEBUG("Begin to send kill session rpc", K(arg.sess_id_),K(create_time));
      if (OB_ISNULL(GCTX.srv_rpc_proxy_) || OB_ISNULL(GCTX.root_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("fail to get srv_rpc_proxy", K(ret), K(GCTX.srv_rpc_proxy_),
                  K(GCTX.root_service_));
      } else if (OB_FAIL(share::ObAllServerTracer::get_instance().get_servers_info(
        zone, servers_info))) {
        LOG_WARN("fail to get servers info", K(ret));
      } else if (FALSE_IT(cs_arg.set_create_time(create_time))) {
      } else if (FALSE_IT(cs_arg.set_client_sess_id(client_sess_id))) {
      } else {
        ObAddr addr;
        const int64_t rpc_timeout = GCONF.rpc_timeout;
        rootserver::ObKillClientSessionProxy proxy(*GCTX.srv_rpc_proxy_,
                            &obrpc::ObSrvRpcProxy::kill_client_session);
        for (int64_t i = 0; OB_SUCC(ret) && i < servers_info.count(); i++) {
          addr = servers_info.at(i).get_server();
          if (addr != GCTX.self_addr() && OB_FAIL(proxy.call(addr, rpc_timeout,
                            curr_sess_info->get_effective_tenant_id(), cs_arg))) {
            LOG_WARN("send rpc failed", KR(ret),
                K(rpc_timeout), K(arg), "server", addr);
            ret = OB_SUCCESS;
          }
        }

        int tmp_ret = OB_SUCCESS;
        ObArray<int> return_code_array;
        if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
          LOG_WARN("wait result failed", KR(tmp_ret));
          is_kill_succ = false;
        } else if (return_code_array.count() != proxy.get_results().count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cnt not match",
              K(ret),
              "return_cnt",
              return_code_array.count(),
              "result_cnt",
              proxy.get_results().count());
        } else {
          for (int64_t i = 0; i < proxy.get_results().count(); i++) {
            if (OB_FAIL(return_code_array.at(i))) {
              if (return_code_array.at(i) == OB_TENANT_NOT_IN_SERVER ||
                return_code_array.at(i) == OB_TIMEOUT) {
                ret = OB_SUCCESS;  // ignore error
              } else {
                LOG_WARN("rpc execute failed", KR(ret));
              }
            } else {
              const obrpc::ObKillClientSessionRes *result = proxy.get_results().at(i);
              if (OB_ISNULL(result)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_ERROR("fail to get result", K(ret), K(i));
              } else if (const_cast<obrpc::ObKillClientSessionRes*>(
                      result)->get_can_kill_client_sess() == false) {
                is_kill_succ = false;
              }
            }
          }
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing.
      } else if (is_kill_succ == false) {
        ret = OB_ERR_KILL_CLIENT_SESSION_FAILED;
        LOG_WARN("Fail to Kill Client Session", K(ret), K(client_sess_id));
      } else {
        LOG_INFO("Succ to Kill Client Session", K(ret), K(client_sess_id));
      }

      // In the end, if everything succeeds here, the current map will be recorded.
      // If it is not completely successful, there is no need to record it, in order
      // to ensure that the recording time is valid.
      if (OB_FAIL(ret)) {
        LOG_WARN("kill client session not all successful", K(ret), K(cs_arg));
      } else {
        if (NULL != sess_info) {
          // The mark maintained here is used to trigger a link
          // break when the next request hits the current session.
          sess_info->set_mark_killed(true);
        }
        // The reason for maintaining the kill session id map is that proxy A's kill
        // request kills proxy B's client link. The next time a new connection is requested,
        // the map will be used to determine whether kill is needed.
        int flag = 1;
        sess_mgr.get_kill_client_sess_map().set_refactored(client_sess_id, create_time, flag);
      }
    }
  }
  if (NULL != sess_info) {
    sess_mgr.revert_session(sess_info);
  }

  return ret;
}

int ObKillExecutor::get_client_session_create_time_and_auth(const ObKillSessionArg &arg, ObExecContext &ctx,
                          common::ObAddr &cs_addr, int64_t &create_time)
{
  int ret = OB_SUCCESS;
  obrpc::ObClientSessionCreateTimeAndAuthArg cs_arg;
  obrpc::ObClientSessionCreateTimeAndAuthRes cs_result;
  common::ObZone zone;
  ObArray<share::ObServerInfoInTable> servers_info;

  if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to get srv_rpc_proxy", K(ret), K(GCTX.srv_rpc_proxy_));
  } else if (OB_FAIL(share::ObAllServerTracer::get_instance().get_servers_info(
    zone, servers_info))) {
    LOG_WARN("fail to get servers info", K(ret));
  } else if (FALSE_IT(cs_arg.set_client_sess_id(arg.sess_id_))) {
  } else if (FALSE_IT(cs_arg.set_tenant_id(arg.tenant_id_))) {
  } else if (FALSE_IT(cs_arg.set_has_user_super_privilege(arg.has_user_super_privilege_))) {
  } else if (FALSE_IT(cs_arg.set_user_id(arg.user_id_))) {
  } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(cs_addr).by(MTL_ID()).
                          client_session_create_time(cs_arg, cs_result))) {
      // rpc fail not kill client session.
      LOG_WARN("fail to rpc", K(ret));
  } else if (cs_result.is_have_kill_auth() == false) {
    ret = OB_ERR_KILL_DENIED;
    LOG_WARN("no permissions for kill", K(ret), K(arg.sess_id_));
  } else if (FALSE_IT(create_time = cs_result.get_client_create_time())) {
  } else if (create_time == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to set client create time", K(ret));
  }

  return ret;
}

//If you are in system tenant, you can kill all threads and statements in any tenant.
//If you have the SUPER privilege, you can kill all threads and statements at your Tenant.
//Otherwise, you can kill only your own threads and statements.
int ObKillSession::kill_session(const ObKillSessionArg &arg, ObSQLSessionMgr &sess_mgr)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSQLSessionInfo *sess_info = NULL;
  ObAddr addr;
  uint32_t sess_id = arg.sess_id_;
  ObSessionGetterGuard guard(sess_mgr, sess_id);
  if (OB_FAIL(guard.get_session(sess_info))) {
    LOG_WARN("fail to get session", K(ret), K(sess_id));
  } else if (OB_ISNULL(sess_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL", K(ret), K(arg));
  } else if ((OB_SYS_TENANT_ID == arg.tenant_id_)
             || ((arg.tenant_id_ == sess_info->get_priv_tenant_id())
                 && (arg.has_user_super_privilege_ || arg.user_id_ == sess_info->get_user_id()))) {
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
  return ret;
}

// is_client_session = true, for finding kill client session
int ObKillExecutor::get_remote_session_location(const ObKillSessionArg &arg,
                  ObExecContext &ctx, ObAddr &addr, bool is_client_session)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLProxy *sql_proxy = ctx.get_sql_proxy();
    sqlclient::ObMySQLResult *result_set = NULL;
    ObSQLSessionInfo *cur_sess = ctx.get_my_session();
    ObSqlString read_sql;
    char svr_ip[OB_IP_STR_BUFF] = "";
    int64_t svr_port = 0;
    int64_t tmp_real_str_len = 0;

    //execute sql
    if (OB_ISNULL(sql_proxy) || OB_ISNULL(cur_sess)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy or session from exec context is NULL", K(ret), K(sql_proxy), K(cur_sess));
    } else if (OB_FAIL(generate_read_sql(arg.sess_id_, read_sql))) {
      LOG_WARN("fail to generate sql", K(ret), K(read_sql), K(*cur_sess), K(arg));
    } else if (OB_FAIL(sql_proxy->read(res, read_sql.ptr()))) {
      LOG_WARN("fail to read by sql proxy", K(ret), K(read_sql));
    } else if (OB_ISNULL(result_set = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result set is NULL", K(ret), K(read_sql));
    } else {/*do nothing*/}
  
    //read result_set
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(result_set->next())) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        read_sql.reuse();
        if (OB_FAIL(generate_read_sql_from_session_info(arg.sess_id_, read_sql))) {
          LOG_WARN("fail to generate sql", K(ret), K(read_sql));
        } else if (OB_FAIL(sql_proxy->read(res, read_sql.ptr()))) {
          LOG_WARN("fail to read by sql proxy", K(ret), K(read_sql));
        } else if (OB_ISNULL(result_set = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result set is NULL", K(ret), K(read_sql));
        } else if (OB_FAIL(result_set->next())) {
          if (OB_LIKELY(OB_ITER_END == ret)) {
            ret = OB_UNKNOWN_CONNECTION;
            LOG_WARN("fail to get next row", K(ret), K(result_set));
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      UNUSED(tmp_real_str_len);
      EXTRACT_STRBUF_FIELD_MYSQL(*result_set, "svr_ip", svr_ip, OB_IP_STR_BUFF, tmp_real_str_len);
      EXTRACT_INT_FIELD_MYSQL(*result_set, "svr_port", svr_port, int64_t);
    }

    //set addr
    if (OB_FAIL(ret)) {
    } else if (!is_client_session && OB_UNLIKELY(OB_ITER_END != result_set->next())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("more than one sessid record", K(ret), K(arg), K(read_sql));
    } else if (OB_UNLIKELY(!addr.set_ip_addr(svr_ip, static_cast<int32_t>(svr_port)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to set ip_addr", K(ret), K(svr_ip), K(svr_port));
    } else {/*do nothing*/}
  
  }
  return ret;
}

int ObKillExecutor::generate_read_sql(uint32_t sess_id, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  const char *sql_str = "select svr_ip, svr_port from oceanbase.__all_virtual_processlist \
              where id = %u";
  if (OB_FAIL(sql.append_fmt(sql_str, sess_id))) {
    LOG_WARN("fail to append sql", K(ret), K(sess_id));
  }
  return ret;
}

int ObKillExecutor::generate_read_sql_from_session_info(uint32_t sess_id, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  const char *sql_str = "select svr_ip, svr_port from oceanbase.__all_virtual_session_info \
              where id = %u";
  if (OB_FAIL(sql.append_fmt(sql_str, sess_id))) {
    LOG_WARN("fail to append sql", K(ret), K(sess_id));
  }
  return ret;
}

int ObKillExecutor::kill_remote_session(ObExecContext &ctx, const ObAddr &addr, const ObKillSessionArg &arg)
{
  int ret = OB_SUCCESS;
  ObSrvRpcProxy *rpc_proxy = GCTX.srv_rpc_proxy_;
  ObSQLSessionInfo *session = ctx.get_my_session();
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx);
  if (OB_ISNULL(rpc_proxy) || OB_ISNULL(session) || OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("some params are NULL", K(ret), K(rpc_proxy), K(session), K(plan_ctx));
  } else {
    int64_t timeout = plan_ctx->get_timeout_timestamp() - ObTimeUtility::current_time();
    uint64_t tenant_id = THIS_WORKER.get_rpc_tenant() > 0 ?
        THIS_WORKER.get_rpc_tenant() : session->get_rpc_tenant_id();
    if (OB_UNLIKELY(timeout <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("task_execute timeout before rpc", K(ret), K(addr), K(timeout), K(arg),
               K(tenant_id), "timeout_ts", plan_ctx->get_timeout_timestamp());
    } else if (OB_FAIL(rpc_proxy->to(addr).by(tenant_id).timeout(timeout).kill_session(arg))) {
      LOG_WARN("fail to kill remote session", K(ret), K(addr), K(tenant_id), K(timeout), K(arg));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObRpcKillSessionP::process()
{
  int ret = OB_SUCCESS;
  ObKillSessionArg &arg = arg_;
  ObSQLSessionMgr *session_mgr = gctx_.session_mgr_;

  if (OB_ISNULL(session_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session mgr from gctx is NULL", K(ret));
  } else if (OB_FAIL(kill_session(arg, *session_mgr))) {
    LOG_WARN("fail to kill session", K(ret), K(arg));
    ret = OB_ENTRY_NOT_EXIST == ret ? OB_UNKNOWN_CONNECTION : ret;
  } else {/*do nothing*/}

  return ret;
}
}// sql
}// oceanbase
