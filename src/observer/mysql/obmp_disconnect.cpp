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

#define USING_LOG_PREFIX SERVER

#include "obmp_disconnect.h"

#include "lib/stat/ob_session_stat.h"
#include "rpc/ob_request.h"
#include "observer/ob_server_struct.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "sql/ob_sql_trans_control.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;

ObMPDisconnect::ObMPDisconnect(const sql::ObFreeSessionCtx &ctx)
    : ctx_(ctx)
{
}

ObMPDisconnect::~ObMPDisconnect()
{

}

int ObMPDisconnect::kill_unfinished_session(uint32_t sessid)
{
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo *session = NULL;
  sql::ObSessionGetterGuard guard(*GCTX.session_mgr_, sessid);
  if (OB_FAIL(guard.get_session(session))) {
    LOG_WARN("get session fail", K(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to get session info", K(session), K(sessid), K(ret));
  } else {
    /* NOTE:
     * 在Disconnect的上下文中，两种可能：
     * (1) 长SQL先执行，并且正在执行，那么它已经能够检测到IS_KILLED标记
     *     此时disconnect_session会等待长SQL执行退出后才结束session中的事务
     * (2) disconnect_session先执行，它会结束事务并返回，然后执行后面的free_session
     *     (free_session 并不是释放 session 内存，只是一个逻辑删除的动作）。
     *     当长SQL拿到query_lock锁，它会立即检测IS_KILLED状态，检测到后立即退出处理流程。
     *     最终引用计数减为0，session被物理回收。
     */
    if (OB_FAIL(GCTX.session_mgr_->disconnect_session(*session))) {
      LOG_WARN("fail to disconnect session", K(session), K(sessid), K(ret));
    }
  }
  return ret;
}

int ObMPDisconnect::run()
{
  int ret = OB_SUCCESS;
  bool is_need_clear = false;
  if (ctx_.sessid_ != 0) {
    if (OB_ISNULL(GCTX.session_mgr_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid session mgr", K(GCTX.session_mgr_), K(ret));
    } else {
      ObSMConnection conn;
      conn.sessid_ = ctx_.sessid_;
      conn.is_need_clear_sessid_ = true;
      // bugfix:
      (void) kill_unfinished_session(ctx_.sessid_); // ignore ret
      if (OB_FAIL(GCTX.session_mgr_->free_session(ctx_))) {
        LOG_WARN("free session fail", K(ctx_));
      } else {
        common::ObTenantStatEstGuard guard(ctx_.tenant_id_);
        EVENT_INC(SQL_USER_LOGOUTS_CUMULATIVE);
        LOG_INFO("free session successfully", "sessid", ctx_.sessid_);
        if (OB_UNLIKELY(OB_FAIL(sql::ObSQLSessionMgr::is_need_clear_sessid(&conn, is_need_clear)))) {
          LOG_ERROR("fail to judge need clear", K(ret), "sessid", conn.sessid_, "server_id", GCTX.server_id_);
        } else if (is_need_clear) {
          if (OB_FAIL(GCTX.session_mgr_->mark_sessid_unused(conn.sessid_))) {
            LOG_WARN("mark session id unused failed", K(ret), "sessid", conn.sessid_);
          } else {
            LOG_INFO("mark session id unused", "sessid", conn.sessid_);
          }
        }
      }
    }
  }
  return ret;
}
