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

ObMPDisconnect::ObMPDisconnect(const sql::ObFreeSessionCtx& ctx) : ctx_(ctx)
{
  set_task_mark();
}

ObMPDisconnect::~ObMPDisconnect()
{}

int ObMPDisconnect::kill_unfinished_session(uint32_t version, uint32_t sessid)
{
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo* session = NULL;
  if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid session mgr", K(GCTX.session_mgr_), K(ret));
  } else if (OB_FAIL(GCTX.session_mgr_->get_session(version, sessid, session))) {
    LOG_WARN("get session fail", K(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to get session info", K(session), K(version), K(sessid), K(ret));
  } else {
    /* NOTE:
     * In the context of Disconnect, there are two possibilities:
     * (1) Long SQL is executed first and is being executed,
     * then it has been able to detect the IS_KILLED mark
     * At this time, disconnect_session will wait for the long SQL execution to exit
     * before ending the transaction in the session
     * (2) Disconnect_session is executed first, it will end the transaction and return,
     * and then execute the following free_session
     * (free_session does not release the session memory,
     * it is just a logical deletion action).
     * When long SQL gets the query_lock lock,
     * it will immediately detect the IS_KILLED state,
     * and exit the processing flow immediately after detection.
     * The final reference count is reduced to 0, and the session is physically recycled.
     */
    if (OB_FAIL(GCTX.session_mgr_->disconnect_session(*session))) {
      LOG_WARN("fail to disconnect session", K(session), K(version), K(sessid), K(ret));
    }
  }

  if (OB_LIKELY(NULL != session)) {
    (void)GCTX.session_mgr_->revert_session(session);
  }

  return ret;
}

int ObMPDisconnect::process()
{
  int ret = OB_SUCCESS;
  if (ctx_.sessid_ != 0) {
    if (OB_ISNULL(GCTX.session_mgr_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid session mgr", K(GCTX.session_mgr_), K(ret));
    } else {
      (void)kill_unfinished_session(ctx_.version_, ctx_.sessid_);  // ignore ret
      if (OB_FAIL(GCTX.session_mgr_->free_session(ctx_))) {
        LOG_WARN("free session fail", K(ctx_));
      } else {
        LOG_INFO("free session successfully", "sessid", ctx_.sessid_, "version", ctx_.version_);
      }
    }
  }
  return ret;
}
