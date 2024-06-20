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
#include "obmp_stmt_close.h"
#include "rpc/ob_request.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "sql/plan_cache/ob_ps_cache.h"
#include "sql/plan_cache/ob_prepare_stmt_struct.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/mysql/obmp_utils.h"
#include "observer/omt/ob_tenant.h"

namespace oceanbase
{
using namespace common;
using namespace rpc;
using namespace obmysql;
using namespace sql;

namespace observer
{

int ObMPStmtClose::deserialize()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(req_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid packet", K(ret), K_(req));
  } else if (OB_UNLIKELY(req_->get_type() != ObRequest::OB_MYSQL)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid packet", K(ret), K_(req), K(req_->get_type()));
  } else {
    const ObMySQLRawPacket &pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
    const char* pos = pkt.get_cdata();
    uint32_t stmt_id = -1; //INVALID_STMT_ID
    ObMySQLUtil::get_uint4(pos, stmt_id);
    stmt_id_ = stmt_id;
  }
  return ret;
}

int ObMPStmtClose::process()
{
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo *session = NULL;
  trace::UUID ps_close_span_id;
  if (OB_ISNULL(req_) || OB_ISNULL(get_conn())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid packet", K(ret), KP(req_));
  } else if (OB_INVALID_STMT_ID == stmt_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt_id is invalid", K(ret));
  } else if (OB_FAIL(get_session(session))) {
    LOG_WARN("get session failed");
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL or invalid", K(ret), K(session));
  } else if (OB_FAIL(update_transmission_checksum_flag(*session))) {
    LOG_WARN("update transmisson checksum flag failed", K(ret));
  } else {
    const ObMySQLRawPacket &pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
    ObSQLSessionInfo::LockGuard lock_guard(session->get_query_lock());
    ObSessionStatEstGuard stat_est_guard(get_conn()->tenant_->id(), session->get_sessid());
    session->init_use_rich_format();
    LOG_TRACE("close ps stmt or cursor", K_(stmt_id), K(session->get_sessid()));
    if (OB_FAIL(sql::ObFLTUtils::init_flt_info(pkt.get_extra_info(), *session,
                     get_conn()->proxy_cap_flags_.is_full_link_trace_support()))) {
      LOG_WARN("failed to init flt extra info", K(ret));
    }
    FLTSpanGuard(ps_close);
    if (OB_FAIL(ret)) {
    } else if (is_cursor_close()) {
      if (OB_FAIL(session->close_cursor(stmt_id_))) {
        LOG_WARN("fail to close cursor", K(ret), K_(stmt_id), K(session->get_sessid()));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_NOT_NULL(session->get_cursor(stmt_id_))) {
        if (OB_FAIL(session->close_cursor(stmt_id_))) {
          tmp_ret = ret;
          LOG_WARN("fail to close cursor", K(ret), K_(stmt_id), K(session->get_sessid()));
        }
      }
      if (OB_FAIL(session->close_ps_stmt(stmt_id_))) {
        // overwrite ret, 优先级低，被覆盖
        LOG_WARN("fail to close ps stmt", K(ret), K_(stmt_id), K(session->get_sessid()));
      }
      if (OB_SUCCESS != tmp_ret) {
        // close_cursor 失败时错误码的优先级比 close_ps_stmt 高，此处进行覆盖
        ret = tmp_ret;
      }
    }
    if (OB_SUCC(ret)) {
      if (pkt.exist_trace_info()
          && OB_FAIL(session->update_sys_variable(share::SYS_VAR_OB_TRACE_INFO,
                                                  pkt.get_trace_info()))) {
        LOG_WARN("fail to update trace info", K(ret));
      }
    }
  }
  if (lib::is_diagnose_info_enabled()) {
    int64_t exec_end = ObTimeUtility::current_time();
    const int64_t time_cost = exec_end - get_receive_timestamp();
    EVENT_INC(SQL_PS_CLOSE_COUNT);
    EVENT_ADD(SQL_PS_CLOSE_TIME, time_cost);
  }
  if (NULL != session) {
    revert_session(session);
  }
  return ret;
}

} //end of namespace sql
} //end of namespace oceanbase
