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
#include "obmp_stmt_reset.h"
#include "rpc/ob_request.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "sql/plan_cache/ob_ps_cache.h"
#include "sql/plan_cache/ob_prepare_stmt_struct.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/mysql/obmp_utils.h"
#include "observer/mysql/obmp_stmt_send_piece_data.h"

namespace oceanbase
{
using namespace common;
using namespace rpc;
using namespace obmysql;
using namespace sql;

namespace observer
{

int ObMPStmtReset::deserialize()
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

int ObMPStmtReset::process()
{
  int ret = OB_SUCCESS;
  bool need_disconnect = true;
  bool need_response_error = true;
  sql::ObSQLSessionInfo *session = NULL;
  const ObMySQLRawPacket &pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
  if (OB_ISNULL(req_)) {
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
  } else if (OB_FAIL(process_kill_client_session(*session))) {
    LOG_WARN("client session has been killed", K(ret));
  } else if (FALSE_IT(session->set_txn_free_route(pkt.txn_free_route()))) {
  } else if (OB_FAIL(process_extra_info(*session, pkt, need_response_error))) {
    LOG_WARN("fail get process extra info", K(ret));
  } else if (FALSE_IT(session->post_sync_session_info())) {
  } else if (FALSE_IT(need_disconnect = false)) {
  } else if (OB_FAIL(update_transmission_checksum_flag(*session))) {
    LOG_WARN("update transmisson checksum flag failed", K(ret));
  } else {
    ObPieceCache *piece_cache = session->get_piece_cache();
    int64_t param_num = 0;
    THIS_WORKER.set_session(session);
    ObSQLSessionInfo::LockGuard lock_guard(session->get_query_lock());
    LOG_TRACE("close ps stmt or cursor", K_(stmt_id), K(session->get_sessid()));
    session->init_use_rich_format();

    // get stmt info
    if (OB_NOT_NULL(session->get_ps_cache())) {
      ObPsStmtInfoGuard guard;
      ObPsStmtInfo *ps_info = NULL;
      ObPsStmtId inner_stmt_id = OB_INVALID_ID;
      OZ (session->get_inner_ps_stmt_id(stmt_id_, inner_stmt_id));
      if (OB_FAIL(session->get_ps_cache()->get_stmt_info_guard(inner_stmt_id, guard))) {
        LOG_WARN("get stmt info guard failed", K(ret), K(stmt_id_), K(inner_stmt_id));
      } else if (OB_ISNULL(ps_info = guard.get_stmt_info())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get stmt info is null", K(ret));
      } else {
        param_num= ps_info->get_num_of_param();
      }
    }

    // remove piece
    if (NULL == piece_cache) {
    } else {
      for (uint64_t i = 0; OB_SUCC(ret) && i < param_num; i++) {
        if (OB_FAIL(piece_cache->remove_piece(
                            piece_cache->get_piece_key(stmt_id_, i),
                            *session))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("remove piece fail", K(stmt_id_), K(i), K(ret));
          }
        }
      }
    }

    // close cursor
    if (OB_NOT_NULL(session->get_cursor(stmt_id_))) {
      if (OB_FAIL(session->close_cursor(stmt_id_))) {
        LOG_WARN("fail to close cursor", K(ret), K_(stmt_id), K(session->get_sessid()));
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

  if (OB_SUCC(ret)) {
    ObOKPParam ok_param;
    ok_param.affected_rows_ = 0;
    ok_param.is_partition_hit_ = session->partition_hit().get_bool();
    ok_param.has_more_result_ = false;
    if (OB_FAIL(send_ok_packet(*session, ok_param))) {
      LOG_WARN("send ok packet fail.", K(ret), K(stmt_id_));
    }
  } else {
    if (need_response_error) {
      send_error_packet(ret, NULL);
    }
    if (OB_ERR_PREPARE_STMT_CHECKSUM == ret || need_disconnect) {
      force_disconnect();
      LOG_WARN("prepare stmt checksum error, disconnect connection", K(ret));
    }
  }
  flush_buffer(true);

  THIS_WORKER.set_session(NULL);
  if (NULL != session) {
    revert_session(session);
  }
  return ret;
}

} //end of namespace sql
} //end of namespace oceanbase
