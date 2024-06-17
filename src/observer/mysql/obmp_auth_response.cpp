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

#include "observer/mysql/obmp_auth_response.h"
#include "observer/mysql/obmp_utils.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "rpc/obmysql/obsm_struct.h"
#include "rpc/ob_request.h"

namespace oceanbase
{
using namespace common;
using namespace obmysql;
using namespace rpc;

namespace observer
{
int ObMPAuthResponse::process()
{
  int ret = common::OB_SUCCESS;
  bool need_disconnect = true;
  bool need_response_error = true;
  ObSMConnection *conn = NULL;
  sql::ObSQLSessionInfo *session = NULL;
  const ObMySQLRawPacket &mysql_pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());

  if (OB_FAIL(packet_sender_.alloc_ezbuf())) {
    LOG_WARN("failed to alloc easy buf", K(ret));
  } else if (OB_FAIL(packet_sender_.update_last_pkt_pos())) {
    LOG_WARN("failed to update last packet pos", K(ret));
  } else if (OB_ISNULL(conn = get_conn())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get connection fail", K(conn), K(ret));
  } else if (OB_FAIL(get_session(session))) {
    LOG_WARN("get session fail", K(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql session info is null", K(ret));
  } else if (FALSE_IT(session->set_txn_free_route(mysql_pkt.txn_free_route()))) {
  } else if (OB_FAIL(process_extra_info(*session, mysql_pkt, need_response_error))) {
    LOG_WARN("fail get process extra info", K(ret));
  } else if (FALSE_IT(session->post_sync_session_info())) {
  } else if (OB_FAIL(update_transmission_checksum_flag(*session))) {
    LOG_WARN("update transmisson checksum flag failed", K(ret));
  } else if (OB_FAIL(session->set_login_auth_data(auth_data_))) {
    LOG_WARN("failed to set login auth data", K(ret));
  } else if (OB_FAIL(load_privilege_info_for_change_user(session))) {
      OB_LOG(WARN,"load privilige info failed", K(ret),K(session->get_sessid()));
  } else {
    conn->set_auth_phase();
    ObOKPParam ok_param; // use default values
    ok_param.is_on_change_user_ = true;
    if (OB_FAIL(send_ok_packet(*session, ok_param))) {
      LOG_WARN("fail to send ok pakcet in statistic response", K(ok_param), K(ret));
    }
  }
  if (OB_LIKELY(NULL != session)) {
    revert_session(session);
  }
  if (OB_FAIL(ret) && need_response_error) {
    send_error_packet(ret, NULL);
  }
  if (OB_FAIL(ret) && need_disconnect) {
    force_disconnect();
    LOG_WARN("disconnect connection", KR(ret));
  }
  return ret;
}

int ObMPAuthResponse::deserialize()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(req_) || OB_UNLIKELY(ObRequest::OB_MYSQL != req_->get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid request", K(req_));
  } else {
    ObSQLSessionInfo *session = NULL;
    ObMySQLCapabilityFlags capability;
    if (OB_FAIL(get_session(session))) {
      LOG_WARN("get session  fail", K(ret));
    } else if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to get session info", K(ret), K(session));
    } else {
      ObSQLSessionInfo::LockGuard lock_guard(session->get_query_lock());
      session->update_last_active_time();
    }
    if (NULL != session) {
      revert_session(session);
    }

    if (OB_SUCC(ret)) {
      obmysql::ObMySQLRawPacket pkt  = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
      const char *buf = pkt.get_cdata();
      const char *pos = pkt.get_cdata();
      // not need skip command byte
      const int64_t len = pkt.get_clen();
      const char *end = buf + len;
      if (OB_LIKELY(pos < end)) {
        auth_data_.assign_ptr(pos, len);
        pos += auth_data_.length();
      }
    }
  }
  return ret;
}
} // namespace observer
} // namespace oceanbase
