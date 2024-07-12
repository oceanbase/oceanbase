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

#include "observer/mysql/obmp_statistic.h"
#include "observer/mysql/obmp_utils.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "rpc/obmysql/packet/ompk_string.h"
#include "rpc/obmysql/obsm_struct.h"

namespace oceanbase
{
using namespace common;
using namespace obmysql;

namespace observer
{
int ObMPStatistic::process()
{
  int ret = common::OB_SUCCESS;
  bool need_disconnect = true;
  bool need_response_error = true;
  //Attention::it is BUG when using like followers (build with release):
  //  obmysql::OMPKString pkt(ObString("Active threads not support"));
  //
  const common::ObString tmp_string("Active threads not support");
  obmysql::OMPKString pkt(tmp_string);
  ObSMConnection *conn = NULL;
  const ObMySQLRawPacket &mysql_pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());

  if (OB_FAIL(packet_sender_.alloc_ezbuf())) {
    LOG_WARN("failed to alloc easy buf", K(ret));
  } else if (OB_FAIL(packet_sender_.update_last_pkt_pos())) {
    LOG_WARN("failed to update last packet pos", K(ret));
  } else if (OB_ISNULL(conn = get_conn())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get connection fail", K(conn), K(ret));
  } else if (conn->proxy_cap_flags_.is_extra_ok_packet_for_statistics_support()) {
    sql::ObSQLSessionInfo *session = NULL;
    if (OB_FAIL(get_session(session))) {
      LOG_WARN("get session fail", K(ret));
    } else if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql session info is null", K(ret));
    } else if (OB_FAIL(process_kill_client_session(*session))) {
      LOG_WARN("client session has been killed", K(ret));
    } else if (FALSE_IT(session->set_txn_free_route(mysql_pkt.txn_free_route()))) {
    } else if (OB_FAIL(process_extra_info(*session, mysql_pkt, need_response_error))) {
      LOG_WARN("fail get process extra info", K(ret));
    } else if (FALSE_IT(session->post_sync_session_info())) {
    } else if (OB_FAIL(update_transmission_checksum_flag(*session))) {
      LOG_WARN("update transmisson checksum flag failed", K(ret));
    } else {
      ObOKPParam ok_param; // use default values
      if (OB_FAIL(send_ok_packet(*session, ok_param, &pkt))) {
        LOG_WARN("fail to send ok pakcet in statistic response", K(ok_param), K(ret));
      }
    }
    if (OB_LIKELY(NULL != session)) {
      revert_session(session);
    }
  } else if (OB_FAIL(response_packet(pkt, NULL))) {
    RPC_OBMYSQL_LOG(WARN, "fail to response statistic packet", K(ret));
  } else {
    // do nothing
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


} // namespace observer
} // namespace oceanbase
