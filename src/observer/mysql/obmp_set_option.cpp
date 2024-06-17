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

#include "observer/mysql/obmp_set_option.h"
#include "observer/mysql/obmp_utils.h"
#include "observer/mysql/obmp_base.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "rpc/obmysql/obsm_struct.h"
#include "sql/ob_sql_utils.h"
#include "rpc/obmysql/packet/ompk_eof.h"
#include "rpc/obmysql/ob_mysql_util.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::rpc;
namespace oceanbase
{
namespace observer
{
int ObMPSetOption::deserialize()
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
    char *buf = const_cast<char *>(pkt.get_cdata());
    ObMySQLUtil::get_uint2(buf, set_opt_);
  }
  return ret;
}

int ObMPSetOption::process()
{
  LOG_TRACE("set option", K_(set_opt));
  int ret = common::OB_SUCCESS;
  bool need_disconnect = true;
  ObSQLSessionInfo *session = NULL;
  bool need_response_error = true;
  ObSMConnection *conn = NULL;
  const ObMySQLRawPacket &mysql_pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());

  if (OB_FAIL(packet_sender_.alloc_ezbuf())) {
    LOG_WARN("failed to alloc easy buf", K(ret));
  } else if (OB_FAIL(packet_sender_.update_last_pkt_pos())) {
    LOG_WARN("failed to update last packet pos", K(ret));
  } else if (OB_FAIL(get_session(session))) {
    LOG_WARN("get session  fail", K(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pointer");
  } else if (OB_ISNULL(conn = get_conn())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get connection fail", K(conn), K(ret));
  } else {
    session->set_txn_free_route(mysql_pkt.txn_free_route());
    if (OB_FAIL(process_extra_info(*session, mysql_pkt, need_response_error))) {
      LOG_WARN("fail get process extra info", K(ret));
    } else {
      session->post_sync_session_info();
    }
  }

  if (OB_SUCC(ret)) {
    bool is_changed = false;
    obmysql::ObMySQLCapabilityFlags flag = session->get_capability();
    if (1 == flag.cap_flags_.OB_CLIENT_MULTI_STATEMENTS
        && set_opt_ == MysqlSetOptEnum::MYSQL_OPTION_MULTI_STATEMENTS_OFF) {
      flag.cap_flags_.OB_CLIENT_MULTI_STATEMENTS = 0;
      is_changed = true;
    } else if (0 == flag.cap_flags_.OB_CLIENT_MULTI_STATEMENTS
        && set_opt_ == MysqlSetOptEnum::MYSQL_OPTION_MULTI_STATEMENTS_ON) {
      flag.cap_flags_.OB_CLIENT_MULTI_STATEMENTS = 1;
      is_changed = true;
    } else {
      // do nothing
    }

    if (!is_changed) {
      // do nothing
    } else {
      session->set_capability(flag);
    }
  }


  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_LIKELY(NULL != session)) {
    ObOKPParam ok_param; // use default values
    if (OB_FAIL(ret)) {
        // do nothing
    } else if (OB_FAIL(send_ok_packet(*session, ok_param))) {
      LOG_WARN("fail to send ok pakcet in statistic response", K(ok_param), K(ret));
    } else if (OB_FAIL(revert_session(session))) {
      LOG_ERROR("failed to revert session", K(ret));
    } else {
      // do nothing
    }
  }

  if (OB_FAIL(ret)) {
    if (need_disconnect && is_conn_valid()) {
      force_disconnect();
      LOG_WARN("disconnect connection when process query", K(ret));
    } else  if (OB_FAIL(send_error_packet(ret, NULL))) { // 覆盖ret, 无需继续抛出
      LOG_WARN("failed to send error packet", K(ret));
    }
  }

  return ret;
}


} // namespace observer
} // namespace oceanbase
