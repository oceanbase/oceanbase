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

#include "observer/mysql/obmp_refresh.h"
#include "observer/mysql/obmp_utils.h"
#include "rpc/obmysql/ob_mysql_packet.h"


namespace oceanbase
{
using namespace common;
using namespace obmysql;

namespace observer
{
ObMPRefresh::ObMPRefresh(const ObGlobalContext &gctx)
    : ObMPBase(gctx)
{
}

ObMPRefresh::~ObMPRefresh()
{
}

int ObMPRefresh::deserialize()
{
  int ret = 0;
  return OB_SUCCESS;
}

int ObMPRefresh::process()
{
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo *session = NULL;
  bool need_response_error = true; //temporary placeholder
  const ObMySQLRawPacket &pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
  if (OB_FAIL(get_session(session))) {
    LOG_WARN("get session fail", K(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql session info is null", K(ret));
  } else if (FALSE_IT(session->set_txn_free_route(pkt.txn_free_route()))) {
  } else if (OB_FAIL(process_extra_info(*session, pkt, need_response_error))) {
    LOG_WARN("fail get process extra info", K(ret));
  } else if (FALSE_IT(session->post_sync_session_info())) {
  } else if (OB_FAIL(update_transmission_checksum_flag(*session))) {
    LOG_WARN("update transmisson checksum flag failed", K(ret));
  } else if (FALSE_IT(session->update_last_active_time())) {
  } else {
    ObOKPParam ok_param; // use default values
    if (OB_FAIL(send_ok_packet(*session, ok_param))) {
      LOG_WARN("fail to send ok pakcet in refresh response", K(ok_param), K(ret));
    }
  }
  if (OB_LIKELY(NULL != session)) {
    revert_session(session);
  }
  if (OB_FAIL(ret)) {
    if (OB_FAIL(send_error_packet(ret, NULL))) { // overwrite ret ?
      OB_LOG(WARN,"response refresh packet fail", K(ret));
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
