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

#include "observer/mysql/obmp_ping.h"
#include "rpc/obmysql/ob_mysql_packet.h"

namespace oceanbase {
using namespace common;
using namespace obmysql;

namespace observer {
ObMPPing::ObMPPing(const ObGlobalContext& gctx) : ObMPBase(gctx), sql_()
{}

ObMPPing::~ObMPPing()
{}

int ObMPPing::deserialize()
{
  return OB_SUCCESS;
}

int ObMPPing::process()
{
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo* session = NULL;
  if (OB_FAIL(get_session(session))) {
    LOG_WARN("get session fail", K(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql session info is null", K(ret));
  } else if (OB_FAIL(update_transmission_checksum_flag(*session))) {
    LOG_WARN("update transmisson checksum flag failed", K(ret));
  } else {
    ObOKPParam ok_param;  // use default values
    if (OB_FAIL(send_ok_packet(*session, ok_param))) {
      LOG_WARN("fail to send ok pakcet in ping response", K(ok_param), K(ret));
    }
  }
  if (OB_LIKELY(NULL != session)) {
    revert_session(session);
  }
  if (OB_FAIL(ret)) {
    disconnect();
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
