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

#include "observer/mysql/obmp_process_kill.h"

namespace oceanbase
{
using namespace common;
using namespace obmysql;
using namespace rpc;

namespace observer
{
ObMPProcessKill::ObMPProcessKill(const ObGlobalContext &gctx)
    : ObMPQuery(gctx)
{
}

ObMPProcessKill::~ObMPProcessKill()
{
}

int ObMPProcessKill::deserialize()
{
  int ret = OB_SUCCESS;
  const char *kill_sql_fmt = "/*+TRIGGERED BY COM_PROCESS_INFO*/ KILL %u";
  uint32_t sessid = 0;
  if ( (OB_ISNULL(req_)) || (req_->get_type() != ObRequest::OB_MYSQL)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid request", K(ret), K(req_));
  } else {
    const ObMySQLRawPacket &pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
    sessid = *(reinterpret_cast<const uint32_t *>(pkt.get_cdata()));
    snprintf(kill_sql_buf_, KILL_SQL_BUF_SIZE, kill_sql_fmt, sessid);
    assign_sql(kill_sql_buf_, STRLEN(kill_sql_buf_));
  }
  return ret;
}


} // namespace observer
} // namespace oceanbase
