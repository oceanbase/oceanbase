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

#include "observer/mysql/obmp_process_info.h"

namespace oceanbase
{
using namespace common;
using namespace obmysql;
using namespace rpc;

namespace observer
{
ObMPProcessInfo::ObMPProcessInfo(const ObGlobalContext &gctx)
    : ObMPQuery(gctx)
{
}

ObMPProcessInfo::~ObMPProcessInfo()
{
}

int ObMPProcessInfo::deserialize()
{
  int ret = OB_SUCCESS;
  const char *process_info_sql = "/*+TRIGGERED BY COM_PROCESS_INFO*/ SHOW PROCESSLIST";
  if ( (OB_ISNULL(req_)) || (req_->get_type() != ObRequest::OB_MYSQL)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid request", K(ret), K(req_));
  } else {
    assign_sql(process_info_sql, STRLEN(process_info_sql));
  }
  return ret;
}


} // namespace observer
} // namespace oceanbase
