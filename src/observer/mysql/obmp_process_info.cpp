/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
