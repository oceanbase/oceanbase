/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include "observer/ob_srv_rpc_handler.h"
#include "observer/table/ob_table_connection_mgr.h"

namespace oceanbase
{
namespace observer
{
ObSrvRpcHandler::ObSrvRpcHandler(rpc::frame::ObReqDeliver &deliver)
  : ObRpcHandler(deliver)
{
  EZ_ADD_CB(on_close);
}

int ObSrvRpcHandler::on_close(easy_connection_t *c)
{
  int eret = EASY_OK;
  table::ObTableConnectionMgr::get_instance().on_conn_close(c);
  return eret;
}

} // namespace observer
} // namespace oceanbase