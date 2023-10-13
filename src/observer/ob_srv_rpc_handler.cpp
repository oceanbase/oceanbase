/**
 * Copyright (c) 2023 OceanBase
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

#include "observer/ob_srv_rpc_handler.h"
#include "rpc/obrpc/ob_rpc_handler.h"
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