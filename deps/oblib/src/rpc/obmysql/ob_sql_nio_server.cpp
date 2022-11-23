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

#define USING_LOG_PREFIX RPC_OBMYSQL
#include "rpc/obmysql/ob_sql_nio_server.h"

namespace oceanbase
{
using namespace common;
namespace obmysql
{

int ObSqlNioServer::start(int port, rpc::frame::ObReqDeliver* deliver, int n_thread)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(io_handler_.init(deliver))) {
    LOG_WARN("handler init fail", K(ret));
  } else if (OB_FAIL(nio_.start(port, &io_handler_, n_thread))) {
    LOG_WARN("sql nio start fail", K(ret));
  }
  return ret;
}

void ObSqlNioServer::stop()
{
  nio_.stop();
}

void ObSqlNioServer::wait()
{
  nio_.wait();
}

void ObSqlNioServer::destroy()
{
  nio_.destroy();
}

ObSqlNioServer* global_sql_nio_server = NULL;
}; // end namespace obmysql
}; // end namespace oceanbase
