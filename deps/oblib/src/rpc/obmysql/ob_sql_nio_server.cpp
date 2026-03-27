/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RPC_OBMYSQL
#include "rpc/obmysql/ob_sql_nio_server.h"

namespace oceanbase
{
using namespace common;
namespace obmysql
{

int ObSqlNioServer::start(int port, rpc::frame::ObReqDeliver* deliver, int n_thread, bool enable_numa_aware)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(io_handler_.init(deliver))) {
    LOG_WARN("handler init fail", K(ret));
  } else if(FALSE_IT(nio_.set_numa_info(OB_INVALID_TENANT_ID, enable_numa_aware, INT32_MAX))) {
  } else if (OB_FAIL(nio_.start(port, &io_handler_, n_thread, tenant_id_))) {
    LOG_WARN("sql nio start fail", K(ret));
  }
  return ret;
}

int ObSqlNioServer::set_thread_count(const int thread_num)
{
  int ret = OB_SUCCESS;
  if(thread_num != get_nio()->get_thread_count()){
    ret = nio_.set_thread_count(thread_num);
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

void ObSqlNioServer::update_tcp_keepalive_params(int keepalive_enabled, uint32_t tcp_keepidle, uint32_t tcp_keepintvl, uint32_t tcp_keepcnt)
{
  nio_.update_tcp_keepalive_params(keepalive_enabled, tcp_keepidle, tcp_keepintvl, tcp_keepcnt);
}

ObSqlSockProcessor& ObSqlNioServer::get_sql_sock_processor()
{
  return thread_processor_;
}

ObSqlNioServer* global_sql_nio_server = NULL;
}; // end namespace obmysql
}; // end namespace oceanbase
