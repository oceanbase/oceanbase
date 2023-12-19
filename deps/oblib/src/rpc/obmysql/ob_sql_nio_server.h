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

#ifndef OCEANBASE_OBMYSQL_OB_SQL_NIO_SERVER_H_
#define OCEANBASE_OBMYSQL_OB_SQL_NIO_SERVER_H_
#include "rpc/obmysql/ob_sql_nio.h"
#include "rpc/obmysql/ob_sql_sock_handler.h"
#include "rpc/obmysql/ob_sql_sock_processor.h"

namespace oceanbase
{
namespace obmysql
{
class ObSqlNioServer
{
public:
  ObSqlNioServer(ObISMConnectionCallback &conn_cb,
                 ObMySQLHandler &mysql_handler,
                 const uint64_t tenant_id = common::OB_INVALID_ID)
      : thread_processor_(mysql_handler),
        io_handler_(conn_cb, thread_processor_, nio_), tenant_id_(tenant_id) {}
  virtual ~ObSqlNioServer() {}
  ObSqlNio *get_nio() { return &nio_; }
  int start(int port, rpc::frame::ObReqDeliver* deliver, int n_thread);
  void revert_sock(void* sess);
  int peek_data(void* sess, int64_t limit, const char*& buf, int64_t& sz);
  int consume_data(void* sess, int64_t sz);
  int write_data(void* sess, const char* buf, int64_t sz);
  int set_thread_count(const int thread_num);
  void stop();
  void wait();
  void destroy();
  void update_tcp_keepalive_params(int keepalive_enabled, uint32_t tcp_keepidle, uint32_t tcp_keepintvl, uint32_t tcp_keepcnt);

  ObSqlSockProcessor& get_sql_sock_processor();

private:
  ObSqlSockProcessor thread_processor_; // for tenant worker
  ObSqlSockHandler io_handler_; // for io thread
  ObSqlNio nio_;
  uint64_t tenant_id_;
};
extern ObSqlNioServer* global_sql_nio_server;
}; // end namespace obmysql
}; // end namespace oceanbase

#endif /* OCEANBASE_OBMYSQL_OB_SQL_NIO_SERVER_H_ */

