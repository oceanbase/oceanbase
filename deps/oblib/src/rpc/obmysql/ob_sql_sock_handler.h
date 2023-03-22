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

#ifndef OCEANBASE_OBMYSQL_OB_SQL_SOCK_HANDLER_H_
#define OCEANBASE_OBMYSQL_OB_SQL_SOCK_HANDLER_H_
#include "rpc/obmysql/ob_i_sm_conn_callback.h"
#include "rpc/obmysql/ob_i_sql_sock_handler.h"
#include "rpc/obmysql/ob_sql_sock_session.h"

namespace oceanbase
{
namespace rpc {
namespace frame { class ObReqDeliver;};
};
namespace obmysql
{
class ObSqlSockProcessor;
class ObSqlNio;
class ObSqlSockHandler: public ObISqlSockHandler
{
public:
  ObSqlSockHandler(ObISMConnectionCallback& conn_cb, ObSqlSockProcessor& sock_processor, ObSqlNio& nio):
      conn_cb_(conn_cb), sock_processor_(sock_processor), deliver_(nullptr), nio_(&nio) {}
  virtual ~ObSqlSockHandler() {}
  int init(rpc::frame::ObReqDeliver* deliver);
  virtual int on_readable(void* sess) override;
  virtual void on_close(void* sess, int err) override;
  virtual int on_connect(void* sess, int fd) override;
  virtual void on_flushed(void* sess) override;
private:
  ObISMConnectionCallback& conn_cb_;
  ObSqlSockProcessor& sock_processor_;
  rpc::frame::ObReqDeliver* deliver_;
  ObSqlNio* nio_;
};

}; // end namespace obmysql
}; // end namespace oceanbase

#endif /* OCEANBASE_OBMYSQL_OB_SQL_SOCK_HANDLER_H_ */

