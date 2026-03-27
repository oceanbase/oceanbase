/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_RPC_OB_EASY_SQL_REQUEST_OPERATOR_H_
#define OCEANBASE_RPC_OB_EASY_SQL_REQUEST_OPERATOR_H_
#include "rpc/ob_sql_request_operator.h"

namespace oceanbase
{
namespace obmysql
{

class ObEasySqlRequestOperator: public rpc::ObISqlRequestOperator
{
public:
  ObEasySqlRequestOperator() {}
  virtual ~ObEasySqlRequestOperator() {}
  virtual void *get_sql_session(rpc::ObRequest* req) override;
  virtual SSL *get_sql_ssl_st(rpc::ObRequest* req) override;
  virtual char* alloc_sql_response_buffer(rpc::ObRequest* req, int64_t size) override;
  virtual char *sql_reusable_alloc(rpc::ObRequest* req, const int64_t size) override;
  virtual common::ObAddr get_peer(const rpc::ObRequest* req) override;
  virtual void disconnect_sql_conn(rpc::ObRequest* req) override;
  virtual void finish_sql_request(rpc::ObRequest* req) override;
  virtual int write_response(rpc::ObRequest* req, const char* buf, int64_t sz) override;
  virtual int async_write_response(rpc::ObRequest* req, const char* buf, int64_t sz) override;
  virtual void get_sock_desc(rpc::ObRequest* req, rpc::ObSqlSockDesc& desc) override;
  virtual void disconnect_by_sql_sock_desc(rpc::ObSqlSockDesc& desc) override;
  virtual void destroy(rpc::ObRequest* req) override;
  virtual void set_sql_session_to_sock_desc(rpc::ObRequest* req, void* sess) override;
};

}; // end namespace rpc
}; // end namespace oceanbase

#endif /* OCEANBASE_RPC_OB_EASY_SQL_REQUEST_OPERATOR_H_ */

