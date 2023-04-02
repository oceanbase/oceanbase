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

#ifndef OCEANBASE_RPC_OB_SQL_REQUEST_OPERATOR_H_
#define OCEANBASE_RPC_OB_SQL_REQUEST_OPERATOR_H_

#include <stdint.h>
#include "lib/net/ob_addr.h"
#include "rpc/obrpc/ob_rpc_opts.h"

namespace oceanbase
{
namespace rpc
{
class ObRequest;
struct ObSqlSockDesc
{
  ObSqlSockDesc(): type_(0), sock_desc_(NULL) {}
  ~ObSqlSockDesc() {}
  void reset() {
    type_ = 0;
    sock_desc_ = NULL;
  }
  void set(int type, void* desc) {
    type_ = type;
    sock_desc_ = desc;
  }

  void clear_sql_session_info();
  int type_;
  void* sock_desc_;
};

class ObISqlRequestOperator
{
public:
  ObISqlRequestOperator() {}
  ~ObISqlRequestOperator() {}
  virtual void *get_sql_session(ObRequest* req) = 0;
  virtual SSL *get_sql_ssl_st(ObRequest* req) = 0;
  virtual char* alloc_sql_response_buffer(ObRequest* req, int64_t size) = 0;
  virtual char *sql_reusable_alloc(ObRequest* req, const int64_t size) = 0;
  virtual common::ObAddr get_peer(const ObRequest* req) = 0;
  virtual void disconnect_sql_conn(ObRequest* req) = 0;
  virtual void finish_sql_request(ObRequest* req) = 0;
  virtual int write_response(ObRequest* req, const char* buf, int64_t sz) = 0;
  virtual int async_write_response(ObRequest* req, const char* buf, int64_t sz) = 0;
  virtual void get_sock_desc(ObRequest* req, ObSqlSockDesc& desc) = 0;
  virtual void disconnect_by_sql_sock_desc(ObSqlSockDesc& desc) = 0;
  virtual void destroy(ObRequest* req) = 0;
  virtual void set_sql_session_to_sock_desc(ObRequest* req, void* sess) = 0;
};

class ObSqlRequestOperator
{
public:
  ObSqlRequestOperator() {}
  ~ObSqlRequestOperator() {}
  void *get_sql_session(ObRequest* req);
  SSL *get_sql_ssl_st(ObRequest* req) {
    return get_operator(req).get_sql_ssl_st(req);
  }
  char* alloc_sql_response_buffer(ObRequest* req, int64_t size) {
    return get_operator(req).alloc_sql_response_buffer(req, size);
  }
  char *sql_reusable_alloc(ObRequest* req, const int64_t size) {
    return get_operator(req).sql_reusable_alloc(req, size);
  }
  common::ObAddr get_peer(const ObRequest* req) {
    return get_operator(req).get_peer(req);
  }
  void disconnect_sql_conn(ObRequest* req) {
    return get_operator(req).disconnect_sql_conn(req);
  }
  void finish_sql_request(ObRequest* req) {
    return get_operator(req).finish_sql_request(req);
  }
  int write_response(ObRequest* req, const char* buf, int64_t sz) {
    return get_operator(req).write_response(req, buf, sz);
  }
  int async_write_response(ObRequest* req, const char* buf, int64_t sz) {
    return get_operator(req).async_write_response(req, buf, sz);
  }
  void get_sock_desc(ObRequest* req, ObSqlSockDesc& desc) {
    return get_operator(req).get_sock_desc(req, desc);
  }
  void disconnect_by_sql_sock_desc(ObSqlSockDesc& desc) {
    return get_operator(desc).disconnect_by_sql_sock_desc(desc);
  }
  void destroy(ObRequest* req) {
    return get_operator(req).destroy(req);
  }
  void set_sql_session_to_sock_desc(ObRequest* req, void* sess) {
    return get_operator(req).set_sql_session_to_sock_desc(req, sess);
  }
private:
  ObISqlRequestOperator& get_operator(const ObRequest* req);
  ObISqlRequestOperator& get_operator(const ObSqlSockDesc& desc);
};
extern ObSqlRequestOperator global_sql_req_operator;
#define SQL_REQ_OP (oceanbase::rpc::global_sql_req_operator)
} // end of namespace rp
} // end of namespace oceanbase

#endif /* OCEANBASE_RPC_OB_SQL_REQUEST_OPERATOR_H_ */

