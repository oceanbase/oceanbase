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
namespace obmysql
{
class ObICSMemPool;
class ObSqlSockProcessor;
} // namespace obmysql

namespace rpc
{
class ObRequest;
class ObPacket;

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
  virtual int create_read_handle(ObRequest* req, void *& read_handle) { return OB_NOT_SUPPORTED; }

  /**
   * read a packet from socket channel
   *
   * @param req the `req` received before
   * @param mem_pool The memory manager to hold the packet returned
   * @param read_handle Read data from socket by this handle
   * @param[out] pkt The packet received or null if no message
   * @return OB_SUCCESS success to read a packet
   *         Other failed
   *
   * read_packet only supported by nio now.
   * In NIO, the message reader will cache the message data and the packet in
   * it's memory buffer when receiving a request. The request would be hold in
   * the memory until the reuqest done.
   * We can not read new packet during the processing of request if using the
   * normal method. We need to create a new buffer to hold new packet and keep
   * the request live.
   * We should `create_read_handle` before we read new packet and
   * `release_read_handle` after things done.
   * You cannot read new packet until the last packet received is dead and you
   * can use `release_packet` to kill it.
   *
   * The whole flow likes below:
   * 1. get a request from client. (`load data local infile`)
   * 2. call `create_read_handle`
   * 3. `read_packet`
   * 4. `release_packet`
   * 5. goto 3 if we should read more packets
   * 6. call `release_read_handle`
   * 7. finish_request. request (`load data local`) will be released.
   */
  virtual int read_packet(ObRequest* req,
                          obmysql::ObICSMemPool& mem_pool,
                          void* read_handle,
                          obmysql::ObSqlSockProcessor& sock_processor,
                          ObPacket*& pkt)
  {
    return OB_NOT_SUPPORTED;
  }
  virtual int release_packet(ObRequest* req, void* read_handle, ObPacket* pkt) { return OB_NOT_SUPPORTED; }
  virtual int release_read_handle(ObRequest* req, void* read_handle) { return OB_NOT_SUPPORTED; }
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

  int create_read_handle(ObRequest* req, void *& read_handle) {
    return get_operator(req).create_read_handle(req, read_handle);
  }

  int release_read_handle(ObRequest* req, void * read_handle) {
    return get_operator(req).release_read_handle(req, read_handle);
  }
  int read_packet(ObRequest* req, obmysql::ObICSMemPool& mem_pool, void *read_handle, ObPacket*& pkt) {
    return get_operator(req).read_packet(req, mem_pool, read_handle, *sock_processor_, pkt);
  }
  int release_packet(ObRequest* req, void* read_handle, ObPacket* pkt) {
    return get_operator(req).release_packet(req, read_handle, pkt);
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

  void set_sql_sock_processor(obmysql::ObSqlSockProcessor& sock_processor) {
    sock_processor_ = &sock_processor;
  }
private:
  ObISqlRequestOperator& get_operator(const ObRequest* req);
  ObISqlRequestOperator& get_operator(const ObSqlSockDesc& desc);

private:
  obmysql::ObSqlSockProcessor *sock_processor_;
};
extern ObSqlRequestOperator global_sql_req_operator;
#define SQL_REQ_OP (oceanbase::rpc::global_sql_req_operator)
} // end of namespace rp
} // end of namespace oceanbase

#endif /* OCEANBASE_RPC_OB_SQL_REQUEST_OPERATOR_H_ */

