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

#ifndef OCEANBASE_OBMYSQL_OB_SQL_SOCK_SESSION_H_
#define OCEANBASE_OBMYSQL_OB_SQL_SOCK_SESSION_H_
#include "rpc/obmysql/ob_i_cs_mem_pool.h"
#include "rpc/obmysql/obsm_struct.h"
#include "rpc/obrpc/ob_rpc_mem_pool.h"
#include "rpc/obmysql/ob_i_sm_conn_callback.h"
#include "rpc/ob_request.h"
#include "rpc/obmysql/ob_sql_nio.h"

namespace oceanbase
{
namespace obmysql
{

class ObSqlSessionMemPool: public ObICSMemPool
{
public:
  ObSqlSessionMemPool(): pool_() {}
  virtual ~ObSqlSessionMemPool() {}
  void* alloc(int64_t sz) { return pool_.alloc(sz); }
  void set_tenant_id(int64_t tenant_id) { pool_.set_tenant_id(tenant_id); }
  void reset() { pool_.destroy(); }
  void reuse() { pool_.reuse(); }
private:
  obrpc::ObRpcMemPool pool_;
};

class ObSqlSockSession
{
public:
  ObSqlSockSession(ObISMConnectionCallback& conn_cb, ObSqlNio* nio);
  ~ObSqlSockSession();
  void* alloc(int64_t sz) { return pool_.alloc(sz); }
  int init();
  void destroy();
  void destroy_sock();
  bool has_error();
  int create_read_handle(void*& read_handle);
  int release_read_handle(void* read_handle);
  int peek_data(void* read_handle, int64_t limit, const char*& buf, int64_t& sz);
  int consume_data(void* read_handle, int64_t sz);
  int write_data(const char* buf, int64_t sz);
  int async_write_data(const char* buf, int64_t sz);
  void on_flushed();
  void revert_sock();
  void set_shutdown();
  void shutdown();
  void set_last_pkt_sz(int64_t sz) { last_pkt_sz_ = sz; }
  void set_last_decode_succ_and_deliver_time(int64_t time);
  int on_disconnect();
  void clear_sql_session_info();
  void set_sql_session_info(void* sess);
  int set_ssl_enabled();
  SSL* get_ssl_st();
  bool is_inited() const { return is_inited_; }
  int write_hanshake_packet(const char *buf, int64_t sz);
  void set_tls_version_option(uint64_t tls_option);
  ObSqlNio* nio_;
  ObISMConnectionCallback& sm_conn_cb_;
  rpc::ObRequest sql_req_;
  ObSqlSessionMemPool pool_;
  observer::ObSMConnection conn_;
  int64_t last_pkt_sz_; // to be consumed
  const char* pending_write_buf_;
  int64_t pending_write_sz_;
  common::ObAddr client_addr_;
  uint32_t sql_session_id_; //debug only
private:
  bool is_inited_;
};

}; // end namespace obmysql
}; // end namespace oceanbase

#endif /* OCEANBASE_OBMYSQL_OB_SQL_SOCK_SESSION_H_ */
