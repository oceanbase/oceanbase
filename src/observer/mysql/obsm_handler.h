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

#ifndef OCEANBASE_OBSERVER_MYSQL_OBSM_HANDLER_H_
#define OCEANBASE_OBSERVER_MYSQL_OBSM_HANDLER_H_

#include "lib/lock/ob_mutex.h"
#include "rpc/obmysql/ob_mysql_handler.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace obmysql
{
class ObMysqlPktContext;
class ObCompressedPktContext;
class ObProto20PktContext;
}

namespace observer
{

class ObSMHandler : public obmysql::ObMySQLHandler
{
public:
  ObSMHandler(rpc::frame::ObReqDeliver &deliver, ObGlobalContext &gctx);
  virtual ~ObSMHandler();
  virtual int on_connect(easy_connection_t *c);
  virtual int on_disconnect(easy_connection_t *c);
  int on_close(easy_connection_t *c);

  virtual bool is_in_connected_phase(easy_connection_t *c) const;
  virtual bool is_in_ssl_connect_phase(easy_connection_t *c) const;
  virtual bool is_in_authed_phase(easy_connection_t *c) const;
  virtual rpc::ConnectionPhaseEnum get_connection_phase(easy_connection_t *c) const;
  virtual void set_ssl_connect_phase(easy_connection_t *c);
  virtual void set_connect_phase(easy_connection_t *c);
  virtual bool is_compressed(easy_connection_t *c) const;
  virtual uint32_t get_sessid(easy_connection_t *c) const;
  virtual common::ObCSProtocolType get_cs_protocol_type(easy_connection_t *c) const;

protected:
  virtual obmysql::ObMysqlPktContext *get_mysql_pkt_context(easy_connection_t *c);
  virtual obmysql::ObCompressedPktContext *get_compressed_pkt_context(easy_connection_t *c);
  virtual obmysql::ObProto20PktContext *get_proto20_pkt_context(easy_connection_t *c);

  int create_scramble_string(char *scramble_buf, const int64_t buf_len,
                             common::ObMysqlRandom &thread_rand);
private:
   ObGlobalContext &gctx_;
   DISALLOW_COPY_AND_ASSIGN(ObSMHandler);
}; // end of class ObSMHandler

} // end of namespace observer
} // end of namespace oceanbase

#endif // OCEANBASE_OBSERVER_MYSQL_OBSM_HANDLER_H_
