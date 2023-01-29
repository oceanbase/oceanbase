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

#ifndef _OB_MYSQL_HANDLER_H_
#define _OB_MYSQL_HANDLER_H_

#include "lib/ob_define.h"
#include "rpc/frame/ob_req_handler.h"
#include "rpc/frame/ob_req_deliver.h"
#include "rpc/obmysql/ob_mysql_protocol_processor.h"
#include "rpc/obmysql/ob_mysql_compress_protocol_processor.h"
#include "rpc/obmysql/ob_2_0_protocol_processor.h"

namespace oceanbase
{
namespace rpc
{
class ObPacket;
}
namespace obmysql
{
class OMPKHandshake;
class ObCompressedPktContext;
class ObMysqlPktContext;
class ObProto20PktContext;
class ObVirtualCSProtocolProcessor;

class ObMySQLHandler
    : public rpc::frame::ObReqHandler
{
public:
  explicit ObMySQLHandler(rpc::frame::ObReqDeliver &deliver);
  virtual ~ObMySQLHandler();

  void *decode(easy_message_t *m);
  int encode(easy_request_t *r, void *packet);
  int process(easy_request_t *r);
  int on_connect(easy_connection_t *c);
  int on_disconnect(easy_connection_t *c);
  int cleanup(easy_request_t *r, void *apacket);

  char *easy_alloc(easy_pool_t *pool, int64_t size) const;
  int send_handshake(int fd, const OMPKHandshake &hsp) const;

  virtual bool is_in_connected_phase(easy_connection_t *c) const = 0;
  virtual bool is_in_ssl_connect_phase(easy_connection_t *c) const = 0;
  virtual bool is_in_authed_phase(easy_connection_t *c) const = 0;
  virtual bool is_compressed(easy_connection_t *c) const = 0;
  virtual void set_ssl_connect_phase(easy_connection_t *c) = 0;
  virtual void set_connect_phase(easy_connection_t *c) = 0;
  virtual rpc::ConnectionPhaseEnum get_connection_phase(easy_connection_t *c) const = 0;
  virtual uint32_t get_sessid(easy_connection_t *c) const = 0;
  virtual ObMysqlPktContext *get_mysql_pkt_context(easy_connection_t *c) = 0;
  virtual ObCompressedPktContext *get_compressed_pkt_context(easy_connection_t *c) = 0;
  virtual ObProto20PktContext *get_proto20_pkt_context(easy_connection_t *c) = 0;

  virtual common::ObCSProtocolType get_cs_protocol_type(easy_connection_t *c) const = 0;

  inline ObVirtualCSProtocolProcessor *get_protocol_processor(easy_connection_t *c);

private:
  DISALLOW_COPY_AND_ASSIGN(ObMySQLHandler);
  /**
   * write data through raw socket
   * just used to send handshake && ok/error to client
   * @param fd       socket handler
   * @param buffer   data to send
   * @param length   length of data
   */
  int write_data(int fd, char *buffer, size_t length) const;
  int read_data(int fd, char *buffer, size_t length) const;

  inline bool check_kv_char(const char ch) {
    return ((ch >= '0' && ch <= '9') ||
            (ch >= 'a' && ch <= 'z') ||
            (ch >= 'A' && ch <= 'Z') ||
            '_' == ch);
  }
  int parse_head_comment(const char *raw_sql, int64_t raw_sql_len);
  int save_head_comment_kv(const char *key, int64_t key_len, const char *value, int64_t value_len);

  int64_t get_sql_req_level_from_kv();

protected:
  ObMysqlProtocolProcessor mysql_processor_;
  ObMysqlCompressProtocolProcessor compress_processor_;
  Ob20ProtocolProcessor ob_2_0_processor_;

private:
  rpc::frame::ObReqDeliver &deliver_;
  common::ObSEArray<ObStringKV, 8> head_comment_array_;
private:
  // head comment
  static const char * comment_header_;
  static int64_t comment_header_len_;
  static const char * comment_tail_;
  static const char comment_kv_sep_ = ';';
  static const char comment_kv_equal_ = '=';

  // sql request level key string
  static const char * sql_req_level_key_;

}; // end of class ObMySQLHandler

} // end of namespace obmysql
} // end of namespace oceanbase

#endif /* _OB_MYSQL_HANDLER_H_ */
