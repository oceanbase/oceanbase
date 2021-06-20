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

#ifndef _OCEABASE_RPC_FRAME_OB_NET_EASY_H_
#define _OCEABASE_RPC_FRAME_OB_NET_EASY_H_

#include "io/easy_io.h"
#include "rpc/frame/ob_req_handler.h"
#include "rpc/frame/ob_req_transport.h"

namespace oceanbase {
namespace common {
extern void easy_log_format_adaptor(
    int level, const char* file, int line, const char* function, uint64_t location_hash_val, const char* fmt, ...);
}
namespace rpc {
namespace frame {

struct ObNetOptions {
  int rpc_io_cnt_;            // rpc io thread count
  int high_prio_rpc_io_cnt_;  // high priority io thread count
  int mysql_io_cnt_;          // mysql io thread count
  int batch_rpc_io_cnt_;      // batch rpc io thread count
  bool use_ipv6_;             // support ipv6 protocol
  int64_t tcp_user_timeout_;
  int64_t tcp_keepidle_;
  int64_t tcp_keepintvl_;
  int64_t tcp_keepcnt_;
  int enable_tcp_keepalive_;
  ObNetOptions()
      : rpc_io_cnt_(0),
        high_prio_rpc_io_cnt_(0),
        mysql_io_cnt_(0),
        batch_rpc_io_cnt_(0),
        use_ipv6_(false),
        tcp_user_timeout_(0),
        tcp_keepidle_(0),
        tcp_keepintvl_(0),
        tcp_keepcnt_(0),
        enable_tcp_keepalive_(0)
  {}
};

class ObNetEasy {
  static const int64_t MAX_LISTEN_CNT = 4;

public:
  ObNetEasy();
  virtual ~ObNetEasy();

  int init(const ObNetOptions& opts);
  int start();
  int mysql_shutdown();
  int rpc_shutdown();
  int high_prio_rpc_shutdown();
  int batch_rpc_shutdown();
  int update_rpc_tcp_keepalive_params(int64_t user_timeout);
  int update_sql_tcp_keepalive_params(
      int64_t user_timeout, int enable_tcp_keepalive, int64_t tcp_keepidle, int64_t tcp_keepintvl, int64_t tcp_keepcnt);
  int stop();
  void destroy();
  void wait();

  int add_batch_rpc_handler(ObReqHandler& handler, ObReqTransport*& transport, const int io_cnt);
  int add_batch_rpc_listen(
      const uint32_t batch_rpc_port, const int io_cnt, ObReqHandler& handler, ObReqTransport*& transpsort);
  int add_rpc_handler(ObReqHandler& handler, ObReqTransport*& transport);
  int add_rpc_listen(const uint32_t port, ObReqHandler& handler, ObReqTransport*& transport);
  int add_high_prio_rpc_listen(const uint32_t port, ObReqHandler& handler, ObReqTransport*& transport);
  int add_mysql_listen(const uint32_t port, ObReqHandler& handler, ObReqTransport*& transport);
  int add_mysql_unix_listen(const char* path, ObReqHandler& handler);
  int set_easy_keepalive(int easy_keepalive_enabled);
  int load_ssl_config(const bool use_bkmi, const bool use_sm, const common::ObString& cert,
      const common::ObString& public_cert, const common::ObString& private_key);

  void on_ioth_start();

private:
  DISALLOW_COPY_AND_ASSIGN(ObNetEasy);
  easy_io_t* create_eio_(const int io_cnt);
  int init_rpc_eio_(easy_io_t* eio, const ObNetOptions& opts);
  int init_mysql_eio_(easy_io_t* eio, const ObNetOptions& opts);
  void init_eio_(easy_io_t* eio, const ObNetOptions& opts);
  int add_unix_listen_(const char* path, easy_io_t* eio, ObReqHandler& handler);
  int add_listen_(const uint32_t port, easy_io_t* eio, ObReqHandler& handler, ObReqTransport*& transport);
  void update_eio_rpc_tcp_keepalive(easy_io_t* eio, int64_t user_timeout);
  void update_eio_sql_tcp_keepalive(easy_io_t* eio, int64_t user_timeout, int enable_tcp_keepalive,
      int64_t tcp_keepidle, int64_t tcp_keepintvl, int64_t tcp_keepcnt);

protected:
  ObReqTransport* transports_[MAX_LISTEN_CNT];
  int64_t proto_cnt_;
  easy_io_t* rpc_eio_;
  easy_io_t* high_prio_rpc_eio_;
  easy_io_t* mysql_eio_;
  easy_io_t* mysql_unix_eio_;
  easy_io_t* batch_rpc_eio_;
  bool is_inited_;
  bool started_;
};  // end of class ObNetEasy

}  // end of namespace frame
}  // end of namespace rpc
}  // end of namespace oceanbase

#endif /* _OCEABASE_RPC_FRAME_OB_NET_EASY_H_ */
