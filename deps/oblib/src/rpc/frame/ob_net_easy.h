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
#include "rpc/obrpc/ob_listener.h"
#include "rpc/obrpc/ob_net_keepalive.h"
#include "lib/ssl/ob_ssl_config.h"

using namespace oceanbase::obrpc;
namespace oceanbase
{
namespace common
{
extern void easy_log_format_adaptor(int level, const char *file, int line, const char *function,
                                    uint64_t location_hash_val,
                                    const char *fmt, ...);
}
namespace rpc
{
namespace frame
{

struct ObNetOptions {
  int rpc_io_cnt_; // rpc io thread count
  int high_prio_rpc_io_cnt_; // high priority io thread count
  int mysql_io_cnt_; // mysql io thread count
  int batch_rpc_io_cnt_; // batch rpc io thread count
  bool use_ipv6_; // support ipv6 protocol
  int64_t tcp_user_timeout_;
  int64_t tcp_keepidle_;
  int64_t tcp_keepintvl_;
  int64_t tcp_keepcnt_;
  int enable_tcp_keepalive_;
  ObNetOptions() : rpc_io_cnt_(0), high_prio_rpc_io_cnt_(0),
      mysql_io_cnt_(0), batch_rpc_io_cnt_(0), use_ipv6_(false), tcp_user_timeout_(0),
      tcp_keepidle_(0), tcp_keepintvl_(0), tcp_keepcnt_(0), enable_tcp_keepalive_(0) {}
  TO_STRING_KV(K(rpc_io_cnt_),
               K(high_prio_rpc_io_cnt_),
               K(mysql_io_cnt_),
               K(batch_rpc_io_cnt_),
               K(use_ipv6_),
               K(tcp_user_timeout_),
               K(tcp_keepidle_),
               K(tcp_keepintvl_),
               K(tcp_keepcnt_),
               K(enable_tcp_keepalive_));
};

typedef int (net_easy_update_s2r_map_pt)(void *args);
class ObNetEasy
{
public:
  static const uint64_t NET_KEEPALIVE_MAGIC       = 0x6683221dd298cc23;
  static const int64_t MAX_LISTEN_CNT = 4;
  static const uint64_t RPC_EIO_MAGIC             = 0x12567348667799aa;
  static const uint64_t HIGH_PRI_RPC_EIO_MAGIC    = 0x1237734866785431;
  static const uint64_t BATCH_RPC_EIO_MAGIC       = 0x5933893228167181;
public:
  ObNetEasy();
  virtual ~ObNetEasy();

  int init(const ObNetOptions &opts, uint8_t negotiation_enable = 0);
  int start();
  int mysql_shutdown();
  int rpc_shutdown();
  int high_prio_rpc_shutdown();
  int batch_rpc_shutdown();
  int unix_rpc_shutdown();
  int update_rpc_tcp_keepalive_params(int64_t user_timeout);
  int update_sql_tcp_keepalive_params(int64_t user_timeout, int enable_tcp_keepalive,
                                      int64_t tcp_keepidle, int64_t tcp_keepintvl,
                                      int64_t tcp_keepcnt);
  int stop();
  void destroy();
  void wait();

  int add_batch_rpc_handler(ObReqHandler &handler, ObReqTransport *&transport, const int io_cnt);
  int add_batch_rpc_listen(const uint32_t batch_rpc_port, const int io_cnt, ObReqHandler &handler, ObReqTransport *&transpsort);
  int add_rpc_handler(ObReqHandler &handler, ObReqTransport *&transport);
  int add_rpc_listen(const uint32_t port, ObReqHandler &handler, ObReqTransport *&transport);
  int add_high_prio_rpc_listen(const uint32_t port, ObReqHandler &handler, ObReqTransport *&transport);
  int rpc_net_register(ObReqHandler &handler, ObReqTransport *& transport);
  int batch_rpc_net_register(ObReqHandler &handler, ObReqTransport *& transport);
  int high_prio_rpc_net_register(ObReqHandler &handler, ObReqTransport *& transport);
  int net_keepalive_register();
  int add_mysql_listen(const uint32_t port, ObReqHandler &handler, ObReqTransport *&transport);
  int add_mysql_unix_listen(const char* path, ObReqHandler &handler);
  int add_rpc_unix_listen(const char* path, ObReqHandler &handler);
  int set_easy_keepalive(int easy_keepalive_enabled);
  int set_easy_region_max_bw(const char *region, int64_t max_bw);
  int get_easy_region_latest_bw(const char* region, int64_t *bw, int64_t *max_bw);
  void set_s2r_map_cb(net_easy_update_s2r_map_pt *cb, void *args);
  int set_easy_s2r_map(ObAddr &addr, char *region);
  void* get_s2r_map_cb_args();
  int notify_easy_s2r_map_changed();
  void set_ratelimit_enable(int easy_ratelimit_enabled);
  void set_easy_ratelimit_stat_period(int64_t stat_period);
  int load_ssl_config(const bool use_bkmi,
                      const bool use_sm,
                      const char *cert,
                      const char *public_cert,
                      const char *private_key,
                      const char *enc_cert,
                      const char *enc_private_key);
  int set_rpc_port(uint32_t rpc_port);

  void on_ioth_start();
  easy_io_t* get_rpc_eio() const {return rpc_eio_;}
  easy_io_t* get_batch_rpc_eio() const {return batch_rpc_eio_;}
  easy_io_t* get_high_prio_eio() const {return high_prio_rpc_eio_;}
  easy_io_t* get_mysql_eio() const {return mysql_eio_;}
private:
  DISALLOW_COPY_AND_ASSIGN(ObNetEasy);
  easy_io_t* create_eio_(const int io_cnt, uint64_t magic = 0, uint8_t negotiation_enable = 0);
  int init_rpc_eio_(easy_io_t *eio, const ObNetOptions &opts);
  int init_mysql_eio_(easy_io_t *eio, const ObNetOptions &opts);
  void init_eio_(easy_io_t *eio, const ObNetOptions &opts);
  int add_unix_listen_(const char* path, easy_io_t * eio, ObReqHandler &handler);
  int add_listen_(const uint32_t port, easy_io_t *eio, ObReqHandler &handler, ObReqTransport *&transport);
  int net_register_and_add_listen_(ObListener &listener, easy_io_t *eio, ObReqHandler &handler, ObReqTransport *&transport);
  void update_eio_rpc_tcp_keepalive(easy_io_t* eio, int64_t user_timeout);
  void update_eio_sql_tcp_keepalive(easy_io_t* eio, int64_t user_timeout,
                                    int enable_tcp_keepalive, int64_t tcp_keepidle,
                                    int64_t tcp_keepintvl, int64_t tcp_keepcnt);

public:
  net_easy_update_s2r_map_pt *net_easy_update_s2r_map_cb_;
  void *net_easy_update_s2r_map_cb_args_;

protected:
  ObReqTransport *transports_[MAX_LISTEN_CNT];
  int64_t proto_cnt_;
  easy_io_t *rpc_eio_;
  easy_io_t *high_prio_rpc_eio_;
  easy_io_t *mysql_eio_;
  easy_io_t *mysql_unix_eio_;
  easy_io_t *batch_rpc_eio_;
  easy_io_t *rpc_unix_eio_;
  bool is_inited_;
  bool started_;
private:
  ObListener rpc_listener_;
  uint32_t rpc_port_;
}; // end of class ObNetEasy

} // end of namespace frame
} // end of namespace rpc
} // end of namespace oceanbase


#endif /* _OCEABASE_RPC_FRAME_OB_NET_EASY_H_ */
