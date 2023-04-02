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

#ifndef OCEANBASE_OBRPC_OB_RPC_KEEPALIVE_H_
#define OCEANBASE_OBRPC_OB_RPC_KEEPALIVE_H_

#include "lib/thread/thread_pool.h"
#include "lib/net/ob_addr.h"
#include "util/easy_inet.h"

namespace oceanbase
{
namespace obrpc
{
struct ObNetKeepAliveData
{
public:
  ObNetKeepAliveData()
    : rs_server_status_(0) {}
  int encode(char *buf, const int64_t buf_len, int64_t &pos) const;
  int decode(const char *buf, const int64_t data_len, int64_t &pos);
  int32_t get_encoded_size() const;
  int32_t rs_server_status_;
  int64_t start_service_time_;
};

class ObNetKeepAlive : public lib::ThreadPool
{
public:
  struct client
  {
    int fd_;
    int status_;
    int is_negotiated_;
  };
  #define MAX_PIN_KEEP_CNT        10
  struct rpc_server
  {
    easy_addr_t svr_addr_;
    int64_t last_write_ts_;
    int64_t last_read_ts_;
    struct client *c_;
    char client_buf_[sizeof(client)];
    int64_t rpins_[MAX_PIN_KEEP_CNT];
    int64_t n_rpin_;
    int64_t wpins_[MAX_PIN_KEEP_CNT];
    int64_t n_wpin_;
    int in_black_;
    int64_t in_black_ts_;
    ObNetKeepAliveData ka_data_;
  };

public:
  ObNetKeepAlive();
  ~ObNetKeepAlive();
  static ObNetKeepAlive &get_instance();
  int start() override;
  void run1();
  void destroy();
  int set_pipefd_listen(int pipefd);
  int in_black(const easy_addr_t &addr, bool &in_blacklist, ObNetKeepAliveData *ka_data);
  int in_black(const common::ObAddr &addr, bool &in_blacklist, ObNetKeepAliveData *ka_data);
  virtual bool in_black(const easy_addr_t &addr);
private:
  void do_server_loop();
  void do_client_loop();
  void mark_white_black();
  rpc_server *regist_rs_if_need(const easy_addr_t &addr);
private:
  int pipefd_;
  static const int MAX_RS_COUNT = 1543;
  struct rpc_server *rss_[MAX_RS_COUNT];
};

extern void keepalive_init_data(ObNetKeepAliveData &ka_data);
extern void keepalive_make_data(ObNetKeepAliveData &ka_data);
}; // end namespace obrpc
}; // end namespace oceanbase

#endif /* OCEANBASE_OBRPC_OB_RPC_KEEPALIVE_H_ */
