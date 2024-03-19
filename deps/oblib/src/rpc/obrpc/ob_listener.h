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

#ifndef OCEANBASE_OBRPC_OB_LISTENER_H_
#define OCEANBASE_OBRPC_OB_LISTENER_H_
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "lib/thread/threads.h"

using namespace oceanbase::lib;

namespace oceanbase
{
namespace obrpc
{
#define NEGOTIATION_PACKET_HEADER_MAGIC_EASY (0x1234567877668833)

#define MAX_PROTOCOL_TYPE_SIZE (5)
#define OB_LISTENER_MAX_THREAD_CNT         64

typedef struct io_threads_pipefd_pool_t{
    int count;
    int pipefd[OB_LISTENER_MAX_THREAD_CNT];
}io_threads_pipefd_pool_t;

typedef struct io_wrpipefd_map_t{
  uint8_t  used;
  uint64_t magic;
  io_threads_pipefd_pool_t ioth_wrpipefd_pool;
}io_wrpipefd_map_t;

class ObListener : public lib::Threads
{
public:
  ObListener();
  ~ObListener();
  void run(int64_t idx);
  int listen_create(int port);
  int ob_listener_set_tcp_opt(int fd, int option, int value);
  int ob_listener_set_opt(int fd, int option, int value);
  int listen_start();
  int regist(uint64_t magic, int count, int *pipefd_array);
  uint8_t compatible_balance_assign(io_threads_pipefd_pool_t *& pipefd_pool);
  void set_port(int port) {port_ = port;}
  int do_one_event(int accept_fd);
  void destroy();

private:
  void do_work();
  int listen_special(int family, int port);
private:
  int listen_fd_;
  int port_;
  io_wrpipefd_map_t io_wrpipefd_map_[MAX_PROTOCOL_TYPE_SIZE];
};

}; // end namespace obrpc
}; // end namespace oceanbase

#endif /* OCEANBASE_OBRPC_OB_LISTENER_H_ */
