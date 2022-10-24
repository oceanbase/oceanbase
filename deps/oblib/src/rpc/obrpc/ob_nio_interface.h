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

#ifndef OCEANBASE_OBRPC_OB_NIO_INTERFACE_H_
#define OCEANBASE_OBRPC_OB_NIO_INTERFACE_H_
#include <stdint.h>
#include <pthread.h>
#include "lib/net/ob_addr.h"

namespace oceanbase
{
namespace obrpc
{

class IReqHandler
{
public:
  IReqHandler() {}
  virtual ~IReqHandler() {}
  virtual int handle_req(int64_t resp_id, char* buf, int64_t sz) = 0;
};

class IRespHandler
{
public:
  IRespHandler() {}
  virtual ~IRespHandler() {}
  virtual void* alloc(int64_t sz) = 0;
  virtual int handle_resp(int io_err, char* buf, int64_t sz) = 0;
};

class ObINio
{
public:
  ObINio(): accept_queue_fd_(-1), req_handler_(NULL) {}
  virtual ~ObINio() {}
  void init(IReqHandler* req_handler, int accept_queue_fd) {
    req_handler_ = req_handler;
    accept_queue_fd_ = accept_queue_fd;
  }
  int start() {
    return pthread_create(&thread_, NULL, thread_func, this);
  }
  virtual int post(const common::ObAddr& addr, const char* req, int64_t req_size,  IRespHandler* resp_handler) = 0;
  virtual int resp(int64_t resp_id, char* buf, int64_t sz) = 0;
private:
  static void* thread_func(void* arg) {
    ((ObINio*)arg)->do_work(0);
    return NULL;
  }
  virtual int do_work(int tid) = 0;
private:
  pthread_t thread_;
protected:
  int accept_queue_fd_;
  IReqHandler* req_handler_;
};

}; // end namespace obrpc
}; // end namespace oceanbase

#endif /* OCEANBASE_OBRPC_OB_NIO_INTERFACE_H_ */

