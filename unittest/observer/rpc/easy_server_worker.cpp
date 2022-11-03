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

#include "lib/thread/ob_thread_name.h"
#define USING_LOG_PREFIX SERVER

#include <string>
#include <functional>

#include "easy_define.h"
#include "io/easy_io_struct.h"
#include "io/easy_connection.h"
#include "rpc/frame/ob_net_easy.h"
#include "observer/mysql/obsm_handler.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/lock/ob_scond.h"


using namespace oceanbase;
using namespace oceanbase::observer;
using namespace oceanbase::common;

class MyReq :public common::ObLink
{
public:
  int len_ = 0;
  char *data_ = nullptr;
  easy_request_t *ez_r_ = nullptr;
};

class MyReqHandler : public rpc::frame::ObReqHandler
{
public:
  MyReqHandler(std::function<int (MyReq *req)> &&do_process) {
    EZ_ADD_CB(decode);
    EZ_ADD_CB(encode);
    EZ_ADD_CB(process);
    EZ_ADD_CB(on_connect);
    EZ_ADD_CB(on_disconnect);
    EZ_ADD_CB(cleanup);
    do_process_ = std::move(do_process);
  }
  void *decode(easy_message_t *m) override;
  int process(easy_request_t *r) override;
  int encode(easy_request_t */* r */, void */* packet */) override;

  std::function<int (MyReq *req)> do_process_;
};

void *MyReqHandler::decode(easy_message_t *m)
{
  // alloc packet
  MyReq *req;
  if ((req = (MyReq *)easy_pool_calloc(m->pool,
                  sizeof(MyReq))) == NULL) {
    m->status = EASY_ERROR;
    return NULL;
  }
  int len = m->input->last - m->input->pos;
  //std::string msg(m->input->pos, len);
  LOG_INFO("decode", K(req), K(len), K(m->c->doing_request_count));
  req->data_ = m->input->pos;
  req->len_ = len;
  m->input->last = m->input->pos;
  return req;
}

void gen_http_resp(std::string &ret, std::string &output)
{
  ret.append("HTTP/1.1");
  ret.append(" 200 OK\r\n",9);
  ret.append("Content-Length: ",16);
  ret.append(std::to_string(output.size()));
  ret.append("\r\n",2);
  //ret.append("Content-Type: text/plain; charset=utf-8\r\n",41);
  ret.append("Connection: keep-alive\r\n\r\n",26);
  ret.append(output);
}

int MyReqHandler::process(easy_request_t *r)
{
  MyReq *req = (MyReq*)r->ipacket;
  req->ez_r_ = r;
  LOG_INFO("easy IO process", K(r));
  int eret = EASY_OK;
  int ret = OB_SUCCESS;
  easy_request_sleeping(r);
  if (OB_FAIL(do_process_(req))) {
    LOG_ERROR("do_process fail", K(ret));
    eret = EASY_ABORT;
  } else {
    eret = EASY_AGAIN;
  }
  return eret;
}

int MyReqHandler::encode(easy_request_t *r, void *packet)
{
  LOG_INFO("encode", K(r), K(packet), K(lbt()));
  int ret = OB_SUCCESS;
  easy_buf_t *buf = reinterpret_cast<easy_buf_t *>(packet);
  easy_request_addbuf(r, buf);
  return ret;
}

void thread_start(void *args)
{
  UNUSED(args);
  lib::set_thread_name("MysqlIO");
}

class SimpleSqlServer
{
public:
  SimpleSqlServer() : eio_(nullptr), req_handler_([this] (MyReq *req) { push(req);return 0; }), req_(0) {}
  ~SimpleSqlServer() {}

  int init(int port = 8080, int io_cnt = 1) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(eio_ = easy_eio_create(eio_, io_cnt))) {
      ret = OB_LIBEASY_ERROR;
    } else if (OB_ISNULL(easy_connection_add_listen(eio_, NULL, port, req_handler_.ez_handler()))) {
      ret = OB_SERVER_LISTEN_ERROR;
      LOG_ERROR("easy_connection_add_listen error", K(port), KERRMSG, K(ret));
    } else if (OB_FAIL(q_.init(256))) {
      LOG_ERROR("init queue fail", K(ret));
    } else {
      easy_eio_set_uthread_start(eio_, thread_start, nullptr);
      int eret = easy_eio_start(eio_);
      if (EASY_OK == eret) {
        LOG_INFO("start mysql easy io");
      } else {
        ret = OB_LIBEASY_ERROR;
        LOG_ERROR("start mysql easy io fail", K(ret));
      }
    }
    return ret;
  }
  MyReq *pop2() {
    ObLink *data = nullptr;
    int ret = OB_SUCCESS;
    while (true) {
      if (OB_FAIL(q2_.pop(data))) {
        cond_.wait(1000);
      } else {
        break;
      }
    }
    return static_cast<MyReq*>(data);
  }
  void push2(MyReq *req) {
    q2_.push(req);
    cond_.signal();
  }
  MyReq *pop() {
    void *data = nullptr;
    int ret = OB_SUCCESS;
    while (true) {
      if (OB_FAIL(q_.pop(data, 10000))) {
      } else {
        break;
      }
    }
    return static_cast<MyReq*>(data);
  }
  void push(MyReq *req) {
    q_.push(req);
  }
  int64_t &get_req() {
    return req_;
  }
private:
  easy_io_t *eio_;
  MyReqHandler req_handler_;
  ObLinkQueue q2_;
  SCond cond_;

  ObLightyQueue q_;
  int64_t req_;
};

int main(int argc, char **argv)
{
  int c = 0;
  int port = 8080;
  int io_cnt = 40;
  int worker_cnt = 40;
  int time_sec = 3600;
  char *log_level = (char*)"DEBUG";
  while(EOF != (c = getopt(argc,argv,"c:w:p:t:l:"))) {
    switch(c) {
    case 'c':
      io_cnt = atoi(optarg);
      break;
    case 'w':
      worker_cnt = atoi(optarg);
      break;
    case 'p':
      port = atoi(optarg);
      break;
    case 't':
      time_sec = atoi(optarg);
      break;
    case 'l':
     log_level = optarg;
     break;
    default:
      break;
    }
  }

  system("rm -rf simple_sql_server.log*");
  OB_LOGGER.set_file_name("simple_sql_server.log", true);
  OB_LOGGER.set_log_level(log_level);
  OB_LOGGER.set_enable_async_log(true);

  int ret = OB_SUCCESS;
  SimpleSqlServer server;
  if (OB_FAIL(server.init(port, io_cnt))) {
    LOG_ERROR("server init fail", K(ret));
    return -1;
  }

  std::vector<std::thread> ths;
  for (int i = 0; i < worker_cnt; i++) {
    std::thread th([&, i] () {
      std::string thread_name("Worker");
      thread_name.append(std::to_string(i));
      lib::set_thread_name(thread_name.c_str());
      while (true) {
        MyReq *req = server.pop();
        easy_request_t *r = req->ez_r_;
        easy_buf_t *b;
        b = easy_buf_create(r->ms->pool, 1024);
        if (b == nullptr) {
          LOG_ERROR("easy_buf_create fail");
          break;
        }

        std::string resp, output;
        resp.reserve(1024);
        gen_http_resp(resp, output);
        LOG_INFO("worker process", K(resp.size()));
        b->last = easy_memcpy(b->last, resp.c_str(), resp.size());

        r->opacket = b;
        r->opacket_size = resp.size();
        easy_request_wakeup(r);
        ATOMIC_INC(&server.get_req());
      }
    });
    ths.push_back(std::move(th));
  }

  int64_t last_req_sum = 0;
  int silent = 0;
  for (int i = 0; i < time_sec; i++) {
    int64_t curr_req_sum = ATOMIC_LOAD(&server.get_req());
    int64_t new_req = curr_req_sum - last_req_sum;
    if (new_req == 0) {
      silent++;
    } else {
      silent = 0;
    }
    if (new_req > 0 || silent < 3) {
      std::cout << curr_req_sum << " " << new_req << std::endl;
    }
    last_req_sum = curr_req_sum;
    ::sleep(1);
  }

  return 0;
}
