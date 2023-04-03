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

#define USING_LOG_PREFIX SERVER
#include "rpc/frame/ob_net_easy.h"
#include "observer/mysql/obsm_handler.h"


using namespace oceanbase;
using namespace oceanbase::observer;


class MyReqHandler : public rpc::frame::ObReqHandler
{
public:
  MyReqHandler() {
    EZ_ADD_CB(decode);
    EZ_ADD_CB(encode);
    EZ_ADD_CB(process);
    EZ_ADD_CB(on_connect);
    EZ_ADD_CB(on_disconnect);
    EZ_ADD_CB(cleanup);
  }
  void *decode(easy_message_t *m) override;
  int process(easy_request_t *r) override;
};

class MyReq
{
public:
  int len_ = 0;
  char *data_ = nullptr;
  easy_request_t *ez_r_ = nullptr;
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
  int ret = OB_SUCCESS;
  easy_buf_t *b;
  b = easy_buf_create(r->ms->pool, 1024);

  std::string resp, output;
  gen_http_resp(resp, output);
  LOG_INFO("process", K(resp.size()));
  b->last = easy_memcpy(b->last, resp.c_str(), resp.size());
  easy_request_addbuf(r, b);
  return ret;
}

void thread_start(void *args)
{
  UNUSED(args);
  lib::set_thread_name("MysqlIO");
}

class EasyServer
{
public:
  EasyServer() : eio_(nullptr){}

  int init(int port = 8080, int io_cnt = 1) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(eio_ = easy_eio_create(eio_, io_cnt))) {
      ret = OB_LIBEASY_ERROR;
    } else if (OB_ISNULL(easy_connection_add_listen(eio_, NULL, port, req_handler_.ez_handler()))) {
      ret = OB_SERVER_LISTEN_ERROR;
      LOG_ERROR("easy_connection_add_listen error", K(port), KERRMSG, K(ret));
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

private:
  easy_io_t *eio_;
  MyReqHandler req_handler_;
};

int main(int argc, char **argv)
{
  int c = 0;
  int time_sec = 3600;
  char *log_level = (char*)"DEBUG";
  int port = 8080;
  int worker = 1;
  while(EOF != (c = getopt(argc,argv,"p:c:t:l:"))) {
    switch(c) {
    case 'p':
      port = atoi(optarg);
      break;
    case 'c':
      worker = atoi(optarg);
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

  system("rm -rf easy_server_basic.log*");
  OB_LOGGER.set_file_name("easy_server_basic.log", true);
  OB_LOGGER.set_log_level(log_level);
  OB_LOGGER.set_enable_async_log(true);

  int ret = OB_SUCCESS;
  EasyServer server;
  if (OB_FAIL(server.init(port, worker))) {
    LOG_ERROR("server init fail", K(ret));
    return -1;
  }

  ::sleep(time_sec);
  return 0;
}
