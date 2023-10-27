/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

const char* _usage = "io=1 client=1 server='' ./test-nio run\n";
#include "cpp/rpc_interface.h"
#include "cpp/sync_resp_cb.h"

Nio nio_;
addr_t dest_addr;
int64_t handle_cnt;
int64_t cb_cnt;

class ReqHandler: public IReqHandler
{
public:
  ReqHandler() {}
  virtual ~ReqHandler() {}
  int handle_req(ReqHandleCtx* ctx, const char* buf, int64_t sz)  {
    const char resp[] = "hello from server";
    ctx->resp(resp, sizeof(resp));
    FAA(&handle_cnt, 1);
    return 0;
  }
private:
  RpcMemPool pool_;
} req_handler;

static char big_req[1<<21];
void* thread_func(void* arg)
{
  unused(arg);
  while(1) {
    SyncRespCallback* resp_cb = SyncRespCallback::create();
    nio_.post(dest_addr, big_req, random() % sizeof(big_req), resp_cb, 1 * 1000000);
    int64_t sz = 0;
    resp_cb->wait(sz);
    FAA(&cb_cnt, 1);
    resp_cb->free_req();
  }
  return NULL;
}

void report()
{
  int64_t last_handle_cnt = 0;
  int64_t last_cb_cnt = 0;
  format_t f;
  format_init(&f, sizeof(f.buf));
  while(1) {
    format_reset(&f);
    mod_report(&f);
    printf("handle: %ld cb: %ld alloc: %s\n", handle_cnt - last_handle_cnt, cb_cnt - last_cb_cnt, format_gets(&f));
    last_handle_cnt = handle_cnt;
    last_cb_cnt = cb_cnt;
    usleep(1000 * 1000);
  }
}

#define cfg(k,v) (getenv(k)?:v)
#define cfgi(k,v) atoi(cfg(k,v))
int main(int argc, char** argv)
{
  int io_thread_count = cfgi("io", "1");
  int client_thread_count = cfgi("client", "1");
  const char* svr_ip = cfg("server", "");
  if (argc < 2) {
    fprintf(stderr, "%s", _usage);
    return -1;
  }
  int listen_port = 8042;
  int need_listen = svr_ip[0] == 0;
  addr_init(&dest_addr, need_listen? "127.0.0.1": svr_ip, listen_port);
  //memset(big_req, 1, sizeof(big_req));
  pthread_t pd[client_thread_count];
  nio_.start(&req_handler, need_listen? listen_port: 0, io_thread_count);
  for(int64_t i = 0; i < client_thread_count; i++) {
    pthread_create(pd + i, NULL, thread_func, (int64_t*)i);
  }
  report();
  return 0;
}

extern "C" {
  #include "pkt-nio.c"
};
