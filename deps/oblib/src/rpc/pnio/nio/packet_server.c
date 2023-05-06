typedef struct pkts_msg_t {
  int64_t sz;
  char* payload;
} pkts_msg_t;

static int64_t pkts_decode(char* b, int64_t s) { return eh_decode(b, s);}

void pkts_flush_cb(pkts_t* io, pkts_req_t* req) {
  PNIO_DELAY_WARN(delay_warn("pkts_flush_cb", req->ctime_us, FLUSH_DELAY_WARN_US));
  req->flush_cb(req);
}

static int pkts_sk_read(void** b, pkts_sk_t* s, int64_t sz, int64_t* avail_bytes) {
  return sk_read_with_ib(b, (sock_t*)s, &s->ib, sz, avail_bytes);
}

static int pkts_sk_handle_msg(pkts_sk_t* s, pkts_msg_t* msg) {
  pkts_t* pkts = structof(s->fty, pkts_t, sf);
#ifdef PERF_MODE
  int ret = pkts->on_req(pkts, ib_ref(&s->ib), msg->payload, msg->sz, s->id);
#else
  int ret = pkts->on_req(pkts, s->ib.b, msg->payload, msg->sz, s->id);
#endif
  ib_consumed(&s->ib, msg->sz);
  return ret;
}

#define tns(x) pkts ## x
#include "nio-tpl-ns.h"
#include "write_queue.t.h"
#include "decode.t.h"
#include "handle_io.t.h"
#include "nio-tpl-ns.h"

#include "pkts_sk_factory.h"
#include "pkts_post.h"

int pkts_init(pkts_t* io, eloop_t* ep, pkts_cfg_t* cfg) {
  int err = 0;
  int lfd = -1;
  io->ep = ep;
  ef(err = pkts_sf_init(&io->sf, cfg));
  sc_queue_init(&io->req_queue);
  ef(err = evfd_init(io->ep, &io->evfd, (handle_event_t)pkts_evfd_cb));
  lfd = cfg->accept_qfd >= 0 ?cfg->accept_qfd: listen_create(cfg->addr);
  ef(err = listenfd_init(io->ep, &io->listenfd, (sf_t*)&io->sf, lfd));
  rk_info("pkts listen at %s", T2S(addr, cfg->addr));
  idm_init(&io->sk_map, arrlen(io->sk_table));
  io->on_req = cfg->handle_func;
  el();
  return err;
}
