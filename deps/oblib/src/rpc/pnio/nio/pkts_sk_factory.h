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

static void* pkts_sk_alloc(int64_t sz) { return salloc(sz); }
static void pkts_sk_free(void* p) { sfree(p); }

static int pkts_sk_init(pkts_sf_t* sf, pkts_sk_t* s) {
  pkts_t* pkts = structof(sf, pkts_t, sf);
  wq_init(&s->wq);
  ib_init(&s->ib, MOD_PKTS_INBUF);
  s->rl_ready_link.next = NULL;
  s->id = idm_set(&pkts->sk_map, s);
  dlink_insert(&pkts->sk_list, &s->list_link);
  rk_info("set pkts_sk_t sock_id s=%p, s->id=%ld", s, s->id);
  return 0;
}

static void pkts_sk_destroy(pkts_sf_t* sf, pkts_sk_t* s) {
  pkts_t* pkts = structof(sf, pkts_t, sf);
  idm_del(&pkts->sk_map, s->id);
  dlink_delete(&s->list_link);
}

int pkts_sk_handle_event(pkts_sk_t* s) {
  return pkts_sk_handle_event_ready(s);
}

static int pkts_sk_rl_handle_event(pkts_sk_t* s, int64_t* read_bytes) {
  return pkts_sk_consume(s, get_epoll_handle_time_limit(), read_bytes);
}

static pkts_sk_t* pkts_sk_new(pkts_sf_t* sf) {
  pkts_sk_t* s = (pkts_sk_t*)pkts_sk_alloc(sizeof(*s));
  if (s) {
    memset(s, 0, sizeof(*s));
    s->fty = (sf_t*)sf;
    s->ep_fd = -1;
    s->handle_event = (handle_event_t)pkts_sk_handle_event;
    s->sk_diag_info.establish_time = rk_get_us();
    pkts_sk_init(sf, s);
  }
  rk_info("sk_new: s=%p", s);
  return s;
}

static void pkts_sk_delete(pkts_sf_t* sf, pkts_sk_t* s) {
  pkts_t* io = structof(sf, pkts_t, sf);
  rk_info("sk_destroy: s=%p io=%p", s, io);
  pkts_sk_destroy(sf, s);
  pkts_write_queue_on_sk_destroy(io, s);
  ib_destroy(&s->ib);
  dlink_delete(&s->rl_ready_link);
  pkts_sk_free(s);
}

static int pkts_sf_init(pkts_sf_t* sf, void* cfg) {
  sf_init((sf_t*)sf, (void*)pkts_sk_new, (void*)pkts_sk_delete);
  return 0;
}
