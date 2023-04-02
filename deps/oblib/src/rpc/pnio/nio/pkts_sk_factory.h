static void* pkts_sk_alloc(int64_t sz) { return salloc(sz); }
static void pkts_sk_free(void* p) { sfree(p); }

static int pkts_sk_init(pkts_sf_t* sf, pkts_sk_t* s) {
  pkts_t* pkts = structof(sf, pkts_t, sf);
  wq_init(&s->wq);
  ib_init(&s->ib, MOD_PKTS_INBUF);
  s->id = idm_set(&pkts->sk_map, s);
  rk_info("set pkts_sk_t sock_id s=%p, s->id=%ld", s, s->id);
  return 0;
}

static void pkts_sk_destroy(pkts_sf_t* sf, pkts_sk_t* s) {
  pkts_t* pkts = structof(sf, pkts_t, sf);
  idm_del(&pkts->sk_map, s->id);
}

static int pkts_sk_handle_event(pkts_sk_t* s) {
  return pkts_sk_handle_event_ready(s);
}

static pkts_sk_t* pkts_sk_new(pkts_sf_t* sf) {
  pkts_sk_t* s = (pkts_sk_t*)pkts_sk_alloc(sizeof(*s));
  if (s) {
    s->fty = (sf_t*)sf;
    s->handle_event = (handle_event_t)pkts_sk_handle_event;
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
  pkts_sk_free(s);
}

static int pkts_sf_init(pkts_sf_t* sf, void* cfg) {
  sf_init((sf_t*)sf, (void*)pkts_sk_new, (void*)pkts_sk_delete);
  return 0;
}
