extern inline void skset(sock_t* s, uint32_t m);
extern inline void skclear(sock_t* s, uint32_t m);
extern inline bool sktest(sock_t* s, uint32_t m);

void sf_init(sf_t* sf, void* create, void* destroy) {
  sf->create = (typeof(sf->create))create;
  sf->destroy = (typeof(sf->destroy))destroy;
}

void sk_init(sock_t* s, sf_t* sf, void* handle_event, int fd) {
  s->fty = sf;
  s->handle_event = (typeof(s->handle_event))handle_event;
  s->fd = fd;
}
