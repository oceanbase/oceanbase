extern inline void ussl_skset(ussl_sock_t *s, uint32_t m);
extern inline void ussl_skclear(ussl_sock_t *s, uint32_t m);
extern inline int ussl_sktest(ussl_sock_t *s, uint32_t m);

void ussl_sf_init(ussl_sf_t *sf, void *create, void *destroy)
{
  sf->create = (typeof(sf->create))create;
  sf->destroy = (typeof(sf->destroy))destroy;
}

void ussl_sk_init(ussl_sock_t *s, ussl_sf_t *sf, void *handle_event, int fd)
{
  s->fty = sf;
  s->handle_event = (typeof(s->handle_event))handle_event;
  s->fd = fd;
}
