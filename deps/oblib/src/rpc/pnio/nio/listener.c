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

int pkt_nio_dispatch_externel(int accept_fd) { return 0; }
#ifndef DISPATCH_EXTERNAL
#define DISPATCH_EXTERNAL(accept_fd) pkt_nio_dispatch_externel(accept_fd)
#endif

typedef struct dispatch_sk_t
{
  SOCK_COMMON;
} dispatch_sk_t;

#define DISPATCH_MAGIC 0xdeadbeef
typedef struct dispatch_head_t
{
  uint32_t magic;
  uint32_t len;
} dispatch_head_t;

int send_dispatch_handshake(int fd, const char* b, int sz)
{
  int err = 0;
  char buf[256];
  dispatch_head_t* h = (typeof(h))buf;
  if (sz + sizeof(*h) > sizeof(buf)) {
    err = -EINVAL;
  } else {
    h->magic = DISPATCH_MAGIC;
    h->len = sz;
    memcpy(h + 1, b, sz);
    ssize_t wbytes = 0;
    rk_info("send handshake: sz=%d", sz);
    while((wbytes = write(fd, buf, sz + sizeof(*h))) < 0
          && EINTR == errno);
    if ((uint64_t)wbytes != sz + sizeof(*h)) {
      err = -EIO;
    }
  }
  return err;
}

static int call_dispatch_func(dispatch_sk_t* s, const char* b, int sz)
{
  int err = 0;
  listen_t* l = structof(s->fty, listen_t, sf);
  if (sz > 0) {
    char buf[sz + sizeof(dispatch_head_t)];
    if ((ssize_t)sizeof(buf) != uintr_read(s->fd, buf, sizeof(buf))) {
      err = EIO;
    }
  }
  if (0 != err) {
  } else if (0 != (err = eloop_unregist(&l->ep, (sock_t*)s))) {
  } else if (0 == err && 0 == (err = l->dispatch_func(s->fd, b, sz))) {
    s->fd = -1;
    err = ENODATA;
  }
  return err;
}

static int dispatch_handle_event(dispatch_sk_t* s)
{
  int err = 0;
  char buf[256];
  ssize_t rbytes = recv(s->fd, buf, sizeof(buf), MSG_PEEK);
  dispatch_head_t* h = (typeof(h))buf;
  if (rbytes == 0) {
    err = ENODATA;
  } else if (rbytes < 0) {
    if (EINTR == errno) {
      // do nothing
    } else if (EAGAIN == errno || EWOULDBLOCK == errno) {
      s->mask &= ~EPOLLIN;
      err = EAGAIN;
    } else {
      err = EIO;
    }
  } else if ((uint64_t)rbytes < sizeof(dispatch_head_t)) {
    err = EAGAIN;
  } else if (h->magic != DISPATCH_MAGIC) {
    listen_t* l = structof(s->fty, listen_t, sf);
    int fd = 0;
    int epoll_fd = 0;
    fd = s->fd;
    if (l != NULL) {
      epoll_fd = l->ep.fd;
    }
    if (0 != eloop_unregist(&l->ep, (sock_t*)s)) {
      rk_info("eloop_unregist failed, err = %d, fd = %d", err, s->fd);
    } else if (DISPATCH_EXTERNAL(fd) != 0) {
      // ObListener dispatch failed
      rk_info("dispatch_external failed, ObListener might not be inited, connection should be destroyed. fd = %d", s->fd);
      err = -EIO;
    } else {
      rk_info("dispatch_external sucecess, fd = %d", s->fd);
      s->fd = -1;
      err = ENODATA;
    }
  } else if (h->len + sizeof(*h) > sizeof(buf)) {
    err = -EINVAL;
  } else if (h->len + sizeof(*h) > (uint64_t)rbytes) {
    // need read more
    err = EAGAIN;
  } else {
    err = call_dispatch_func(s, (const char*)(h+1), h->len);
  }
  return err;
}

static dispatch_sk_t* dispatch_sk_new(sf_t* sf)
{
  dispatch_sk_t* s = (typeof(s))salloc(sizeof(*s));
  if (s) {
    s->handle_event = (handle_event_t)dispatch_handle_event;
  }
  rk_info("dispatch_sk_new: %p", s);
  return s;
}

static void dispatch_sk_delete(sf_t* sf, dispatch_sk_t* s)
{
  rk_info("dispatch_sk_delete: %p", s);
  sfree(s);
}

static int dispatch_sf_init(sf_t* sf)
{
  sf_init((sf_t*)sf, (void*)dispatch_sk_new, (void*)dispatch_sk_delete);
  return 0;
}

int listen_init(listen_t* l, addr_t addr, dispatch_fd_func_t dispatch_func)
{
  int err = 0;
  l->dispatch_func = dispatch_func;
  ef(err = dispatch_sf_init(&l->sf));
  ef(err = eloop_init(&l->ep));
  ef(err = listenfd_init(&l->ep, &l->listenfd, (sf_t*)&l->sf, listen_create(addr)));
  el();
  return err;
}
