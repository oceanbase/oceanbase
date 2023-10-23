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
